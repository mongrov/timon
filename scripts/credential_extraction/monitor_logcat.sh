#!/bin/bash

# Script to monitor logcat for credential-related information
# This script will automatically exit after the specified duration

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./logcat_monitor_$(date +%Y%m%d_%H%M%S)"
DURATION="${2:-60}"  # Monitor for 60 seconds by default

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Logcat Monitoring Script${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output: $OUTPUT_DIR"
echo "Duration: ${DURATION} seconds"
echo ""

mkdir -p "$OUTPUT_DIR"

# Check if device is connected
if ! adb devices | grep -q "device$"; then
    echo -e "${RED}Error: No device connected${NC}"
    exit 1
fi

# Check if app is running
PID=$(adb shell pidof "$PACKAGE_NAME" 2>/dev/null | tr -d '\r')
if [ -z "$PID" ]; then
    echo -e "${YELLOW}[!] Process not running. Starting app...${NC}"
    adb shell monkey -p "$PACKAGE_NAME" -c android.intent.category.LAUNCHER 1
    sleep 3
    PID=$(adb shell pidof "$PACKAGE_NAME" 2>/dev/null | tr -d '\r')
fi

if [ -z "$PID" ]; then
    echo -e "${RED}Error: Could not find process${NC}"
    exit 1
fi

echo -e "${GREEN}[+] Process ID: $PID${NC}"
echo "$PID" > "${OUTPUT_DIR}/pid.txt"

# Clear logcat first
echo -e "${YELLOW}[*] Clearing logcat...${NC}"
adb logcat -c > /dev/null 2>&1

# Start monitoring
echo -e "${YELLOW}[*] Starting logcat monitoring for ${DURATION} seconds...${NC}"
echo -e "${YELLOW}[!] Please trigger initBucket() in the app now...${NC}"
echo ""

# Monitor logcat and save to file
LOG_FILE="${OUTPUT_DIR}/logcat_full.txt"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}[*] Stopping logcat...${NC}"
    if [ -n "$LOGCAT_PID" ]; then
        kill $LOGCAT_PID 2>/dev/null || true
        sleep 0.5
        kill -9 $LOGCAT_PID 2>/dev/null || true
    fi
    # Kill any remaining adb logcat processes
    pkill -f "adb.*logcat" 2>/dev/null || true
    sleep 0.5
}

trap cleanup EXIT INT TERM

# Start logcat in background
adb logcat > "$LOG_FILE" 2>&1 &
LOGCAT_PID=$!

# Wait for specified duration
echo -e "${YELLOW}[*] Monitoring for ${DURATION} seconds (will auto-stop)...${NC}"
sleep "$DURATION"

# Cleanup will be called by trap
cleanup

echo -e "${GREEN}[+] Logcat monitoring complete${NC}"

# Filter for credential-related content
echo -e "${YELLOW}[*] Analyzing logcat for credentials...${NC}"

# Search for AWS access keys
echo -e "${YELLOW}[*] Searching for AWS Access Key IDs...${NC}"
grep -E "AKIA[0-9A-Z]{16}" "$LOG_FILE" > "${OUTPUT_DIR}/logcat_access_keys.txt" 2>/dev/null
if [ -s "${OUTPUT_DIR}/logcat_access_keys.txt" ]; then
    echo -e "${GREEN}[!] FOUND ACCESS KEYS in logcat:${NC}"
    cat "${OUTPUT_DIR}/logcat_access_keys.txt"
else
    echo -e "${YELLOW}[!] No access keys found with pattern AKIA...${NC}"
fi

# Search for secret keys
echo -e "${YELLOW}[*] Searching for secret keys...${NC}"
grep -E "ckCcy10mEtAjPOo|secret|Secret" "$LOG_FILE" | head -20 > "${OUTPUT_DIR}/logcat_secrets.txt" 2>/dev/null

# Search for credential keywords
echo -e "${YELLOW}[*] Searching for credential keywords...${NC}"
grep -iE "access_key|secret|credential|aws|s3|bucket|initBucket|CloudStorageManager" "$LOG_FILE" > "${OUTPUT_DIR}/logcat_credentials.txt" 2>/dev/null

CRED_COUNT=$(wc -l < "${OUTPUT_DIR}/logcat_credentials.txt" 2>/dev/null || echo "0")
echo -e "${GREEN}[+] Found $CRED_COUNT lines with credential keywords${NC}"

# Search for errors that might leak information
echo -e "${YELLOW}[*] Searching for errors...${NC}"
grep -iE "error|exception|fail" "$LOG_FILE" | grep -iE "bucket|s3|aws|credential|access" > "${OUTPUT_DIR}/logcat_errors.txt" 2>/dev/null

# Search for app-specific logs
echo -e "${YELLOW}[*] Searching for app-specific logs...${NC}"
grep "$PACKAGE_NAME" "$LOG_FILE" | grep -iE "bucket|s3|aws|init|cloud" > "${OUTPUT_DIR}/logcat_app_specific.txt" 2>/dev/null

# Get summary statistics
TOTAL_LINES=$(wc -l < "$LOG_FILE" 2>/dev/null || echo "0")
echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Total logcat lines captured: $TOTAL_LINES"
echo "Output directory: $OUTPUT_DIR"
echo ""
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2

if [ -s "${OUTPUT_DIR}/logcat_access_keys.txt" ]; then
    echo ""
    echo -e "${GREEN}[!] CREDENTIALS FOUND in logcat!${NC}"
    echo -e "${RED}[!] This indicates the app is logging credentials - SECURITY ISSUE!${NC}"
else
    echo ""
    echo -e "${YELLOW}[!] No credentials found in logcat.${NC}"
    echo -e "${GREEN}[+] This is good - app is not logging credentials.${NC}"
    if [ -s "${OUTPUT_DIR}/logcat_credentials.txt" ]; then
        echo -e "${YELLOW}[!] However, found credential-related keywords. Check logcat_credentials.txt${NC}"
    fi
fi
