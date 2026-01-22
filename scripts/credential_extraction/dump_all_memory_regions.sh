#!/bin/bash

# Script to dump ALL rw-p memory regions (not just the largest)
# This is an improvement over dump_native_memory.sh which only dumps selected regions

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./all_memory_regions_dump_$(date +%Y%m%d_%H%M%S)"
MAX_DUMP_SIZE_MB="${2:-50}"  # Default 50MB per region, can be overridden
MAX_TOTAL_SIZE_MB="${3:-500}"  # Default 500MB total, can be overridden

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}Dump All Memory Regions Script${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output: $OUTPUT_DIR"
echo "Max per region: ${MAX_DUMP_SIZE_MB}MB"
echo "Max total: ${MAX_TOTAL_SIZE_MB}MB"
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
USE_SU=""
if adb shell id 2>/dev/null | grep -q "uid=0"; then
    echo -e "${GREEN}[+] ADB is running as root${NC}"
elif adb shell "su -c id 2>/dev/null" | grep -q "uid=0"; then
    echo -e "${GREEN}[+] Root access via su confirmed${NC}"
    USE_SU="su -c"
else
    echo -e "${YELLOW}[!] Device may not be rooted. Memory dump may fail.${NC}"
fi

# Get memory maps
echo -e "${YELLOW}[*] Getting memory maps...${NC}"
if [ -n "$USE_SU" ]; then
    adb shell "$USE_SU 'cat /proc/$PID/maps'" > "${OUTPUT_DIR}/maps.txt" 2>/dev/null
else
    adb shell "cat /proc/$PID/maps" > "${OUTPUT_DIR}/maps.txt" 2>/dev/null
fi

if [ ! -s "${OUTPUT_DIR}/maps.txt" ]; then
    echo -e "${RED}[-] Could not get memory maps${NC}"
    exit 1
fi

echo -e "${GREEN}[+] Memory maps saved${NC}"

# Find ALL rw-p regions
echo -e "${YELLOW}[*] Finding all rw-p regions...${NC}"
RW_REGIONS=$(grep "rw-p" "${OUTPUT_DIR}/maps.txt")

REGION_COUNT=$(echo "$RW_REGIONS" | grep -c "rw-p" || echo "0")
echo -e "${GREEN}[+] Found $REGION_COUNT rw-p regions${NC}"

# Save all regions to a file for reference
echo "$RW_REGIONS" > "${OUTPUT_DIR}/all_rw_regions.txt"

# Process each region
DUMP_COUNT=0
TOTAL_DUMPED=0
MAX_DUMP_SIZE=$((MAX_DUMP_SIZE_MB * 1024 * 1024))
MAX_TOTAL_SIZE=$((MAX_TOTAL_SIZE_MB * 1024 * 1024))

echo ""
echo -e "${BLUE}[*] Starting to dump regions...${NC}"
echo ""

# Function to convert hex to decimal
hex_to_dec() {
    local hex=$1
    local dec=$(printf '%d' "0x$hex" 2>/dev/null)
    if [ -z "$dec" ] || [ "$dec" = "0" ]; then
        dec=$(python3 -c "print(int('$hex', 16))" 2>/dev/null || echo "0")
    fi
    echo "$dec"
}

# Process each rw-p region
echo "$RW_REGIONS" | while IFS= read -r line; do
    if [ -z "$line" ]; then continue; fi
    
    RANGE=$(echo "$line" | awk '{print $1}')
    if [ -z "$RANGE" ]; then continue; fi
    
    START=$(echo "$RANGE" | cut -d'-' -f1)
    END=$(echo "$RANGE" | cut -d'-' -f2)
    
    START_DEC=$(hex_to_dec "$START")
    END_DEC=$(hex_to_dec "$END")
    
    if [ "$START_DEC" = "0" ] || [ "$END_DEC" = "0" ]; then
        continue
    fi
    
    SIZE=$((END_DEC - START_DEC))
    
    # Skip if too large
    if [ $SIZE -gt $MAX_DUMP_SIZE ]; then
        echo -e "${YELLOW}[!] Skipping region $START-$END (${SIZE} bytes, > ${MAX_DUMP_SIZE_MB}MB)${NC}"
        continue
    fi
    
    # Check total size limit
    if [ $((TOTAL_DUMPED + SIZE)) -gt $MAX_TOTAL_SIZE ]; then
        echo -e "${YELLOW}[!] Total size limit reached (${MAX_TOTAL_SIZE_MB}MB). Stopping.${NC}"
        break
    fi
    
    DUMP_COUNT=$((DUMP_COUNT + 1))
    DUMP_FILE="${OUTPUT_DIR}/region_${DUMP_COUNT}_${START}.dump"
    
    echo -e "${BLUE}[*] Dumping region $DUMP_COUNT/$REGION_COUNT: $START-$END (${SIZE} bytes)...${NC}"
    
    # Dump the region
    if [ -n "$USE_SU" ]; then
        adb shell "$USE_SU 'dd if=/proc/$PID/mem of=/sdcard/region_${DUMP_COUNT}.dump bs=1 count=$SIZE skip=$START_DEC 2>/dev/null'" || {
            echo -e "${RED}[-] Failed to dump region $START-$END${NC}"
            continue
        }
        adb pull "/sdcard/region_${DUMP_COUNT}.dump" "$DUMP_FILE" 2>/dev/null || {
            echo -e "${RED}[-] Failed to pull dump${NC}"
            continue
        }
        adb shell "$USE_SU 'rm /sdcard/region_${DUMP_COUNT}.dump'" 2>/dev/null || true
    else
        adb shell "dd if=/proc/$PID/mem of=/sdcard/region_${DUMP_COUNT}.dump bs=1 count=$SIZE skip=$START_DEC 2>/dev/null" || {
            echo -e "${RED}[-] Failed to dump region $START-$END${NC}"
            continue
        }
        adb pull "/sdcard/region_${DUMP_COUNT}.dump" "$DUMP_FILE" 2>/dev/null || {
            echo -e "${RED}[-] Failed to pull dump${NC}"
            continue
        }
        adb shell "rm /sdcard/region_${DUMP_COUNT}.dump" 2>/dev/null || true
    fi
    
    if [ -f "$DUMP_FILE" ] && [ -s "$DUMP_FILE" ]; then
        ACTUAL_SIZE=$(stat -f%z "$DUMP_FILE" 2>/dev/null || stat -c%s "$DUMP_FILE" 2>/dev/null || echo "0")
        TOTAL_DUMPED=$((TOTAL_DUMPED + ACTUAL_SIZE))
        echo -e "${GREEN}[+] Dumped ${ACTUAL_SIZE} bytes to $(basename $DUMP_FILE)${NC}"
    else
        echo -e "${RED}[-] Dump file is empty or missing${NC}"
    fi
done

echo ""
echo -e "${YELLOW}[*] Searching for credentials in all dumps...${NC}"

# Search all dump files for credentials
CREDENTIALS_FOUND=false

for dump_file in "${OUTPUT_DIR}"/region_*.dump; do
    if [ ! -f "$dump_file" ]; then continue; fi
    
    if command -v strings &> /dev/null; then
        # Search for access key pattern
        ACCESS_KEY=$(strings "$dump_file" 2>/dev/null | grep -E "^AKIA[0-9A-Z]{16}$" | head -1)
        if [ -n "$ACCESS_KEY" ]; then
            echo -e "${GREEN}[!] FOUND ACCESS KEY in $(basename $dump_file): $ACCESS_KEY${NC}"
            echo "$ACCESS_KEY" >> "${OUTPUT_DIR}/found_access_keys.txt"
            CREDENTIALS_FOUND=true
        fi
        
        # Search for secret key (look for the known pattern)
        SECRET_KEY=$(strings "$dump_file" 2>/dev/null | grep -E "ckCcy10mEtAjPOo" | head -1)
        if [ -n "$SECRET_KEY" ]; then
            echo -e "${GREEN}[!] FOUND SECRET KEY in $(basename $dump_file): $SECRET_KEY${NC}"
            echo "$SECRET_KEY" >> "${OUTPUT_DIR}/found_secret_keys.txt"
            CREDENTIALS_FOUND=true
        fi
    fi
done

if [ "$CREDENTIALS_FOUND" = true ]; then
    echo ""
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}[!] CREDENTIALS FOUND IN MEMORY DUMPS!${NC}"
    echo -e "${GREEN}============================================================${NC}"
    if [ -f "${OUTPUT_DIR}/found_access_keys.txt" ]; then
        echo -e "${GREEN}Access Keys:${NC}"
        cat "${OUTPUT_DIR}/found_access_keys.txt"
    fi
    if [ -f "${OUTPUT_DIR}/found_secret_keys.txt" ]; then
        echo -e "${GREEN}Secret Keys:${NC}"
        cat "${OUTPUT_DIR}/found_secret_keys.txt"
    fi
else
    echo -e "${YELLOW}[!] No credentials found in dumps (may need to search manually)${NC}"
fi

echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Output directory: $OUTPUT_DIR"
echo "Total regions found: $REGION_COUNT"
echo "Regions dumped: $(ls -1 "${OUTPUT_DIR}"/region_*.dump 2>/dev/null | wc -l)"
echo "Total size dumped: $(du -sh "$OUTPUT_DIR" | awk '{print $1}')"
echo ""
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2
