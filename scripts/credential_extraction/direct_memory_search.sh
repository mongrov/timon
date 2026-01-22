#!/bin/bash

# Script to search /proc/$PID/mem directly without dumping
# This is a variation of Method 2/3 that searches memory in-place

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./direct_memory_search_$(date +%Y%m%d_%H%M%S)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Direct Memory Search Script${NC}"
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
USE_SU=""
if adb shell id 2>/dev/null | grep -q "uid=0"; then
    echo -e "${GREEN}[+] ADB is running as root${NC}"
elif adb shell "su -c id 2>/dev/null" | grep -q "uid=0"; then
    echo -e "${GREEN}[+] Root access via su confirmed${NC}"
    USE_SU="su -c"
else
    echo -e "${RED}Error: Root access required for /proc/$PID/mem${NC}"
    exit 1
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

# Function to convert hex to decimal
hex_to_dec() {
    local hex=$1
    local dec=$(printf '%d' "0x$hex" 2>/dev/null)
    if [ -z "$dec" ] || [ "$dec" = "0" ]; then
        dec=$(python3 -c "print(int('$hex', 16))" 2>/dev/null || echo "0")
    fi
    echo "$dec"
}

# Search for credentials in memory regions
echo -e "${YELLOW}[*] Searching for credentials in memory...${NC}"

# Search patterns
ACCESS_KEY_PATTERN="AKIA[0-9A-Z]{16}"
SECRET_KEY_PATTERN="ckCcy10mEtAjPOo"

FOUND_CREDENTIALS=false

# Get rw-p regions (most likely to contain credentials)
RW_REGIONS=$(grep "rw-p" "${OUTPUT_DIR}/maps.txt")

REGION_COUNT=0
SEARCHED_COUNT=0

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
    
    # Skip very large regions (would be too slow)
    if [ $SIZE -gt 104857600 ]; then  # 100MB
        echo -e "${YELLOW}[!] Skipping large region $START-$END (${SIZE} bytes)${NC}"
        continue
    fi
    
    REGION_COUNT=$((REGION_COUNT + 1))
    echo -e "${YELLOW}[*] Searching region $REGION_COUNT: $START-$END (${SIZE} bytes)...${NC}"
    
    # Try to read and search the region
    # Note: This may not work on all devices due to /proc/$PID/mem restrictions
    TEMP_FILE="/sdcard/mem_search_${REGION_COUNT}.tmp"
    
    if [ -n "$USE_SU" ]; then
        # Try to read a sample and search
        SAMPLE_SIZE=$((SIZE > 1048576 ? 1048576 : SIZE))  # Max 1MB sample
        
        adb shell "$USE_SU 'dd if=/proc/$PID/mem of=$TEMP_FILE bs=1 count=$SAMPLE_SIZE skip=$START_DEC 2>/dev/null'" || {
            echo -e "${RED}[-] Failed to read region $START-$END${NC}"
            continue
        }
    else
        adb shell "dd if=/proc/$PID/mem of=$TEMP_FILE bs=1 count=$SAMPLE_SIZE skip=$START_DEC 2>/dev/null" || {
            echo -e "${RED}[-] Failed to read region $START-$END${NC}"
            continue
        }
    fi
    
    # Pull and search
    adb pull "$TEMP_FILE" "${OUTPUT_DIR}/region_${REGION_COUNT}_sample.bin" 2>/dev/null || continue
    adb shell "rm $TEMP_FILE" 2>/dev/null || true
    
    if [ -f "${OUTPUT_DIR}/region_${REGION_COUNT}_sample.bin" ]; then
        # Search for access key
        if command -v strings &> /dev/null; then
            ACCESS_KEY=$(strings "${OUTPUT_DIR}/region_${REGION_COUNT}_sample.bin" 2>/dev/null | grep -E "^$ACCESS_KEY_PATTERN$" | head -1)
            if [ -n "$ACCESS_KEY" ]; then
                echo -e "${GREEN}[!] FOUND ACCESS KEY in region $START-$END: $ACCESS_KEY${NC}"
                echo "$ACCESS_KEY" >> "${OUTPUT_DIR}/found_access_keys.txt"
                FOUND_CREDENTIALS=true
            fi
            
            # Search for secret key
            SECRET_KEY=$(strings "${OUTPUT_DIR}/region_${REGION_COUNT}_sample.bin" 2>/dev/null | grep "$SECRET_KEY_PATTERN" | head -1)
            if [ -n "$SECRET_KEY" ]; then
                echo -e "${GREEN}[!] FOUND SECRET KEY in region $START-$END: $SECRET_KEY${NC}"
                echo "$SECRET_KEY" >> "${OUTPUT_DIR}/found_secret_keys.txt"
                FOUND_CREDENTIALS=true
            fi
        fi
    fi
    
    SEARCHED_COUNT=$((SEARCHED_COUNT + 1))
done

echo ""
if [ "$FOUND_CREDENTIALS" = true ]; then
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}[!] CREDENTIALS FOUND!${NC}"
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
    echo -e "${YELLOW}[!] No credentials found in searched regions${NC}"
    echo -e "${YELLOW}[!] Note: Direct memory search may be limited by kernel restrictions${NC}"
fi

echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Output directory: $OUTPUT_DIR"
echo "Regions searched: $SEARCHED_COUNT"
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2
