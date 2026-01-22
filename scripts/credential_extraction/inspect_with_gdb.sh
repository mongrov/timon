#!/bin/bash

# Script to inspect process memory using GDB

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./gdb_inspection_$(date +%Y%m%d_%H%M%S)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}GDB Memory Inspection Script${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output: $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Check if device is connected
if ! adb devices | grep -q "device$"; then
    echo -e "${RED}Error: No device connected${NC}"
    exit 1
fi

# Check if GDB is available on device
echo -e "${YELLOW}[*] Checking for GDB on device...${NC}"
if ! adb shell "which gdb" > /dev/null 2>&1; then
    echo -e "${RED}Error: GDB not found on device${NC}"
    echo -e "${YELLOW}[!] GDB may not be available on production devices${NC}"
    echo -e "${YELLOW}[!] This method typically requires:${NC}"
    echo "   - Debug build of Android"
    echo "   - GDB server installed"
    echo "   - Or use gdbserver from Android NDK"
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
    echo -e "${YELLOW}[!] Device may not be rooted. GDB may need root access.${NC}"
    USE_SU=""
fi

# Alternative: Use gdbserver if available
echo -e "${YELLOW}[*] Attempting to use GDB for memory inspection...${NC}"

# Method 1: Try to attach GDB directly
echo -e "${YELLOW}[*] Method 1: Direct GDB attachment...${NC}"
GDB_SCRIPT="${OUTPUT_DIR}/gdb_commands.txt"
cat > "$GDB_SCRIPT" << 'EOF'
# GDB commands to inspect memory
info proc mappings
# Try to dump heap
generate-core-file /data/local/tmp/core.dump
quit
EOF

if [ -n "$USE_SU" ]; then
    adb shell "$USE_SU 'gdb -p $PID < /dev/null'" > "${OUTPUT_DIR}/gdb_output.txt" 2>&1 || {
        echo -e "${YELLOW}[!] Direct GDB attachment failed${NC}"
    }
else
    adb shell "gdb -p $PID < /dev/null" > "${OUTPUT_DIR}/gdb_output.txt" 2>&1 || {
        echo -e "${YELLOW}[!] Direct GDB attachment failed (may need root)${NC}"
    }
fi

# Method 2: Use gdbserver (if available)
echo -e "${YELLOW}[*] Method 2: Using gdbserver (if available)...${NC}"
if adb shell "which gdbserver" > /dev/null 2>&1; then
    echo -e "${GREEN}[+] gdbserver found${NC}"
    echo -e "${YELLOW}[!] gdbserver requires port forwarding and remote GDB connection${NC}"
    echo -e "${YELLOW}[!] This is more complex - skipping for now${NC}"
else
    echo -e "${YELLOW}[!] gdbserver not found${NC}"
fi

# Method 3: Use /proc/<pid>/mem directly (simpler alternative)
echo -e "${YELLOW}[*] Method 3: Direct memory inspection via /proc...${NC}"
echo -e "${YELLOW}[*] Getting memory maps...${NC}"

if [ -n "$USE_SU" ]; then
    adb shell "$USE_SU 'cat /proc/$PID/maps'" > "${OUTPUT_DIR}/maps.txt" 2>/dev/null
else
    adb shell "cat /proc/$PID/maps" > "${OUTPUT_DIR}/maps.txt" 2>/dev/null
fi

if [ -s "${OUTPUT_DIR}/maps.txt" ]; then
    echo -e "${GREEN}[+] Memory maps saved${NC}"
    
    # Find heap regions
    HEAP_REGIONS=$(grep -E "\[heap\]|rw-p" "${OUTPUT_DIR}/maps.txt" | head -5)
    if [ -n "$HEAP_REGIONS" ]; then
        echo -e "${GREEN}[+] Found heap regions:${NC}"
        echo "$HEAP_REGIONS" | head -3
        echo ""
        
        # Try to read memory directly (if possible)
        echo -e "${YELLOW}[*] Attempting to read memory regions...${NC}"
        FIRST_HEAP=$(echo "$HEAP_REGIONS" | head -1 | awk '{print $1}')
        if [ -n "$FIRST_HEAP" ]; then
            START=$(echo "$FIRST_HEAP" | cut -d'-' -f1)
            START_DEC=$(printf '%d' "0x$START" 2>/dev/null || echo "0")
            
            if [ "$START_DEC" != "0" ]; then
                # Try to read a small chunk
                READ_SIZE=10240  # 10KB
                if [ -n "$USE_SU" ]; then
                    adb shell "$USE_SU 'dd if=/proc/$PID/mem of=/data/local/tmp/mem_sample.bin bs=1 count=$READ_SIZE skip=$START_DEC 2>/dev/null'" || true
                    adb pull /data/local/tmp/mem_sample.bin "${OUTPUT_DIR}/mem_sample.bin" 2>/dev/null || true
                    adb shell "$USE_SU 'rm /data/local/tmp/mem_sample.bin'" 2>/dev/null || true
                else
                    adb shell "dd if=/proc/$PID/mem of=/data/local/tmp/mem_sample.bin bs=1 count=$READ_SIZE skip=$START_DEC 2>/dev/null" || true
                    adb pull /data/local/tmp/mem_sample.bin "${OUTPUT_DIR}/mem_sample.bin" 2>/dev/null || true
                    adb shell "rm /data/local/tmp/mem_sample.bin" 2>/dev/null || true
                fi
                
                if [ -f "${OUTPUT_DIR}/mem_sample.bin" ] && [ -s "${OUTPUT_DIR}/mem_sample.bin" ]; then
                    echo -e "${GREEN}[+] Memory sample captured${NC}"
                    if command -v strings &> /dev/null; then
                        strings "${OUTPUT_DIR}/mem_sample.bin" | grep -E "AKIA[0-9A-Z]{16}" > "${OUTPUT_DIR}/gdb_access_keys.txt" 2>/dev/null
                        if [ -s "${OUTPUT_DIR}/gdb_access_keys.txt" ]; then
                            echo -e "${GREEN}[!] FOUND ACCESS KEYS:${NC}"
                            cat "${OUTPUT_DIR}/gdb_access_keys.txt"
                        fi
                    fi
                fi
            fi
        fi
    fi
else
    echo -e "${RED}[-] Could not get memory maps${NC}"
fi

echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Output directory: $OUTPUT_DIR"
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2

echo ""
echo -e "${YELLOW}[!] Note: GDB inspection is complex and may not work on all devices.${NC}"
echo -e "${YELLOW}[!] For production devices, memory dump methods (Method 2/3) are more reliable.${NC}"
