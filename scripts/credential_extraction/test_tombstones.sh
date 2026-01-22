#!/bin/bash

# Script to check tombstone files (crash dumps)

echo "Checking tombstone files..."

# Check if adb is running as root
if adb shell id | grep -q "uid=0"; then
    echo "[+] ADB is running as root - no need for su"
    USE_SU=""
else
    echo "[!] ADB is not root - will try su commands"
    USE_SU="su -c"
fi

# Method 1: Direct command (if adb is root)
echo ""
echo "Method 1: Direct command (adb root)"
adb shell "ls -lt /data/tombstones/ 2>/dev/null | head -5" || echo "Method 1 failed or directory doesn't exist"

# Method 2: Check if directory exists
echo ""
echo "Method 2: Check if directory exists"
if adb shell "test -d /data/tombstones/ 2>/dev/null && echo 'exists' || echo 'not found'"; then
    echo "Directory check completed"
else
    echo "Cannot check directory"
fi

# Method 3: List files
echo ""
echo "Method 3: List tombstone files"
adb shell "ls /data/tombstones/ 2>/dev/null" || echo "Cannot access /data/tombstones/ or directory doesn't exist"

# Method 4: Check permissions
echo ""
echo "Method 4: Check permissions"
adb shell "ls -ld /data/tombstones/ 2>/dev/null" || echo "Cannot check permissions"

# Method 5: Alternative location (some devices use different paths)
echo ""
echo "Method 5: Check alternative locations"
echo "Checking /data/anr/ (ANR traces):"
adb shell "ls /data/anr/ 2>/dev/null | head -3" || echo "No ANR traces found"

echo ""
echo "Note: Tombstones may not exist if app hasn't crashed recently."
echo "To test, you could trigger a crash: adb shell kill -11 <PID>"
