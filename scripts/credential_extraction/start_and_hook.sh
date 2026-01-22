#!/bin/bash

# Script to start the app and hook it with Frida

PACKAGE_NAME="com.rustexample"
FRIDA_SCRIPT="scripts/credential_extraction/frida_extract_credentials.js"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Starting App and Hooking with Frida ===${NC}"

# Check if app is installed
if ! adb shell pm list packages | grep -q "$PACKAGE_NAME"; then
    echo -e "${RED}Error: App $PACKAGE_NAME is not installed${NC}"
    exit 1
fi

# Check if app is already running
PID=$(adb shell pidof "$PACKAGE_NAME" 2>/dev/null | tr -d '\r')

if [ -n "$PID" ]; then
    echo -e "${YELLOW}[!] App is already running (PID: $PID)${NC}"
    echo -e "${GREEN}[+] Attaching Frida to running process by PID...${NC}"
    # Use PID directly to attach (not spawn)
    frida -U -p "$PID" -l "$FRIDA_SCRIPT"
else
    echo -e "${YELLOW}[*] Starting app...${NC}"
    adb shell monkey -p "$PACKAGE_NAME" -c android.intent.category.LAUNCHER 1
    sleep 3
    
    # Check if it started
    PID=$(adb shell pidof "$PACKAGE_NAME" 2>/dev/null | tr -d '\r')
    if [ -z "$PID" ]; then
        echo -e "${RED}Error: App failed to start${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}[+] App started (PID: $PID)${NC}"
    echo -e "${GREEN}[+] Attaching Frida by PID...${NC}"
    sleep 1
    # Use PID directly to attach (not spawn)
    frida -U -p "$PID" -l "$FRIDA_SCRIPT"
fi
