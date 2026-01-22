#!/bin/bash

# Automated Frida Setup Script
# This script downloads and sets up frida-server on your Android device

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Frida Setup Script ===${NC}"

# Check if adb is available
if ! command -v adb &> /dev/null; then
    echo -e "${RED}Error: adb not found. Please install Android SDK Platform Tools.${NC}"
    exit 1
fi

# Check if device is connected
if ! adb devices | grep -q "device$"; then
    echo -e "${RED}Error: No Android device connected.${NC}"
    echo "Please connect your device and enable USB debugging."
    exit 1
fi

echo -e "${GREEN}[+] ADB connection OK${NC}"

# Step 1: Determine device architecture
echo -e "${YELLOW}[*] Detecting device architecture...${NC}"
ARCH=$(adb shell getprop ro.product.cpu.abi | tr -d '\r')
echo -e "${GREEN}[+] Architecture: $ARCH${NC}"

# Map architecture to Frida download name
case "$ARCH" in
    arm64-v8a|aarch64)
        FRIDA_ARCH="android-arm64"
        ;;
    armeabi-v7a|arm)
        FRIDA_ARCH="android-arm"
        ;;
    x86_64)
        FRIDA_ARCH="android-x86_64"
        ;;
    x86)
        FRIDA_ARCH="android-x86"
        ;;
    *)
        echo -e "${RED}Error: Unsupported architecture: $ARCH${NC}"
        echo "Please download frida-server manually from:"
        echo "https://github.com/frida/frida/releases"
        exit 1
        ;;
esac

echo -e "${GREEN}[+] Frida architecture: $FRIDA_ARCH${NC}"

# Step 2: Get Frida version
echo -e "${YELLOW}[*] Checking Frida version...${NC}"
if command -v frida &> /dev/null; then
    FRIDA_VERSION=$(frida --version)
    echo -e "${GREEN}[+] Local Frida version: $FRIDA_VERSION${NC}"
else
    echo -e "${YELLOW}[!] Frida not installed locally. Using latest version.${NC}"
    echo -e "${YELLOW}[!] Install Frida with: pip install frida-tools${NC}"
    
    # Try to get latest version from GitHub API
    if command -v curl &> /dev/null; then
        FRIDA_VERSION=$(curl -s https://api.github.com/repos/frida/frida/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/' | sed 's/v//')
        echo -e "${GREEN}[+] Latest Frida version: $FRIDA_VERSION${NC}"
    else
        echo -e "${YELLOW}[!] Could not determine version. Using 16.1.0 as default.${NC}"
        FRIDA_VERSION="16.1.0"
    fi
fi

# Step 3: Download frida-server
FRIDA_SERVER_FILE="frida-server-${FRIDA_VERSION}-${FRIDA_ARCH}"
FRIDA_SERVER_XZ="${FRIDA_SERVER_FILE}.xz"
DOWNLOAD_URL="https://github.com/frida/frida/releases/download/${FRIDA_VERSION}/${FRIDA_SERVER_XZ}"

echo -e "${YELLOW}[*] Downloading frida-server...${NC}"
echo "URL: $DOWNLOAD_URL"

if [ -f "$FRIDA_SERVER_FILE" ]; then
    echo -e "${GREEN}[+] frida-server already exists: $FRIDA_SERVER_FILE${NC}"
elif [ -f "$FRIDA_SERVER_XZ" ]; then
    echo -e "${YELLOW}[*] Found compressed file, extracting...${NC}"
    if command -v unxz &> /dev/null; then
        unxz "$FRIDA_SERVER_XZ"
    elif command -v xz &> /dev/null; then
        xz -d "$FRIDA_SERVER_XZ"
    else
        echo -e "${RED}Error: xz-utils not installed. Install with: sudo apt-get install xz-utils${NC}"
        exit 1
    fi
else
    # Download
    if command -v wget &> /dev/null; then
        wget "$DOWNLOAD_URL" || {
            echo -e "${RED}Error: Download failed.${NC}"
            echo "Please download manually from: https://github.com/frida/frida/releases"
            exit 1
        }
    elif command -v curl &> /dev/null; then
        curl -L -o "$FRIDA_SERVER_XZ" "$DOWNLOAD_URL" || {
            echo -e "${RED}Error: Download failed.${NC}"
            exit 1
        }
    else
        echo -e "${RED}Error: wget or curl not found.${NC}"
        echo "Please download manually: $DOWNLOAD_URL"
        exit 1
    fi
    
    # Extract
    echo -e "${YELLOW}[*] Extracting...${NC}"
    if command -v unxz &> /dev/null; then
        unxz "$FRIDA_SERVER_XZ"
    elif command -v xz &> /dev/null; then
        xz -d "$FRIDA_SERVER_XZ"
    else
        echo -e "${RED}Error: xz-utils not installed. Install with: sudo apt-get install xz-utils${NC}"
        exit 1
    fi
fi

if [ ! -f "$FRIDA_SERVER_FILE" ]; then
    echo -e "${RED}Error: frida-server file not found after download/extraction${NC}"
    exit 1
fi

echo -e "${GREEN}[+] frida-server ready: $FRIDA_SERVER_FILE${NC}"

# Step 4: Check root access
echo -e "${YELLOW}[*] Checking root access...${NC}"
if adb shell su -c "id" | grep -q "uid=0"; then
    echo -e "${GREEN}[+] Root access confirmed${NC}"
    USE_SU="su -c"
else
    echo -e "${YELLOW}[!] Device is not rooted.${NC}"
    echo -e "${YELLOW}[!] Trying without root (may not work)...${NC}"
    USE_SU=""
fi

# Step 5: Push to device
echo -e "${YELLOW}[*] Pushing frida-server to device...${NC}"
adb push "$FRIDA_SERVER_FILE" /data/local/tmp/frida-server || {
    echo -e "${RED}Error: Failed to push frida-server${NC}"
    exit 1
}
echo -e "${GREEN}[+] frida-server pushed${NC}"

# Step 6: Make executable
echo -e "${YELLOW}[*] Making frida-server executable...${NC}"
if [ -n "$USE_SU" ]; then
    adb shell su -c "chmod 755 /data/local/tmp/frida-server"
else
    adb shell chmod 755 /data/local/tmp/frida-server
fi
echo -e "${GREEN}[+] frida-server is executable${NC}"

# Step 7: Kill existing frida-server (if running)
echo -e "${YELLOW}[*] Checking for existing frida-server...${NC}"
if [ -n "$USE_SU" ]; then
    adb shell su -c "killall frida-server 2>/dev/null" || true
else
    adb shell "killall frida-server 2>/dev/null" || true
fi

# Step 8: Start frida-server
echo -e "${YELLOW}[*] Starting frida-server...${NC}"
if [ -n "$USE_SU" ]; then
    adb shell su -c "/data/local/tmp/frida-server &"
else
    adb shell "/data/local/tmp/frida-server &"
fi

sleep 2

# Step 9: Verify frida-server is running
echo -e "${YELLOW}[*] Verifying frida-server...${NC}"
if command -v frida-ps &> /dev/null; then
    if frida-ps -U &> /dev/null; then
        echo -e "${GREEN}[+] frida-server is running!${NC}"
        echo ""
        echo -e "${GREEN}=== Setup Complete ===${NC}"
        echo ""
        echo "You can now use Frida:"
        echo "  frida-ps -U                    # List processes"
        echo "  frida -U com.rustexample -l frida_extract_credentials.js"
    else
        echo -e "${RED}[-] frida-server may not be running properly${NC}"
        echo -e "${YELLOW}[!] Try manually: adb shell su -c '/data/local/tmp/frida-server &'${NC}"
    fi
else
    echo -e "${YELLOW}[!] frida-ps not found. Install with: pip install frida-tools${NC}"
    echo -e "${GREEN}[+] frida-server should be running on device${NC}"
fi

echo ""
echo -e "${GREEN}Setup complete!${NC}"
