#!/bin/bash

# Script to inspect React Native bridge for credential passing

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./react_native_bridge_$(date +%Y%m%d_%H%M%S)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}React Native Bridge Inspection Script${NC}"
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

# Method 1: Monitor logcat for React Native bridge calls
echo -e "${YELLOW}[*] Method 1: Monitoring logcat for React Native bridge calls...${NC}"
adb logcat -c > /dev/null 2>&1
LOG_FILE="${OUTPUT_DIR}/bridge_logcat.txt"
timeout 10 adb logcat | grep -iE "ReactNativeJS|bridge|native|initBucket" > "$LOG_FILE" 2>&1 &
LOGCAT_PID=$!

sleep 5
kill $LOGCAT_PID 2>/dev/null || true
pkill -f "adb logcat" 2>/dev/null || true

if [ -s "$LOG_FILE" ]; then
    echo -e "${GREEN}[+] Captured React Native bridge logs${NC}"
    grep -iE "initBucket|access_key|secret|credential" "$LOG_FILE" > "${OUTPUT_DIR}/bridge_credentials.txt" 2>/dev/null
else
    echo -e "${YELLOW}[!] No bridge logs captured${NC}"
fi

# Method 2: Use Frida to hook React Native bridge
echo -e "${YELLOW}[*] Method 2: Using Frida to hook React Native bridge...${NC}"
FRIDA_SCRIPT="${OUTPUT_DIR}/frida_bridge_hook.js"
cat > "$FRIDA_SCRIPT" << 'EOF'
// Frida script to hook React Native bridge
Java.perform(function() {
    console.log("[*] Hooking React Native bridge...");
    
    try {
        // Hook Promise class
        var Promise = Java.use("com.facebook.react.bridge.Promise");
        
        Promise.resolve.implementation = function(value) {
            if (value && value.toString) {
                var str = value.toString();
                if (str.indexOf("bucket") !== -1 || str.indexOf("CloudStorageManager") !== -1) {
                    console.log("[*] Promise resolved with: " + str);
                }
            }
            return this.resolve(value);
        };
        
        // Hook ReactMethod
        var ReactMethod = Java.use("com.facebook.react.bridge.ReactMethod");
        
        // Hook TimonModule methods
        var TimonModule = Java.use("com.rustexample.TimonModule");
        
        // Hook all methods that might pass credentials
        var methods = TimonModule.class.getDeclaredMethods();
        for (var i = 0; i < methods.length; i++) {
            var method = methods[i];
            var methodName = method.getName();
            console.log("[*] Found method: " + methodName);
        }
        
        console.log("[+] React Native bridge hooks installed");
    } catch (e) {
        console.log("[-] Error: " + e);
    }
});
EOF

echo -e "${GREEN}[+] Frida script created: $FRIDA_SCRIPT${NC}"
echo -e "${YELLOW}[!] To use Frida bridge hook, run:${NC}"
echo "   frida -U -p $PID -l $FRIDA_SCRIPT"

# Method 3: Check JavaScript bundle for credential references
echo -e "${YELLOW}[*] Method 3: Checking JavaScript bundle for credential references...${NC}"
JS_BUNDLE_PATH="/data/data/$PACKAGE_NAME/files/BridgeReactNativeDevBundle.js"
if adb shell "test -f $JS_BUNDLE_PATH" 2>/dev/null; then
    echo -e "${GREEN}[+] Found JavaScript bundle${NC}"
    adb pull "$JS_BUNDLE_PATH" "${OUTPUT_DIR}/js_bundle.js" 2>/dev/null || {
        # Try with run-as
        adb shell "run-as $PACKAGE_NAME cat $JS_BUNDLE_PATH" > "${OUTPUT_DIR}/js_bundle.js" 2>/dev/null || true
    }
    
    if [ -f "${OUTPUT_DIR}/js_bundle.js" ] && [ -s "${OUTPUT_DIR}/js_bundle.js" ]; then
        echo -e "${GREEN}[+] JavaScript bundle extracted${NC}"
        grep -iE "initBucket|access_key|secret|credential|aws|s3" "${OUTPUT_DIR}/js_bundle.js" | head -20 > "${OUTPUT_DIR}/js_bundle_credentials.txt" 2>/dev/null
        if [ -s "${OUTPUT_DIR}/js_bundle_credentials.txt" ]; then
            echo -e "${YELLOW}[!] Found credential-related code in JS bundle:${NC}"
            head -10 "${OUTPUT_DIR}/js_bundle_credentials.txt"
        else
            echo -e "${YELLOW}[!] No credential-related code found in JS bundle${NC}"
        fi
    fi
else
    echo -e "${YELLOW}[!] JavaScript bundle not found at expected path${NC}"
fi

# Method 4: Monitor network traffic (if credentials are sent)
echo -e "${YELLOW}[*] Method 4: Checking for network traffic monitoring...${NC}"
echo -e "${YELLOW}[!] Network monitoring requires additional setup (tcpdump/mitmproxy)${NC}"
echo -e "${YELLOW}[!] Skipping for now - would require network capture setup${NC}"

echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Output directory: $OUTPUT_DIR"
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2

echo ""
echo -e "${YELLOW}[!] Note: React Native bridge inspection is indirect.${NC}"
echo -e "${YELLOW}[!] Credentials are passed from JS to native, but may not be logged.${NC}"
echo -e "${YELLOW}[!] Frida (Method 1) is more direct for intercepting credentials.${NC}"
