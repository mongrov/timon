#!/bin/bash

# Script to check app's private files for cached credentials
# This checks various locations where credentials might be stored

PACKAGE_NAME="${1:-com.rustexample}"
OUTPUT_DIR="./app_files_check_$(date +%Y%m%d_%H%M%S)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}App Files Check Script${NC}"
echo "Package: $PACKAGE_NAME"
echo "Output: $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Check if device is connected
if ! adb devices | grep -q "device$"; then
    echo -e "${RED}Error: No device connected${NC}"
    exit 1
fi

# App data directories to check
APP_DATA_DIR="/data/data/$PACKAGE_NAME"
APP_FILES_DIR="$APP_DATA_DIR/files"
APP_CACHE_DIR="$APP_DATA_DIR/cache"
APP_SHARED_PREFS="$APP_DATA_DIR/shared_prefs"
EXTERNAL_STORAGE="/sdcard/Android/data/$PACKAGE_NAME"

echo -e "${YELLOW}[*] Checking app files for cached credentials...${NC}"
echo ""

FOUND_CREDENTIALS=false

# Function to search for credentials in a file
search_file() {
    local file_path=$1
    local file_name=$2
    
    if [ ! -f "$file_path" ]; then
        return
    fi
    
    # Search for access key pattern
    if grep -qE "AKIA[0-9A-Z]{16}" "$file_path" 2>/dev/null; then
        echo -e "${GREEN}[!] FOUND ACCESS KEY in $file_name${NC}"
        grep -E "AKIA[0-9A-Z]{16}" "$file_path" >> "${OUTPUT_DIR}/found_access_keys.txt" 2>/dev/null
        FOUND_CREDENTIALS=true
    fi
    
    # Search for secret key pattern
    if grep -qE "ckCcy10mEtAjPOo" "$file_path" 2>/dev/null; then
        echo -e "${GREEN}[!] FOUND SECRET KEY in $file_name${NC}"
        grep "ckCcy10mEtAjPOo" "$file_path" >> "${OUTPUT_DIR}/found_secret_keys.txt" 2>/dev/null
        FOUND_CREDENTIALS=true
    fi
    
    # Search for AWS-related strings
    if grep -qiE "access.*key|secret.*key|aws|s3|credential" "$file_path" 2>/dev/null; then
        echo -e "${BLUE}[*] Found AWS-related content in $file_name${NC}"
        grep -iE "access.*key|secret.*key|aws|s3|credential" "$file_path" | head -5 >> "${OUTPUT_DIR}/aws_related_content.txt" 2>/dev/null
    fi
}

# Method 1: Check files directory
echo -e "${YELLOW}[*] Method 1: Checking /data/data/$PACKAGE_NAME/files/...${NC}"
if adb shell "run-as $PACKAGE_NAME ls $APP_FILES_DIR" > /dev/null 2>&1; then
    echo -e "${GREEN}[+] Files directory accessible${NC}"
    
    # List files
    adb shell "run-as $PACKAGE_NAME ls -la $APP_FILES_DIR" > "${OUTPUT_DIR}/files_list.txt" 2>/dev/null
    
    # Try to pull files
    for file in $(adb shell "run-as $PACKAGE_NAME ls $APP_FILES_DIR" 2>/dev/null | tr -d '\r'); do
        if [ -n "$file" ] && [ "$file" != "." ] && [ "$file" != ".." ]; then
            echo -e "${BLUE}[*] Checking file: $file${NC}"
            adb shell "run-as $PACKAGE_NAME cat $APP_FILES_DIR/$file" > "${OUTPUT_DIR}/files_${file}" 2>/dev/null
            if [ -f "${OUTPUT_DIR}/files_${file}" ]; then
                search_file "${OUTPUT_DIR}/files_${file}" "files/$file"
            fi
        fi
    done
else
    echo -e "${RED}[-] Cannot access files directory (may need root)${NC}"
    
    # Try with root
    if adb shell "su -c 'ls $APP_FILES_DIR'" > /dev/null 2>&1; then
        echo -e "${GREEN}[+] Files directory accessible with root${NC}"
        adb shell "su -c 'ls -la $APP_FILES_DIR'" > "${OUTPUT_DIR}/files_list.txt" 2>/dev/null
        
        for file in $(adb shell "su -c 'ls $APP_FILES_DIR'" 2>/dev/null | tr -d '\r'); do
            if [ -n "$file" ] && [ "$file" != "." ] && [ "$file" != ".." ]; then
                echo -e "${BLUE}[*] Checking file: $file${NC}"
                adb shell "su -c 'cat $APP_FILES_DIR/$file'" > "${OUTPUT_DIR}/files_${file}" 2>/dev/null
                if [ -f "${OUTPUT_DIR}/files_${file}" ]; then
                    search_file "${OUTPUT_DIR}/files_${file}" "files/$file"
                fi
            fi
        done
    fi
fi

# Method 2: Check cache directory
echo ""
echo -e "${YELLOW}[*] Method 2: Checking /data/data/$PACKAGE_NAME/cache/...${NC}"
if adb shell "run-as $PACKAGE_NAME ls $APP_CACHE_DIR" > /dev/null 2>&1; then
    echo -e "${GREEN}[+] Cache directory accessible${NC}"
    adb shell "run-as $PACKAGE_NAME ls -la $APP_CACHE_DIR" > "${OUTPUT_DIR}/cache_list.txt" 2>/dev/null
else
    echo -e "${YELLOW}[!] Cache directory not accessible without root${NC}"
fi

# Method 3: Check SharedPreferences
echo ""
echo -e "${YELLOW}[*] Method 3: Checking SharedPreferences...${NC}"
if adb shell "run-as $PACKAGE_NAME ls $APP_SHARED_PREFS" > /dev/null 2>&1; then
    echo -e "${GREEN}[+] SharedPreferences accessible${NC}"
    adb shell "run-as $PACKAGE_NAME ls -la $APP_SHARED_PREFS" > "${OUTPUT_DIR}/shared_prefs_list.txt" 2>/dev/null
    
    for prefs_file in $(adb shell "run-as $PACKAGE_NAME ls $APP_SHARED_PREFS" 2>/dev/null | tr -d '\r'); do
        if [ -n "$prefs_file" ] && [ "$prefs_file" != "." ] && [ "$prefs_file" != ".." ]; then
            echo -e "${BLUE}[*] Checking SharedPrefs: $prefs_file${NC}"
            adb shell "run-as $PACKAGE_NAME cat $APP_SHARED_PREFS/$prefs_file" > "${OUTPUT_DIR}/shared_prefs_${prefs_file}" 2>/dev/null
            if [ -f "${OUTPUT_DIR}/shared_prefs_${prefs_file}" ]; then
                search_file "${OUTPUT_DIR}/shared_prefs_${prefs_file}" "shared_prefs/$prefs_file"
            fi
        fi
    done
else
    echo -e "${YELLOW}[!] SharedPreferences not accessible without root${NC}"
fi

# Method 4: Check external storage
echo ""
echo -e "${YELLOW}[*] Method 4: Checking external storage...${NC}"
if adb shell "ls $EXTERNAL_STORAGE" > /dev/null 2>&1; then
    echo -e "${GREEN}[+] External storage accessible${NC}"
    adb shell "ls -laR $EXTERNAL_STORAGE" > "${OUTPUT_DIR}/external_storage_list.txt" 2>/dev/null
else
    echo -e "${YELLOW}[!] External storage directory not found${NC}"
fi

# Method 5: Check databases
echo ""
echo -e "${YELLOW}[*] Method 5: Checking databases...${NC}"
DB_DIR="$APP_DATA_DIR/databases"
if adb shell "run-as $PACKAGE_NAME ls $DB_DIR" > /dev/null 2>&1; then
    echo -e "${GREEN}[+] Databases directory accessible${NC}"
    adb shell "run-as $PACKAGE_NAME ls -la $DB_DIR" > "${OUTPUT_DIR}/databases_list.txt" 2>/dev/null
    
    for db_file in $(adb shell "run-as $PACKAGE_NAME ls $DB_DIR" 2>/dev/null | tr -d '\r'); do
        if [ -n "$db_file" ] && [ "$db_file" != "." ] && [ "$db_file" != ".." ]; then
            echo -e "${BLUE}[*] Checking database: $db_file${NC}"
            adb shell "run-as $PACKAGE_NAME cat $DB_DIR/$db_file" > "${OUTPUT_DIR}/db_${db_file}" 2>/dev/null
            if [ -f "${OUTPUT_DIR}/db_${db_file}" ]; then
                search_file "${OUTPUT_DIR}/db_${db_file}" "databases/$db_file"
            fi
        fi
    done
else
    echo -e "${YELLOW}[!] Databases directory not accessible${NC}"
fi

echo ""
if [ "$FOUND_CREDENTIALS" = true ]; then
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}[!] CREDENTIALS FOUND IN APP FILES!${NC}"
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
    echo -e "${YELLOW}[!] No credentials found in app files${NC}"
    echo -e "${YELLOW}[!] This is expected - credentials are stored in memory, not files${NC}"
fi

echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo "Output directory: $OUTPUT_DIR"
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2
