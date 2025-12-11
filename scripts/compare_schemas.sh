#!/bin/bash

# Paths to compare
PATH1="/home/ahmed/mongrov/rn-timon/timon/tmp/group/7TQBn6aSe49wfnuox_roshann/zivaring/hrv_table"
PATH2="/home/ahmed/mongrov/rn-timon/timon/tmp/group/MtvcHGtLWZ23hS3KT_spalaniswamy/zivaring/hrv_table"

echo "=========================================="
echo "SCHEMA COMPARISON REPORT"
echo "=========================================="
echo ""
echo "Path 1: 7TQBn6aSe49wfnuox_roshann"
echo "Path 2: MtvcHGtLWZ23hS3KT_spalaniswamy"
echo ""

# Function to get schema columns
get_schema_columns() {
    local file=$1
    datafusion-cli -q <<EOF 2>/dev/null | grep -E "^\|" | grep -v "^+--" | tail -n +3 | awk -F'|' '{print $2}' | tr -d ' ' | sort
CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '$file';
DESCRIBE t;
EOF
}

# Get all unique columns from path 1
echo "=== Collecting schemas from Path 1 ==="
PATH1_COLUMNS=()
for f in "$PATH1"/*.parquet; do
    if [ -f "$f" ]; then
        echo "Reading: $(basename $f)"
        while IFS= read -r col; do
            if [ ! -z "$col" ]; then
                PATH1_COLUMNS+=("$col")
            fi
        done < <(get_schema_columns "$f")
    fi
done

# Get all unique columns from path 2
echo ""
echo "=== Collecting schemas from Path 2 ==="
PATH2_COLUMNS=()
for f in "$PATH2"/*.parquet; do
    if [ -f "$f" ]; then
        echo "Reading: $(basename $f)"
        while IFS= read -r col; do
            if [ ! -z "$col" ]; then
                PATH2_COLUMNS+=("$col")
            fi
        done < <(get_schema_columns "$f")
    fi
done

# Get unique columns
UNIQUE_PATH1=$(printf '%s\n' "${PATH1_COLUMNS[@]}" | sort -u)
UNIQUE_PATH2=$(printf '%s\n' "${PATH2_COLUMNS[@]}" | sort -u)

echo ""
echo "=========================================="
echo "COLUMN COMPARISON"
echo "=========================================="
echo ""
echo "Path 1 (7TQBn6aSe49wfnuox_roshann) columns:"
echo "$UNIQUE_PATH1" | sed 's/^/  - /'
echo ""
echo "Path 2 (MtvcHGtLWZ23hS3KT_spalaniswamy) columns:"
echo "$UNIQUE_PATH2" | sed 's/^/  - /'
echo ""

# Find differences
ONLY_IN_PATH1=$(comm -23 <(echo "$UNIQUE_PATH1") <(echo "$UNIQUE_PATH2"))
ONLY_IN_PATH2=$(comm -13 <(echo "$UNIQUE_PATH1") <(echo "$UNIQUE_PATH2"))
COMMON=$(comm -12 <(echo "$UNIQUE_PATH1") <(echo "$UNIQUE_PATH2"))

echo "=========================================="
echo "DIFFERENCES"
echo "=========================================="
echo ""
if [ ! -z "$ONLY_IN_PATH1" ]; then
    echo "Columns ONLY in Path 1 (7TQBn6aSe49wfnuox_roshann):"
    echo "$ONLY_IN_PATH1" | sed 's/^/  - /'
    echo ""
fi

if [ ! -z "$ONLY_IN_PATH2" ]; then
    echo "Columns ONLY in Path 2 (MtvcHGtLWZ23hS3KT_spalaniswamy):"
    echo "$ONLY_IN_PATH2" | sed 's/^/  - /'
    echo ""
fi

if [ ! -z "$COMMON" ]; then
    echo "Common columns in both paths:"
    echo "$COMMON" | sed 's/^/  - /'
    echo ""
fi

echo "=========================================="
echo "SUMMARY"
echo "=========================================="
echo ""
echo "Key Difference:"
echo "  Path 1 uses: systolicBP, diastolicBP"
echo "  Path 2 uses: highBP, lowBP"
echo ""
echo "These appear to be different naming conventions for the same data:"
echo "  - systolicBP (Path 1) <-> highBP (Path 2)"
echo "  - diastolicBP (Path 1) <-> lowBP (Path 2)"
echo ""

