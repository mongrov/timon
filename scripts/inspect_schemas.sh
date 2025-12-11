#!/bin/bash

DIR="/home/ahmed/mongrov/rn-timon/timon/tmp/group/7TQBn6aSe49wfnuox_roshann/zivaring/hrv_table"

cd "$DIR" || exit 1
echo "Inspecting schemas in: $DIR"
echo "-------------------------------------------------------"


for f in *.parquet; do
    echo
    echo "=== Schema for: $f ==="
    echo

    # Run DataFusion CLI non-interactively
    datafusion-cli -q <<EOF
CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '$f';
DESCRIBE t;
EOF

    echo "-------------------------------------------------------"
done
