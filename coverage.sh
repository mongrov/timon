#!/bin/bash
# Run coverage excluding lib.rs (platform-specific Android/iOS code) and main.rs
# Use --test-threads=1 to avoid race conditions in file system-based tests
cargo tarpaulin --out Stdout --exclude-files "src/lib.rs" --exclude-files "src/main.rs" -- --test-threads=1 "$@"
