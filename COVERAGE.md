# Code Coverage Guide

This project uses `cargo-tarpaulin` for code coverage analysis.

## Installation

If you don't have `cargo-tarpaulin` installed:

```bash
cargo install cargo-tarpaulin
```

## Recommended Usage: Use `coverage.sh`

**We recommend using the `coverage.sh` script instead of calling `cargo tarpaulin` directly:**

```bash
bash coverage.sh
```

Or if you have execute permissions:

```bash
./coverage.sh
```

### Why use `coverage.sh`?

The `coverage.sh` script is configured with project-specific settings:

1. **Excludes platform-specific code**: Automatically excludes `src/lib.rs` (Android/iOS platform code) and `src/main.rs` from coverage, as these contain platform-specific implementations that shouldn't be included in coverage metrics.

2. **Prevents race conditions**: Uses `--test-threads=1` to avoid race conditions in file system-based tests, ensuring consistent and reliable test results.

3. **Consistent output**: Outputs to stdout in a standardized format, making it easy to parse and integrate with CI/CD pipelines.

4. **Accepts additional arguments**: You can still pass additional arguments to customize the run:
   ```bash
   bash coverage.sh --out Html --output-dir ./coverage-report
   ```

## Basic Usage (Alternative)

If you need to run `cargo tarpaulin` directly, you can use:

### Run coverage for all tests

```bash
cargo tarpaulin --out Html --output-dir ./coverage-report
```

This will:
- Run all tests
- Generate an HTML coverage report in `./coverage-report/`
- Open `coverage-report/tarpaulin-report.html` in your browser to view the report

### Run coverage with terminal output

```bash
cargo tarpaulin --out Stdout
```

**Note**: When running directly, remember to exclude `src/lib.rs` and `src/main.rs`, and use `--test-threads=1` for consistency:
```bash
cargo tarpaulin --out Stdout --exclude-files "src/lib.rs" --exclude-files "src/main.rs" -- --test-threads=1
```

### Generate multiple output formats

```bash
cargo tarpaulin --out Html --out Xml --out Stdout --output-dir ./coverage-report
```

### Exclude files from coverage

```bash
cargo tarpaulin --exclude-files 'src/tests/*' --out Html --output-dir ./coverage-report
```

### Set coverage threshold

```bash
cargo tarpaulin --out Html --output-dir ./coverage-report --fail-under 80
```

This will fail if coverage is below 80%.

## Advanced Options

### Include integration tests

```bash
cargo tarpaulin --tests --out Html --output-dir ./coverage-report
```

### Include examples

```bash
cargo tarpaulin --examples --out Html --output-dir ./coverage-report
```

### Include benchmarks

```bash
cargo tarpaulin --benches --out Html --output-dir ./coverage-report
```

### Timeout for tests

```bash
cargo tarpaulin --timeout 300 --out Html --output-dir ./coverage-report
```

### Skip tests that take too long

```bash
cargo tarpaulin --skip-clean --out Html --output-dir ./coverage-report
```

## Alternative: cargo-llvm-cov

For more detailed coverage information, you can use `cargo-llvm-cov`:

```bash
# Install
cargo install cargo-llvm-cov

# Run coverage
cargo llvm-cov --html --output-dir ./coverage-report

# Generate report
cargo llvm-cov --lcov --output-path ./coverage/lcov.info
```

## CI/CD Integration

### GitHub Actions Example

```yaml
- name: Generate coverage report
  run: |
    cargo install cargo-tarpaulin
    cargo tarpaulin --out Xml --output-dir ./coverage-report
    cargo tarpaulin --out Html --output-dir ./coverage-report

- name: Upload coverage to Codecov
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage-report/cobertura.xml
```

## Viewing Reports

After generating HTML reports, open them in your browser:

```bash
# Linux
xdg-open coverage-report/tarpaulin-report.html

# macOS
open coverage-report/tarpaulin-report.html

# Windows
start coverage-report/tarpaulin-report.html
```

## Common Issues

1. **Tests timeout**: Increase timeout with `--timeout` flag (e.g., `bash coverage.sh --timeout 300`)
2. **Missing coverage for some files**: Check if they're excluded (lib.rs and main.rs are excluded by default in `coverage.sh`) or not compiled
3. **Slow coverage runs**: Use `--skip-clean` to avoid rebuilding (e.g., `bash coverage.sh --skip-clean`)
4. **Race conditions in tests**: The `coverage.sh` script already uses `--test-threads=1` to prevent this. If running directly, make sure to include this flag.
