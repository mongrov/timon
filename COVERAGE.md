# Code Coverage Guide

This project uses `cargo-tarpaulin` for code coverage analysis.

## Installation

If you don't have `cargo-tarpaulin` installed:

```bash
cargo install cargo-tarpaulin
```

## Basic Usage

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

1. **Tests timeout**: Increase timeout with `--timeout` flag
2. **Missing coverage for some files**: Check if they're excluded or not compiled
3. **Slow coverage runs**: Use `--skip-clean` to avoid rebuilding
