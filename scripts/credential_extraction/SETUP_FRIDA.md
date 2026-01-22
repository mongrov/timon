# Setting Up Frida for Credential Extraction

## Step 1: Determine Your Device Architecture

First, you need to know your device's CPU architecture:

```bash
# Method 1: Check device architecture
adb shell getprop ro.product.cpu.abi

# Method 2: Check all supported ABIs
adb shell getprop ro.product.cpu.abilist

# Method 3: Check via uname
adb shell uname -m
```

Common architectures:
- `arm64-v8a` or `aarch64` → Download `frida-server-*-android-arm64.xz`
- `armeabi-v7a` or `arm` → Download `frida-server-*-android-arm.xz`
- `x86_64` → Download `frida-server-*-android-x86_64.xz`
- `x86` → Download `frida-server-*-android-x86.xz`

## Step 2: Download Frida Server

### Option A: Download from GitHub (Recommended)

1. **Get the latest Frida version**:
   ```bash
   # Check your local Frida version (if installed)
   frida --version
   # Example output: 16.1.0
   ```

2. **Download matching frida-server**:
   Go to: https://github.com/frida/frida/releases/latest
   
   Or use direct download (replace VERSION and ARCH):
   ```bash
   # For arm64 (most common on modern Android devices)
   wget https://github.com/frida/frida/releases/download/16.1.0/frida-server-16.1.0-android-arm64.xz
   
   # For arm (older devices)
   wget https://github.com/frida/releases/download/16.1.0/frida-server-16.1.0-android-arm.xz
   
   # For x86_64 (emulators)
   wget https://github.com/frida/releases/download/16.1.0/frida-server-16.1.0-android-x86_64.xz
   ```

3. **Extract the archive**:
   ```bash
   # Install xz-utils if needed
   sudo apt-get install xz-utils
   
   # Extract
   unxz frida-server-*.xz
   # Or: xz -d frida-server-*.xz
   ```

### Option B: Auto-download Script

I'll create a script that does this automatically (see below).

## Step 3: Push to Device

```bash
# Push frida-server to device
adb push frida-server /data/local/tmp/

# Make it executable
adb shell chmod 755 /data/local/tmp/frida-server
```

## Step 4: Start Frida Server (Requires Root)

```bash
# Start frida-server in background
adb shell su -c '/data/local/tmp/frida-server &'

# Verify it's running
frida-ps -U
# Should list processes on your device
```

## Step 5: Test Frida

```bash
# List processes (should work now)
frida-ps -U

# Try attaching to your app
frida -U com.rustexample -l frida_extract_credentials.js
```

## Troubleshooting

### "Permission denied" when starting frida-server
- **Solution**: Ensure device is rooted: `adb shell su -c "id"` should show `uid=0`

### "frida-ps: error: unable to connect to remote frida-server"
- **Solution**: 
  1. Check frida-server is running: `adb shell su -c "ps | grep frida"`
  2. Restart frida-server: `adb shell su -c "killall frida-server; /data/local/tmp/frida-server &"`
  3. Check adb connection: `adb devices`

### "No such file or directory" when pushing
- **Solution**: 
  1. Check you're in the right directory: `ls -la frida-server`
  2. Use full path: `adb push /full/path/to/frida-server /data/local/tmp/`

### Architecture mismatch
- **Solution**: Download the correct architecture for your device (see Step 1)

## Alternative: Use Frida on Emulator (Easier)

If you're using an Android emulator, it's easier:

```bash
# Start emulator with root
emulator -avd <your_avd_name> -writable-system

# Enable root
adb root

# Now you can push and run frida-server without 'su'
adb push frida-server /data/local/tmp/
adb shell chmod 755 /data/local/tmp/frida-server
adb shell /data/local/tmp/frida-server &
```

## Quick Setup Script

See `setup_frida.sh` for an automated setup script.
