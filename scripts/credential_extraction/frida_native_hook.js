/**
 * Frida Script to Hook Native Library Functions (JNI Level)
 * 
 * This script hooks into the native Rust functions directly at the JNI level,
 * which is more reliable than hooking Java methods.
 * 
 * Usage:
 *   frida -U -p <PID> -l frida_native_hook.js
 */

console.log("[*] Frida Native Hook Script loaded");
console.log("[*] Hooking native library functions at JNI level...");

// Hook JNI functions
var JNIEnv = null;
var jniEnvPtr = null;

// Get JNI environment
Java.perform(function() {
    console.log("[*] Java VM attached");
    
    // Get JNI environment pointer
    var vm = Java.vm;
    var env = vm.getEnv();
    jniEnvPtr = env.handle;
    console.log("[+] JNI Environment pointer: " + jniEnvPtr);
});

// Hook native library functions
// First, try to find the native function
var nativeInitBucketAddr = null;

// Try different methods to find the function
var functionNames = [
    "Java_com_rustexample_TimonModule_nativeInitBucket",
    "_ZN7rustexample10TimonModule17nativeInitBucketEP7_JNIEnvP8_jobjectP8_jstringS5_S5_S5_S5_"
];

// Method 1: Try to find in libtsdb_timon.so
try {
    var libTimon = Process.findModuleByName("libtsdb_timon.so");
    if (libTimon) {
        console.log("[+] Found libtsdb_timon.so at: " + libTimon.base);
        
        for (var i = 0; i < functionNames.length; i++) {
            var addr = Module.findExportByName("libtsdb_timon.so", functionNames[i]);
            if (addr) {
                nativeInitBucketAddr = addr;
                console.log("[+] Found " + functionNames[i] + " at: " + addr);
                break;
            }
        }
        
        // If not found by name, try to enumerate exports
        if (!nativeInitBucketAddr) {
            console.log("[*] Function not found by name, enumerating exports...");
            Module.enumerateExports("libtsdb_timon.so").forEach(function(exp) {
                if (exp.name.indexOf("nativeInitBucket") !== -1 || exp.name.indexOf("InitBucket") !== -1) {
                    console.log("[+] Found potential function: " + exp.name + " at " + exp.address);
                    if (!nativeInitBucketAddr) {
                        nativeInitBucketAddr = exp.address;
                    }
                }
            });
        }
    }
} catch (e) {
    console.log("[-] Error finding libtsdb_timon.so: " + e);
}

// Method 2: Try to find in all modules
if (!nativeInitBucketAddr) {
    console.log("[*] Searching all modules for nativeInitBucket...");
    Process.enumerateModules().forEach(function(module) {
        for (var i = 0; i < functionNames.length; i++) {
            try {
                var addr = Module.findExportByName(module.name, functionNames[i]);
                if (addr) {
                    nativeInitBucketAddr = addr;
                    console.log("[+] Found " + functionNames[i] + " in " + module.name + " at: " + addr);
                    return;
                }
            } catch (e) {
                // Continue searching
            }
        }
    });
}

// If we found the function, hook it
if (nativeInitBucketAddr) {
    console.log("[+] Hooking nativeInitBucket at: " + nativeInitBucketAddr);
    
    Interceptor.attach(nativeInitBucketAddr, {
        onEnter: function(args) {
            console.log("\n" + "=".repeat(60));
            console.log("[!] NATIVE FUNCTION CALLED: nativeInitBucket");
            console.log("=".repeat(60));
            console.log("[*] Timestamp: " + new Date().toISOString());
            console.log("[*] Function address: " + this.returnAddress);
            
            // JNI function signature:
            // JNIEXPORT void JNICALL Java_com_rustexample_TimonModule_nativeInitBucket
            //   (JNIEnv *env, jobject thiz, jstring bucket_endpoint, jstring bucket_name, 
            //    jstring access_key_id, jstring secret_access_key, jstring bucket_region)
            
            // args[0] = JNIEnv*
            // args[1] = jobject (this)
            // args[2] = jstring (bucket_endpoint)
            // args[3] = jstring (bucket_name)
            // args[4] = jstring (access_key_id)
            // args[5] = jstring (secret_access_key)
            // args[6] = jstring (bucket_region)
            
            try {
                // Get JNI environment from args[0]
                var jniEnvPtr = args[0];
                var env = Java.vm.getEnv();
                
                // Extract jstring parameters
                if (args[2] != ptr(0)) {
                    var bucket_endpoint = env.getStringUtfChars(args[2], null);
                    if (bucket_endpoint) {
                        console.log("[*] Bucket Endpoint: " + bucket_endpoint.readCString());
                    }
                }
                
                if (args[3] != ptr(0)) {
                    var bucket_name = env.getStringUtfChars(args[3], null);
                    if (bucket_name) {
                        console.log("[*] Bucket Name: " + bucket_name.readCString());
                    }
                }
                
                if (args[4] != ptr(0)) {
                    var access_key_id = env.getStringUtfChars(args[4], null);
                    if (access_key_id) {
                        console.log("[*] Access Key ID: " + access_key_id.readCString());
                    }
                }
                
                if (args[5] != ptr(0)) {
                    var secret_access_key = env.getStringUtfChars(args[5], null);
                    if (secret_access_key) {
                        console.log("[*] Secret Access Key: " + secret_access_key.readCString());
                    }
                }
                
                if (args[6] != ptr(0)) {
                    var bucket_region = env.getStringUtfChars(args[6], null);
                    if (bucket_region) {
                        console.log("[*] Bucket Region: " + bucket_region.readCString());
                    }
                }
                
                console.log("=".repeat(60) + "\n");
                
            } catch (e) {
                console.log("[-] Error extracting parameters: " + e);
                console.log("[-] Stack trace: " + e.stack);
            }
        },
        onLeave: function(retval) {
            console.log("[*] nativeInitBucket returned");
        }
    });
} else {
    console.log("[-] Could not find nativeInitBucket function");
    console.log("[*] Will try alternative hooking methods...");
}

// Alternative: Hook JNI RegisterNatives to catch when native methods are registered
try {
    // Find JNI RegisterNatives function
    var RegisterNatives = Module.findExportByName(null, "RegisterNatives");
    if (RegisterNatives) {
        console.log("[+] Found RegisterNatives at: " + RegisterNatives);
        
        Interceptor.attach(RegisterNatives, {
            onEnter: function(args) {
                // args[0] = JNIEnv*
                // args[1] = jclass
                // args[2] = const JNINativeMethod*
                // args[3] = jint nMethods
                
                try {
                    var nMethods = args[3].toInt32();
                    var methodsPtr = args[2];
                    
                    console.log("[*] RegisterNatives called with " + nMethods + " methods");
                    
                    // Read the JNINativeMethod structures
                    for (var i = 0; i < nMethods; i++) {
                        var namePtr = methodsPtr.add(i * Process.pointerSize * 3).readPointer();
                        var sigPtr = methodsPtr.add(i * Process.pointerSize * 3 + Process.pointerSize).readPointer();
                        var fnPtr = methodsPtr.add(i * Process.pointerSize * 3 + Process.pointerSize * 2).readPointer();
                        
                        if (namePtr) {
                            var methodName = namePtr.readCString();
                            if (methodName && methodName.indexOf("InitBucket") !== -1) {
                                console.log("[!] Found nativeInitBucket registration!");
                                console.log("[*] Method name: " + methodName);
                                console.log("[*] Function pointer: " + fnPtr);
                                
                                // Hook the function
                                Interceptor.attach(fnPtr, {
                                    onEnter: function(args) {
                                        console.log("\n" + "=".repeat(60));
                                        console.log("[!] NATIVE FUNCTION CALLED via RegisterNatives hook");
                                        console.log("=".repeat(60));
                                        console.log("[*] Timestamp: " + new Date().toISOString());
                                        
                                        try {
                                            var env = Java.vm.getEnv();
                                            
                                            // Try to extract string arguments
                                            for (var j = 2; j < 7; j++) {
                                                if (args[j] != ptr(0)) {
                                                    try {
                                                        var str = env.getStringUtfChars(args[j], null);
                                                        if (str) {
                                                            console.log("[*] Arg[" + j + "]: " + str.readCString());
                                                        }
                                                    } catch (e) {
                                                        // Ignore
                                                    }
                                                }
                                            }
                                            
                                            console.log("=".repeat(60) + "\n");
                                        } catch (e) {
                                            console.log("[-] Error: " + e);
                                        }
                                    }
                                });
                            }
                        }
                    }
                } catch (e) {
                    // Ignore errors
                }
            }
        });
    }
} catch (e) {
    console.log("[-] Could not hook RegisterNatives: " + e);
}

// Hook JNI string functions to catch all string operations
var GetStringUTFChars = null;
try {
    // Find JNI GetStringUTFChars function
    var jniFunctions = [
        "_ZN3art3JNI12GetStringUTFCharsEP7_JNIEnvP8_jstringPh",
        "GetStringUTFChars"
    ];
    
    for (var i = 0; i < jniFunctions.length; i++) {
        var func = Module.findExportByName(null, jniFunctions[i]);
        if (func) {
            console.log("[+] Found JNI GetStringUTFChars at: " + func);
            GetStringUTFChars = func;
            break;
        }
    }
    
    if (GetStringUTFChars) {
        Interceptor.attach(GetStringUTFChars, {
            onEnter: function(args) {
                // args[0] = JNIEnv*
                // args[1] = jstring
                // args[2] = jboolean*
                
                // Try to read the string if possible
                try {
                    var env = Java.vm.getEnv();
                    if (args[1] != ptr(0)) {
                        var str = env.getStringUtfChars(args[1], null);
                        if (str) {
                            var strValue = str.readCString();
                            // Check if it looks like credentials
                            if (strValue && (strValue.indexOf("AKIA") !== -1 || strValue.length > 20)) {
                                console.log("[*] JNI GetStringUTFChars called with potential credential: " + strValue.substring(0, 50));
                            }
                        }
                    }
                } catch (e) {
                    // Ignore errors
                }
            }
        });
    }
} catch (e) {
    console.log("[-] Could not hook JNI GetStringUTFChars: " + e);
}

console.log("[+] Native hook script ready. Waiting for function calls...");
