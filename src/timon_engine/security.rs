// Security module for detecting debugging, tampering, and Frida hooks

/// Security module for detecting debugging, tampering, and Frida hooks
pub mod security {

  /// Check if debugging is detected
  #[cfg(target_os = "android")]
  pub fn is_debugging_detected() -> bool {
    // Check if debugger is attached via /proc/self/status
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
      for line in status.lines() {
        if line.starts_with("TracerPid:") {
          let pid = line.split_whitespace().nth(1).unwrap_or("0");
          if pid != "0" {
            eprintln!("Security: Debugger detected (TracerPid: {})", pid);
            return true;
          }
        }
      }
    }

    false
  }

  #[cfg(not(target_os = "android"))]
  pub fn is_debugging_detected() -> bool {
    false
  }

  /// Check if Frida is detected
  #[cfg(target_os = "android")]
  pub fn is_frida_detected() -> bool {
    // Check for frida-server process
    if let Ok(output) = std::process::Command::new("sh").arg("-c").arg("ps | grep -i frida").output() {
      let output_str = String::from_utf8_lossy(&output.stdout);
      if output_str.contains("frida") {
        eprintln!("Security: Frida detected in process list");
        return true;
      }
    }

    // Check for Frida library in memory maps
    if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
      if maps.contains("frida") || maps.contains("gum-js") {
        eprintln!("Security: Frida library detected in memory maps");
        return true;
      }
    }

    // Check for Frida environment variables
    if std::env::var("FRIDA_SERVER").is_ok() {
      eprintln!("Security: Frida environment variable detected");
      return true;
    }

    false
  }

  #[cfg(not(target_os = "android"))]
  pub fn is_frida_detected() -> bool {
    false
  }

  /// Perform all security checks
  pub fn perform_security_checks() -> Result<(), String> {
    if is_debugging_detected() {
      return Err("Debugging detected. Operation refused for security reasons.".to_string());
    }

    if is_frida_detected() {
      return Err("Frida or tampering detected. Operation refused for security reasons.".to_string());
    }

    Ok(())
  }
}
