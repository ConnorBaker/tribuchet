//! Shared tokio runtime construction and thread naming, so a profile or
//! `top -H` can tell which role (and which kind of work) a thread runs.

#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::io;

#[cfg(target_os = "linux")]
use rustix::thread::set_name;

/// A multi-threaded runtime whose worker and blocking-pool threads carry
/// `name`. Mirrors the defaults of `Runtime::new` otherwise.
pub fn runtime(name: &'static str) -> io::Result<tokio::runtime::Runtime> {
    builder().thread_name(name).build()
}

/// Eager driver hand-off: a worker woken by the I/O or timer driver wakes
/// another to take the driver over before it polls a task, so one
/// blocked task no longer stops every timer and socket (crate::stalltest).
pub(crate) fn builder() -> tokio::runtime::Builder {
    let mut b = tokio::runtime::Builder::new_multi_thread();
    b.enable_all().enable_eager_driver_handoff();
    b
}

/// Rename the calling OS thread (Linux caps the name at 15 bytes), to tag
/// blocking-pool threads that the runtime otherwise names uniformly.
pub fn name_current_thread(name: &str) {
    #[cfg(target_os = "linux")]
    if let Ok(c) = CString::new(name) {
        let _ = set_name(&c);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = name;
}
