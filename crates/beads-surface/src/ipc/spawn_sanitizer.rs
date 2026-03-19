use std::io;
use std::path::Path;
use std::process::ExitStatus;

#[cfg(target_vendor = "apple")]
use std::ffi::CString;
#[cfg(target_vendor = "apple")]
use std::os::unix::ffi::OsStrExt;
#[cfg(all(unix, not(target_vendor = "apple")))]
use std::os::unix::process::CommandExt;
#[cfg(target_vendor = "apple")]
use std::os::unix::process::ExitStatusExt;
#[cfg(all(unix, not(target_vendor = "apple")))]
use std::process::{Command, Stdio};

/// Configure daemon autostart spawn behavior.
///
/// This boundary owns Unix descriptor sanitization so IPC client logic can stay
/// free of ad-hoc `unsafe` process setup details.
pub(super) fn spawn_daemon_process(
    program: &Path,
    args: &[std::ffi::OsString],
) -> io::Result<SpawnedDaemon> {
    #[cfg(target_vendor = "apple")]
    {
        spawn_daemon_process_apple(program, args)
    }

    #[cfg(not(target_vendor = "apple"))]
    {
        let mut cmd = Command::new(program);
        cmd.args(args);
        prepare_daemon_spawn_command(&mut cmd);
        return cmd.spawn().map(SpawnedDaemon::Child);
    }
}

pub(super) enum SpawnedDaemon {
    #[cfg(not(target_vendor = "apple"))]
    Child(std::process::Child),
    #[cfg(target_vendor = "apple")]
    Pid(AppleSpawnedDaemon),
}

impl SpawnedDaemon {
    pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match self {
            #[cfg(not(target_vendor = "apple"))]
            Self::Child(child) => child.try_wait(),
            #[cfg(target_vendor = "apple")]
            Self::Pid(pid) => pid.try_wait(),
        }
    }
}

#[cfg(target_vendor = "apple")]
pub(super) struct AppleSpawnedDaemon {
    pid: nix::libc::pid_t,
    cached_status: Option<i32>,
}

#[cfg(target_vendor = "apple")]
impl AppleSpawnedDaemon {
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.cached_status {
            return Ok(Some(ExitStatus::from_raw(status)));
        }

        let mut status = 0;
        let rc = unsafe { nix::libc::waitpid(self.pid, &mut status, nix::libc::WNOHANG) };
        if rc == 0 {
            return Ok(None);
        }
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        self.cached_status = Some(status);
        Ok(Some(ExitStatus::from_raw(status)))
    }
}

#[cfg(all(test, unix, not(target_vendor = "apple")))]
impl SpawnedDaemon {
    fn terminate_for_test(&mut self) {
        if let Self::Child(child) = self {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(all(test, unix, target_vendor = "apple"))]
impl SpawnedDaemon {
    fn terminate_for_test(&mut self) {
        let Self::Pid(pid) = self;
        if pid.try_wait().ok().flatten().is_some() {
            return;
        }

        // Safety: `kill` and `waitpid` operate on the spawned child pid.
        unsafe {
            let _ = nix::libc::kill(pid.pid, nix::libc::SIGKILL);
        }

        let mut status = 0;
        // Safety: `waitpid` reaps the child we just signaled if it is still live.
        let rc = unsafe { nix::libc::waitpid(pid.pid, &mut status, 0) };
        if rc > 0 {
            pid.cached_status = Some(status);
        }
    }
}

#[cfg(not(target_vendor = "apple"))]
fn prepare_daemon_spawn_command(cmd: &mut Command) {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    install_fd_sanitizer(cmd);
}

#[cfg(all(unix, not(target_vendor = "apple")))]
fn install_fd_sanitizer(cmd: &mut Command) {
    // Safety: `pre_exec` runs in the child process after `fork` and before
    // `exec`. The closure uses libc calls only and avoids heap allocation.
    unsafe {
        cmd.pre_exec(move || {
            reset_inherited_signal_handlers();
            close_non_stdio_fds();
            Ok(())
        });
    }
}

#[cfg(all(unix, not(target_vendor = "apple")))]
fn reset_inherited_signal_handlers() {
    // Safety: `signal` is called with valid signal numbers and disposition constants.
    unsafe {
        nix::libc::signal(nix::libc::SIGTERM, nix::libc::SIG_DFL);
    }
}

#[cfg(all(unix, not(target_vendor = "apple")))]
fn close_non_stdio_fds() {
    if try_close_non_stdio_fds_bounded() {
        return;
    }

    let max_fd = unsafe { nix::libc::sysconf(nix::libc::_SC_OPEN_MAX) };
    let upper = if max_fd > 0 && max_fd <= i32::MAX as i64 {
        max_fd as i32
    } else {
        1024
    };

    for fd in 3..upper {
        unsafe {
            nix::libc::close(fd);
        }
    }
}

#[cfg(all(
    not(target_vendor = "apple"),
    any(target_os = "linux", target_os = "android")
))]
fn try_close_non_stdio_fds_bounded() -> bool {
    let rc = unsafe {
        nix::libc::syscall(
            nix::libc::SYS_close_range,
            3 as nix::libc::c_long,
            u32::MAX as nix::libc::c_long,
            0 as nix::libc::c_long,
        )
    };
    rc == 0
}

#[cfg(all(
    not(target_vendor = "apple"),
    not(any(target_os = "linux", target_os = "android"))
))]
fn try_close_non_stdio_fds_bounded() -> bool {
    false
}

#[cfg(target_vendor = "apple")]
fn spawn_daemon_process_apple(
    program: &Path,
    args: &[std::ffi::OsString],
) -> io::Result<SpawnedDaemon> {
    let program_cstr = os_str_to_cstring(program.as_os_str())?;
    let argv = argv_for_spawn(program, args)?;
    let env = envp_for_spawn()?;
    let mut argv_ptrs: Vec<*mut nix::libc::c_char> =
        argv.iter().map(|arg| arg.as_ptr().cast_mut()).collect();
    let mut env_ptrs: Vec<*mut nix::libc::c_char> =
        env.iter().map(|entry| entry.as_ptr().cast_mut()).collect();
    argv_ptrs.push(std::ptr::null_mut());
    env_ptrs.push(std::ptr::null_mut());

    let mut file_actions = std::ptr::null_mut();
    cvt_spawn(unsafe { nix::libc::posix_spawn_file_actions_init(&mut file_actions) })?;
    let mut file_actions = PosixSpawnFileActions(file_actions);
    add_devnull_stdio(&mut file_actions)?;

    let mut attrs = std::ptr::null_mut();
    cvt_spawn(unsafe { nix::libc::posix_spawnattr_init(&mut attrs) })?;
    let mut attrs = PosixSpawnAttrs(attrs);
    configure_spawn_attrs(&mut attrs)?;

    let mut pid = 0;
    let spawn_rc = unsafe {
        if program.as_os_str().as_bytes().contains(&b'/') {
            nix::libc::posix_spawn(
                &mut pid,
                program_cstr.as_ptr(),
                &file_actions.0,
                &attrs.0,
                argv_ptrs.as_ptr(),
                env_ptrs.as_ptr(),
            )
        } else {
            nix::libc::posix_spawnp(
                &mut pid,
                program_cstr.as_ptr(),
                &file_actions.0,
                &attrs.0,
                argv_ptrs.as_ptr(),
                env_ptrs.as_ptr(),
            )
        }
    };
    cvt_spawn(spawn_rc)?;

    Ok(SpawnedDaemon::Pid(AppleSpawnedDaemon {
        pid,
        cached_status: None,
    }))
}

#[cfg(target_vendor = "apple")]
struct PosixSpawnFileActions(nix::libc::posix_spawn_file_actions_t);

#[cfg(target_vendor = "apple")]
impl Drop for PosixSpawnFileActions {
    fn drop(&mut self) {
        unsafe {
            let _ = nix::libc::posix_spawn_file_actions_destroy(&mut self.0);
        }
    }
}

#[cfg(target_vendor = "apple")]
struct PosixSpawnAttrs(nix::libc::posix_spawnattr_t);

#[cfg(target_vendor = "apple")]
impl Drop for PosixSpawnAttrs {
    fn drop(&mut self) {
        unsafe {
            let _ = nix::libc::posix_spawnattr_destroy(&mut self.0);
        }
    }
}

#[cfg(target_vendor = "apple")]
fn add_devnull_stdio(file_actions: &mut PosixSpawnFileActions) -> io::Result<()> {
    let devnull = CString::new("/dev/null").expect("static path has no NUL");
    cvt_spawn(unsafe {
        nix::libc::posix_spawn_file_actions_addopen(
            &mut file_actions.0,
            nix::libc::STDIN_FILENO,
            devnull.as_ptr(),
            nix::libc::O_RDONLY,
            0,
        )
    })?;
    for fd in [nix::libc::STDOUT_FILENO, nix::libc::STDERR_FILENO] {
        cvt_spawn(unsafe {
            nix::libc::posix_spawn_file_actions_addopen(
                &mut file_actions.0,
                fd,
                devnull.as_ptr(),
                nix::libc::O_WRONLY,
                0,
            )
        })?;
    }
    Ok(())
}

#[cfg(target_vendor = "apple")]
fn configure_spawn_attrs(attrs: &mut PosixSpawnAttrs) -> io::Result<()> {
    let flags = (nix::libc::POSIX_SPAWN_CLOEXEC_DEFAULT | nix::libc::POSIX_SPAWN_SETSIGDEF) as i16;
    cvt_spawn(unsafe { nix::libc::posix_spawnattr_setflags(&mut attrs.0, flags) })?;

    let mut defaults = unsafe { std::mem::zeroed::<nix::libc::sigset_t>() };
    let empty_rc = unsafe { nix::libc::sigemptyset(&mut defaults) };
    if empty_rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let add_rc = unsafe { nix::libc::sigaddset(&mut defaults, nix::libc::SIGTERM) };
    if add_rc != 0 {
        return Err(io::Error::last_os_error());
    }
    cvt_spawn(unsafe { nix::libc::posix_spawnattr_setsigdefault(&mut attrs.0, &defaults) })?;
    Ok(())
}

#[cfg(target_vendor = "apple")]
fn argv_for_spawn(program: &Path, args: &[std::ffi::OsString]) -> io::Result<Vec<CString>> {
    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push(os_str_to_cstring(program.as_os_str())?);
    for arg in args {
        argv.push(os_str_to_cstring(arg.as_os_str())?);
    }
    Ok(argv)
}

#[cfg(target_vendor = "apple")]
fn envp_for_spawn() -> io::Result<Vec<CString>> {
    let mut env = Vec::new();
    for (key, value) in std::env::vars_os() {
        let mut bytes = key.as_os_str().as_bytes().to_vec();
        bytes.push(b'=');
        bytes.extend_from_slice(value.as_os_str().as_bytes());
        env.push(CString::new(bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "environment entry contains interior NUL",
            )
        })?);
    }
    Ok(env)
}

#[cfg(target_vendor = "apple")]
fn os_str_to_cstring(value: &std::ffi::OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "spawn path or argument contains interior NUL",
        )
    })
}

#[cfg(target_vendor = "apple")]
fn cvt_spawn(rc: nix::libc::c_int) -> io::Result<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(rc))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::Read;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::net::UnixStream;
    use std::path::Path;
    use std::time::Duration;

    use super::spawn_daemon_process;

    struct ChildGuard(super::SpawnedDaemon);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            self.0.terminate_for_test();
        }
    }

    fn clear_cloexec(fd: i32) {
        // Safety: `fcntl` is called with valid command constants and a live fd.
        let flags = unsafe { nix::libc::fcntl(fd, nix::libc::F_GETFD) };
        assert!(flags >= 0, "F_GETFD failed for fd {fd}");
        let new_flags = flags & !nix::libc::FD_CLOEXEC;
        // Safety: `fcntl` is called with valid command constants and bitflags.
        let rc = unsafe { nix::libc::fcntl(fd, nix::libc::F_SETFD, new_flags) };
        assert_eq!(rc, 0, "F_SETFD failed for fd {fd}");
    }

    fn wait_readable(fd: i32, timeout: Duration) -> bool {
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut poll_fd = nix::libc::pollfd {
            fd,
            events: nix::libc::POLLIN | nix::libc::POLLHUP,
            revents: 0,
        };
        // Safety: pointer references a valid pollfd for exactly one entry.
        let rc = unsafe { nix::libc::poll(&mut poll_fd as *mut nix::libc::pollfd, 1, timeout_ms) };
        assert!(rc >= 0, "poll failed for fd {fd}");
        rc > 0
    }

    fn dup_to_min_fd(fd: i32, min_fd: i32) -> OwnedFd {
        // Safety: `fcntl` is called with a live fd and valid command arguments.
        let dup_fd = unsafe { nix::libc::fcntl(fd, nix::libc::F_DUPFD, min_fd) };
        assert!(dup_fd >= min_fd, "F_DUPFD failed for fd {fd}");
        // Safety: `dup_fd` is a fresh descriptor returned by `fcntl`.
        unsafe { OwnedFd::from_raw_fd(dup_fd) }
    }

    #[test]
    fn spawn_command_closes_non_stdio_fds_in_child() {
        let (mut reader, writer) = UnixStream::pair().expect("unix pair");
        clear_cloexec(writer.as_raw_fd());

        let child = spawn_daemon_process(
            Path::new("/bin/sh"),
            &[
                std::ffi::OsString::from("-c"),
                std::ffi::OsString::from("sleep 5"),
            ],
        )
        .expect("spawn child");
        let _child_guard = ChildGuard(child);

        drop(writer);

        assert!(
            wait_readable(reader.as_raw_fd(), Duration::from_secs(2)),
            "timed out waiting for EOF; leaked fd in child"
        );
        let mut buf = [0_u8; 1];
        let bytes = reader.read(&mut buf).unwrap_or_else(|err| {
            panic!("expected immediate EOF after parent close; leaked fd in child: {err}")
        });
        assert_eq!(bytes, 0, "expected EOF, child still holds inherited fd");
    }

    #[test]
    fn spawn_command_closes_sparse_high_fd_in_child() {
        let (mut reader, writer) = UnixStream::pair().expect("unix pair");
        let sparse_fd = dup_to_min_fd(writer.as_raw_fd(), 4096);
        clear_cloexec(sparse_fd.as_raw_fd());

        let child = spawn_daemon_process(
            Path::new("/bin/sh"),
            &[
                std::ffi::OsString::from("-c"),
                std::ffi::OsString::from("sleep 5"),
            ],
        )
        .expect("spawn child");
        let _child_guard = ChildGuard(child);

        drop(sparse_fd);
        drop(writer);

        assert!(
            wait_readable(reader.as_raw_fd(), Duration::from_secs(2)),
            "timed out waiting for EOF on sparse fd; leaked fd in child"
        );
        let mut buf = [0_u8; 1];
        let bytes = reader.read(&mut buf).unwrap_or_else(|err| {
            panic!("expected EOF after sparse parent close; leaked fd in child: {err}")
        });
        assert_eq!(
            bytes, 0,
            "expected EOF, child still holds sparse inherited fd"
        );
    }
}
