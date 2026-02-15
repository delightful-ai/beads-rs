use std::process::{Command, Stdio};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

/// Configure daemon autostart spawn behavior.
///
/// This boundary owns Unix descriptor sanitization so IPC client logic can stay
/// free of ad-hoc `unsafe` process setup details.
pub(super) fn prepare_daemon_spawn_command(cmd: &mut Command) {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    #[cfg(unix)]
    install_fd_sanitizer(cmd);
}

#[cfg(unix)]
fn install_fd_sanitizer(cmd: &mut Command) {
    // Safety: `pre_exec` runs in the child process after `fork` and before
    // `exec`. The closure uses libc calls only and avoids heap allocation.
    unsafe {
        cmd.pre_exec(|| {
            reset_inherited_signal_handlers();
            close_non_stdio_fds();
            Ok(())
        });
    }
}

#[cfg(unix)]
fn reset_inherited_signal_handlers() {
    // Safety: `signal` is called with valid signal numbers and disposition constants.
    unsafe {
        nix::libc::signal(nix::libc::SIGTERM, nix::libc::SIG_DFL);
    }
}

#[cfg(unix)]
fn close_non_stdio_fds() {
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

#[cfg(all(test, unix))]
mod tests {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::process::Command;
    use std::time::Duration;

    use super::prepare_daemon_spawn_command;

    struct ChildGuard(std::process::Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
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

    #[test]
    fn spawn_command_closes_non_stdio_fds_in_child() {
        let (mut reader, writer) = UnixStream::pair().expect("unix pair");
        clear_cloexec(writer.as_raw_fd());

        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("sleep 5");
        prepare_daemon_spawn_command(&mut cmd);
        let child = cmd.spawn().expect("spawn child");
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
}
