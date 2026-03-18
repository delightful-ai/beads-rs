use std::path::Path;
use std::process::Child;
use std::time::Duration;

use beads_daemon::test_utils::poll_until_with_backoff as daemon_poll_until_with_backoff;

use super::timing;

const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_millis(100);

pub fn poll_until<F>(timeout: Duration, condition: F) -> bool
where
    F: FnMut() -> bool,
{
    poll_until_with_backoff(
        timeout,
        DEFAULT_INITIAL_BACKOFF,
        DEFAULT_MAX_BACKOFF,
        condition,
    )
}

pub fn poll_until_with_backoff<F>(
    timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    condition: F,
) -> bool
where
    F: FnMut() -> bool,
{
    daemon_poll_until_with_backoff(timeout, initial_backoff, max_backoff, condition)
}

pub fn poll_until_with_phase<F>(
    phase: &'static str,
    context: impl std::fmt::Display,
    timeout: Duration,
    condition: F,
) -> bool
where
    F: FnMut() -> bool,
{
    let _phase = timing::scoped_phase_with_context(phase, context);
    poll_until(timeout, condition)
}

pub fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    poll_until_with_phase("fixture.wait.path_exists", path.display(), timeout, || {
        path.exists()
    })
}

pub fn process_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    kill(Pid::from_raw(pid as i32), None).is_ok()
}

pub fn wait_for_process_exit(pid: u32, timeout: Duration) -> bool {
    poll_until_with_phase(
        "fixture.wait.process_exit",
        format!("pid={pid}"),
        timeout,
        || !process_alive(pid),
    )
}

pub fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let pid = child.id();
    let _phase =
        timing::scoped_phase_with_context("fixture.wait.process_exit", format!("pid={pid}"));
    let mut exited = false;
    let _ = poll_until(timeout, || {
        match child.try_wait().expect("poll child exit") {
            Some(_) => {
                exited = true;
                true
            }
            None => false,
        }
    });
    exited
}

pub fn kill_child_and_wait(child: &mut Child, timeout: Duration) -> bool {
    let pid = child.id();
    let _phase =
        timing::scoped_phase_with_context("fixture.wait.process_kill", format!("pid={pid}"));
    let _ = child.kill();
    wait_for_child_exit(child, timeout)
}

pub fn retry_with_backoff<T, E, F, R>(
    phase: &'static str,
    context: impl std::fmt::Display,
    timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    mut action: F,
    mut retryable: R,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    R: FnMut(&E) -> bool,
{
    let _phase = timing::scoped_phase_with_context(phase, context);
    let mut result = None;
    let mut last_retryable_error = None;
    let completed =
        poll_until_with_backoff(timeout, initial_backoff, max_backoff, || match action() {
            Ok(value) => {
                result = Some(Ok(value));
                true
            }
            Err(err) if retryable(&err) => {
                last_retryable_error = Some(err);
                false
            }
            Err(err) => {
                result = Some(Err(err));
                true
            }
        });
    if completed {
        return result.expect("completed wait should record a result");
    }
    Err(last_retryable_error.expect("timed out wait should end on a retryable error"))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    #[test]
    fn wait_for_child_exit_reaps_exited_child() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .expect("spawn test child");

        assert!(
            wait_for_child_exit(&mut child, Duration::from_secs(1)),
            "child should exit within timeout"
        );
        assert!(
            child.try_wait().expect("poll child after reap").is_some(),
            "wait_for_child_exit should reap the child process"
        );
    }

    #[test]
    fn kill_child_and_wait_reaps_running_child() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .expect("spawn test child");

        assert!(
            kill_child_and_wait(&mut child, Duration::from_secs(1)),
            "kill_child_and_wait should reap the child process"
        );
        assert!(
            child.try_wait().expect("poll child after kill").is_some(),
            "kill_child_and_wait should reap the child process"
        );
    }
}
