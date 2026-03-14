use std::path::Path;
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
