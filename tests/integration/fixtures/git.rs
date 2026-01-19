use std::process::Command;

pub fn apply_test_git_env(cmd: &mut Command) {
    cmd.env("GIT_AUTHOR_NAME", "Test");
    cmd.env("GIT_AUTHOR_EMAIL", "test@test.com");
    cmd.env("GIT_COMMITTER_NAME", "Test");
    cmd.env("GIT_COMMITTER_EMAIL", "test@test.com");
}
