#[must_use]
pub fn executor_name(base: &str) -> String {
    format!("{base}-side-effect-executor")
}

#[must_use]
pub fn loader_name(base: &str) -> String {
    format!("{base}-side-effect-loader")
}

#[must_use]
pub fn runner_name(base: &str) -> String {
    format!("{base}-side-effect-runner")
}
