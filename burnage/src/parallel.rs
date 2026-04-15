// Minimal scatter-gather over std::thread. Used by `burnage quota` to fire
// the ~12 independent HTTP calls concurrently — `ureq` is blocking, so this
// just spawns one OS thread per task and joins them in-order. No tokio.

use std::thread;

/// A boxed task usable as input to `scatter`. Each task is a move closure
/// that captures whatever it needs and returns `anyhow::Result<T>`. Boxing
/// is the price of a heterogeneous task vec — fine at N=12.
pub type Task<T> = Box<dyn FnOnce() -> anyhow::Result<T> + Send>;

/// Run `tasks` in parallel, collect their results in submission order.
/// A panic in a worker thread becomes an `Err` so callers never see
/// `Result<Result<T, E>, PanicPayload>`.
pub fn scatter<T: Send + 'static>(tasks: Vec<Task<T>>) -> Vec<anyhow::Result<T>> {
    let handles: Vec<thread::JoinHandle<anyhow::Result<T>>> =
        tasks.into_iter().map(thread::spawn).collect();
    handles
        .into_iter()
        .map(|h| match h.join() {
            Ok(r) => r,
            Err(_) => Err(anyhow::anyhow!("worker thread panicked")),
        })
        .collect()
}
