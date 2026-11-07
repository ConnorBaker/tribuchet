//! What the hub's model tests share (H1 loss ledger, H2 replay, H4
//! daemon): the system under test on a runtime of its own whose clock is
//! paused, stepped by a proptest-state-machine model.

use std::future::Future;
use std::time::Duration;

use futures_util::FutureExt;

/// A system under test and its runtime, whose clock is paused.
pub(in crate::hub) struct Paused<S> {
    pub(in crate::hub) rt: tokio::runtime::Runtime,
    pub(in crate::hub) sut: S,
}

impl<S> Paused<S> {
    pub(in crate::hub) fn new(make: impl Future<Output = S>) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let sut = rt.block_on(make);
        Self { rt, sut }
    }
}

/// `fut`'s output if it is done at its first poll: it waited on nothing.
/// Tokio's cooperative budget, a yield to the scheduler, is lifted.
pub(in crate::hub) fn at_once<F: Future>(fut: F) -> Option<F::Output> {
    tokio::task::unconstrained(fut).now_or_never()
}

/// Let every task go as far as it can: paused, the clock moves only once
/// none can.
pub(in crate::hub) async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}
