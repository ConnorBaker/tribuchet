//! A build's clients as the hub answers them: whether each one's own Nix
//! can build the key, and where in the log it was declined (SEMANTICS-7 L12).

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use tokio::sync::mpsc;
use tonic::Status;

use super::Replay;
use crate::hub::semantics as sem;
use crate::hub::shadow::Client;
use crate::proto::AttachEvent;

/// One client of the build, as the hub answers it.
pub(super) struct Listener {
    tx: mpsc::WeakSender<Result<AttachEvent, Status>>,
    /// Its own Nix can build the key (canLocal).
    can_local: bool,
    /// Its number in the shadow's log (Client::id).
    id: sem::Client,
    /// Where in the log it was declined (Defer), once it was.
    declined: Arc<OnceLock<usize>>,
}

impl Listener {
    /// `client`, reading from `tx`, and the cell its reader learns its
    /// decline from.
    pub(super) fn new(
        tx: &mpsc::Sender<Result<AttachEvent, Status>>,
        client: &Client,
    ) -> (Self, Arc<OnceLock<usize>>) {
        let declined = Arc::new(OnceLock::new());
        let listener = Self {
            tx: tx.downgrade(),
            can_local: client.can_local,
            id: client.id,
            declined: declined.clone(),
        };
        (listener, declined)
    }

    /// Still reading, and not declined.
    fn waits(&self) -> bool {
        self.declined.get().is_none() && self.tx.upgrade().is_some_and(|tx| !tx.is_closed())
    }
}

impl Replay {
    /// Whether each client still waiting can build the key itself.
    pub(in crate::hub) fn can_local(&self) -> Vec<bool> {
        let clients = self.clients.lock().unwrap();
        clients
            .iter()
            .filter(|l| l.waits())
            .map(|l| l.can_local)
            .collect()
    }

    /// Decline each client still waiting whose own Nix can build the key
    /// (Defer), after all the log holds now: those clients' numbers, and
    /// how many others still wait.
    pub(in crate::hub) async fn decline_local(&self) -> (Vec<sem::Client>, usize) {
        let at = {
            let inner = self.inner.lock().await;
            inner.base + inner.events.len()
        };
        let answered = decline(&self.clients.lock().unwrap(), true, at);
        self.grew.notify_waiters();
        answered
    }

    /// Clients that joined so far: what `hold` is to be given.
    pub(in crate::hub) fn joins(&self) -> usize {
        let _clients = self.clients.lock().unwrap();
        self.joins.load(Ordering::Relaxed)
    }

    /// Hold the build out of placement until a client joins, unless one
    /// joined since `joins` said `since`: whether it is held.
    pub(in crate::hub) fn hold(&self, since: usize) -> bool {
        let _clients = self.clients.lock().unwrap();
        let held = self.joins.load(Ordering::Relaxed) == since;
        self.held.store(held, Ordering::Relaxed);
        held
    }

    /// Held, with a client still waiting: a take passes it over. One with
    /// none is taken, to be ended as any build whose clients left.
    pub(in crate::hub) fn held(&self) -> bool {
        let clients = self.clients.lock().unwrap();
        self.held.load(Ordering::Relaxed) && clients.iter().any(Listener::waits)
    }

    /// Decide the build's answer at this instant (SEMANTICS-7 cut-over 10):
    /// no client joins after it; each still waiting whose own Nix can build
    /// the key is declined after all the log holds now, if `local`; and
    /// `decided` is told those declined and how many others wait, all under
    /// the clients' lock, so that none joins between.
    pub(in crate::hub) fn close(
        &self,
        local: bool,
        decided: impl FnOnce(&[sem::Client], usize),
    ) -> (Vec<sem::Client>, usize) {
        let at = self.published.load(Ordering::Acquire);
        let clients = self.clients.lock().unwrap();
        self.closed.store(true, Ordering::Relaxed);
        let (declined, waiting) = decline(&clients, local, at);
        self.grew.notify_waiters();
        decided(&declined, waiting);
        (declined, waiting)
    }
}

/// Decline at `at` each of `clients` still waiting whose own Nix can build
/// the key, if `local`: their numbers, and how many others still wait.
fn decline(clients: &[Listener], local: bool, at: usize) -> (Vec<sem::Client>, usize) {
    let (mut declined, mut waiting) = (Vec::new(), 0);
    for l in clients.iter().filter(|l| l.waits()) {
        if local && l.can_local {
            l.declined.get_or_init(|| at);
            declined.push(l.id);
        } else {
            waiting += 1;
        }
    }
    (declined, waiting)
}
