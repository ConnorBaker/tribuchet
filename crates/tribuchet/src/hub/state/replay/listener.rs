//! A build's clients as the hub answers them: whether each one's own Nix
//! can build the key, and where in the log it was declined (SEMANTICS-7 L12).

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
        let (mut declined, mut waiting) = (Vec::new(), 0);
        for l in self.clients.lock().unwrap().iter().filter(|l| l.waits()) {
            if l.can_local {
                l.declined.get_or_init(|| at);
                declined.push(l.id);
            } else {
                waiting += 1;
            }
        }
        self.grew.notify_waiters();
        (declined, waiting)
    }
}
