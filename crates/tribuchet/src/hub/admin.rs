//! The operator's commands, on the hub's local socket (proto HubAdmin):
//! retiring a worker for good (SEMANTICS-7 U4).

use std::path::Path;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::known::Forget;
use super::state::HubState;
use crate::errors::{Result, err_ctx};
use crate::proto::hub_admin_client::HubAdminClient;
use crate::proto::hub_admin_server::HubAdmin;
use crate::proto::{DecommissionReply, DecommissionRequest};
use crate::{attach, rt};

pub(super) struct AdminSvc {
    pub(super) state: Arc<HubState>,
}

#[tonic::async_trait]
impl HubAdmin for AdminSvc {
    /// The worker leaves the known set and is no longer awaited; the queue
    /// is answered anew (L11′). Whether it is connected is asked under the
    /// known set's lock, which a registration holds to connect, so a
    /// connected worker is never forgotten.
    async fn decommission(
        &self,
        request: Request<DecommissionRequest>,
    ) -> Result<Response<DecommissionReply>, Status> {
        let name = request.into_inner().worker_name;
        let state = &self.state;
        let connected = || (state.worker_caps.lock().unwrap().values()).any(|c| c.name == name);
        match state.known.forget(&name, connected) {
            Ok(()) => {}
            Err(Forget::Unknown) => {
                return Err(Status::not_found(format!("no worker {name} is known")));
            }
            Err(Forget::Connected) => {
                let err = format!("worker {name} is connected; stop it first");
                return Err(Status::failed_precondition(err));
            }
        }
        state
            .departed
            .lock()
            .unwrap()
            .retain(|(c, _)| c.name != name);
        state.regen_nix_config();
        state.shadow.decommissioned(&name);
        tracing::warn!(worker = name, "decommissioned");
        state.hold_unservable().await;
        Ok(Response::new(DecommissionReply {}))
    }
}

#[cfg(test)]
#[path = "admin/tests.rs"]
mod tests;

/// `tribuchet decommission`: ask the hub at `socket` to retire `worker`.
pub fn decommission(socket: &Path, worker: &str) -> Result<()> {
    let rt = rt::runtime("trib-admin").map_err(err_ctx("creating the tokio runtime"))?;
    rt.block_on(async {
        let mut client = HubAdminClient::new(attach::connect(socket).await?);
        let worker_name = worker.to_owned();
        let request = DecommissionRequest { worker_name };
        (client.decommission(request).await).map_err(err_ctx("decommissioning"))?;
        Ok(())
    })
}
