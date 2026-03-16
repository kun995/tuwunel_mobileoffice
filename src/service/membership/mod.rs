mod ban;
mod invite;
mod join;
mod kick;
mod knock;
mod leave;
mod unban;

use std::{
	collections::HashMap,
	sync::Arc,
	time::{Duration, Instant},
};

use ruma::{OwnedRoomId, OwnedUserId};
use tokio::sync::Mutex;
use tuwunel_core::Result;

pub struct Service {
	services: Arc<crate::services::OnceServices>,
	invite_batches: Mutex<HashMap<(OwnedUserId, OwnedRoomId), InviteBatchState>>,
}

#[derive(Clone)]
struct InviteBatchState {
	batch_id: String,
	updated_at: Instant,
}

const AUTO_INVITE_BATCH_WINDOW: Duration = Duration::from_secs(3);

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			invite_batches: Mutex::new(HashMap::new()),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}
