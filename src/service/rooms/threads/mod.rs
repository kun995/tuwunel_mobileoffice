use std::{collections::BTreeMap, sync::Arc};

use futures::{Stream, StreamExt, TryFutureExt};
use ruma::{
	CanonicalJsonValue, EventId, OwnedUserId, RoomId, UserId,
	api::client::threads::get_threads::v1::IncludeThreads, events::relation::BundledThread, uint,
};
use serde_json::json;
use tuwunel_core::{
	Event, Result, err,
	matrix::pdu::{PduCount, PduEvent, PduId, RawPduId},
	trace,
	utils::{
		ReadyExt,
		stream::{TryIgnore, WidebandExt},
	},
};
use tuwunel_database::{Deserialized, Interfix, Map};

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
}

pub(super) struct Data {
	threadid_userids: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				threadid_userids: args.db["threadid_userids"].clone(),
			},
			services: args.services.clone(),
		}))
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

impl Service {
	pub async fn add_to_thread<E>(&self, root_event_id: &EventId, event: &E) -> Result
	where
		E: Event,
	{
		let root_id = self
			.services
			.timeline
			.get_pdu_id(root_event_id)
			.await
			.map_err(|e| {
				err!(Request(InvalidParam("Invalid event_id in thread message: {e:?}")))
			})?;

		let root_pdu = self
			.services
			.timeline
			.get_pdu_from_id(&root_id)
			.await
			.map_err(|e| err!(Request(InvalidParam("Thread root not found: {e:?}"))))?;

		let mut root_pdu_json = self
			.services
			.timeline
			.get_pdu_json_from_id(&root_id)
			.await
			.map_err(|e| err!(Request(InvalidParam("Thread root pdu not found: {e:?}"))))?;

		if let CanonicalJsonValue::Object(unsigned) = root_pdu_json
			.entry("unsigned".into())
			.or_insert_with(|| CanonicalJsonValue::Object(BTreeMap::default()))
		{
			if let Some(mut relations) = unsigned
				.get("m.relations")
				.and_then(|r| r.as_object())
				.and_then(|r| r.get("m.thread"))
				.and_then(|relations| {
					serde_json::from_value::<BundledThread>(relations.clone().into()).ok()
				}) {
				// Thread already existed
				relations.count = relations.count.saturating_add(uint!(1));
				relations.latest_event = event.to_format();

				let content = serde_json::to_value(relations).expect("to_value always works");

				unsigned.insert(
					"m.relations".into(),
					json!({ "m.thread": content })
						.try_into()
						.expect("thread is valid json"),
				);
			} else {
				// New thread
				let relations = BundledThread {
					latest_event: event.to_format(),
					count: uint!(1),
					current_user_participated: true,
				};

				let content = serde_json::to_value(relations).expect("to_value always works");

				unsigned.insert(
					"m.relations".into(),
					json!({ "m.thread": content })
						.try_into()
						.expect("thread is valid json"),
				);
			}

			self.services
				.timeline
				.replace_pdu(&root_id, &root_pdu_json)
				.await?;
		}

		let mut users = Vec::new();
		match self.get_participants(&root_id).await {
			| Ok(userids) => users.extend_from_slice(&userids),
			| _ => users.push(root_pdu.sender().to_owned()),
		}

		users.push(event.sender().to_owned());
		self.update_participants(&root_id, &users)
	}

	pub fn threads_until<'a>(
		&'a self,
		user_id: &'a UserId,
		room_id: &'a RoomId,
		count: PduCount,
		_inc: &'a IncludeThreads,
	) -> impl Stream<Item = Result<(PduCount, PduEvent)>> + Send {
		self.services
			.short
			.get_shortroomid(room_id)
			.map_ok(move |shortroomid| PduId {
				shortroomid,
				count: count.saturating_sub(1),
			})
			.map_ok(Into::into)
			.map_ok(move |current: RawPduId| {
				self.db
					.threadid_userids
					.rev_raw_keys_from(&current)
					.ignore_err()
					.map(RawPduId::from)
					.map(move |pdu_id| (pdu_id, user_id))
					.ready_take_while(move |(pdu_id, _)| {
						pdu_id.shortroomid() == current.shortroomid()
					})
					.wide_filter_map(async |(raw_pdu_id, user_id)| {
						let pdu_id: PduId = raw_pdu_id.into();
						let mut pdu = self
							.services
							.timeline
							.get_pdu_from_id(&raw_pdu_id)
							.await
							.ok()?;

						if pdu.sender() != user_id {
							pdu.as_mut_pdu().remove_transaction_id().ok();
						}

						Some((pdu_id.count, pdu))
					})
					.map(Ok)
			})
			.try_flatten_stream()
	}

	pub(super) fn update_participants(
		&self,
		root_id: &RawPduId,
		participants: &[OwnedUserId],
	) -> Result {
		let users = participants
			.iter()
			.map(|user| user.as_bytes())
			.collect::<Vec<_>>()
			.join(&[0xFF][..]);

		self.db.threadid_userids.insert(root_id, &users);

		Ok(())
	}

	pub(super) async fn get_participants(&self, root_id: &RawPduId) -> Result<Vec<OwnedUserId>> {
		self.db
			.threadid_userids
			.get(root_id)
			.await
			.deserialized()
	}

	pub(super) async fn delete_all_rooms_threads(&self, room_id: &RoomId) -> Result {
		let prefix = (room_id, Interfix);

		self.db
			.threadid_userids
			.keys_prefix_raw(&prefix)
			.ignore_err()
			.ready_for_each(|key| {
				trace!("Removing key: {key:?}");
				self.db.threadid_userids.remove(key);
			})
			.await;

		Ok(())
	}
}
