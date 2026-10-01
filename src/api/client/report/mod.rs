mod report_event;
mod report_room;
mod report_user;

pub(crate) use self::{
	report_event::report_event_route, report_room::report_room_route,
	report_user::report_user_route,
};

pub(super) const REASON_MAX_LEN: usize = 2000;

/// Preserve the fork's 2–4 second response delay before looking up reported rooms/events.
async fn delay_response() {
	let seconds = rand::random_range(2..5);
	tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
}
