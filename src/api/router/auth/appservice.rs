use ruma::{OwnedDeviceId, OwnedUserId};
use tuwunel_core::{Err, Result, err};
use tuwunel_service::{Services, appservice::RegistrationInfo};

use super::{Auth, Request};

pub(super) async fn auth_appservice(
	services: &Services,
	request: &Request,
	info: Box<RegistrationInfo>,
) -> Result<Auth> {
	let user_id = request
		.query
		.user_id
		.as_deref()
		.map(OwnedUserId::parse)
		.transpose()
		.map_err(|_| err!(Request(InvalidUsername("Username is invalid."))))?
		.unwrap_or_else(|| info.sender.clone());

	if !info.is_user_match(&user_id) {
		return Err!(Request(Exclusive("User is not in namespace.")));
	}

	// MSC4326: appservices may assert a device_id alongside user_id.
	let sender_device = request.query.device_id().map(OwnedDeviceId::from);

	if let Some(device_id) = sender_device.as_deref()
		&& !services
			.users
			.device_exists(&user_id, device_id)
			.await
	{
		return Err!(Request(InvalidParam("Unknown device for user.")));
	}

	Ok(Auth {
		sender_user: Some(user_id),
		sender_device,
		appservice_info: Some(*info),
		..Auth::default()
	})
}
