#![cfg(test)]

use std::time::Duration;

use serde_json::json;
use tokio::time::timeout;
use tuwunel_core::{Result, err, ruma::{RoomId, UserId, events::room::{member::MembershipState, power_levels::UserPowerLevel}}};
use tuwunel_service::Services;

use self::{client::{Client, register}, fixture::boot};

mod client;
mod fixture;

const ADMIN_TOKEN: &str = "make-room-admin-admin-test-token";
const OWNER_TOKEN: &str = "make-room-admin-owner-test-token";
const TARGET_TOKEN: &str = "make-room-admin-target-test-token";

/// Granting power in a private room must finish even when the target needs
/// an invite, whose service acquires the same room state mutex itself.
#[test]
fn grants_without_relocking_during_invite() -> Result {
	boot("make-room-admin-lock", [
		"create_admin_room=true",
		"grant_admin_to_first_user=false",
		"auto_accept_invites=false",
		"log_enable=false",
	], exercise)
}

async fn exercise(services: &Services, base: &str) -> Result {
	let admin_id = register(services, "grantadmin", ADMIN_TOKEN).await?;
	register(services, "grantowner", OWNER_TOKEN).await?;
	let target = register(services, "granttarget", TARGET_TOKEN).await?;
	services.admin.make_user_admin(&admin_id).await?;

	let admin = Client { services, base, token: ADMIN_TOKEN };
	let owner = Client { services, base, token: OWNER_TOKEN };
	let target_client = Client { services, base, token: TARGET_TOKEN };
	let private = owner.create_room(&json!({"preset": "private_chat", "room_version": "11"})).await?;
	let public = owner.create_room(&json!({"preset": "public_chat", "room_version": "11"})).await?;

	grant(&admin, &private, &target).await?;
	assert!(services.state_cache.is_invited(&target, &private).await);
	assert_eq!(services.state_accessor.get_power_levels(&private).await?.for_user(&target), UserPowerLevel::Int(100.into()));

	// Already invited and already joined targets reuse the existing guard.
	grant(&admin, &private, &target).await?;
	target_client.post(&format!("rooms/{private}/join"), &json!({})).await?;
	grant(&admin, &private, &target).await?;
	assert!(services.state_cache.is_joined(&target, &private).await);

	// A public room grants power without adding an unnecessary invite.
	grant(&admin, &public, &target).await?;
	assert_eq!(services.state_cache.user_membership(&target, &public).await, None::<MembershipState>);
	assert_eq!(services.state_accessor.get_power_levels(&public).await?.for_user(&target), UserPowerLevel::Int(100.into()));
	Ok(())
}

async fn grant(admin: &Client<'_>, room_id: &RoomId, target: &UserId) -> Result {
	let url = format!("{}/_synapse/admin/v1/rooms/{room_id}/make_room_admin", admin.base);
	let response = timeout(Duration::from_secs(5), admin.post_url(&url, &json!({"user_id": target})))
		.await.map_err(|_| err!("make_room_admin timed out while inviting {target} in {room_id}"))??;
	let status = response.status().as_u16();
	let body = response.text().await?;
	assert_eq!(status, 200, "make_room_admin: {body}");
	Ok(())
}
