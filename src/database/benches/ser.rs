#![allow(unused_features)] // 1.96.0-nightly 2026-03-07 bug

use criterion::{Criterion, criterion_group, criterion_main};

criterion_group!(benches, ser_str);

criterion_main!(benches);

fn ser_str(b: &mut Criterion) {
	b.bench_function("ser_str", |c| {
		use tuwunel_core::ruma::{RoomId, UserId};
		use tuwunel_database::serialize_to_vec;

		let user_id: &UserId = "@user:example.com".try_into().unwrap();
		let room_id: &RoomId = "!room:example.com".try_into().unwrap();
		c.iter(|| {
			let key = (user_id, room_id);
			let _s = serialize_to_vec(key).expect("failed to serialize user_id");
		});
	});
}
