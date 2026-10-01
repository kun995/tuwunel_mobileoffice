use std::{
	cmp::Ordering::{Equal, Greater, Less},
	pin::Pin,
	task::ready,
};

use futures::{
	Stream, StreamExt,
	stream::{Fuse, FusedStream},
	task::{Context, Poll},
};
use pin_project_lite::pin_project;

use super::poll_head;
use crate::ready_some;

pin_project! {
	struct DifferenceSortedStream2<A, B, Item> {
		#[pin] a: Fuse<A>,
		#[pin] b: Fuse<B>,
		peeked_a: Option<Item>,
		peeked_b: Option<Item>,
	}
}

/// Difference of sets
///
/// Outputs the set of elements found in `a` which are not found in `b`. Both
/// streams must be ascending under a total order; multiplicity is preserved
/// (a value with n copies in `a` and m copies in `b` appears `max(n - m, 0)`
/// times in the output).
pub fn difference_sorted_stream2<Item, A, B>(a: A, b: B) -> impl Stream<Item = Item> + Send
where
	A: Stream<Item = Item> + Send,
	B: Stream<Item = Item> + Send,
	Item: Ord + Send + Sync,
{
	DifferenceSortedStream2 {
		a: a.fuse(),
		b: b.fuse(),
		peeked_a: None,
		peeked_b: None,
	}
}

impl<A, B, Item> Stream for DifferenceSortedStream2<A, B, Item>
where
	A: Stream<Item = Item>,
	B: Stream<Item = Item>,
	Item: Ord,
{
	type Item = Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();
		loop {
			let a = ready_some!(poll_head(this.a.as_mut(), this.peeked_a, cx));
			let Some(b) = ready!(poll_head(this.b.as_mut(), this.peeked_b, cx)) else {
				return Poll::Ready(this.peeked_a.take());
			};

			match a.cmp(b) {
				| Less => return Poll::Ready(this.peeked_a.take()),
				| Greater => *this.peeked_b = None,
				| Equal => {
					*this.peeked_a = None;
					*this.peeked_b = None;
				},
			}
		}
	}
}

impl<A, B, Item> FusedStream for DifferenceSortedStream2<A, B, Item>
where
	A: Stream<Item = Item>,
	B: Stream<Item = Item>,
	Item: Ord,
{
	fn is_terminated(&self) -> bool { self.peeked_a.is_none() && self.a.is_terminated() }
}
