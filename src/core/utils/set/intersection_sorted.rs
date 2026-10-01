use crate::{is_equal_to, is_less_than};

/// Intersection of sets
///
/// Outputs the set of elements common to all input sets. Inputs must be sorted.
pub fn intersection_sorted<Item, Iter, Iters>(
	mut input: Iters,
) -> impl Iterator<Item = Item> + Send
where
	Iters: Iterator<Item = Iter> + Clone + Send,
	Iter: Iterator<Item = Item> + Send,
	Item: Eq + Ord,
{
	input.next().into_iter().flat_map(move |first| {
		let mut input = input.clone().collect::<Vec<_>>();
		first.filter(move |targ| {
			input.iter_mut().all(|it| {
				it.by_ref()
					.skip_while(is_less_than!(targ))
					.peekable()
					.peek()
					.is_some_and(is_equal_to!(targ))
			})
		})
	})
}
