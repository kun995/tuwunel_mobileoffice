pub trait ExpectInto {
	#[inline]
	#[must_use]
	fn expect_into<Dst: TryFrom<Self>>(self) -> Dst
	where
		Self: Sized,
	{
		super::expect_into::<Dst, Self>(self)
	}
}

impl<T> ExpectInto for T {}
