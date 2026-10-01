mod change_password;
mod deactivate;
#[path = "3pid/mod.rs"]
mod threepid;
mod whoami;

pub(crate) use self::{
	change_password::change_password_route, deactivate::deactivate_route, threepid::*,
	whoami::whoami_route,
};
