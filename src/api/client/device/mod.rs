mod delete_devices;
mod devices;
mod devices_device;

pub(crate) use self::{
	delete_devices::delete_devices_route,
	devices::get_devices_route,
	devices_device::{delete_device_route, get_device_route, update_device_route},
};
