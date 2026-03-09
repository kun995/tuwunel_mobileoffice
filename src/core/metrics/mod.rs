use std::sync::atomic::{AtomicU32, AtomicU64};

use tokio::runtime;
#[cfg(tokio_unstable)]
use tokio_metrics::{RuntimeIntervals, RuntimeMonitor};
use tokio_metrics::{TaskMetrics, TaskMonitor};

pub struct Metrics {
	_runtime: Option<runtime::Handle>,

	runtime_metrics: Option<runtime::RuntimeMetrics>,

	task_monitor: Option<TaskMonitor>,

	task_intervals: std::sync::Mutex<Option<Box<dyn Iterator<Item = TaskMetrics> + Send>>>,

	#[cfg(tokio_unstable)]
	_runtime_monitor: Option<RuntimeMonitor>,

	#[cfg(tokio_unstable)]
	runtime_intervals: std::sync::Mutex<Option<RuntimeIntervals>>,

	// TODO: move stats
	pub requests_count: AtomicU64,
	pub requests_handle_finished: AtomicU64,
	pub requests_handle_active: AtomicU32,
	pub requests_panic: AtomicU32,
}

impl Metrics {
	#[must_use]
	pub fn new(runtime: Option<&runtime::Handle>) -> Self {
		#[cfg(tokio_unstable)]
		let runtime_monitor = runtime.map(RuntimeMonitor::new);

		#[cfg(tokio_unstable)]
		let runtime_intervals = runtime_monitor
			.as_ref()
			.map(RuntimeMonitor::intervals);

		let task_monitor = cfg!(tokio_unstable).then(|| {
			TaskMonitor::builder()
				.with_slow_poll_threshold(TaskMonitor::DEFAULT_SLOW_POLL_THRESHOLD)
				.with_long_delay_threshold(TaskMonitor::DEFAULT_LONG_DELAY_THRESHOLD)
				.clone()
				.build()
		});

		let task_intervals = task_monitor.as_ref().map(
			|task_monitor| -> Box<dyn Iterator<Item = TaskMetrics> + Send> {
				Box::new(task_monitor.intervals())
			},
		);

		Self {
			_runtime: runtime.cloned(),

			runtime_metrics: runtime.map(runtime::Handle::metrics),

			task_monitor,

			task_intervals: task_intervals.into(),

			#[cfg(tokio_unstable)]
			_runtime_monitor: runtime_monitor,

			#[cfg(tokio_unstable)]
			runtime_intervals: std::sync::Mutex::new(runtime_intervals),

			requests_count: AtomicU64::new(0),
			requests_handle_finished: AtomicU64::new(0),
			requests_handle_active: AtomicU32::new(0),
			requests_panic: AtomicU32::new(0),
		}
	}

	#[inline]
	pub async fn instrument<F, Output>(&self, f: F) -> Output
	where
		F: Future<Output = Output>,
	{
		if let Some(monitor) = self.task_metrics() {
			monitor.instrument(f).await
		} else {
			f.await
		}
	}

	pub fn task_interval(&self) -> Option<TaskMetrics> {
		self.task_intervals
			.lock()
			.expect("locked")
			.as_mut()
			.and_then(Iterator::next)
	}

	#[cfg(tokio_unstable)]
	pub fn runtime_interval(&self) -> Option<tokio_metrics::RuntimeMetrics> {
		self.runtime_intervals
			.lock()
			.expect("locked")
			.as_mut()
			.map(Iterator::next)
			.expect("next interval")
	}

	#[inline]
	pub fn num_workers(&self) -> usize {
		self.runtime_metrics()
			.map_or(0, runtime::RuntimeMetrics::num_workers)
	}

	#[inline]
	pub fn task_metrics(&self) -> Option<&TaskMonitor> { self.task_monitor.as_ref() }

	#[inline]
	pub fn runtime_metrics(&self) -> Option<&runtime::RuntimeMetrics> {
		self.runtime_metrics.as_ref()
	}
}
