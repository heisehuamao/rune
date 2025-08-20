use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

pub(crate) enum TaskState {
    Running,
    Cancelled,
    Finished,
}

pub(crate) struct Task {
    pub(crate) task_id: u64,
    pub(crate) task_name: String,
    pub(crate) task_state: TaskState,
    pub(crate) task_fut: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
}