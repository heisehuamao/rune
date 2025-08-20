use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;

pub(crate) enum TaskState {
    Running,
    Cancelled,
    Finished,
}

pub(crate) struct Task {
    pub(crate) task_id: u64,
    pub(crate) task_name: String,
    pub(crate) task_state: TaskState,
    pub(crate) task_fut: RefCell<Pin<Box<dyn Future<Output = ()> + Send>>>,
}