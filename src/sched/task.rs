use std::cell::{RefCell, RefMut};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use futures::task::ArcWake;

pub(crate) enum TaskState {
    Init,
    Running,
    Cancelled,
    Finished,
}

pub(crate) struct Task {
    task_id: u64,
    task_name: String,
    task_state: TaskState,
    task_fut: RefCell<Pin<Box<dyn Future<Output = ()>>>>,
}

impl Task {

    pub(crate) fn get_id(&self) -> u64 {
        self.task_id
    }

    pub(crate) fn get_name(&self) -> &str {
        self.task_name.as_str()
    }

    pub(crate) fn set_name(&mut self, name: &str) {
        self.task_name.clear();
        self.task_name.push_str(name);
    }

    pub(crate) fn borrow_fut(&self) -> RefMut<'_, Pin<Box<dyn Future<Output = ()>>>> {
        self.task_fut.borrow_mut()
    }
    pub(crate) fn new<F>(id: u64, fut: F) -> Rc<Self>
        where F: Future<Output = ()> + 'static,
    {
        Rc::new(Self {
            task_fut: RefCell::new(Box::pin(fut)),
            task_id: id,
            task_name: format!("Task_{}", id),
            task_state: TaskState::Init,
        })
    }
}