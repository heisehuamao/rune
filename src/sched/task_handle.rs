use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Wake, Waker};
use crate::sched::my_wake::LocalWake;
use crate::sched::runtime::Executor;

#[derive(Clone, PartialEq)]
pub(crate) struct TaskHandle {
    pub(crate) task_id: u64,
}

impl TaskHandle {
    pub(crate) fn new(task_id: u64) -> Self {
        Self {
            task_id,
        }
    }
}

impl LocalWake for TaskHandle {
    fn wake(self: Rc<TaskHandle>) {
        println!("--------wake----------");
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Rc<TaskHandle>) {
        println!("--------wake_by_ref----------");
        if let Err(e) = Executor::resume_task(self.task_id) {
            eprintln!("Failed to resume task {}: {:?}", self.task_id, e);
        }
    }
}


pub(crate) fn my_waker_extract_task_handle(waker: &Waker) -> Rc<TaskHandle> {
    unsafe {
        let ptr = waker.data();
        let rc = Rc::from_raw(ptr as *const TaskHandle);
        let cloned = rc.clone();
        std::mem::forget(rc);
        cloned
    }
}
