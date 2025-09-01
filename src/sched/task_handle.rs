use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Wake, Waker};
use crate::sched::my_wake::LocalWake;

#[derive(Clone)]
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
        println!("--------wake----------")
    }

    fn wake_by_ref(self: &Rc<TaskHandle>) {
        println!("--------wake_by_ref----------")
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

// pub(crate) fn my_waker_extract_rc_cloned(waker: &Waker) -> Rc<TaskHandle> {
//     unsafe {
//         let raw = waker.clone();
//         let ptr = raw.data();
//         let rc = Rc::from_raw(ptr as *const TaskHandle);
//         let cloned = rc.clone();
//         std::mem::forget(rc);
//         cloned
//     }
// }



//
// impl Wake for TaskHandle {
//     fn wake(self: Arc<TaskHandle>) {
//         println!("--------wake----------")
//     }
//
//     fn wake_by_ref(self: &Arc<TaskHandle>) {
//         println!("--------wake_by_ref----------")
//     }
// }
//
// pub(crate) fn make_waker(task_id: u64) -> Waker {
//     let handle = Arc::new(TaskHandle::new(task_id));
//     Waker::from(handle)
// }

