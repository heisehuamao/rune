use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Wake;

struct TaskHandle {
    task_id: u64,
    task_run_queue: Rc<RefCell<VecDeque<u64>>>,
}

impl Wake for TaskHandle {
    fn wake(self: Arc<TaskHandle>) {
        self.task_run_queue.borrow_mut().push_back(self.task_id);
    }

    fn wake_by_ref(self: &Arc<TaskHandle>) {
        self.task_run_queue.borrow_mut().push_back(self.task_id);
    }
}