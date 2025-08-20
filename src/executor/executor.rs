use std::cell::{Cell, RefCell};
use std::collections::{HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;
use crate::executor::task::{Task, TaskState};

// Thread-local storage for the currently running task id
thread_local! {
    static TASK_ID_RUNNING: Cell<u64> = Cell::new(0);
    static TASK_NAME_RUNNING: RefCell<String> = RefCell::new(String::new());
}

pub struct Executor {
    task_id_allocator: AtomicU64,
    task_id_map: HashMap<u64, Arc<Task>>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            task_id_allocator: AtomicU64::new(1),
            task_id_map: HashMap::new(),
        }
    }
    pub fn spawn<F>(&mut self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let new_task_id = self.task_id_allocator.fetch_add(1, Ordering::Relaxed);
        let new_task = Arc::new(Task {
            task_fut: RefCell::new(Box::pin(fut)),
            task_id: new_task_id,
            task_name: format!("Task_{}", new_task_id),
            task_state: TaskState::Running,
        });
        self.task_id_map.insert(new_task_id, new_task);
    }

    pub fn task_id() -> u64 {
        TASK_ID_RUNNING.get()
    }

    pub fn task_name() -> String {
        TASK_NAME_RUNNING.with(|name| name.borrow().clone())
    }

    pub fn run(&mut self) {
        loop {
            let mut finished = vec![];
            for (task_id, task) in &self.task_id_map {
                TASK_ID_RUNNING.set(*task_id);
                TASK_NAME_RUNNING.with(|name| {
                    name.borrow_mut().clear();
                    name.borrow_mut().push_str(task.task_name.as_str());
                });
                println!("task id is {}", task_id);

                // run the task
                let waker = futures::task::noop_waker(); // minimal waker
                let mut cx = Context::from_waker(&waker);
                
                let mut fut = task.task_fut.borrow_mut();
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        println!("Task {} completed", task_id);
                        finished.push(*task_id);
                    }
                    Poll::Pending => {
                        // Still not ready, keep it for next loop
                    }
                }
            }

            // clear info
            TASK_ID_RUNNING.set(0);
            TASK_NAME_RUNNING.with(|name| {
                name.borrow_mut().clear();
            });

            // cleanup finished tasks
            for id in finished {
                self.task_id_map.remove(&id);
            }

            // Tiny sleep to avoid busy loop
            thread::sleep(Duration::from_secs(1));
        }
    }

}