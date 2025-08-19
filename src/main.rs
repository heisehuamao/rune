use futures::task::{waker_ref, ArcWake};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

/// Sleep future returned by Executor::sleep()
struct Sleep {
    deadline: Instant,
    waker_slot: Arc<Mutex<Option<Waker>>>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.deadline {
            Poll::Ready(())
        } else {
            let mut waker_guard = self.waker_slot.lock().unwrap();
            *waker_guard = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// A task wrapper
struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ready_queue: Arc<Mutex<VecDeque<Arc<Task>>>>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let mut queue = arc_self.ready_queue.lock().unwrap();
        queue.push_back(arc_self.clone());
    }
}

/// Tiny executor
struct Executor {
    ready_queue: Arc<Mutex<VecDeque<Arc<Task>>>>,
    timers: Arc<Mutex<Vec<(Instant, Arc<Mutex<Option<Waker>>>)>>>,
}

impl Executor {
    fn new() -> Self {
        Self {
            ready_queue: Arc::new(Mutex::new(VecDeque::new())),
            timers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Spawn an async task
    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(fut)),
            ready_queue: self.ready_queue.clone(),
        });
        self.ready_queue.lock().unwrap().push_back(task);
    }

    /// Sleep for a duration; can `.await` inside tasks
    fn sleep(&self, dur: Duration) -> Sleep {
        let waker_slot = Arc::new(Mutex::new(None));
        let deadline = Instant::now() + dur;
        self.timers.lock().unwrap().push((deadline, waker_slot.clone()));
        Sleep {
            deadline,
            waker_slot,
        }
    }

    /// Run the executor
    fn run(&self) {
        loop {
            let task_opt = self.ready_queue.lock().unwrap().pop_front();
            match task_opt {
                Some(task) => {
                    let mut future_slot = task.future.lock().unwrap();
                    let waker = waker_ref(&task);
                    let mut cx = Context::from_waker(&*waker);
                    if let Poll::Pending = future_slot.as_mut().poll(&mut cx) {
                        // Not done, keep future
                    }
                }
                None => {
                    if self.timers.lock().unwrap().is_empty() {
                        break; // exit when no tasks or timers
                    }
                }
            }

            // Wake expired sleepers
            let now = Instant::now();
            let mut timers = self.timers.lock().unwrap();
            let mut i = 0;
            while i < timers.len() {
                if timers[i].0 <= now {
                    if let Some(w) = timers[i].1.lock().unwrap().take() {
                        w.wake();
                    }
                    timers.swap_remove(i);
                } else {
                    i += 1;
                }
            }

            // Tiny sleep to avoid busy loop
            thread::sleep(Duration::from_micros(50));
        }
    }
}

/// Demo
fn main() {
    let exec = Executor::new();

    exec.spawn({
        let exec_clone = exec.clone();
        async move {
            let start = Instant::now();
            exec_clone.sleep(Duration::from_micros(100)).await;
            println!("Task 1 slept for {:?}", Instant::now().duration_since(start));

            exec_clone.sleep(Duration::from_micros(200)).await;
            println!("Task 1 slept again for 200us");
        }
    });

    exec.spawn({
        let exec_clone = exec.clone();
        async move {
            let start = Instant::now();
            exec_clone.sleep(Duration::from_micros(150)).await;
            println!("Task 2 slept for {:?}", Instant::now().duration_since(start));
        }
    });

    exec.run();
}

// Implement clone manually for Executor
impl Clone for Executor {
    fn clone(&self) -> Self {
        Self {
            ready_queue: self.ready_queue.clone(),
            timers: self.timers.clone(),
        }
    }
}
