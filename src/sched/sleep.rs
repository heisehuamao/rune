use std::pin::Pin;
use std::task::{Context, Poll};
use crate::sched::runtime::{thread_sleep_mod, thread_wall_clock};
use crate::sched::task_handle::{my_waker_extract_task_handle};

pub struct TaskSleep {
    until_clock: u64,
}

impl TaskSleep {
    pub(crate) fn new(until_clock: u64) -> Self {
        Self {
            until_clock,
        }
    }
}

impl Future for TaskSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let latest_clock = thread_wall_clock();
        if self.until_clock <= latest_clock {
            Poll::Ready(())
        } else {
            if let Some(sm) = thread_sleep_mod() {
                let waker = cx.waker().clone();
                let handle = my_waker_extract_task_handle(&waker);
                match sm.add_sleep_task(self.until_clock - latest_clock, handle) {
                    Ok(_) => Poll::Pending,
                    _ => {
                        eprintln!("poll of tasksleep enqueue error");
                        Poll::Ready(())
                    }
                }
                
            } else {
                Poll::Ready(())
            }
        }
    }
}