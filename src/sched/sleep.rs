use std::pin::Pin;
use std::task::{Context, Poll};
use crate::sched::runtime::{thread_runtime_wall_clock, thread_runtime_sleep_mod};
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
        let waker = cx.waker().clone();
        let handle = my_waker_extract_task_handle(&waker);
        let latest_clock = thread_runtime_wall_clock();
        println!("task id:{}, Sleep Poll, until:{}, latest:{}", handle.task_id, self.until_clock, latest_clock);
        if self.until_clock <= latest_clock {
            Poll::Ready(())
        } else {
            if let Some(sm) = thread_runtime_sleep_mod() {
                println!("Sleep Poll for {}", handle.task_id);
                match sm.push_back_sleep_task(self.until_clock - latest_clock, handle) {
                    Ok(_) => Poll::Pending,
                    _ => {
                        eprintln!("poll of tasksleep enqueue error");
                        Poll::Ready(())
                    }
                }
                
            } else {
                eprintln!("Sleep mod is none");
                Poll::Ready(())
            }
        }
    }
}