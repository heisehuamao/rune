use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;
use crate::sched::my_wake::{my_waker_create};
use crate::sched::sleep::TaskSleep;
use crate::sched::task::{Task, TaskState};
use crate::sched::task_handle::{TaskHandle};

// Thread-local storage for the currently running task id
thread_local! {
    // for executor
    static EXE_ID_GENERATOR: AtomicU32 = AtomicU32::new(1);
    static EXE_REGISTERED: Cell<Option<u32>> = Cell::new(None);
    
    // for registered executor
    static TASK_RUN_QUEUE: RefCell<Option<Rc<RefCell<VecDeque<u64>>>>> = RefCell::new(None);
    static TASK_SLEEP: RefCell<Option<Rc<ExeSubmodSleep>>> = RefCell::new(None);
    static WALL_CLOCK: Cell<u64> = Cell::new(0);
    
    // for running task info
    static TASK_ID_RUNNING: Cell<u64> = Cell::new(0);
    static TASK_NAME_RUNNING: RefCell<String> = RefCell::new(String::new());
    
}

pub(crate) fn thread_wall_clock() -> u64 {
    WALL_CLOCK.get()
}

pub(crate) fn thread_sleep_mod() -> Option<Rc<ExeSubmodSleep>> {
    TASK_SLEEP.with(|content| {
        match content.borrow().as_ref() {
            Some(sm) => Some(sm.clone()),
            _ => None,
        }
    })
}

fn thread_exe_id_gen() -> u32 {
    EXE_ID_GENERATOR.with(|id| id.fetch_add(1, Ordering::Relaxed))
}

fn thread_executor_data_register(exe: &Executor) {
    TASK_RUN_QUEUE.with(|rq| {
        *rq.borrow_mut() = Some(exe.task_run_queue.clone());
    });
    TASK_SLEEP.with(|sm| {
        *sm.borrow_mut() = Some(exe.task_sleep.clone());
    });
    // WALL_CLOCK.with(|wc| {
    //     wc.set(Some(exe.wall_clock.clone()));
    // })
}

fn thread_executor_data_unregister() {
    TASK_RUN_QUEUE.with(|rq| {
        *rq.borrow_mut() = None;
    });
    TASK_SLEEP.with(|sm| {
        *sm.borrow_mut() = None;
    });
    // WALL_CLOCK.with(|wc| {
    //     wc.set(None);
    // })
}

fn thread_runtime_init(exe: &Executor) -> Result<(), ()> {
    EXE_REGISTERED.with(|flag| {
         match flag.get() {
             Some(_) => Err(()),
             None => {
                 flag.set(Some(exe.exe_id));
                 thread_executor_data_register(exe);
                 Ok(())
             },
         }
    })
}

fn thread_runtime_exe_id() -> Option<u32> {
    EXE_REGISTERED.get()
}

fn thread_runtime_check(exe: &Executor) -> bool {
    EXE_REGISTERED.with(|flag| {
        match flag.get() {
            Some(running_id) => running_id == exe.exe_id,
            _ => false,
        }
    })
}

fn thread_runtime_exit(exe: &Executor) -> Result<(), ()> {
    EXE_REGISTERED.with(|flag| {
        match flag.get() {
            Some(running_id) if running_id == exe.exe_id => {
                flag.set(None);
                thread_executor_data_unregister();
                Ok(())
            }
            _ => Err(()),
        }
    })
}

pub(crate) struct ExeSubmodSleep {
    sleep_ring: RefCell<Vec<VecDeque<Rc<TaskHandle>>>>,
    curr_clock: Cell<u64>,
    exe_clock: u64,
}

impl ExeSubmodSleep {
    fn new() -> Self {
        Self {
            sleep_ring: RefCell::new(vec![VecDeque::new(); 1000]),
            curr_clock: Cell::new(0),
            exe_clock: 0,
        }
    }

    pub(crate) fn add_sleep_task(&self, offset_clock: u64, th: Rc<TaskHandle>) -> Result<(), ()> {
        let mut ring = self.sleep_ring.borrow_mut();
        let size = ring.len() as u64;
        if size <= 0 {
            return Err(());
        }
        
        let mut offset_to_exe = 0u64;
        let exe_clock = self.exe_clock;
        let latest_clock = thread_wall_clock();
        if exe_clock > latest_clock {
            eprintln!("abnormal clock, exe:{}, latest:{}", exe_clock, latest_clock)
        } else {
            offset_to_exe = offset_clock + latest_clock - exe_clock;
        }
        let idx = (offset_to_exe + exe_clock) % size;
        let slot = ring.get_mut(idx as usize);
        match slot {
            Some(s) => {
                s.push_back(th);
                Ok(())
            }
            _ => Err(())
        }
    }
}

pub struct Executor {
    exe_id: u32,
    task_id_allocator: AtomicU64,
    task_id_map: HashMap<u64, Rc<Task>>,
    task_run_queue: Rc<RefCell<VecDeque<u64>>>,
    task_sleep: Rc<ExeSubmodSleep>,
    
    // // a running clock
    // wall_clock: Rc<Cell<u64>>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            exe_id: thread_exe_id_gen(),
            task_id_allocator: AtomicU64::new(1),
            task_id_map: HashMap::new(),
            task_run_queue: Rc::new(RefCell::new(VecDeque::new())),
            task_sleep: Rc::new(ExeSubmodSleep::new()),
            // wall_clock: Rc::new(Cell::new(0)),
        }
    }
    
    pub fn register(&self) -> Result<(), ()> {
        thread_runtime_init(&self)
    }
    
    pub fn unregister(&self) -> Result<(), ()> {
        thread_runtime_exit(&self)
    }
    
    pub fn spawn<F>(&mut self, fut: F) ->Result<(), ()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match thread_runtime_check(&self) { 
            true => {
                let new_task_id = self.task_id_allocator.fetch_add(1, Ordering::Relaxed);
                let new_task = Task::new(new_task_id, fut);
                self.task_id_map.insert(new_task_id, new_task);
                self.task_run_queue.borrow_mut().push_back(new_task_id);
                Ok(())
            }
            _ => {
                eprintln!("current running executor is {:?}, my executor is {}",
                          thread_runtime_exe_id(), self.exe_id);
                Err(())
            }
        }
    }

    pub fn task_id() -> u64 {
        TASK_ID_RUNNING.get()
    }

    pub fn task_name() -> String {
        TASK_NAME_RUNNING.with(|name| name.borrow().clone())
    }
    
    pub fn sleep(duration: Duration) -> TaskSleep {
        let usec = duration.as_micros() as u64;
        let wait_clock = usec / 50;
        
        TaskSleep::new(wait_clock)
    }

    pub fn run(&mut self) {
        loop {
            let mut finished = vec![];
            let mut rq = self.task_run_queue.borrow_mut();
            loop {
                let Some(task_id) = rq.pop_front() else {
                    // no task anymore
                    break;
                };

                let Some(task) = self.task_id_map.get(&task_id) else {
                    // invalid task
                    continue;
                };

                TASK_ID_RUNNING.set(task_id);
                TASK_NAME_RUNNING.with(|name| {
                    name.borrow_mut().clear();
                    name.borrow_mut().push_str(task.get_name());
                });
                println!("task id is {}", task_id);

                // run the task
                // let waker: Waker = futures::task::noop_waker(); // minimal waker
                let handle = Rc::new(TaskHandle::new(task_id));
                let waker = my_waker_create(handle);
                let mut cx = Context::from_waker(&waker);
                
                let mut fut = task.borrow_fut();
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        println!("Task {} completed", task_id);
                        finished.push(task_id);
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