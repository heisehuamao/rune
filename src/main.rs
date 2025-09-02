mod sched;

use std::time::Duration;
use crate::sched::runtime::Executor;

fn main() {
    let mut exec = Executor::new();

    if let Err(_) = Executor::register(&exec) {
        eprintln!("failed to register executor");
        return;
    }
    
    _ = exec.spawn({
        async move {
            for i in 1..5 {
                println!("A Round:{i} Task name is: {}, id: {}", Executor::task_name(), Executor::task_id());
                Executor::sleep(Duration::from_micros(1)).await;
                println!("A Round:{i} After sleep: {}, id:{}", Executor::task_name(), Executor::task_id());
            }
        }
    });

    _ = exec.spawn({
        async move {
            for i in 1..3 {
                println!("B Round:{i} Task name is: {}, id: {}", Executor::task_name(), Executor::task_id());
                Executor::sleep(Duration::from_micros(3)).await;
                println!("B Round:{i} After sleep: {}, id:{}", Executor::task_name(), Executor::task_id());
            }
        }
    });

    exec.run();
}

