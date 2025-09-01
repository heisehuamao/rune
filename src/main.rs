mod sched;
use crate::sched::runtime::Executor;

fn main() {
    let mut exec = Executor::new();

    if let Err(_) = Executor::register(&exec) {
        eprintln!("failed to register executor");
        return;
    }
    
    _ = exec.spawn({
        async move {
            println!("Task name is: {}, id: {}", Executor::task_name(), Executor::task_id());
        }
    });

    _ = exec.spawn({
        async move {
            println!("Task name is: {}, id: {}", Executor::task_name(), Executor::task_id());
        }
    });

    exec.run();
}

