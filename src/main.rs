mod sched;
use crate::sched::runtime::Executor;

fn main() {
    let mut exec = Executor::new();

    exec.spawn({
        async move {
            println!("Task name is: {}, id: {}", Executor::task_name(), Executor::task_id());
        }
    });

    exec.spawn({
        async move {
            println!("Task name is: {}, id: {}", Executor::task_name(), Executor::task_id());
        }
    });

    exec.run();
}

