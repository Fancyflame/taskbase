use anyhow::Result;
use taskbase::db_backend::DbBackend;

#[tokio::main]
async fn main() -> Result<()> {
    let namespaces: Vec<String> = {
        let mut args: Vec<String> = std::env::args().skip(1).collect();
        if args.is_empty() {
            args.push("default".to_string());
        }
        args
    };

    let backend = DbBackend::new_from_env().await?;
    let mut service = backend.serve(namespaces.clone().into_iter()).await?;

    println!("开始监听命名空间: {:?}", namespaces);
    println!("可通过 pg_notify('task_ready/<namespace>', '<payload>') 触发");

    loop {
        let tasks = service.recv().await?;
        for task in tasks {
            println!(
                "[task_ready] namespace={} task_id={} task_name={}",
                task.namespace, task.id, task.task_name
            );
        }
    }
}
