use anyhow::Result;
use taskbase::db_backend::{
    DbBackend,
    service::{PushTask, TaskStatus},
};

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

    service
        .push_blocking_tasks(&test_tasks(namespaces[0].clone()))
        .await?;

    println!("开始监听命名空间: {:?}", namespaces);
    println!("可通过 pg_notify('task_ready/<namespace>', '<payload>') 触发");

    loop {
        let tasks = service.fetch_ready_tasks().await?;
        for task in tasks {
            println!(
                "[task_ready] namespace={} task_id={} task_name={}",
                task.namespace, task.id, task.task_name
            );
        }
    }
}

fn test_tasks(available_namespace: String) -> [PushTask; 3] {
    [
        PushTask {
            id: None,
            namespace: available_namespace.clone(),
            task_name: "test-task-name".into(),
            context: b"awawawdsd".into(),
            status: TaskStatus::Processing,
        },
        PushTask {
            id: Some(3),
            namespace: available_namespace.clone(),
            task_name: "unreachable".into(),
            context: Vec::new(),
            status: TaskStatus::Processing,
        },
        PushTask {
            id: Some(99999),
            namespace: available_namespace,
            task_name: "unreachable".into(),
            context: Vec::new(),
            status: TaskStatus::Processing,
        },
    ]
}
