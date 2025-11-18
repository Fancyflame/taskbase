use crate::db_backend::service::DbService;
use std::collections::HashSet;

use anyhow::Result;
use sqlx::Row;

use crate::db_backend::service::channel_name::parse_channel_route;

#[derive(Clone, Debug)]
pub struct ReadyTask {
    pub id: i64,
    pub namespace: String,
    pub task_name: String,
    pub context: Vec<u8>,
}

impl DbService {
    pub async fn fetch_ready_tasks(&mut self) -> Result<Vec<ReadyTask>> {
        let mut namespaces_to_fetch: HashSet<&str> = HashSet::new();

        loop {
            let msg = if namespaces_to_fetch.is_empty() {
                // 如果一个消息也没有就等待
                self.listener.recv().await?
            } else if let Some(msg) = self.listener.next_buffered() {
                // 如果还有消息就拿下来
                msg
            } else {
                // 暂时没有消息了，先返回已有的
                break;
            };

            if let Some(["task_ready", namespace]) = parse_channel_route(msg.channel()) {
                if let Some(matched) = self.namespaces.get(namespace) {
                    namespaces_to_fetch.insert(matched.as_str());
                }
            }
        }

        self.fetch_ready_tasks_from_ns(&namespaces_to_fetch).await
    }

    async fn fetch_ready_tasks_from_ns(
        &self,
        namespaces: &HashSet<&str>,
    ) -> Result<Vec<ReadyTask>> {
        let ns_vec: Vec<&str> = namespaces.iter().copied().collect();

        let rows = sqlx::query(
            "SELECT id, namespace, task_name, context \
            FROM fetch_tasks($1::text[])", // 使用 SQL 函数过滤 ready 任务
        )
        .bind(&ns_vec)
        .fetch_all(&self.pool)
        .await?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            tasks.push(ReadyTask {
                id: row.try_get("id")?,
                namespace: row.try_get("namespace")?,
                task_name: row.try_get("task_name")?,
                context: row.try_get("context")?,
            });
        }

        Ok(tasks)
    }
}
