use std::collections::HashSet;

use anyhow::Result;
use sqlx::{PgPool, Row, postgres::PgListener, query};

use crate::db_backend::service::channel_name::{build_channel_route, parse_channel_route};

mod channel_name;

pub struct DbService {
    namespaces: HashSet<String>,
    listener: PgListener,
    pool: PgPool,
}

#[derive(Clone, Debug)]
pub struct ReadyTask {
    pub id: i64,
    pub namespace: String,
    pub task_name: String,
    pub context: Vec<u8>,
}

impl DbService {
    pub(super) async fn connect(pool: PgPool, ns: impl Iterator<Item = String>) -> Result<Self> {
        let mut this = Self {
            namespaces: ns.collect(),
            listener: PgListener::connect_with(&pool).await?,
            pool,
        };

        this.register_namespace().await?;
        this.start_listen().await?;
        Ok(this)
    }

    async fn register_namespace(&self) -> Result<()> {
        query(
            "INSERT INTO namespaces (id) \
            SELECT unnest($1::text[]) \
            ON CONFLICT (id) DO NOTHING",
        )
        .bind(
            self.namespaces
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>(),
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn start_listen(&mut self) -> Result<()> {
        let channels: Vec<String> = self
            .namespaces
            .iter()
            .map(|ns| build_channel_route(["task_ready", ns]))
            .collect();
        self.listener
            .listen_all(channels.iter().map(String::as_str))
            .await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<ReadyTask>> {
        let mut out: HashSet<&str> = HashSet::new();

        loop {
            let msg = if out.is_empty() {
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
                    out.insert(matched.as_str());
                }
            }
        }

        self.fetch_ready_tasks(&out).await
    }

    async fn fetch_ready_tasks(&self, namespaces: &HashSet<&str>) -> Result<Vec<ReadyTask>> {
        if namespaces.is_empty() {
            return Ok(Vec::new());
        }

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

    /// 发送通知
    pub async fn notify(&self, channel: &str, payload: &str) -> Result<()> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
