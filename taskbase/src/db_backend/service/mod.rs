use std::collections::HashSet;

use anyhow::Result;
use sqlx::{PgPool, postgres::PgListener, query};

use crate::db_backend::service::channel_name::build_channel_route;

pub use fetch_ready_tasks::ReadyTask;
pub use push_tasks::{PushTask, TaskStatus};

mod channel_name;
mod fetch_ready_tasks;
mod push_tasks;

pub struct DbService {
    namespaces: HashSet<String>,
    listener: PgListener,
    pool: PgPool,
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
}
