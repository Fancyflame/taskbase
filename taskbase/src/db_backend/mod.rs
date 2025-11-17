use anyhow::Result;
use const_format::formatcp;
use env_config::DATABASE_URL;
use sqlx::{
    Executor, PgPool,
    postgres::{PgListener, PgPoolOptions},
};
use std::sync::Arc;

use crate::{SCHEMA, db_backend::service::DbService};

mod service;

pub struct DbBackend {
    pool: PgPool,
}

const SQL_SET_SCHEMA: &str = formatcp!("SET search_path TO {SCHEMA}");

impl DbBackend {
    /// 从数据库 URL 创建连接池
    pub async fn new_from_env() -> Result<Self> {
        let pool = PgPoolOptions::new()
            .after_connect(|conn, _meta| {
                Box::pin(async {
                    conn.execute(SQL_SET_SCHEMA).await?;
                    Ok(())
                })
            })
            .connect(&*DATABASE_URL)
            .await?;
        Ok(Self { pool })
    }

    pub async fn serve(&self, namespaces: impl Iterator<Item = String>) -> Result<DbService> {
        let service = DbService::connect(self.pool.clone(), namespaces).await?;
        Ok(service)
    }
}
