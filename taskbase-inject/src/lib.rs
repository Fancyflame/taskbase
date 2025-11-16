use anyhow::Result;
use env_config::DATABASE_URL;
use tokio_postgres::NoTls;

macro_rules! include_sql {
    ($($file:literal,)*) => {
        [$(
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../sql/", $file)),
        )*]
    };
}

const SQLS: &[&str] = &include_sql!["entry.sql", "tasks.sql", "shared_objects.sql",];

pub async fn inject_database() -> Result<()> {
    let (mut client, connection) = tokio_postgres::connect(&DATABASE_URL, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("{e}");
        }
    });

    let transaction = client.build_transaction().start().await?;

    // 批量执行所有 SQL 语句
    for sql in SQLS {
        transaction.batch_execute(sql).await?;
    }

    // 提交事务
    transaction.commit().await?;

    Ok(())
}
