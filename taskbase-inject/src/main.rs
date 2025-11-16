use std::env::args;

#[tokio::main]
async fn main() {
    let drop_schema = matches!(args().nth(1).as_deref(), Some("drop"));
    taskbase_inject::inject_database(drop_schema).await.unwrap();
}
