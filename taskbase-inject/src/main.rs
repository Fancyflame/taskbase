#[tokio::main]
async fn main() {
    taskbase_inject::inject_database().await.unwrap();
}
