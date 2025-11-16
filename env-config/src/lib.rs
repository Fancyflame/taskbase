use lazy_env::lazy_env;

lazy_env! {
    pub static ref DATABASE_URL: String =
        panic!("environment variable DATABASE_URL must be set");
}
