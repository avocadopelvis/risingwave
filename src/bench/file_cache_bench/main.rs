#[cfg(target_os = "linux")]
mod bench;

#[tokio::main]
async fn main() {
    if !cfg!(target_os = "linux") {
        panic!("only support linux")
    }

    #[cfg(target_os = "linux")]
    bench::run().await;
}
