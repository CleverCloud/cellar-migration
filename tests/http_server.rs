use hyper::{body::to_bytes, Client, Uri};
use std::error::Error;
use std::net::TcpListener;
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn test_http_server_responds() -> TestResult {
    // Ask the OS for a free port and release it immediately; the tiny race window is acceptable for tests.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let mut child = Command::new("cargo")
        .args(["run", "--bin", "http-server"])
        .env("PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

    // Give the server a moment to bind; retry the health-check a few times to avoid flakes.
    let client = Client::new();
    let uri: Uri = format!("http://127.0.0.1:{}/", port)
        .parse()
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

    let mut attempts = 0;
    let response = loop {
        match client.get(uri.clone()).await {
            Ok(resp) => break resp,
            Err(_err) if attempts < 10 => {
                attempts += 1;
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(err) => {
                let _ = child.kill().await; // best-effort cleanup
                return Err(Box::new(err) as Box<dyn Error + Send + Sync>);
            }
        }
    };

    assert_eq!(response.status(), 200);
    let body = to_bytes(response.into_body()).await?;
    assert_eq!(&body[..], b"Clever Cloud S3 Migration Tool");

    // Shut the server down to keep the test suite tidy.
    let _ = child.kill().await;
    let _ = child.wait().await;

    Ok(())
}
