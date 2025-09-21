use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;

type ResponseBody = Full<Bytes>;

async fn hello_world(_req: Request<Incoming>) -> Result<Response<ResponseBody>, Infallible> {
    Ok(Response::new(Full::new(Bytes::from_static(
        b"Clever Cloud S3 Migration Tool",
    ))))
}

#[tokio::main]
async fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = SocketAddr::from((
        [0, 0, 0, 0],
        port.parse::<u16>().expect("Port variable should be a u16"),
    ));

    let listener = TcpListener::bind(addr)
        .await
        .expect("Unable to bind TCP listener");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(tuple) => tuple,
            Err(err) => {
                eprintln!("accept error: {}", err);
                continue;
            }
        };

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(hello_world);
            let builder = Builder::new(TokioExecutor::new());

            if let Err(err) = builder.serve_connection(io, service).await {
                eprintln!("server error: {}", err);
            }
        });
    }
}
