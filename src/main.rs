use std::convert::Infallible;
use std::net::SocketAddr;
use std::ops::Range;
use std::time::Duration;

use flume::Sender;
use http_body::Frame;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{header, Method, Request, Response, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio::time::timeout;
use db::{PageBodyRequest, PageInfoRequest};

mod db;

const BLOCK_SIZE_BYTES: usize = 16 * 1024;

#[derive(Clone)]
struct Repository {
    // channels to get page data from db tasks
    info_request_sender: Sender<PageInfoRequest>,
    body_request_sender: Sender<PageBodyRequest>,
}

impl Repository {
    fn new(
        info_request_sender: Sender<PageInfoRequest>,
        body_request_sender: Sender<PageBodyRequest>,
    ) -> Repository {
        Repository {
            info_request_sender,
            body_request_sender,
        }
    }

    async fn get_response_for_path(
        &self,
        path: &str,
    ) -> hyper::http::Result<Response<BoxBody<Bytes, Infallible>>> {
        let (info_sender, info_receiver) = oneshot::channel();

        let _ = &self
            .info_request_sender
            .clone()
            .send_async(PageInfoRequest {
                path: path.to_string(),
                info_sender,
            })
            .await;

        // TODO:
        // timeout here and at the other send points,
        // or somehow wrap the timeout around the caller of this function?
        let page_response = timeout(Duration::from_secs(1), info_receiver).await;

        match page_response {
            Err(_elapsed) => Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(BoxBody::default()),
            Ok(Err(e)) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(http_body_util::Full::from(e.to_string()).boxed()),
            Ok(Ok(Err(e))) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(http_body_util::Full::from(e.to_string()).boxed()),
            Ok(Ok(Ok(None))) => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(BoxBody::default()),
            Ok(Ok(Ok(Some(info)))) => {
                let lm = info.format_last_modified_timestamp();
                let ct = info.content_type;
                let row_id = info.row_id;
                let content_length = info.content_length;

                let body_request_sender = self.body_request_sender.clone();

                let body = if content_length <= BLOCK_SIZE_BYTES {
                    Self::get_body_entire(row_id, &body_request_sender, content_length)
                        .await
                        .boxed()
                } else {
                    // return chunked response
                    Self::get_body_streamed(row_id, content_length, body_request_sender)
                };

                Response::builder()
                    .header(header::CONTENT_TYPE, ct)
                    .header(header::LAST_MODIFIED, lm)
                    .body(body)
            }
        }
    }

    async fn get_body_entire(
        row_id: i64,
        body_request_sender: &Sender<PageBodyRequest>,
        content_length: usize,
    ) -> Full<Bytes> {
        // get entire response
        let body_bytes =
            Self::get_body_range(row_id, 0..content_length, body_request_sender).await;
        let bytes = Bytes::from(body_bytes);
        http_body_util::Full::from(bytes)
    }

    fn get_body_streamed(
        row_id: i64,
        content_length: usize,
        body_request_sender: Sender<PageBodyRequest>,
    ) -> BoxBody<Bytes, Infallible> {
        let body_stream = async_stream::stream! {
            let mut next_start = 0_usize;

            while next_start < content_length {
                let next_end = std::cmp::min(next_start + BLOCK_SIZE_BYTES, content_length);
                let byte_range = next_start..next_end;
                let body_bytes = Self::get_body_range(row_id, byte_range, &body_request_sender).await;

                let bytes = Bytes::from(body_bytes);
                let frame = Frame::data(bytes);
                yield Result::<Frame<Bytes>, Infallible>::Ok(frame);
                next_start = next_end;
            };
        };

        StreamBody::new(body_stream).boxed()
    }

    async fn get_body_range(
        row_id: i64,
        byte_range: Range<usize>,
        body_request_sender: &Sender<PageBodyRequest>,
    ) -> Box<[u8]> {
        let (body_sender, body_receiver) = oneshot::channel();
        let _ = body_request_sender
            .send_async(PageBodyRequest {
                row_id,
                byte_range,
                body_sender,
            })
            .await;

        body_receiver
            .await
            .expect("body received")
            .expect("no db errors")
    }
}

async fn get_response(
    repository: Repository,
    req: Request<hyper::body::Incoming>,
) -> hyper::http::Result<Response<BoxBody<Bytes, Infallible>>> {
    // only allow GET methods
    match (req.method(), req.uri().path()) {
        (&Method::GET, path) => repository.get_response_for_path(path).await,
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(BoxBody::default()),
    }
}

#[tokio::main]
async fn main() {
    // database reads are blocking calls,
    // so move that work into blocking threads,
    // and request the data over channels.
    // This separates the HTTP stuff from the DB stuff (ownership of connections etc),
    // and allows reading blocks from the database to be interleaved
    // with writing chunked responses

    // allow a limited amount of queuing of requests to the database
    // so that
    const DB_INFLIGHT_COUNT: usize = 4;

    let (info_request_sender, info_request_receiver) =
        flume::bounded::<PageInfoRequest>(DB_INFLIGHT_COUNT);
    let (body_request_sender, body_request_receiver) =
        flume::bounded::<PageBodyRequest>(DB_INFLIGHT_COUNT);

    let repository = Repository::new(info_request_sender, body_request_sender);

    const DB_RECEIVER_COUNT: i32 = 2;
    for i in 0..DB_RECEIVER_COUNT {
        let r = info_request_receiver.clone();
        let _ = spawn_blocking(move || db::db_read_page_infos_task(i, r));
    }

    for i in 0..DB_RECEIVER_COUNT {
        let r = body_request_receiver.clone();
        let _ = spawn_blocking(move || db::db_read_page_bodies_task(i, r));
    }

    // We'll bind to 127.0.0.1:8080
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let listener = TcpListener::bind(addr).await.expect("tcp listener bound");

    loop {
        let (stream, _) = listener.accept().await.expect("listener accepted");

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = hyper_util::rt::TokioIo::new(stream);

        let repository = repository.clone();
        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(
                    io,
                    service_fn(move |req| get_response(repository.clone(), req)),
                )
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
