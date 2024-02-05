use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use db::ResourceRequest;
use flume::Sender;
use http_body::Frame;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{header, Method, Request, Response, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio::time::timeout;

mod db;

#[derive(Clone)]
struct Repository {
    resource_request_sender: Sender<ResourceRequest>,
}

impl Repository {
    fn new(resource_request_sender: Sender<ResourceRequest>) -> Repository {
        Repository {
            resource_request_sender,
        }
    }

    async fn get_response_for_path(
        &self,
        path: &str,
    ) -> hyper::http::Result<Response<BoxBody<Bytes, Infallible>>> {
        let (meta_sender, meta_receiver) = oneshot::channel();
        let (body_sender, mut body_receiver) = tokio::sync::mpsc::unbounded_channel();

        let _ = &self
            .resource_request_sender
            .clone()
            .send_async(ResourceRequest {
                path: path.to_string(),
                meta_sender,
                body_sender,
            })
            .await;

        // TODO:
        // timeout here and at the other send points,
        // or somehow wrap the timeout around the caller of this function?
        let page_response = timeout(Duration::from_secs(1), meta_receiver).await;

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
                match body_receiver.recv().await {
                    None => {
                        // error, page exists but no content?
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(
                                http_body_util::Full::from("Page exists but content not found")
                                    .boxed(),
                            )
                    }
                    Some(first_block) => {
                        let lm = info.format_last_modified_timestamp();
                        let ct = info.content_type;
                        let content_length = info.content_length;

                        // if the body stream is still open, then use that?
                        // if it's the whole content the return it,
                        if first_block.len() >= content_length {
                            let bytes = Bytes::from(first_block);
                            let body = http_body_util::Full::from(bytes).boxed();
                            return Response::builder()
                                .header(header::CONTENT_TYPE, ct)
                                .header(header::LAST_MODIFIED, lm)
                                .body(body);
                        }

                        // return stream of chunks until sender closed
                        let mut next_block = Some(first_block);
                        let body_stream = async_stream::stream! {
                            while let Some(block) = next_block {
                                let bytes = Bytes::from(block);
                                let frame = Frame::data(bytes);
                                yield Result::<Frame<Bytes>, Infallible>::Ok(frame);

                                next_block = body_receiver.recv().await;
                            }
                        };

                        let body = StreamBody::new(body_stream).boxed();
                        Response::builder()
                            .header(header::CONTENT_TYPE, ct)
                            .header(header::LAST_MODIFIED, lm)
                            .body(body)
                    }
                }
            }
        }
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

    let (resource_request_sender, resource_request_receiver) =
        flume::bounded::<ResourceRequest>(DB_INFLIGHT_COUNT);

    let repository = Repository::new(resource_request_sender);

    const DB_RECEIVER_COUNT: i32 = 2;
    for i in 0..DB_RECEIVER_COUNT {
        let r = resource_request_receiver.clone();
        let _ = spawn_blocking(move || db::send_resources_for_path_task(i, r));
    }

    // We'll bind to 127.0.0.1:8080
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let listener = TcpListener::bind(addr).await.expect("tcp listener bound");

    loop {
        let (stream, _) = listener.accept().await.expect("listener accepted");

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = hyper_util::rt::TokioIo::new(stream);

        let rep = repository.clone();

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(move |req| get_response(rep.clone(), req)))
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
