use httpdate::HttpDate;
use hyper::body::Bytes;
use hyper::server::conn::{AddrStream};
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server, StatusCode};
use rusqlite::blob::Blob;
use rusqlite::{Connection, DatabaseName, OpenFlags, OptionalExtension};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::ops::{Add, Range};
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_blocking;
use tokio::time::timeout;

#[derive(Clone)]
struct Repository {
    info_request_sender: mpsc::Sender::<PageInfoRequest>,
    body_request_sender: mpsc::Sender::<PageBodyRequest>,
}

struct PageInfoRequest {
    path: String,
    info_sender: oneshot::Sender<rusqlite::Result<Option<PageInfo>>>,
}

struct PageBodyRequest {
    row_id: i64,
    byte_range: Range<usize>,
    body_sender: oneshot::Sender<rusqlite::Result<Vec<u8>>>,
}

impl Repository {
    fn new(
        info_request_sender: mpsc::Sender<PageInfoRequest>,
        body_request_sender: mpsc::Sender<PageBodyRequest>,
    ) -> Repository {
        Repository {
            info_request_sender,
            body_request_sender,
        }
    }

    async fn get_response_for_path(&self, path: &str) -> hyper::http::Result<Response<Body>> {
        let (info_sender, info_receiver) = oneshot::channel();

        let _ = &self
            .info_request_sender
            .clone()
            .send(
                PageInfoRequest {
                    path: path.to_string(),
                    info_sender,
                }
            )
            .await;

        // TODO:
        // timeout here and at the other send points,
        // or somehow wrap the timeout around the caller of this function?
        let page_response = timeout(Duration::from_secs(1), info_receiver).await;

        match page_response {
            Err(_elapsed) => Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Body::empty()),
            Ok(Err(e)) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(e.to_string())),
            Ok(Ok(Err(e))) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(e.to_string())),
            Ok(Ok(Ok(None))) => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty()),
            Ok(Ok(Ok(Some(info)))) => {
                let ct = &info.content_type;
                let lm = info.format_last_modified_timestamp();
                let row_id = info.row_id;
                let content_length = info.content_length;
                let body_request_sender = self.body_request_sender.clone();
                const BLOCK_SIZE_BYTES: usize = 16 * 1024;

                let body = if content_length <= BLOCK_SIZE_BYTES {
                    // get entire response
                    let (body_sender, body_receiver) = oneshot::channel();
                    let _ = body_request_sender
                        .send(
                            PageBodyRequest {
                                row_id,
                                byte_range: (0..BLOCK_SIZE_BYTES),
                                body_sender,
                            }
                        )
                        .await;

                    let body_vec = body_receiver
                        .await
                        .expect("body received")
                        .expect("no db errors");
                    Body::from(body_vec)
                } else {
                    // return chunked response
                    let body_stream = async_stream::stream! {
                        let mut next_start = 0_usize;

                        while next_start < content_length {
                            let (body_sender, body_receiver) = oneshot::channel();
                            let byte_range = next_start..(next_start + BLOCK_SIZE_BYTES);
                            let _ = body_request_sender.send(PageBodyRequest {
                                    row_id,
                                    byte_range,
                                    body_sender,
                                })
                                .await;

                            let body_vec = body_receiver.await.expect("body received").expect("no db errors");
                            let bytes = Bytes::from(body_vec);
                            yield Result::<Bytes, std::io::Error>::Ok(bytes);
                            next_start += BLOCK_SIZE_BYTES;
                        };
                    };

                    Body::wrap_stream(body_stream)
                };

                Response::builder()
                    .header(header::CONTENT_TYPE, ct)
                    .header(header::LAST_MODIFIED, lm)
                    .body(body)
            }
        }
    }
}

struct PageInfo {
    last_modified_uxt: u64,
    content_type: String,
    content_length: usize,
    row_id: i64,
}

impl PageInfo {
    fn format_last_modified_timestamp(&self) -> String {
        let last_modified_systime = SystemTime::UNIX_EPOCH.add(Duration::from_secs(self.last_modified_uxt));
        let http_date = HttpDate::from(last_modified_systime);
        http_date.to_string()
    }
}

async fn get_response(
    repository: Repository,
    req: Request<Body>,
) -> hyper::http::Result<Response<Body>> {
    // only allow GET methods
    match (req.method(), req.uri().path()) {
        (&Method::GET, path) => repository.get_response_for_path(path).await,
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty()),
    }
}

#[tokio::main]
async fn main() {
    let (info_request_sender, info_request_receiver) = mpsc::channel::<PageInfoRequest>(4);
    let (body_request_sender, body_request_receiver) = mpsc::channel::<PageBodyRequest>(4);
    let repository = Repository::new(info_request_sender, body_request_sender);

    // threads to make blocking calls to the database
    let _ = spawn_blocking(move || db_read_page_task(info_request_receiver));

    let _ = spawn_blocking(move || db_read_body_task(body_request_receiver));

    // A `Service` is needed for every connection, so this
    // creates one
    let make_service = make_service_fn(|_conn: &AddrStream| {
        let repository = repository.clone();
        let service = service_fn(move |req| get_response(repository.clone(), req));

        // return the service to hyper
        async move { Ok::<_, Infallible>(service) }
    });

    // We'll bind to 127.0.0.1:8080
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let server = Server::bind(&addr).serve(make_service);

    // Run this server for... forever!
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

fn db_read_page_task(mut info_request_receiver: mpsc::Receiver<PageInfoRequest>) {
    let conn = Connection::open_with_flags(
        "site.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("connect to db");

    while let Some(request) = info_request_receiver.blocking_recv() {
        let page = get_page_info_from_database(&request.path, &conn);
        let _ = request.info_sender.send(page);
    }
}

fn db_read_body_task(mut body_request_receiver: mpsc::Receiver<PageBodyRequest>) {
    let conn = Connection::open_with_flags(
        "site.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("connect to db");

    while let Some(request) = body_request_receiver.blocking_recv() {
        let body = get_body_buf_from_db(request.row_id, request.byte_range, &conn);
        let _ = request.body_sender.send(body);
    }
}

fn get_page_info_from_database(
    path: &str,
    conn: &Connection,
) -> rusqlite::Result<Option<PageInfo>> {
    let mut stmt = conn
        .prepare("select last_modified_uxt, content_type, rowid, length(body) as content_length from pages where path = ?")
        .expect("SQL statement prepared");

    stmt.query_row([path], |row| {
        let lm = row.get(0)?;
        let ct = row.get(1)?;
        let row_id = row.get(2)?;
        let content_length = row.get(3)?;

        Ok(PageInfo {
            last_modified_uxt: lm ,
            content_type: ct,
            content_length,
            row_id,
        })
    })
    .optional()
}

fn get_body_buf_from_db(
    row_id: i64,
    byte_range: Range<usize>,
    conn: &Connection,
) -> rusqlite::Result<Vec<u8>> {
    let body_blob: Blob = conn.blob_open(DatabaseName::Main, "pages", "body", row_id, true)?;

    let mut buf = Vec::<u8>::with_capacity(byte_range.len());
    buf.resize(byte_range.len(), 0);

    let read_length = body_blob.read_at(&mut buf, byte_range.start)?;

    buf.resize(read_length, 0);
    Ok(buf)
}
