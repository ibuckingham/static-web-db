use async_stream::stream;
use chrono::{DateTime, NaiveDateTime, Utc};
use hyper::body::Bytes;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server, StatusCode};
use rusqlite::blob::Blob;
use rusqlite::{Connection, DatabaseName, OpenFlags, OptionalExtension};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Clone)]
struct Repository {
    // Std::Mutex means grabbing a lock on every request
    // Tokio::Mutex might be more concurrent alternative, if needed
    // ... or create multiple connections in a pool?
    conn_shared: Arc<Mutex<Connection>>,
}

struct SqlRequest {
    page_sender: oneshot::Sender<PageContent>,
}

impl Repository {
    fn new() -> Repository {
        let conn = Connection::open_with_flags(
            "site.db",
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .expect("connect to db");

        Repository {
            conn_shared: Arc::new(Mutex::new(conn)),
        }
    }

    async fn get_response_for_path(&self, path: &str) -> hyper::http::Result<Response<Body>> {
        let conn = &self.conn_shared;
        let conn_locked = conn.lock().await;

        let (sql_request_sender, mut sql_request_receiver) = mpsc::channel::<SqlRequest>(1);

        let page_data = {
            let mut stmt = conn_locked
                .prepare("select last_modified_uxt, content_type, rowid, length(body) as content_length from pages where path = ?")
                .expect("SQL statement preparable");

            stmt.query_row([path], |row| {
                let lm = row.get(0)?;
                let ct = row.get(1)?;
                let row_id = row.get(2)?;
                let content_length = row.get(3)?;

                Ok(PageContent {
                    last_modified_uxt: lm,
                    content_type: ct,
                    content_length: content_length,
                    row_id: row_id,
                    body: Vec::new(),
                })
            })
            .optional()
        };

        match page_data {
            Err(e) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(e.to_string())),
            Ok(None) => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty()),
            Ok(Some(mut page)) => {
                {
                    let body_blob: Blob = conn_locked
                        .blob_open(DatabaseName::Main, "pages", "body", page.row_id, true)
                        .expect("open body blob");

                    let mut buf: Vec<u8> = Vec::with_capacity(page.content_length);
                    buf.resize(page.content_length, 0);
                    let read_length = body_blob.read_at(&mut buf, 0).expect("read body blob");
                    buf.resize(read_length, 0);
                    page.body = buf;

                    let _t_sender = tokio::spawn(async move {
                        if let Some(request) = sql_request_receiver.recv().await {
                            let _ = request.page_sender.send(page);
                        }
                    });
                };

                let (page_sender, page_receiver) = oneshot::channel();
                let _ = sql_request_sender
                    .send(SqlRequest {
                        page_sender: page_sender,
                    })
                    .await;
                let page_response = page_receiver.await.expect("received buffer");

                let ct = &page_response.content_type;
                let lm = page_response.format_last_modified_timestamp();
                let bytes = Bytes::from(page_response.body);

                let body_stream = stream! {
                    yield Result::<Bytes, std::io::Error>::Ok(bytes);
                };

                let body = Body::wrap_stream(body_stream);
                Response::builder()
                    .header(header::CONTENT_TYPE, ct)
                    .header(header::LAST_MODIFIED, lm)
                    .body(body)
            }
        }
    }
}

struct PageContent {
    last_modified_uxt: i64,
    content_type: String,
    content_length: usize,
    row_id: i64,
    body: Vec<u8>,
}

impl PageContent {
    fn format_last_modified_timestamp(&self) -> String {
        let last_modified_timestamp = NaiveDateTime::from_timestamp_opt(self.last_modified_uxt, 0)
            .expect("timestamp i64 within range");
        let last_modified_date_time: DateTime<Utc> =
            DateTime::from_utc(last_modified_timestamp, Utc);

        last_modified_date_time
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string()
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
    let repository = Repository::new();

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
