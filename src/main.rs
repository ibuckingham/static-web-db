use chrono::{DateTime, NaiveDateTime, Utc};
use hyper::body::Bytes;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server, StatusCode};
use rusqlite::blob::Blob;
use rusqlite::{Connection, DatabaseName, OpenFlags, OptionalExtension};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
struct Repository {
    sql_request_sender: mpsc::Sender<SqlRequest>,
    body_request_sender: mpsc::Sender<BodyRequest>,
}

struct SqlRequest {
    path: String,
    page_sender: oneshot::Sender<rusqlite::Result<Option<PageInfo>>>,
}

struct BodyRequest {
    row_id: i64,
    content_length: usize,
    body_sender: oneshot::Sender<rusqlite::Result<Vec<u8>>>,
}

impl Repository {
    fn new(sql_request_sender: mpsc::Sender<SqlRequest>, body_request_sender: mpsc::Sender<BodyRequest>) -> Repository {
        Repository {
            sql_request_sender,
            body_request_sender,
        }
    }

    async fn get_response_for_path(&self, path: &str) -> hyper::http::Result<Response<Body>> {
        let (page_sender, page_receiver) = oneshot::channel();

        let _ = &self
            .sql_request_sender
            .clone()
            .send(SqlRequest {
                path: path.to_string(),
                page_sender,
            })
            .await;

        let page_response = page_receiver.await.expect("received something");

        match page_response {
            Err(e) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(e.to_string())),
            Ok(None) => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty()),
            Ok(Some(info)) => {
                let (body_sender, body_receiver) = oneshot::channel();

                let _ = &self
                    .body_request_sender
                    .clone()
                    .send(BodyRequest {
                        row_id: info.row_id,
                        content_length: info.content_length,
                        body_sender,
                    })
                    .await;

                let ct = &info.content_type;
                let lm = info.format_last_modified_timestamp();

                let body_vec = body_receiver.await.expect("body received").expect("no db errors");
                let bytes = Bytes::from(body_vec);

                // how to convert from channel to stream?
                let body_stream = async_stream::stream! {
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

struct PageInfo {
    last_modified_uxt: i64,
    content_type: String,
    content_length: usize,
    row_id: i64,
}

impl PageInfo {
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
    let (sql_request_sender, mut sql_request_receiver) = mpsc::channel::<SqlRequest>(1);
    let (body_request_sender, mut body_request_receiver) = mpsc::channel::<BodyRequest>(1);
    let repository = Repository::new(sql_request_sender, body_request_sender);

    let _page_info_sender = tokio::spawn(async move {
        let conn = Connection::open_with_flags(
            "site.db",
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
            .expect("connect to db");

        while let Some(request) = sql_request_receiver.recv().await {
            let page_recvd = get_page_info_from_database(&request.path, &conn);
            let _ = request.page_sender.send(page_recvd);
        }
    });

    let _body_sender = tokio::spawn(async move {
        let conn = Connection::open_with_flags(
            "site.db",
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
            .expect("connect to db");

        while let Some(request) = body_request_receiver.recv().await {
            let body = get_body_buf_from_db(request.row_id, request.content_length, &conn);
            let _ = request.body_sender.send(body);
        }
    });

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

fn get_page_info_from_database(path: &str, conn: &Connection) -> rusqlite::Result<Option<PageInfo>> {
    let mut stmt = conn
        .prepare("select last_modified_uxt, content_type, rowid, length(body) as content_length from pages where path = ?")
        .expect("SQL statement preparable");

    stmt.query_row([path], |row| {
        let lm = row.get(0)?;
        let ct = row.get(1)?;
        let row_id = row.get(2)?;
        let content_length = row.get(3)?;

        Ok(PageInfo {
            last_modified_uxt: lm,
            content_type: ct,
            content_length,
            row_id,
        })
    })
        .optional()
}

fn get_body_buf_from_db(row_id: i64, content_length: usize, conn: &Connection) -> rusqlite::Result<Vec<u8>> {
    let mut buf = Vec::<u8>::with_capacity(content_length);
    let body_blob: Blob = conn
        .blob_open(DatabaseName::Main, "pages", "body", row_id, true)?;

    buf.resize(content_length, 0);
    let read_length = body_blob.read_at(&mut buf, 0)?;
    buf.resize(read_length, 0);
    Ok(buf)
}
