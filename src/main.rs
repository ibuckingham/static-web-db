use chrono::{DateTime, NaiveDateTime, Utc};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server, StatusCode};
use rusqlite::{Connection, OptionalExtension};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Repository {
    // Std::Mutex means grabbing a lock on every request
    // Tokio::Mutex might be more concurrent alternative, if needed
    // ... or create multiple connections in a pool?
    conn_shared: Arc<Mutex<Connection>>,
}

impl Repository {
    fn new() -> Repository {
        let conn = Connection::open("site.db").expect("connect to db");

        Repository {
            conn_shared: Arc::new(Mutex::new(conn)),
        }
    }

    fn get_response_for_path(&self, path: &str) -> hyper::http::Result<Response<Body>> {
        let last_modified_text = Self::query_last_modified(&self.conn_shared);

        match Self::query_body(&self.conn_shared, path) {
            Err(_) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty()),
            Ok(None) => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty()),
            Ok(Some(body)) => Response::builder()
                .header(header::CONTENT_TYPE, "text/html; charset=UTF-8")
                .header(header::LAST_MODIFIED, last_modified_text)
                .body(Body::from(body)),
        }
    }

    fn query_last_modified(conn: &Arc<Mutex<Connection>>) -> String {
        let last_modified_uxt = Self::query_last_modified_utx(conn);

        let last_modified_timestamp = NaiveDateTime::from_timestamp_opt(last_modified_uxt, 0)
            .expect("timestamp i64 within range");
        let last_modified_date_time: DateTime<Utc> =
            DateTime::from_utc(last_modified_timestamp, Utc);

        last_modified_date_time
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string()
    }

    fn query_last_modified_utx(conn: &Arc<Mutex<Connection>>) -> i64 {
        let conn_locked = conn
            .lock().unwrap();

        let mut stmt = conn_locked
            .prepare("select last_modified_uxt from properties")
            .expect("SQL statement preparable");

        stmt.query_row([], |row| row.get(0)).expect("db queryable")
    }

    fn query_body(conn: &Arc<Mutex<Connection>>, path: &str) -> rusqlite::Result<Option<String>> {
        let conn_locked = conn
            .lock().unwrap();

        let mut stmt = conn_locked
            .prepare("select body from pages where path = ?")
            .expect("SQL statement preparable");

        stmt.query_row([path], |row| row.get(0)).optional()
    }
}

async fn get_response(
    repository: Repository,
    req: Request<Body>,
) -> hyper::http::Result<Response<Body>> {
    // only allow GET methods
    match (req.method(), req.uri().path()) {
        (&Method::GET, path) => repository.get_response_for_path(path),
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
