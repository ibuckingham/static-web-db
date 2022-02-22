use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::net::SocketAddr;

async fn get_response(req: Request<Body>) -> hyper::http::Result<Response<Body>> {
    // only allow GET methods
    match (req.method(), req.uri().path()) {
        (&Method::GET, path) => get_get_response(path),
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty()),
    }
}

fn get_get_response(path: &str) -> hyper::http::Result<Response<Body>> {
    match path {
        "/" | "/index" => Response::builder()
            .header(header::CONTENT_TYPE, "text/html; charset=UTF-8")
            .body(Body::from(
                "<!DOCTYPE html><title>Iain's Blog</title><p>index of posts",
            )),
        "/about" => Response::builder()
            .header(header::CONTENT_TYPE, "text/html; charset=UTF-8")
            .body(Body::from(
                "<!DOCTYPE html><title>Iain's Blog</title><p>About this blog",
            )),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty()),
    }
}

#[tokio::main]
async fn main() {
    // We'll bind to 127.0.0.1:8080
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    // A `Service` is needed for every connection, so this
    // creates one
    let make_svc = make_service_fn(|_conn| async {
        // service_fn converts our function into a `Service`
        Ok::<_, Infallible>(service_fn(get_response))
    });

    let server = Server::bind(&addr).serve(make_svc);

    // Run this server for... forever!
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
