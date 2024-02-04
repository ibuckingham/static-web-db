use rusqlite::{Connection, DatabaseName, OpenFlags, OptionalExtension};
use std::ops::{Add, Range};
use rusqlite::blob::Blob;
use tokio::sync::oneshot;
use std::time::{Duration, SystemTime};
use httpdate::HttpDate;

pub struct PageInfoRequest {
    pub path: String,
    pub info_sender: oneshot::Sender<rusqlite::Result<Option<PageInfo>>>,
}

pub struct PageBodyRequest {
    pub row_id: i64,
    pub byte_range: Range<usize>,
    pub body_sender: oneshot::Sender<rusqlite::Result<Box<[u8]>>>,
}

pub struct PageInfo {
    pub content_type: String,
    pub content_length: usize,
    pub row_id: i64,
    last_modified_uxt: u64,
}

impl PageInfo {
    pub fn format_last_modified_timestamp(&self) -> String {
        let last_modified_systime =
            SystemTime::UNIX_EPOCH.add(Duration::from_secs(self.last_modified_uxt));
        let http_date = HttpDate::from(last_modified_systime);
        http_date.to_string()
    }
}

pub fn db_read_page_infos_task(_i: i32, info_request_receiver: flume::Receiver<PageInfoRequest>) {
    let conn = Connection::open_with_flags(
        "site.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("connect to db");

    while let Ok(request) = info_request_receiver.recv() {
        // println!("info from {i}");
        let page = get_page_info_from_database(&request.path, &conn);
        let _ignore_dropped_receiver = request.info_sender.send(page);
    }
}

pub fn db_read_page_bodies_task(_i: i32, body_request_receiver: flume::Receiver<PageBodyRequest>) {
    let conn = Connection::open_with_flags(
        "site.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("connect to db");

    while let Ok(request) = body_request_receiver.recv() {
        // println!("body from {i}");
        let body = get_body_buf_from_db(request.row_id, request.byte_range, &conn);
        let _ignore_dropped_receiver = request.body_sender.send(body);
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
            last_modified_uxt: lm,
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
) -> rusqlite::Result<Box<[u8]>> {
    let body_blob: Blob = conn.blob_open(DatabaseName::Main, "pages", "body", row_id, true)?;

    let mut buf = vec![0u8; byte_range.len()];
    let read_length = body_blob.read_at(&mut buf, byte_range.start)?;
    // log partial reads?

    buf.resize(read_length, 0);

    // if there was a partial read then this may (will?) first copy to a smaller buffer
    Ok(buf.into_boxed_slice())
}

