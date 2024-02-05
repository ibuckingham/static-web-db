use httpdate::HttpDate;
use rusqlite::blob::Blob;
use rusqlite::{Connection, DatabaseName, OpenFlags, OptionalExtension, Statement};
use std::ops::{Add, Range};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

const BLOCK_SIZE_BYTES: usize = 16 * 1024;

pub struct PageInfoRequest {
    pub path: String,
    pub info_sender: oneshot::Sender<rusqlite::Result<Option<PageInfo>>>,
    pub body_sender: tokio::sync::mpsc::UnboundedSender<Box<[u8]>>,
}

pub struct PageInfo {
    pub content_type: String,
    pub content_length: usize,
    row_id: i64,
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

    let mut stmt = conn
        .prepare("select last_modified_uxt, content_type, rowid, length(body) as content_length from pages where path = ?")
        .expect("SQL statement prepared");

    while let Ok(request) = info_request_receiver.recv() {
        // println!("info from {i}");
        handle_resource_request(&conn, &mut stmt, request);
    }
}

fn handle_resource_request(conn: &Connection, mut stmt: &mut Statement, request: PageInfoRequest) {
    let page = get_page_info_from_database(&mut stmt, &request.path);
    match page {
        Ok(Some(info)) => {
            let row_id = info.row_id;
            let content_length = info.content_length;

            if request.info_sender.send(Ok(Some(info))).is_err() {
                return;
            };

            send_content_blocks(&conn, request.body_sender, row_id, content_length);
        }
        _ => {
            let _ignore_dropped_receiver = request.info_sender.send(page);
        }
    }
}

fn send_content_blocks(
    conn: &Connection,
    sender: UnboundedSender<Box<[u8]>>,
    row_id: i64,
    content_length: usize,
) {
    let mut next_start = 0_usize;

    while next_start < content_length {
        let next_end = std::cmp::min(next_start + BLOCK_SIZE_BYTES, content_length);
        let byte_range = next_start..next_end;
        let maybe_body = get_body_buf_from_db(conn, row_id, byte_range);
        match maybe_body {
            Ok(body) => {
                let actual_length = body.len();
                if actual_length == 0 || sender.send(body).is_err() {
                    // nothing listening, so stop sending
                    return;
                };
                next_start += actual_length;
            }
            _ => {
                // TODO : db error while reading BLOB, stop sending - and report somehow?
                // probably should immediately close the connection,
                // so the 0-length terminating chunk isn't sent
                // and the client is aware there was an interruption
                return;
            }
        }
    }
}

fn get_page_info_from_database(
    stmt: &mut Statement,
    path: &str,
) -> rusqlite::Result<Option<PageInfo>> {
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
    conn: &Connection,
    row_id: i64,
    byte_range: Range<usize>,
) -> rusqlite::Result<Box<[u8]>> {
    let body_blob: Blob = conn.blob_open(DatabaseName::Main, "pages", "body", row_id, true)?;

    let mut buf = vec![0u8; byte_range.len()];
    let read_length = body_blob.read_at(&mut buf, byte_range.start)?;
    // log partial reads?

    buf.resize(read_length, 0);

    // if there was a partial read then this may (will?) first copy to a smaller buffer
    Ok(buf.into_boxed_slice())
}
