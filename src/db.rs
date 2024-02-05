use httpdate::HttpDate;
use rusqlite::blob::Blob;
use rusqlite::{Connection, DatabaseName, OpenFlags, OptionalExtension, Statement};
use std::ops::{Add, Range};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

const BLOCK_SIZE_BYTES: usize = 16 * 1024;

pub struct ResourceRequest {
    pub path: String,
    pub meta_sender: oneshot::Sender<rusqlite::Result<Option<ResourceMeta>>>,
    pub body_sender: tokio::sync::mpsc::UnboundedSender<Box<[u8]>>,
}

pub struct ResourceMeta {
    pub content_type: String,
    pub content_length: usize,
    row_id: i64,
    last_modified_uxt: u64,
}

impl ResourceMeta {
    pub fn format_last_modified_timestamp(&self) -> String {
        let last_modified_systime =
            SystemTime::UNIX_EPOCH.add(Duration::from_secs(self.last_modified_uxt));
        let http_date = HttpDate::from(last_modified_systime);
        http_date.to_string()
    }
}

pub fn send_resources_for_path_task(
    _i: i32,
    resource_request_receiver: flume::Receiver<ResourceRequest>,
) {
    let conn = Connection::open_with_flags(
        "file:site.db?immutable=1",
        OpenFlags::SQLITE_OPEN_URI | OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("connect to db");

    let mut stmt = conn
        .prepare("select last_modified_uxt, content_type, rowid, length(body) as content_length from pages where path = ?")
        .expect("SQL statement prepared");

    while let Ok(request) = resource_request_receiver.recv() {
        // println!("info from {i}");
        handle_resource_request(&conn, &mut stmt, request);
    }
}

fn handle_resource_request(conn: &Connection, mut stmt: &mut Statement, request: ResourceRequest) {
    let meta = get_meta_for_path(&mut stmt, &request.path);
    match meta {
        Ok(Some(meta)) => {
            let row_id = meta.row_id;
            let content_length = meta.content_length;

            if request.meta_sender.send(Ok(Some(meta))).is_err() {
                return;
            };

            send_body_blocks(&conn, request.body_sender, row_id, content_length);
        }
        _ => {
            let _ignore_dropped_receiver = request.meta_sender.send(meta);
        }
    }
}

fn send_body_blocks(
    conn: &Connection,
    sender: UnboundedSender<Box<[u8]>>,
    row_id: i64,
    content_length: usize,
) {
    let mut next_start = 0_usize;

    while next_start < content_length {
        let next_end = std::cmp::min(next_start + BLOCK_SIZE_BYTES, content_length);
        let byte_range = next_start..next_end;
        let maybe_body = get_body_range_for_row(conn, row_id, byte_range);
        match maybe_body {
            Ok(body_block) => {
                let actual_length = body_block.len();
                if actual_length == 0 || sender.send(body_block).is_err() {
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

fn get_meta_for_path(stmt: &mut Statement, path: &str) -> rusqlite::Result<Option<ResourceMeta>> {
    stmt.query_row([path], |row| {
        let lm = row.get(0)?;
        let ct = row.get(1)?;
        let row_id = row.get(2)?;
        let content_length = row.get(3)?;

        Ok(ResourceMeta {
            last_modified_uxt: lm,
            content_type: ct,
            content_length,
            row_id,
        })
    })
    .optional()
}

fn get_body_range_for_row(
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
