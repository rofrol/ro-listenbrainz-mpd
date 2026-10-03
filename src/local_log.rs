//! Fork: local JSON-lines logs next to the submission cache, read by musicdb;
//! nothing here is sent to ListenBrainz.
//!
//! - `listens.jsonl`: every listen when it counts, so play counts don't depend
//!   on reading ListenBrainz back.
//! - `skips.jsonl`: songs changed to another song before their end without
//!   counting as a listen.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use mpd_client::{responses::Song, tag::Tag};
use serde_json::{Value, json};
use tracing::{debug, warn};

fn path(name: &str) -> PathBuf {
    let mut p = dirs::data_local_dir().expect("No state/cache directory");
    p.push(crate::STORAGE_DIR);
    p.push(name);
    p
}

fn secs(d: Duration) -> f64 {
    (d.as_secs_f64() * 10.0).round() / 10.0
}

fn mbid(song: &Song) -> Option<&String> {
    song.tags.get(&Tag::MusicBrainzRecordingId).and_then(|v| v.first())
}

/// Append one line. One write of a whole line in append mode: lines never interleave.
fn append(name: &str, line: &Value) {
    let path = path(name);
    let written = fs::create_dir_all(path.parent().unwrap()).and_then(|()| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?
            .write_all(format!("{line}\n").as_bytes())
    });
    match written {
        Ok(()) => debug!(%line, "logged to {name}"),
        Err(error) => warn!(?error, path = %path.display(), "cannot write"),
    }
}

/// A listen that counts, with the time it started (the timestamp sent to ListenBrainz).
pub fn record_listen(song: &Song, started: u64) {
    append(
        "listens.jsonl",
        &json!({
            "ts": started,
            "file": song.url,
            "mbid": mbid(song),
            "duration_s": song.duration.map(secs),
        }),
    );
}

/// A skip: where the song was left and how long its playtime was.
pub fn record_skip(song: &Song, duration: Duration, position: Duration, run: Duration) {
    append(
        "skips.jsonl",
        &json!({
            "ts": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            "file": song.url,
            "mbid": mbid(song),
            "position_s": secs(position),
            "duration_s": secs(duration),
            "run_s": secs(run),
        }),
    );
}
