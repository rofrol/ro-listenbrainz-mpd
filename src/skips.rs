//! Fork: songs changed to another song before their end, without counting as a
//! listen, are appended as JSON lines to `skips.jsonl` next to the submission
//! cache. musicdb imports them; nothing is sent to ListenBrainz.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use mpd_client::{responses::Song, tag::Tag};
use serde_json::json;
use tracing::{debug, warn};

fn path() -> PathBuf {
    let mut p = dirs::data_local_dir().expect("No state/cache directory");
    p.push(crate::STORAGE_DIR);
    p.push("skips.jsonl");
    p
}

fn secs(d: Duration) -> f64 {
    (d.as_secs_f64() * 10.0).round() / 10.0
}

/// Append one skip: where the song was left and how long its last uninterrupted run was.
pub fn record(song: &Song, duration: Duration, position: Duration, run: Duration) {
    let line = json!({
        "ts": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        "file": song.url,
        "mbid": song.tags.get(&Tag::MusicBrainzRecordingId).and_then(|v| v.first()),
        "position_s": secs(position),
        "duration_s": secs(duration),
        "run_s": secs(run),
    });
    let path = path();
    // One write of a whole line in append mode: lines never interleave.
    let written = fs::create_dir_all(path.parent().unwrap()).and_then(|()| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?
            .write_all(format!("{line}\n").as_bytes())
    });
    match written {
        Ok(()) => debug!(song = %song.url, ?position, "recorded skip"),
        Err(error) => warn!(?error, path = %path.display(), "cannot record skip"),
    }
}
