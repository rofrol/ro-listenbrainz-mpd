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
    path::{Path, PathBuf},
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
fn append(path: &Path, line: &Value) {
    let written = fs::create_dir_all(path.parent().unwrap()).and_then(|()| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?
            .write_all(format!("{line}\n").as_bytes())
    });
    match written {
        Ok(()) => debug!(%line, path = %path.display(), "logged"),
        Err(error) => warn!(?error, path = %path.display(), "cannot write"),
    }
}

/// A listen that counts, with the time it started (the timestamp sent to ListenBrainz).
pub fn record_listen(song: &Song, started: u64) {
    let line = listen_line(&song.url, mbid(song), song.duration, started);
    append(&path("listens.jsonl"), &line);
}

/// A skip: where the song was left and how long its playtime was.
pub fn record_skip(song: &Song, duration: Duration, position: Duration, run: Duration) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let line = skip_line(&song.url, mbid(song), now, duration, position, run);
    append(&path("skips.jsonl"), &line);
}

/// One line of `listens.jsonl`; musicdb's `import-local` reads it.
fn listen_line(
    file: &str,
    mbid: Option<&String>,
    duration: Option<Duration>,
    started: u64,
) -> Value {
    json!({
        "ts": started,
        "file": file,
        "mbid": mbid,
        "duration_s": duration.map(secs),
    })
}

/// One line of `skips.jsonl`; musicdb's `import-skips` reads it.
fn skip_line(
    file: &str,
    mbid: Option<&String>,
    ts: u64,
    duration: Duration,
    position: Duration,
    run: Duration,
) -> Value {
    json!({
        "ts": ts,
        "file": file,
        "mbid": mbid,
        "position_s": secs(position),
        "duration_s": secs(duration),
        "run_s": secs(run),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_line_shape() {
        let mbid = "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string();
        let line = listen_line(
            "yt/a.mp3",
            Some(&mbid),
            Some(Duration::from_millis(213_456)),
            1_790_000_000,
        );
        assert_eq!(
            line,
            json!({"ts": 1_790_000_000, "file": "yt/a.mp3", "mbid": mbid, "duration_s": 213.5})
        );
        let line = listen_line("stream", None, None, 1);
        assert_eq!(
            line,
            json!({"ts": 1, "file": "stream", "mbid": null, "duration_s": null})
        );
    }

    #[test]
    fn skip_line_shape_rounds_to_a_tenth() {
        let line = skip_line(
            "cd/b.flac",
            None,
            1_790_000_100,
            Duration::from_secs(200),
            Duration::from_millis(5_049),
            Duration::from_millis(4_951),
        );
        assert_eq!(
            line,
            json!({"ts": 1_790_000_100, "file": "cd/b.flac", "mbid": null,
                   "position_s": 5.0, "duration_s": 200.0, "run_s": 5.0})
        );
    }

    #[test]
    fn append_writes_whole_lines_and_creates_the_directory() {
        let dir = std::env::temp_dir().join(format!("ro-lb-mpd-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let file = dir.join("sub").join("listens.jsonl");
        append(&file, &listen_line("a", None, None, 1));
        append(&file, &listen_line("b", None, None, 2));
        let text = fs::read_to_string(&file).unwrap();
        let lines: Vec<Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(text.ends_with('\n'));
        assert_eq!(
            lines
                .iter()
                .map(|l| l["file"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        fs::remove_dir_all(&dir).unwrap();
    }
}
