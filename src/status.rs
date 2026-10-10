//! Fork: `status.json` next to the submission cache, the current play's
//! progress toward a listen as this daemon counts it, so a client (rormpc)
//! shows it without re-implementing the listen rule. Written on each change
//! (song, play state, seek, sent, a manual request's answer), never on a timer:
//! while playing, a client adds MPD's elapsed time since `position_s` to
//! `counted_s`. Replaced atomically (a temporary file renamed over it), so a
//! reader never sees half a file; watch its directory, not its inode.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;
use tracing::{debug, warn};

/// Format of `status.json`; bump it when a field changes meaning.
pub const VERSION: u32 = 1;

/// MPD channel on which the daemon takes `submit <instance> <play>`: send the
/// play's listen now, whatever the rule says. `instance` and `play` come from
/// `status.json`, so a request meant for an earlier play (or an earlier daemon)
/// is refused rather than sent for the song playing now.
pub const LISTEN_CHANNEL: &str = "listenbrainz_listen";

fn path() -> PathBuf {
    // tests never touch the machine's status file
    #[cfg(test)]
    let mut p = std::env::temp_dir().join(format!("ro-lb-mpd-status-{}", std::process::id()));
    #[cfg(not(test))]
    let mut p = dirs::data_local_dir().expect("No state/cache directory");
    p.push(crate::STORAGE_DIR);
    p.push("status.json");
    p
}

/// Replace `status.json` with `status`.
pub fn write(status: &Value) {
    write_to(&path(), status);
}

fn write_to(path: &Path, status: &Value) {
    let tmp = path.with_extension("json.tmp");
    let written = fs::create_dir_all(path.parent().unwrap())
        .and_then(|()| fs::write(&tmp, format!("{status}\n")))
        .and_then(|()| fs::rename(&tmp, path));
    match written {
        Ok(()) => debug!(%status, "status written"),
        Err(error) => warn!(?error, path = %path.display(), "cannot write the status"),
    }
}

/// A manual send request, `submit <instance> <play>`.
pub fn parse_submit(message: &str) -> Option<(&str, u64)> {
    let mut words = message.split_whitespace();
    let (Some("submit"), Some(instance), Some(play), None) =
        (words.next(), words.next(), words.next(), words.next())
    else {
        return None;
    };
    Some((instance, play.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn write_replaces_the_whole_file() {
        let dir =
            std::env::temp_dir().join(format!("ro-lb-mpd-status-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let file = dir.join("sub").join("status.json");
        write_to(&file, &json!({"play": 1}));
        write_to(&file, &json!({"play": 2}));
        let read: Value = serde_json::from_str(&fs::read_to_string(&file).unwrap()).unwrap();
        assert_eq!(read, json!({"play": 2}));
        assert!(!file.with_extension("json.tmp").exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn submit_needs_the_instance_and_the_play() {
        assert_eq!(parse_submit("submit 12-345 7"), Some(("12-345", 7)));
        assert_eq!(parse_submit("submit 12-345"), None);
        assert_eq!(parse_submit("submit 12-345 x"), None);
        assert_eq!(parse_submit("submit 12-345 7 8"), None);
        assert_eq!(parse_submit("love"), None);
    }
}
