//! Fork: the sent listen of the current play, kept in the submission cache's
//! database, so a daemon restarted in the middle of a song does not send that
//! play's listen a second time (and `status.json` shows it as `sent`).
//!
//! While a play whose listen was sent stays current, each change the daemon
//! sees (play state, seek, …) updates its row with the last observed position
//! and when it was observed. After a restart, the play MPD reports is the same
//! play if it is the same queue entry (MPD's song id and file), playback is
//! not stopped, and MPD could not have played the song to its end and started
//! it again in the time since that observation: see `is_same_play`.

use std::time::Duration;

use mpd_client::responses::PlayState;
use rusqlite::{Connection, OptionalExtension};

/// A new table, so this is also the migration of an existing database.
pub const SCHEMA: &str = "create table if not exists sent_listens
(
    instance text not null,
    play integer not null,
    song_id integer not null,
    file text not null,
    duration_s real,
    listened_at integer not null,
    manual integer not null,
    position_s real not null,
    playing integer not null,
    seen_at real not null,
    primary key (instance, play)
)";

/// Rows observed longer ago than this are deleted: only the latest one is ever
/// read, the rest stay for a day to help debugging.
const KEEP_SECS: f64 = 24.0 * 60.0 * 60.0;

/// A play whose listen was sent, as last observed.
#[derive(Debug, Clone, PartialEq)]
pub struct SentPlay {
    /// The daemon run and play that observed it (`status.json`'s `instance`
    /// and `play`); one row per play.
    pub instance: String,
    pub play: u64,
    /// MPD's song id (the queue entry) and file.
    pub song_id: u64,
    pub file: String,
    pub duration: Option<Duration>,
    /// The listen's timestamp as sent to ListenBrainz.
    pub listened_at: u64,
    pub manual: bool,
    /// Playback position, play state and Unix time of the last observation.
    pub position: Duration,
    pub playing: bool,
    pub seen_at: f64,
}

/// Insert or update the play's row and delete old rows.
pub fn store(db: &Connection, sent: &SentPlay) -> rusqlite::Result<()> {
    db.prepare_cached(
        "insert or replace into sent_listens (instance, play, song_id, file, duration_s, \
         listened_at, manual, position_s, playing, seen_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?
    .execute((
        &sent.instance,
        sent.play as i64,
        sent.song_id as i64,
        &sent.file,
        sent.duration.map(|d| d.as_secs_f64()),
        sent.listened_at as i64,
        sent.manual,
        sent.position.as_secs_f64(),
        sent.playing,
        sent.seen_at,
    ))?;
    db.prepare_cached("delete from sent_listens where seen_at < ?")?
        .execute((sent.seen_at - KEEP_SECS,))?;
    Ok(())
}

/// The most recently observed sent play.
pub fn last(db: &Connection) -> rusqlite::Result<Option<SentPlay>> {
    db.query_row(
        "select instance, play, song_id, file, duration_s, listened_at, manual, position_s, \
         playing, seen_at from sent_listens order by seen_at desc limit 1",
        (),
        |row| {
            Ok(SentPlay {
                instance: row.get(0)?,
                play: row.get::<_, i64>(1)? as u64,
                song_id: row.get::<_, i64>(2)? as u64,
                file: row.get(3)?,
                duration: row.get::<_, Option<f64>>(4)?.map(Duration::from_secs_f64),
                listened_at: row.get::<_, i64>(5)? as u64,
                manual: row.get(6)?,
                position: Duration::from_secs_f64(row.get(7)?),
                playing: row.get(8)?,
                seen_at: row.get(9)?,
            })
        },
    )
    .optional()
}

/// Whether MPD now playing (or paused on) `song_id`/`file` at `position`, seen
/// at Unix time `now`, continues the sent play: the same queue entry, not
/// stopped, and either still at the observed position (a pause) or too little
/// time has passed for the rest of the song plus `position` to have played,
/// i.e. it cannot be the song started again (repeat, or the queue played round).
/// A seek in either direction stays the same play, as it does while the daemon
/// runs.
pub fn is_same_play(
    sent: &SentPlay,
    song_id: u64,
    file: &str,
    play_state: PlayState,
    position: Duration,
    now: f64,
) -> bool {
    if play_state == PlayState::Stopped || sent.song_id != song_id || sent.file != file {
        return false;
    }
    let tolerance = crate::SEEK_TOLERANCE;
    if !sent.playing && position.abs_diff(sent.position) <= tolerance {
        return true;
    }
    // unknown duration: assume the song could have ended right away
    let rest = sent
        .duration
        .map_or(Duration::ZERO, |d| d.saturating_sub(sent.position));
    let since = Duration::from_secs_f64((now - sent.seen_at).max(0.0));
    since + tolerance < rest + position
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// Sent at 120 s of a 200 s song, observed playing at Unix time 1000.
    fn sent() -> SentPlay {
        SentPlay {
            instance: "1-2".to_owned(),
            play: 1,
            song_id: 7,
            file: "a.mp3".to_owned(),
            duration: Some(s(200)),
            listened_at: 880,
            manual: false,
            position: s(120),
            playing: true,
            seen_at: 1000.0,
        }
    }

    #[test]
    fn the_table_is_added_to_an_existing_database_and_pruned() {
        let db = Connection::open_in_memory().unwrap();
        // the database as the upstream daemon leaves it
        db.execute(
            "create table pending_submissions (id integer primary key, submission text not null)",
            (),
        )
        .unwrap();
        db.execute(
            "insert into pending_submissions (submission) values ('{}')",
            (),
        )
        .unwrap();
        db.execute(SCHEMA, ()).unwrap();
        db.execute(SCHEMA, ()).unwrap(); // every start runs it again
        assert_eq!(last(&db).unwrap(), None);

        let old = SentPlay {
            duration: None,
            manual: true,
            ..sent()
        };
        store(&db, &old).unwrap();
        assert_eq!(last(&db).unwrap(), Some(old.clone()));
        let newer = SentPlay {
            play: 2,
            seen_at: 1000.0 + KEEP_SECS + 1.0,
            ..sent()
        };
        store(&db, &newer).unwrap();
        assert_eq!(last(&db).unwrap(), Some(newer.clone()));
        let rows: i64 = db
            .query_row("select count(*) from sent_listens", (), |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1, "the day-old row is gone");
        let updated = SentPlay {
            position: s(150),
            ..newer
        };
        store(&db, &updated).unwrap();
        assert_eq!(last(&db).unwrap(), Some(updated), "one row per play");
        let pending: i64 = db
            .query_row("select count(*) from pending_submissions", (), |r| r.get(0))
            .unwrap();
        assert_eq!(pending, 1);
    }

    #[test]
    fn a_restart_continues_the_play() {
        let p = PlayState::Playing;
        assert!(is_same_play(&sent(), 7, "a.mp3", p, s(125), 1005.0));
        assert!(
            is_same_play(&sent(), 7, "a.mp3", p, s(20), 1005.0),
            "a seek back while the daemon was down"
        );
        assert!(!is_same_play(&sent(), 8, "a.mp3", p, s(125), 1005.0));
        assert!(!is_same_play(&sent(), 7, "b.mp3", p, s(125), 1005.0));
        assert!(!is_same_play(
            &sent(),
            7,
            "a.mp3",
            PlayState::Stopped,
            s(125),
            1005.0
        ));
    }

    #[test]
    fn a_replay_is_a_new_play() {
        // 80 s to the end, then 10 s of the replay: 90 s
        assert!(!is_same_play(
            &sent(),
            7,
            "a.mp3",
            PlayState::Playing,
            s(10),
            1090.0
        ));
        assert!(is_same_play(
            &sent(),
            7,
            "a.mp3",
            PlayState::Playing,
            s(10),
            1080.0
        ));
    }

    #[test]
    fn a_long_pause_is_the_same_play() {
        let paused = SentPlay {
            playing: false,
            ..sent()
        };
        assert!(is_same_play(
            &paused,
            7,
            "a.mp3",
            PlayState::Paused,
            s(120),
            5000.0
        ));
        assert!(is_same_play(
            &paused,
            7,
            "a.mp3",
            PlayState::Playing,
            s(121),
            5000.0
        ));
        assert!(
            !is_same_play(&paused, 7, "a.mp3", PlayState::Playing, s(30), 5000.0),
            "long enough for the rest and a replay"
        );
    }
}
