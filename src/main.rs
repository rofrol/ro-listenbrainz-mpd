mod cache_actor;
mod cli;
mod config;
mod local_log;
mod status;
mod submission_actor;

use std::{
    net::SocketAddr,
    pin::Pin,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use config::{Configuration, ListenRule};
use mpd_client::{
    client::{Client, ConnectionEvent, ConnectionEvents, Subsystem},
    commands::{self, SingleMode},
    responses::{PlayState, Song, SongInQueue, Status},
    tag::Tag,
};
use serde::{Serialize, Serializer};
use serde_json::{Value, json};
#[cfg(unix)]
use tokio::net::{UnixStream, unix::SocketAddr as UnixSocketAddr};
use tokio::{
    net::TcpStream,
    signal::ctrl_c,
    time::{Sleep, sleep},
};
use tracing::{Instrument, debug, error, info, info_span, level_filters::LevelFilter, trace, warn};
use tracing_subscriber::EnvFilter;

use crate::{
    cache_actor::CacheActor,
    cli::{CliArgs, Feedback},
    config::MpdAddress,
    submission_actor::SubmissionActor,
};

/// How far the playback position may drift from where uninterrupted playback
/// would put it before the change counts as a seek. MPD reports the position
/// with millisecond precision; this only absorbs the latency between the idle
/// notification and our status request.
const SEEK_TOLERANCE: Duration = Duration::from_secs(2);

/// Directory name of the config and the submission cache. Kept as upstream's so
/// the fork (package ro-listenbrainz-mpd) reads the same token and keeps
/// pending listens across the rename.
pub const STORAGE_DIR: &str = "listenbrainz-mpd";

/// Name of the client-to-client channel used to send ListenBrainz feedback
/// commands.
const FEEDBACK_CHANNEL_NAME: &str = "listenbrainz_feedback";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt().with_env_filter(
        EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .with_env_var("LISTENBRAINZ_MPD_LOG")
            .from_env_lossy(),
    );

    // Disable timestamps when running under systemd since journald adds them by
    // itself
    #[cfg(feature = "systemd")]
    subscriber.without_time().init();
    #[cfg(not(feature = "systemd"))]
    subscriber.init();

    let args = CliArgs::parse();

    if args.create_default_config {
        return config::create_default_config();
    }

    let config = config::load(args.config).context("Failed to load configuration")?;

    let listen_rule = config.listen_rule;
    let cache_actor = CacheActor::start(&config)?;
    let (mpd_client, state_changes) = connect(&config).await?;
    let (http_actor, http_actor_handle) = SubmissionActor::start(config, cache_actor);

    if let Some(feedback) = args.send_feedback {
        return send_feedback(mpd_client, feedback).await;
    }

    let res = run(mpd_client, state_changes, http_actor, listen_rule).await;

    #[cfg(feature = "systemd")]
    {
        let _ = sd_notify::notify(&[sd_notify::NotifyState::Stopping]);
    }

    // Wait for actors to exit
    http_actor_handle.await.expect("HTTP actor panicked");

    res
}

async fn send_feedback(mpd_client: Client, feedback: Feedback) -> Result<()> {
    mpd_client
        .command(commands::SendChannelMessage::new(
            FEEDBACK_CHANNEL_NAME,
            feedback.as_command(),
        ))
        .await
        .context("Failed to send feedback message (Is a daemon instance running?)")?;

    Ok(())
}

async fn connect(config: &Configuration) -> Result<(Client, ConnectionEvents)> {
    let password = config.mpd_password.as_deref();

    match &config.mpd_address {
        MpdAddress::Tcp {
            raw_address,
            resolved,
        } => connect_tcp(resolved, password)
            .await
            .with_context(|| format!("Failed to connect to {raw_address:?} via TCP")),
        #[cfg(unix)]
        MpdAddress::Unix(socket) => connect_unix(socket, password)
            .await
            .with_context(|| format!("Failed to connect to Unix socket at {socket:?}")),
    }
}

async fn connect_tcp(
    addrs: &[SocketAddr],
    password: Option<&str>,
) -> Result<(Client, ConnectionEvents)> {
    assert_ne!(addrs.len(), 0);
    debug!(?addrs, "connecting via TCP");

    let socket = TcpStream::connect(addrs).await?;
    Client::connect_with_password_opt(socket, password)
        .await
        .map_err(Into::into)
}

#[cfg(unix)]
async fn connect_unix(
    socket: &UnixSocketAddr,
    password: Option<&str>,
) -> Result<(Client, ConnectionEvents)> {
    debug!(?socket, "connecting via Unix socket");
    let socket = UnixStream::connect_addr(socket).await?;
    Client::connect_with_password_opt(socket, password)
        .await
        .map_err(Into::into)
}

#[derive(Debug)]
struct State {
    /// Current play state of the server.
    play_state: PlayState,
    /// The current playing song, if any.
    song: Option<SongInQueue>,
    /// When a played song counts as a listen.
    rule: ListenRule,
    /// Playback position where the current stretch of playback without a seek
    /// started; a pause doesn't end it.
    segment_start: Duration,
    /// Playtime of the current listen before `segment_start`: always zero with
    /// `rule.uninterrupted`, where a seek starts the count again.
    played_before: Duration,
    /// Last observed playback position and when it was observed, used to tell
    /// a seek from normal playback.
    last_position: Duration,
    last_seen: Instant,
    /// The system timestamp when the listen was started. This is used during
    /// submission to the ListenBrainz API.
    listen_timestamp: SystemTime,
    /// How long the current song has to play to count as a listen, `None` if it
    /// never counts (unknown duration and no `rule.max`).
    listen_required: Option<Duration>,
    /// The future that completes when the required duration is reached.
    listen_finished: Pin<Box<Sleep>>,
    /// `true` if a listen record for the current song has already been
    /// submitted.
    listen_submitted: bool,
    /// Counter for completed listens
    completed_listens: u64,
    /// Fork: this daemon run (`<pid>-<start time>`) and the number of the
    /// current play within it, one per listen started; a manual send names
    /// both (see `status.rs`).
    instance: String,
    play: u64,
    /// `true` if the current play's listen was sent by a manual request.
    submitted_manually: bool,
    /// The answer to the last manual send request, published in `status.json`.
    manual_answer: Option<Value>,
}

impl State {
    fn should_poll(&self) -> bool {
        self.play_state == PlayState::Playing
            && !self.listen_submitted
            && self.listen_required.is_some()
    }
}

async fn run(
    mpd_client: Client,
    mut connection_events: ConnectionEvents,
    http_actor: SubmissionActor,
    rule: ListenRule,
) -> Result<()> {
    // Setup initial state
    let (status, song) = get_status_and_song(&mpd_client).await?;

    let listen_required = required_time_for_song(song.as_ref(), rule);

    // Subscribe to the client-to-client channel used for feedback
    mpd_client
        .command(commands::SubscribeToChannel(FEEDBACK_CHANNEL_NAME))
        .await?;
    mpd_client
        .command(commands::SubscribeToChannel(status::LISTEN_CHANNEL))
        .await?;

    let mut state = State {
        play_state: status.state,
        song,
        rule,
        segment_start: Duration::ZERO,
        played_before: Duration::ZERO,
        last_position: Duration::ZERO,
        last_seen: Instant::now(),
        listen_timestamp: SystemTime::now(),
        listen_required,
        listen_finished: Box::pin(sleep(Duration::ZERO)),
        listen_submitted: false,
        completed_listens: 0,
        instance: format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ),
        play: 0,
        submitted_manually: false,
        manual_answer: None,
    };
    track_position(&mut state, &status, true);

    #[cfg(feature = "systemd")]
    let _ = sd_notify::notify(&[sd_notify::NotifyState::Ready]);

    // Send initial now_playing if we start while a song is playing
    if let Some(song) = &state.song
        && state.play_state == PlayState::Playing
    {
        debug!(
            song = %song.song.url,
            required_playtime = ?listen_required,
            "starting with initial song"
        );
        http_actor.now_playing(song.song.clone());
    }
    publish(&state);

    debug!("entering main loop");

    loop {
        #[cfg(feature = "systemd")]
        let _ = sd_notify::notify(&[sd_notify::NotifyState::Status(&format!(
            "Watching for listens; {} completed",
            state.completed_listens
        ))]);

        tokio::select! {
            event = connection_events.next() => {
                match event {
                    Some(ConnectionEvent::SubsystemChange(subsystem)) => {
                        handle_subsystem_event(
                            subsystem,
                            &mut state,
                            &mpd_client,
                            &http_actor,
                        ).await?;
                    }
                    Some(ConnectionEvent::ConnectionClosed(e)) => {
                        error!(error = ?e, "MPD error");
                        return Err(e.into());
                    }
                    None => {
                        debug!("MPD server closed connection");
                        return Ok(());
                    }
                }
            }
            _ = &mut state.listen_finished, if state.should_poll() => {
                handle_listen_complete(&mut state, &http_actor);
                publish(&state);
            }
            _ = ctrl_c() => {
                debug!("received interrupt");
                return Ok(());
            }
        }
    }
}

async fn handle_subsystem_event(
    subsystem: Subsystem,
    state: &mut State,
    mpd_client: &Client,
    http_actor: &SubmissionActor,
) -> Result<()> {
    trace!(?subsystem, "Subsystem change");
    match subsystem {
        // Something about the player changed (e.g. play state, current song)
        Subsystem::Player | Subsystem::Queue => {
            handle_state_change(state, mpd_client, http_actor.clone()).await
        }
        // Received a message on one of our subscribed channels (for feedback)
        Subsystem::Message => handle_message_event(state, mpd_client, http_actor.clone()).await,
        // Nothing relevant for us
        _ => Ok(()),
    }
}

async fn handle_state_change(
    state: &mut State,
    mpd_client: &Client,
    http_actor: SubmissionActor,
) -> Result<()> {
    let (new_status, new_song) = get_status_and_song(mpd_client).await?;
    let new_play_state = new_status.state;
    let same_song = is_same_song(state.song.as_ref(), new_song.as_ref());

    if !same_song {
        record_skip(state, new_song.is_some());
        start_new_listen(new_song.as_ref(), &new_status, state, &new_play_state, http_actor);
    } else if state.play_state == new_play_state
        && state.listen_submitted
        && is_same_track_on_repeat(&new_status)
    {
        // Apply a heuristic to guess when a single track is being played on repeat.
        trace!("same track is being played on repeat");
        start_new_listen(new_song.as_ref(), &new_status, state, &new_play_state, http_actor);
    } else {
        // Same song: paused, resumed, stopped, seeked, or only player options changed
        let stop_transition = (state.play_state == PlayState::Stopped)
            != (new_play_state == PlayState::Stopped);
        if stop_transition && new_play_state == PlayState::Stopped {
            // If the playback starts again with the same song, count it as a new listen
            trace!("stopped");
            state.listen_submitted = false;
        }
        track_position(state, &new_status, stop_transition);
    }

    state.play_state = new_play_state;
    state.song = new_song;
    publish(state);

    Ok(())
}

/// Start the progress on a new listen and send a "Now playing" notification.
fn start_new_listen(
    new_song: Option<&SongInQueue>,
    new_status: &Status,
    state: &mut State,
    new_play_state: &PlayState,
    http_actor: SubmissionActor,
) {
    let required_playtime = required_time_for_song(new_song, state.rule);
    debug!(
        song = song_url(new_song.map(|s| &s.song)),
        ?required_playtime,
        "song changed"
    );

    state.listen_required = required_playtime;
    state.listen_submitted = false;
    track_position(state, new_status, true);

    if let Some(song) = &new_song
        && *new_play_state == PlayState::Playing
    {
        http_actor.now_playing(song.song.clone());
    }
}

fn handle_listen_complete(state: &mut State, http_actor: &SubmissionActor) {
    info!(
        song = song_url(state.song.as_ref().map(|s| &s.song)),
        "submitting listen entry"
    );
    let (song, timestamp) = claim_listen(state, false).expect("no song to submit");
    local_log::record_listen(&song, timestamp, false);
    http_actor.listen(song, timestamp);
}

/// Mark the current play's listen as sent, once: the timer is disarmed and a
/// second claim (the timer after a manual send, or the other way round) gets
/// `None`. Returns the song and the listen's start, which is the timestamp sent
/// for a manual listen too.
fn claim_listen(state: &mut State, manual: bool) -> Option<(Song, u64)> {
    if state.listen_submitted {
        return None;
    }
    let song = state.song.clone()?.song;
    state.listen_submitted = true;
    state.submitted_manually = manual;
    state.completed_listens += 1;
    let timestamp = state
        .listen_timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Some((song, timestamp))
}

/// Fork: a `submit <instance> <play>` request. Claims the listen if the request
/// names the current play and it was not sent yet; the answer goes to
/// `state.manual_answer`.
fn manual_submit(state: &mut State, message: &str) -> Option<(Song, u64)> {
    let (outcome, play) = match status::parse_submit(message) {
        None => (Err("invalid request"), None),
        Some((instance, play)) if instance != state.instance || play != state.play => {
            (Err("the song changed"), Some(play))
        }
        Some((_, play)) if state.song.is_none() || state.play_state == PlayState::Stopped => {
            (Err("nothing is playing"), Some(play))
        }
        Some((_, play)) if state.listen_submitted => (Err("already sent"), Some(play)),
        Some((_, play)) => (
            claim_listen(state, true).ok_or("nothing is playing"),
            Some(play),
        ),
    };
    // `n` counts the answers, so a client tells its answer from an earlier one
    // with the same content
    let n = state
        .manual_answer
        .as_ref()
        .and_then(|a| a["n"].as_u64())
        .unwrap_or(0)
        + 1;
    state.manual_answer = Some(json!({
        "n": n,
        "play": play,
        "ok": outcome.is_ok(),
        "error": outcome.as_ref().err(),
    }));
    outcome.ok()
}

/// Fork: replace `status.json` with the current play's progress.
fn publish(state: &State) {
    status::write(&status_json(state));
}

/// Where the current play stands: `sent`, `never` (it cannot count: unknown
/// duration and no maximum), `impossible` (not enough of the song is left to
/// reach the required playtime from here, e.g. after a seek with the
/// uninterrupted rule) or `counting`.
fn listen_kind(state: &State) -> &'static str {
    if state.listen_submitted {
        return "sent";
    }
    let Some(required) = state.listen_required else {
        return "never";
    };
    let duration = state.song.as_ref().and_then(|s| s.song.duration);
    match duration {
        Some(duration)
            if state.played_before + duration.saturating_sub(state.segment_start) < required =>
        {
            "impossible"
        }
        _ => "counting",
    }
}

/// `status.json`: see `status.rs`. `counted_s` is the playtime at `position_s`.
fn status_json(state: &State) -> Value {
    let song = state.song.as_ref();
    let rule = state.rule;
    json!({
        "version": status::VERSION,
        "pid": std::process::id(),
        "instance": state.instance,
        "play": state.play,
        "id": song.map(|s| s.id.0),
        "file": song.map(|s| &*s.song.url),
        "duration_s": song.and_then(|s| s.song.duration).map(|d| d.as_secs_f64()),
        "state": match state.play_state {
            PlayState::Playing => "play",
            PlayState::Paused => "pause",
            PlayState::Stopped => "stop",
        },
        "rule": {
            "fraction": rule.fraction,
            "max_s": rule.max.map(|m| m.as_secs_f64()),
            "uninterrupted": rule.uninterrupted,
        },
        "required_s": state.listen_required.map(|r| r.as_secs_f64()),
        "segment_start_s": state.segment_start.as_secs_f64(),
        "position_s": state.last_position.as_secs_f64(),
        "counted_s": played(state, state.last_position).as_secs_f64(),
        "listen": listen_kind(state),
        "sent": state.listen_submitted.then_some(if state.submitted_manually { "manual" } else { "auto" }),
        "manual": state.manual_answer,
        "updated_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64(),
    })
}

/// Fork: record the outgoing song as skipped if playback moved to another song
/// before its end and it did not count as a listen. A stop is not a skip.
fn record_skip(state: &State, changed_to_song: bool) {
    let Some(old) = &state.song else { return };
    if !changed_to_song || state.listen_submitted || state.play_state == PlayState::Stopped {
        return;
    }
    let Some(duration) = old.song.duration else { return };
    let position = state.last_position
        + if state.play_state == PlayState::Playing {
            state.last_seen.elapsed()
        } else {
            Duration::ZERO
        };
    if duration.saturating_sub(position) <= SEEK_TOLERANCE {
        return; // played to the end
    }
    local_log::record_skip(&old.song, duration, position, played(state, position));
}

/// Playtime of the current listen up to `position`.
fn played(state: &State, position: Duration) -> Duration {
    state.played_before + position.saturating_sub(state.segment_start)
}

/// Follow the playback position. A position away from where playback without a
/// seek would have put it is a seek: with `rule.uninterrupted` it starts the
/// count again, as does `new_listen`, otherwise the playtime so far is kept. A
/// pause changes nothing. Re-arms the listen timer for the playtime still
/// needed. Call it before `state.play_state` is updated.
fn track_position(state: &mut State, status: &Status, new_listen: bool) {
    let position = status.elapsed.unwrap_or_default();
    track_position_at(state, position, Instant::now(), new_listen);
}

/// `track_position` with the position MPD reported and the time it was seen.
fn track_position_at(state: &mut State, position: Duration, now: Instant, new_listen: bool) {
    let expected = if state.play_state == PlayState::Playing {
        state.last_position + now.duration_since(state.last_seen)
    } else {
        state.last_position
    };
    let seek = position.abs_diff(expected) > SEEK_TOLERANCE;

    if new_listen {
        state.play += 1;
        state.submitted_manually = false;
    }
    if new_listen || (seek && state.rule.uninterrupted) {
        trace!(?position, ?expected, new_listen, "counting playtime from zero");
        state.played_before = Duration::ZERO;
        state.segment_start = position;
        state.listen_timestamp = SystemTime::now();
    } else if seek {
        trace!(?position, ?expected, "seek, keeping the playtime so far");
        state.played_before = played(state, expected);
        state.segment_start = position;
    }
    state.last_position = position;
    state.last_seen = now;

    if let Some(required) = state.listen_required {
        let remaining = required.saturating_sub(played(state, position));
        state.listen_finished = Box::pin(sleep(remaining));
    }
}

async fn handle_message_event(
    state: &mut State,
    mpd_client: &Client,
    http_actor: SubmissionActor,
) -> Result<()> {
    // Read our messages
    let messages = mpd_client
        .command(commands::ReadChannelMessages)
        .await
        .context("Failed to read messages")?;

    let mut feedback = None;
    for (channel, message) in messages {
        if channel == FEEDBACK_CHANNEL_NAME {
            feedback.get_or_insert(message);
        } else if channel == status::LISTEN_CHANNEL {
            debug!(?message, "manual listen request received");
            if let Some((song, timestamp)) = manual_submit(state, &message) {
                info!(song = %song.url, "submitting listen entry on request");
                local_log::record_listen(&song, timestamp, true);
                http_actor.listen(song, timestamp);
            }
            publish(state);
        }
    }
    let Some(message) = feedback else {
        debug!("no feedback message");
        return Ok(());
    };
    debug!(?message, "feedback command received");

    let Some(feedback) = Feedback::from_command(&message) else {
        warn!(?message, "invalid feedback command, ignoring");
        return Ok(());
    };

    let Some(song) = state.song.clone().map(|s| s.song) else {
        debug!("no current song to submit feedback for");
        return Ok(());
    };

    let span = info_span!("submit_feedback", ?feedback, song = ?song.url);
    tokio::spawn(
        async move {
            if let Err(error) = submit_feedback(song, http_actor, feedback).await {
                error!(?error, "Failed to submit feedback");
            }
        }
        .instrument(span),
    );

    Ok(())
}

async fn submit_feedback(
    mut song: Song,
    http_actor: SubmissionActor,
    feedback: Feedback,
) -> Result<()> {
    debug!("submitting feedback");

    let mbid = song
        .tags
        .remove(&Tag::MusicBrainzRecordingId)
        .and_then(|mut v| {
            trace!("found existing recording MBID tag");
            if v.len() > 1 {
                warn!(
                    values = v.len(),
                    "more than one recording MBID tag, ignoring all but the first"
                );
            }

            let mbid = v.remove(0);

            if is_valid_mbid(&mbid) {
                Some(mbid)
            } else {
                warn!("invalid recording MBID, ignoring");
                None
            }
        });

    let mbid = if let Some(mbid) = mbid {
        mbid
    } else {
        debug!("requesting MBID mapping from ListenBrainz API");
        http_actor
            .lookup_recording_mbid(song)
            .await
            .context("Failed to look up MBID mapping for recording")?
    };

    trace!(?mbid);
    http_actor
        .submit_feedback(mbid, feedback)
        .await
        .context("Failed to submit feedback")
}

impl Serialize for Feedback {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i8(match self {
            Feedback::Love => 1,
            Feedback::Hate => -1,
            Feedback::Clear => 0,
        })
    }
}

fn is_same_song(a: Option<&SongInQueue>, b: Option<&SongInQueue>) -> bool {
    let Some((a, b)) = a.zip(b) else { return false };
    a.id == b.id && a.position == b.position && a.song.url == b.song.url
}

/// Try to guess if a new state indicates the current track being played is
/// potentially the same track on repeat. This function assumes the play state
/// and track URI remained the same.
///
/// This can happen if:
///   - The "single" mode is enabled and the "repeat" mode is enabled
///   - "Repeat" mode is enabled and there is only a single track in the play
///     queue
fn is_same_track_on_repeat(status: &Status) -> bool {
    // Check if the elapsed time is very close to the start of the track. We cannot
    // just check for the time going to zero because the server sending the idle
    // notification and us requesting the new state introduces latency.
    // Then apply the rules to detect the situations listed above. This may
    // interpret seeking to the very beginning of the current track as starting a
    // new listen, but this is unavoidable.
    let (Some(elapsed), Some(duration)) = (status.elapsed, status.duration) else {
        // The length heuristic cannot be applied if the elapsed time and total duration
        // aren't known.
        return false;
    };

    // Check if the new position is in the first 1% of the tracks total length
    elapsed.div_duration_f64(duration) <= 0.01
        && status.repeat
        && (status.single != SingleMode::Disabled || status.playlist_length == 1)
}

async fn get_status_and_song(client: &Client) -> Result<(Status, Option<SongInQueue>)> {
    client
        .command_list((commands::Status, commands::CurrentSong))
        .await
        .map_err(Into::into)
}

/// Calculate how long the given song has to play to count as a completed
/// ListenBrainz listen. `None` means it never counts.
fn required_time_for_song(song: Option<&SongInQueue>, rule: ListenRule) -> Option<Duration> {
    required_time(song?.song.duration, rule)
}

/// How long a song of the given duration has to play to count, see
/// `required_time_for_song`.
fn required_time(duration: Option<Duration>, rule: ListenRule) -> Option<Duration> {
    let Some(duration) = duration else {
        warn!(?rule.max, "song with unknown duration, using the maximum listen time");
        return rule.max;
    };
    let needed = duration.mul_f64(rule.fraction);
    Some(rule.max.map_or(needed, |max| needed.min(max)))
}

fn song_url(s: Option<&Song>) -> &str {
    s.map_or("<none>", |s| &*s.url)
}

/// Validate that a given MBID string conforms to the expected format (dashed
/// lowercase).
fn is_valid_mbid(mbid: &str) -> bool {
    if mbid.len() != 36 {
        return false;
    }

    for range in [0..8, 9..13, 14..18, 19..23, 24..36] {
        if mbid[range].chars().any(|c| !c.is_ascii_alphanumeric()) {
            return false;
        }
    }

    for dash_position in [8, 13, 18, 23] {
        if &mbid[dash_position..=dash_position] != "-" {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const RULE: ListenRule = ListenRule {
        fraction: 0.5,
        max: Some(Duration::from_secs(240)),
        uninterrupted: false,
    };

    fn s(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn required_time_is_the_fraction_capped_by_the_max() {
        assert_eq!(required_time(Some(s(200)), RULE), Some(s(100)));
        assert_eq!(required_time(Some(s(600)), RULE), Some(s(240)));
        assert_eq!(
            required_time(None, RULE),
            Some(s(240)),
            "unknown duration: the max"
        );
        let no_max = ListenRule { max: None, ..RULE };
        assert_eq!(required_time(Some(s(600)), no_max), Some(s(300)));
        assert_eq!(
            required_time(None, no_max),
            None,
            "unknown duration, no max: never counts"
        );
        let whole = ListenRule {
            fraction: 1.0,
            ..no_max
        };
        assert_eq!(required_time(Some(s(200)), whole), Some(s(200)));
        assert_eq!(required_time_for_song(None, RULE), None);
    }

    /// A song that needs 100 s, started playing at position 0 at `t0`.
    fn playing(rule: ListenRule, t0: Instant) -> State {
        let mut state = State {
            play_state: PlayState::Stopped,
            song: None,
            rule,
            segment_start: Duration::ZERO,
            played_before: Duration::ZERO,
            last_position: Duration::ZERO,
            last_seen: t0,
            listen_timestamp: SystemTime::UNIX_EPOCH,
            listen_required: Some(s(100)),
            listen_finished: Box::pin(sleep(Duration::ZERO)),
            listen_submitted: false,
            completed_listens: 0,
            instance: "1-2".to_owned(),
            play: 0,
            submitted_manually: false,
            manual_answer: None,
        };
        see(&mut state, PlayState::Playing, 0, t0, true);
        state
    }

    /// A queued song as MPD's `currentsong` describes it: `mpd_client` builds
    /// one only from a server response, so this answers it over an in-memory
    /// connection.
    async fn queued(url: &str, duration: u64) -> SongInQueue {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let (client_io, server_io) = tokio::io::duplex(4096);
        let url = url.to_owned();
        tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(server_io);
            write.write_all(b"OK MPD 0.24.0\n").await.unwrap();
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let reply = match line.as_str() {
                    "idle" => continue, // answered by `noidle`
                    "currentsong" => {
                        format!("file: {url}\nduration: {duration}.000\nPos: 0\nId: 7\nOK\n")
                    }
                    _ => "OK\n".to_owned(),
                };
                write.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        let (client, _events) = Client::connect(client_io).await.unwrap();
        client
            .command(commands::CurrentSong)
            .await
            .unwrap()
            .unwrap()
    }

    /// `playing` with a 200 s song in the state, so a listen can be claimed.
    async fn playing_song(rule: ListenRule, t0: Instant) -> State {
        let mut state = playing(rule, t0);
        state.song = Some(queued("a.mp3", 200).await);
        state
    }

    const UNINTERRUPTED: ListenRule = ListenRule {
        uninterrupted: true,
        ..RULE
    };

    #[tokio::test]
    async fn status_reports_the_rule_and_the_progress() {
        let t0 = Instant::now();
        let mut state = playing_song(RULE, t0).await;
        see(&mut state, PlayState::Playing, 30, t0 + s(30), false);
        let status = status_json(&state);
        assert_eq!(status["listen"], "counting");
        assert_eq!(status["required_s"], 100.0);
        assert_eq!(status["counted_s"], 30.0);
        assert_eq!(status["position_s"], 30.0);
        assert_eq!(status["duration_s"], 200.0);
        assert_eq!(status["id"], 7);
        assert_eq!(status["state"], "play");
        assert_eq!(status["rule"]["fraction"], 0.5);
        assert_eq!(status["rule"]["max_s"], 240.0);
        assert_eq!(status["sent"], Value::Null);
        assert_eq!(status["play"], 1);
    }

    #[tokio::test]
    async fn seek_late_with_the_uninterrupted_rule_makes_the_listen_impossible() {
        let t0 = Instant::now();
        let mut state = playing_song(UNINTERRUPTED, t0).await;
        see(&mut state, PlayState::Playing, 99, t0 + s(5), false); // 101 s left: still enough
        assert_eq!(listen_kind(&state), "counting");
        see(&mut state, PlayState::Playing, 120, t0 + s(10), false); // 80 s left of 100 needed
        assert_eq!(listen_kind(&state), "impossible");
        assert_eq!(status_json(&state)["segment_start_s"], 120.0);
        see(&mut state, PlayState::Playing, 0, t0 + s(20), false); // back to the start
        assert_eq!(listen_kind(&state), "counting");
        // the default rule keeps the playtime: 10 s played + 90 s left is
        // enough
        let mut state = playing_song(RULE, t0).await;
        see(&mut state, PlayState::Playing, 110, t0 + s(10), false);
        assert_eq!(listen_kind(&state), "counting");
        see(&mut state, PlayState::Playing, 190, t0 + s(20), false);
        assert_eq!(listen_kind(&state), "impossible");
    }

    #[tokio::test]
    async fn unknown_duration_without_a_max_never_counts() {
        let mut state = playing(ListenRule { max: None, ..RULE }, Instant::now());
        state.listen_required = None;
        assert_eq!(listen_kind(&state), "never");
    }

    #[tokio::test]
    async fn manual_send_claims_the_listen_once() {
        let t0 = Instant::now();
        let mut state = playing_song(UNINTERRUPTED, t0).await;
        see(&mut state, PlayState::Playing, 20, t0 + s(20), false);
        let started = state
            .listen_timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (song, timestamp) = manual_submit(&mut state, "submit 1-2 1").expect("sent");
        assert_eq!(song.url, "a.mp3");
        assert_eq!(timestamp, started, "listened_at is the listen's start");
        assert_eq!(
            state.manual_answer,
            Some(json!({"n": 1, "play": 1, "ok": true, "error": null}))
        );
        let status = status_json(&state);
        assert_eq!(
            (status["listen"].as_str(), status["sent"].as_str()),
            (Some("sent"), Some("manual"))
        );
        assert!(!state.should_poll(), "the timer is disarmed");
        assert!(manual_submit(&mut state, "submit 1-2 1").is_none());
        assert_eq!(
            state.manual_answer.as_ref().unwrap()["error"],
            "already sent"
        );
        assert_eq!(state.manual_answer.as_ref().unwrap()["n"], 2);
        assert!(
            claim_listen(&mut state, false).is_none(),
            "no automatic listen after a manual one"
        );
        assert_eq!(state.completed_listens, 1);
    }

    #[tokio::test]
    async fn manual_send_for_another_play_is_refused() {
        let t0 = Instant::now();
        let mut state = playing_song(RULE, t0).await;
        assert!(
            manual_submit(&mut state, "submit 1-2 0").is_none(),
            "an earlier play"
        );
        assert_eq!(
            state.manual_answer.as_ref().unwrap()["error"],
            "the song changed"
        );
        assert!(
            manual_submit(&mut state, "submit 9-9 1").is_none(),
            "another daemon run"
        );
        assert!(manual_submit(&mut state, "submit").is_none());
        assert!(!state.listen_submitted);
        see(&mut state, PlayState::Playing, 0, t0 + s(5), true); // the next song (or a repeat) starts
        assert!(manual_submit(&mut state, "submit 1-2 1").is_none());
        assert!(manual_submit(&mut state, "submit 1-2 2").is_some());
    }

    #[tokio::test]
    async fn automatic_listen_is_not_sent_again_by_a_manual_request() {
        let t0 = Instant::now();
        let mut state = playing_song(RULE, t0).await;
        assert!(claim_listen(&mut state, false).is_some());
        assert_eq!(status_json(&state)["sent"], "auto");
        assert!(manual_submit(&mut state, "submit 1-2 1").is_none());
        assert_eq!(state.completed_listens, 1);
    }

    /// MPD reports `play_state` at `position` seconds, observed at `at`.
    fn see(state: &mut State, play_state: PlayState, position: u64, at: Instant, new_listen: bool) {
        track_position_at(state, s(position), at, new_listen);
        state.play_state = play_state;
    }

    /// Time left until the listen counts, as the armed timer says.
    fn remaining(state: &State) -> Duration {
        state.listen_finished.deadline() - tokio::time::Instant::now()
    }

    fn close(a: Duration, b: Duration) -> bool {
        a.abs_diff(b) < Duration::from_secs(1)
    }

    #[tokio::test]
    async fn normal_playback_counts_toward_the_listen() {
        let t0 = Instant::now();
        let mut state = playing(RULE, t0);
        assert!(close(remaining(&state), s(100)));
        see(&mut state, PlayState::Playing, 31, t0 + s(30), false); // 1 s of latency: not a seek
        assert_eq!(played(&state, s(31)), s(31));
        assert!(close(remaining(&state), s(69)));
    }

    #[tokio::test]
    async fn seek_keeps_the_playtime_by_default() {
        let t0 = Instant::now();
        let mut state = playing(RULE, t0);
        let started = state.listen_timestamp;
        see(&mut state, PlayState::Playing, 150, t0 + s(60), false);
        assert_eq!(played(&state, s(150)), s(60));
        assert!(close(remaining(&state), s(40)));
        assert_eq!(state.listen_timestamp, started);
        see(&mut state, PlayState::Playing, 170, t0 + s(80), false);
        assert_eq!(played(&state, s(170)), s(80));
    }

    #[tokio::test]
    async fn seek_restarts_the_count_when_uninterrupted() {
        let t0 = Instant::now();
        let mut state = playing(
            ListenRule {
                uninterrupted: true,
                ..RULE
            },
            t0,
        );
        // a marker, not the clock: two readings of the system clock can be
        // equal
        state.listen_timestamp = SystemTime::UNIX_EPOCH;
        see(&mut state, PlayState::Playing, 10, t0 + s(60), false); // a seek back
        assert_eq!(played(&state, s(10)), Duration::ZERO);
        assert!(close(remaining(&state), s(100)));
        assert_ne!(
            state.listen_timestamp,
            SystemTime::UNIX_EPOCH,
            "the listen starts at the seek"
        );
        see(&mut state, PlayState::Playing, 40, t0 + s(90), false);
        assert_eq!(played(&state, s(40)), s(30));
    }

    #[tokio::test]
    async fn pause_is_neutral_in_both_rules() {
        for uninterrupted in [false, true] {
            let t0 = Instant::now();
            let mut state = playing(
                ListenRule {
                    uninterrupted,
                    ..RULE
                },
                t0,
            );
            let started = state.listen_timestamp;
            see(&mut state, PlayState::Paused, 60, t0 + s(60), false);
            see(&mut state, PlayState::Playing, 60, t0 + s(600), false); // resumed ten minutes later
            assert_eq!(
                played(&state, s(60)),
                s(60),
                "uninterrupted: {uninterrupted}"
            );
            assert!(close(remaining(&state), s(40)));
            assert_eq!(state.listen_timestamp, started);
            see(&mut state, PlayState::Playing, 100, t0 + s(640), false);
            assert_eq!(played(&state, s(100)), s(100));
        }
    }

    #[tokio::test]
    async fn new_listen_starts_from_zero_after_a_seek_kept_playtime() {
        let t0 = Instant::now();
        let mut state = playing(RULE, t0);
        see(&mut state, PlayState::Playing, 150, t0 + s(60), false);
        see(&mut state, PlayState::Playing, 0, t0 + s(70), true); // next song
        assert_eq!(played(&state, s(0)), Duration::ZERO);
        assert!(close(remaining(&state), s(100)));
    }
}
