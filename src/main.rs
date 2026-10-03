mod cache_actor;
mod cli;
mod config;
mod submission_actor;

use std::{
    net::SocketAddr,
    pin::Pin,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use config::Configuration;
use mpd_client::{
    client::{Client, ConnectionEvent, ConnectionEvents, Subsystem},
    commands::{self, SingleMode},
    responses::{PlayState, Song, SongInQueue, Status},
    tag::Tag,
};
use serde::{Serialize, Serializer};
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

/// Fraction of a song that has to be played without a seek or a stop (pauses
/// don't matter) before it counts as a listen.
const LISTEN_FRACTION: f64 = 0.9;

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

    let cache_actor = CacheActor::start(&config)?;
    let (mpd_client, state_changes) = connect(&config).await?;
    let (http_actor, http_actor_handle) = SubmissionActor::start(config, cache_actor);

    if let Some(feedback) = args.send_feedback {
        return send_feedback(mpd_client, feedback).await;
    }

    let res = run(mpd_client, state_changes, http_actor).await;

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
    /// Playback position where the current uninterrupted run started. A seek
    /// or a stop starts a new run; a pause doesn't.
    run_start: Duration,
    /// Last observed playback position and when it was observed, used to tell
    /// a seek from normal playback.
    last_position: Duration,
    last_seen: Instant,
    /// The system timestamp when the listen was started. This is used during
    /// submission to the ListenBrainz API.
    listen_timestamp: SystemTime,
    /// How long the current song has to play in one run to count as a listen,
    /// `None` if its duration is unknown (it is never submitted).
    listen_required: Option<Duration>,
    /// The future that completes when the required duration is reached.
    listen_finished: Pin<Box<Sleep>>,
    /// `true` if a listen record for the current song has already been
    /// submitted.
    listen_submitted: bool,
    /// Counter for completed listens
    completed_listens: u64,
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
) -> Result<()> {
    // Setup initial state
    let (status, song) = get_status_and_song(&mpd_client).await?;

    let listen_required = required_time_for_song(song.as_ref());

    // Subscribe to the client-to-client channel used for feedback
    mpd_client
        .command(commands::SubscribeToChannel(FEEDBACK_CHANNEL_NAME))
        .await?;

    let mut state = State {
        play_state: status.state,
        song,
        run_start: Duration::ZERO,
        last_position: Duration::ZERO,
        last_seen: Instant::now(),
        listen_timestamp: SystemTime::now(),
        listen_required,
        listen_finished: Box::pin(sleep(Duration::ZERO)),
        listen_submitted: false,
        completed_listens: 0,
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
    let required_playtime = required_time_for_song(new_song);
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
    state.listen_submitted = true;
    state.completed_listens += 1;

    let song = state.song.clone().expect("no song to submit");

    let timestamp = state
        .listen_timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    http_actor.listen(song.song, timestamp);
}

/// Follow the playback position. A position away from where uninterrupted
/// playback would have put it is a seek and starts a new run, as does
/// `new_run`; a pause keeps the run. Re-arms the listen timer for what the
/// current run still needs. Call it before `state.play_state` is updated.
fn track_position(state: &mut State, status: &Status, new_run: bool) {
    let now = Instant::now();
    let position = status.elapsed.unwrap_or_default();
    let expected = if state.play_state == PlayState::Playing {
        state.last_position + now.duration_since(state.last_seen)
    } else {
        state.last_position
    };

    if new_run || position.abs_diff(expected) > SEEK_TOLERANCE {
        trace!(?position, ?expected, new_run, "starting a new uninterrupted run");
        state.run_start = position;
        state.listen_timestamp = SystemTime::now();
    }
    state.last_position = position;
    state.last_seen = now;

    if let Some(required) = state.listen_required {
        let remaining = (state.run_start + required).saturating_sub(position);
        state.listen_finished = Box::pin(sleep(remaining));
    }
}

async fn handle_message_event(
    state: &State,
    mpd_client: &Client,
    http_actor: SubmissionActor,
) -> Result<()> {
    // Read our messages
    let messages = mpd_client
        .command(commands::ReadChannelMessages)
        .await
        .context("Failed to read messages")?;

    let Some((_, message)) = messages
        .into_iter()
        .find(|(channel, _)| channel == FEEDBACK_CHANNEL_NAME)
    else {
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

/// Calculate how long the given song has to play in one run to count as a
/// completed ListenBrainz listen. `None` means it is never submitted.
fn required_time_for_song(song: Option<&SongInQueue>) -> Option<Duration> {
    let duration = song?.song.duration;
    if duration.is_none() {
        warn!("song with unknown duration, it will not be submitted");
    }
    duration.map(|d| d.mul_f64(LISTEN_FRACTION))
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
