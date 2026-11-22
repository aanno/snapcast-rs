//! Streams added and removed while the server runs (`ServerCommand::AddAudioStream`,
//! `ServerCommand::RemoveStream`).

use snapcast_client::ClientEvent;
use snapcast_server::{
    AudioData, AudioFrame, ServerCommand, ServerEvent, SnapServer, StreamConfig,
};
use snapcast_tests::{connect_client_with_id, expect_event, spawn_serving};
use tokio::sync::{mpsc, oneshot};

const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn add_audio_stream(
    cmd: &mpsc::Sender<ServerCommand>,
    name: &str,
) -> Result<mpsc::Sender<AudioFrame>, String> {
    let (tx, rx) = oneshot::channel();
    cmd.send(ServerCommand::AddAudioStream {
        name: name.into(),
        config: StreamConfig::default(),
        response_tx: tx,
    })
    .await
    .unwrap();
    rx.await.unwrap()
}

/// The group and stream of a client, from the server status.
async fn client_group(cmd: &mpsc::Sender<ServerCommand>, client_id: &str) -> (String, String) {
    let (tx, rx) = oneshot::channel();
    cmd.send(ServerCommand::GetStatus { response_tx: tx })
        .await
        .unwrap();
    let status = rx.await.unwrap();
    status
        .server
        .groups
        .iter()
        .find(|g| g.clients.iter().any(|c| c.id == client_id))
        .map(|g| (g.id.clone(), g.stream_id.clone()))
        .unwrap_or_else(|| panic!("client {client_id} is in no group"))
}

async fn stream_ids(cmd: &mpsc::Sender<ServerCommand>) -> Vec<String> {
    let (tx, rx) = oneshot::channel();
    cmd.send(ServerCommand::GetStatus { response_tx: tx })
        .await
        .unwrap();
    let status = rx.await.unwrap();
    status.server.streams.iter().map(|s| s.id.clone()).collect()
}

async fn wait_for_server_event(
    events: &mut mpsc::Receiver<ServerEvent>,
    mut pred: impl FnMut(&ServerEvent) -> bool,
) -> ServerEvent {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(ev)) if pred(&ev) => return ev,
            Ok(Some(_)) => continue,
            _ => panic!("timed out waiting for a server event"),
        }
    }
}

async fn wait_for_connect(events: &mut mpsc::Receiver<ServerEvent>) -> String {
    match wait_for_server_event(events, |e| matches!(e, ServerEvent::ClientConnected { .. })).await
    {
        ServerEvent::ClientConnected { id, .. } => id,
        _ => unreachable!(),
    }
}

/// Push `n` 10 ms frames of a ramp.
async fn push_audio(audio: &mpsc::Sender<AudioFrame>, n: usize) {
    let samples: Vec<f32> = (0..960).map(|i| (i as f32 / 960.0) * 2.0 - 1.0).collect();
    let mut ts = 1_000_000_000;
    for _ in 0..n {
        audio
            .send(AudioFrame {
                data: AudioData::F32(samples.clone()),
                timestamp_usec: ts,
            })
            .await
            .unwrap();
        ts += 10_000;
    }
}

#[tokio::test]
async fn a_stream_added_at_runtime_plays_to_a_client_moved_onto_it() {
    let (mut server, mut events) = SnapServer::new(Default::default());
    let _first = server.add_stream("first");
    let cmd = server.command_sender();
    let port = spawn_serving(server).await;

    let mut client = connect_client_with_id(port, "runtime-client").await;
    let client_id = wait_for_connect(&mut events).await;
    expect_event(&mut client.events, 5000, |e| match e {
        ClientEvent::TimeSyncUsable { .. } => Some(()),
        _ => None,
    })
    .await;

    let added = add_audio_stream(&cmd, "added").await.unwrap();
    wait_for_server_event(&mut events, |e| matches!(e, ServerEvent::ServerUpdated)).await;
    assert_eq!(stream_ids(&cmd).await, ["first", "added"]);
    // Adding does not move anyone.
    let (group_id, stream) = client_group(&cmd, &client_id).await;
    assert_eq!(stream, "first");

    cmd.send(ServerCommand::SetGroupStream {
        group_id,
        stream_id: "added".into(),
    })
    .await
    .unwrap();
    wait_for_server_event(&mut events, |e| {
        matches!(e, ServerEvent::GroupStreamChanged { .. })
    })
    .await;

    push_audio(&added, 20).await;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let mut total = 0;
    while total < 9600 {
        let frame = tokio::time::timeout_at(deadline, client.audio_rx.recv())
            .await
            .expect("audio of the added stream reaches the client")
            .expect("audio channel open");
        total += frame.samples.len();
    }
}

#[tokio::test]
async fn removing_the_first_stream_starts_new_clients_on_the_next() {
    let (mut server, mut events) = SnapServer::new(Default::default());
    let _first = server.add_stream("first");
    let cmd = server.command_sender();
    let port = spawn_serving(server).await;

    let _second = add_audio_stream(&cmd, "second").await.unwrap();
    cmd.send(ServerCommand::RemoveStream {
        stream_id: "first".into(),
    })
    .await
    .unwrap();
    // Two ServerUpdated: the addition and the removal.
    for _ in 0..2 {
        wait_for_server_event(&mut events, |e| matches!(e, ServerEvent::ServerUpdated)).await;
    }
    assert_eq!(stream_ids(&cmd).await, ["second"]);

    let mut client = connect_client_with_id(port, "late-client").await;
    let client_id = wait_for_connect(&mut events).await;
    // It gets the codec header of its stream, so the stream can start.
    expect_event(&mut client.events, 5000, |e| match e {
        ClientEvent::StreamStarted { .. } => Some(()),
        _ => None,
    })
    .await;
    assert_eq!(client_group(&cmd, &client_id).await.1, "second");
}

#[tokio::test]
async fn a_removed_stream_closes_its_sender_and_its_name_is_free_again() {
    let (mut server, _events) = SnapServer::new(Default::default());
    let _first = server.add_stream("first");
    let cmd = server.command_sender();
    let _port = spawn_serving(server).await;

    let zone = add_audio_stream(&cmd, "zone").await.unwrap();
    assert!(add_audio_stream(&cmd, "zone").await.is_err());

    cmd.send(ServerCommand::RemoveStream {
        stream_id: "zone".into(),
    })
    .await
    .unwrap();
    tokio::time::timeout(TIMEOUT, zone.closed())
        .await
        .expect("the removed stream's sender is closed");

    let again = add_audio_stream(&cmd, "zone").await.unwrap();
    push_audio(&again, 2).await;
}
