//! [`SnapControl`] — the contract between a control front end and a Snapcast server.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;

use serde_json::Value;
use snapcast_server::ServerCommand;
use snapcast_server::status::ServerStatus;
use tokio::sync::{mpsc, oneshot};

/// Why a [`SnapControl`] operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ControlError {
    /// The server is not running any more (its command channel is closed), or it
    /// dropped the reply.
    ServerGone,
    /// The server rejected the request, with its reason.
    Rejected(String),
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerGone => f.write_str("server is not running"),
            Self::Rejected(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for ControlError {}

/// What a control front end (JSON-RPC over TCP or WebSocket, an embedding
/// application, a test) may ask of a Snapcast server.
///
/// One method per [`ServerCommand`] that the JSON-RPC API needs. The
/// implementation for `mpsc::Sender<ServerCommand>` (from
/// [`SnapServer::command_sender`](snapcast_server::SnapServer::command_sender))
/// drives an embedded [`SnapServer`](snapcast_server::SnapServer); other
/// implementations can forward to a remote server or record calls in tests.
///
/// Setters return once the command is queued, not when it took effect; the
/// resulting state change arrives as a [`ServerEvent`](snapcast_server::ServerEvent).
pub trait SnapControl: Send + Sync {
    /// The full server status: server info, groups with their clients, streams.
    fn status(&self) -> impl Future<Output = Result<ServerStatus, ControlError>> + Send;

    /// Set a client's volume (0–100) and mute state.
    fn set_client_volume(
        &self,
        client_id: &str,
        volume: u16,
        muted: bool,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Set a client's latency offset in milliseconds.
    fn set_client_latency(
        &self,
        client_id: &str,
        latency: i32,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Set a client's display name.
    fn set_client_name(
        &self,
        client_id: &str,
        name: &str,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Remove a client from the server.
    fn delete_client(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Assign a stream to a group.
    fn set_group_stream(
        &self,
        group_id: &str,
        stream_id: &str,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Mute or unmute a group.
    fn set_group_mute(
        &self,
        group_id: &str,
        muted: bool,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Set a group's display name.
    fn set_group_name(
        &self,
        group_id: &str,
        name: &str,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Move the given clients into a group.
    fn set_group_clients(
        &self,
        group_id: &str,
        clients: Vec<String>,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Set a stream's metadata/properties.
    fn set_stream_meta(
        &self,
        stream_id: &str,
        metadata: HashMap<String, Value>,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Forward a player command (`play`, `pause`, `next`, `seek`, …) to a stream.
    fn stream_control(
        &self,
        stream_id: &str,
        command: &str,
        params: Value,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Ask the application to add a stream from a source URI; returns the new
    /// stream's ID.
    fn add_stream(&self, uri: &str) -> impl Future<Output = Result<String, ControlError>> + Send;

    /// Remove a stream.
    fn remove_stream(
        &self,
        stream_id: &str,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;
}

/// Drives an embedded server through its command channel.
impl SnapControl for mpsc::Sender<ServerCommand> {
    async fn status(&self) -> Result<ServerStatus, ControlError> {
        let (response_tx, response_rx) = oneshot::channel();
        send(self, ServerCommand::GetStatus { response_tx }).await?;
        response_rx.await.map_err(|_| ControlError::ServerGone)
    }

    async fn set_client_volume(
        &self,
        client_id: &str,
        volume: u16,
        muted: bool,
    ) -> Result<(), ControlError> {
        let client_id = client_id.to_string();
        send(
            self,
            ServerCommand::SetClientVolume {
                client_id,
                volume,
                muted,
            },
        )
        .await
    }

    async fn set_client_latency(&self, client_id: &str, latency: i32) -> Result<(), ControlError> {
        let client_id = client_id.to_string();
        send(self, ServerCommand::SetClientLatency { client_id, latency }).await
    }

    async fn set_client_name(&self, client_id: &str, name: &str) -> Result<(), ControlError> {
        let (client_id, name) = (client_id.to_string(), name.to_string());
        send(self, ServerCommand::SetClientName { client_id, name }).await
    }

    async fn delete_client(&self, client_id: &str) -> Result<(), ControlError> {
        let client_id = client_id.to_string();
        send(self, ServerCommand::DeleteClient { client_id }).await
    }

    async fn set_group_stream(&self, group_id: &str, stream_id: &str) -> Result<(), ControlError> {
        let (group_id, stream_id) = (group_id.to_string(), stream_id.to_string());
        send(
            self,
            ServerCommand::SetGroupStream {
                group_id,
                stream_id,
            },
        )
        .await
    }

    async fn set_group_mute(&self, group_id: &str, muted: bool) -> Result<(), ControlError> {
        let group_id = group_id.to_string();
        send(self, ServerCommand::SetGroupMute { group_id, muted }).await
    }

    async fn set_group_name(&self, group_id: &str, name: &str) -> Result<(), ControlError> {
        let (group_id, name) = (group_id.to_string(), name.to_string());
        send(self, ServerCommand::SetGroupName { group_id, name }).await
    }

    async fn set_group_clients(
        &self,
        group_id: &str,
        clients: Vec<String>,
    ) -> Result<(), ControlError> {
        let group_id = group_id.to_string();
        send(self, ServerCommand::SetGroupClients { group_id, clients }).await
    }

    async fn set_stream_meta(
        &self,
        stream_id: &str,
        metadata: HashMap<String, Value>,
    ) -> Result<(), ControlError> {
        let stream_id = stream_id.to_string();
        send(
            self,
            ServerCommand::SetStreamMeta {
                stream_id,
                metadata,
            },
        )
        .await
    }

    async fn stream_control(
        &self,
        stream_id: &str,
        command: &str,
        params: Value,
    ) -> Result<(), ControlError> {
        let (stream_id, command) = (stream_id.to_string(), command.to_string());
        send(
            self,
            ServerCommand::StreamControl {
                stream_id,
                command,
                params,
            },
        )
        .await
    }

    async fn add_stream(&self, uri: &str) -> Result<String, ControlError> {
        let (response_tx, response_rx) = oneshot::channel();
        let uri = uri.to_string();
        send(self, ServerCommand::AddStream { uri, response_tx }).await?;
        match response_rx.await {
            Ok(result) => result.map_err(ControlError::Rejected),
            Err(_) => Err(ControlError::ServerGone),
        }
    }

    async fn remove_stream(&self, stream_id: &str) -> Result<(), ControlError> {
        let stream_id = stream_id.to_string();
        send(self, ServerCommand::RemoveStream { stream_id }).await
    }
}

async fn send(
    tx: &mpsc::Sender<ServerCommand>,
    command: ServerCommand,
) -> Result<(), ControlError> {
    tx.send(command).await.map_err(|_| ControlError::ServerGone)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sender_impl_maps_calls_to_commands() {
        let (tx, mut rx) = mpsc::channel::<ServerCommand>(4);
        tx.set_client_volume("c1", 42, true).await.unwrap();
        match rx.recv().await.unwrap() {
            ServerCommand::SetClientVolume {
                client_id,
                volume,
                muted,
            } => assert_eq!((client_id.as_str(), volume, muted), ("c1", 42, true)),
            other => panic!("unexpected command {other:?}"),
        }
        tx.set_group_stream("g1", "music").await.unwrap();
        assert!(matches!(
            rx.recv().await.unwrap(),
            ServerCommand::SetGroupStream { group_id, stream_id } if group_id == "g1" && stream_id == "music"
        ));
    }

    #[tokio::test]
    async fn closed_channel_reports_server_gone() {
        let (tx, rx) = mpsc::channel::<ServerCommand>(1);
        drop(rx);
        assert_eq!(tx.delete_client("c1").await, Err(ControlError::ServerGone));
        assert_eq!(tx.status().await.err(), Some(ControlError::ServerGone));
    }

    #[tokio::test]
    async fn add_stream_passes_the_rejection_reason_through() {
        let (tx, mut rx) = mpsc::channel::<ServerCommand>(1);
        tokio::spawn(async move {
            if let Some(ServerCommand::AddStream { response_tx, .. }) = rx.recv().await {
                let _ = response_tx.send(Err("unsupported scheme".into()));
            }
        });
        assert_eq!(
            tx.add_stream("foo://bar").await,
            Err(ControlError::Rejected("unsupported scheme".into()))
        );
    }
}
