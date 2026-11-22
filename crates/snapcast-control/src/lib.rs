#![deny(unsafe_code)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::implicit_clone)]
#![warn(clippy::uninlined_format_args)]
#![warn(missing_docs)]

//! Snapcast JSON-RPC control API for an embedded [`SnapServer`](snapcast_server::SnapServer).
//!
//! Serves the Snapcast control protocol (JSON-RPC 2.0, API version 2, as the C++
//! snapserver) to controllers such as Snapweb, Snapdroid or Home Assistant:
//!
//! - over TCP, newline-delimited ([`ControlServer::serve_tcp`], port 1705 by convention);
//! - with the `http` feature (default), over HTTP `POST /jsonrpc` and WebSocket
//!   `GET /jsonrpc`, optionally serving Snapweb's static files
//!   ([`ControlServer::serve_http`], port 1780 by convention).
//!
//! Like the server library, this crate opens no port itself: the embedder binds the
//! listeners and hands them over.
//!
//! The server is reached only through the [`SnapControl`] trait. It is implemented
//! for the server's command channel (`mpsc::Sender<ServerCommand>`), so an embedded
//! server needs no glue; tests or proxies can supply their own implementation.
//!
//! [`ControlServer::forward_events`] turns the server's [`ServerEvent`]s into
//! JSON-RPC notifications (`Client.OnConnect`, `Server.OnUpdate`, …) for all
//! connected controllers, and passes every event on to the embedder.
//!
//! ```no_run
//! use snapcast_control::ControlServer;
//! use snapcast_server::{ServerConfig, SnapServer};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let (mut server, events) = SnapServer::new(ServerConfig::default());
//! let _audio = server.add_stream("default");
//!
//! let control = ControlServer::new(server.command_sender());
//! let (event_tx, mut app_events) = tokio::sync::mpsc::channel(64);
//! tokio::spawn({
//!     let control = control.clone();
//!     async move { control.forward_events(events, Some(event_tx)).await }
//! });
//! tokio::spawn(async move {
//!     while let Some(event) = app_events.recv().await {
//!         // e.g. react to ServerEvent::StreamControl
//!         let _ = event;
//!     }
//! });
//!
//! let tcp = tokio::net::TcpListener::bind(("0.0.0.0", snapcast_proto::DEFAULT_CONTROL_PORT)).await?;
//! tokio::spawn({
//!     let control = control.clone();
//!     async move { control.serve_tcp(tcp).await }
//! });
//! let http = tokio::net::TcpListener::bind(("0.0.0.0", snapcast_proto::DEFAULT_HTTP_PORT)).await?;
//! tokio::spawn(async move { control.serve_http(http, Some("/usr/share/snapserver/snapweb".into())).await });
//!
//! let audio = tokio::net::TcpListener::bind(("0.0.0.0", snapcast_proto::DEFAULT_STREAM_PORT)).await?;
//! server.serve(audio).await
//! # }
//! ```

pub mod auth;
mod control;
#[cfg(feature = "http")]
mod http;
mod jsonrpc;
mod notify;
mod tcp;

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;
use snapcast_server::ServerEvent;
use tokio::sync::{broadcast, mpsc, oneshot};

pub use control::{ControlError, SnapControl};

use crate::auth::AuthConfig;

/// Capacity of the notification broadcast channel. A controller that falls this
/// far behind skips the oldest notifications.
const NOTIFY_CHANNEL_SIZE: usize = 256;

/// A JSON-RPC request this crate does not handle itself, handed to the embedder
/// (see [`ControlServer::with_extensions`]).
#[derive(Debug)]
#[non_exhaustive]
pub enum ControlEvent {
    /// An unrecognized JSON-RPC method or a registered notification.
    JsonRpc {
        /// Control client that sent the request.
        client_id: String,
        /// The full JSON-RPC request object.
        request: Value,
        /// Response channel (`Some` for methods, `None` for notifications).
        response_tx: Option<oneshot::Sender<Value>>,
    },
}

/// Custom JSON-RPC methods and notifications handled by the embedder.
struct Extensions {
    event_tx: mpsc::Sender<ControlEvent>,
    methods: HashSet<String>,
    notifications: HashSet<String>,
}

/// The JSON-RPC control server: shared by all transports and connections.
///
/// Cheap to clone; clones share the notification channel, so a notification
/// caused through one transport reaches the controllers on all of them.
pub struct ControlServer<C> {
    control: C,
    auth: Arc<AuthConfig>,
    notify_tx: broadcast::Sender<Value>,
    extensions: Option<Arc<Extensions>>,
}

impl<C: Clone> Clone for ControlServer<C> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
            auth: Arc::clone(&self.auth),
            notify_tx: self.notify_tx.clone(),
            extensions: self.extensions.clone(),
        }
    }
}

impl<C: SnapControl + Clone + 'static> ControlServer<C> {
    /// A control server for `control`, without authentication or extensions.
    pub fn new(control: C) -> Self {
        let (notify_tx, _) = broadcast::channel(NOTIFY_CHANNEL_SIZE);
        Self {
            control,
            auth: Arc::new(AuthConfig::default()),
            notify_tx,
            extensions: None,
        }
    }

    /// Require authentication (`Server.GetToken` / `Server.Authenticate`) as
    /// configured. Call [`AuthConfig::validate`] first. Needs the feature `auth`;
    /// without it an enabled config fails closed (see [`auth`]).
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Arc::new(auth);
        self
    }

    /// Hand requests this crate does not handle to the embedder via `event_tx`.
    ///
    /// Over TCP, `methods` get a reply from the embedder (through the event's
    /// `response_tx`, within 5 s), `notifications` none; other unknown methods are
    /// answered with "Method not found". Over HTTP and WebSocket, every unknown
    /// request is forwarded without a reply channel.
    pub fn with_extensions(
        mut self,
        event_tx: mpsc::Sender<ControlEvent>,
        methods: HashSet<String>,
        notifications: HashSet<String>,
    ) -> Self {
        self.extensions = Some(Arc::new(Extensions {
            event_tx,
            methods,
            notifications,
        }));
        self
    }

    /// The server this control server talks to.
    pub fn control(&self) -> &C {
        &self.control
    }

    /// Send a notification to every connected controller (e.g. a custom one).
    pub fn notify(&self, notification: Value) {
        let _ = self.notify_tx.send(notification);
    }

    /// Serve JSON-RPC over TCP (newline-delimited) on `listener` until accepting fails.
    pub async fn serve_tcp(&self, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
        tcp::serve(self.clone(), listener).await
    }

    /// Serve JSON-RPC over HTTP `POST /jsonrpc` and WebSocket `GET /jsonrpc` on
    /// `listener`, and the static files in `doc_root` (e.g. Snapweb) for all other paths.
    #[cfg(feature = "http")]
    pub async fn serve_http(
        &self,
        listener: tokio::net::TcpListener,
        doc_root: Option<std::path::PathBuf>,
    ) -> anyhow::Result<()> {
        http::serve(self.clone(), listener, doc_root).await
    }

    /// Turn the server's events into notifications for all controllers, until the
    /// server stops.
    ///
    /// `events` is the receiver from [`SnapServer::new`](snapcast_server::SnapServer::new).
    /// Every event is then passed on to `passthrough`, if given, so the embedder
    /// still sees all of them (e.g. [`ServerEvent::StreamControl`] or
    /// [`ServerEvent::StateChanged`]). The embedder must keep draining it: when it
    /// is full this loop waits, and the server drops events meanwhile.
    pub async fn forward_events(
        &self,
        mut events: mpsc::Receiver<ServerEvent>,
        passthrough: Option<mpsc::Sender<ServerEvent>>,
    ) {
        while let Some(event) = events.recv().await {
            if let Some(notification) = notify::for_event(&event, &self.control).await {
                self.notify(notification);
            }
            if let Some(tx) = &passthrough {
                let _ = tx.send(event).await;
            }
        }
    }
}
