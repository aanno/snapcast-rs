//! Stream startup — encoder creation and the per-stream encode threads.
//!
//! Shared by [`SnapServer::serve`](crate::SnapServer::serve), which starts the
//! streams added before it runs, and the command dispatcher, which starts
//! streams added at runtime ([`ServerCommand::AddAudioStream`](crate::ServerCommand::AddAudioStream))
//! and stops removed ones.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use snapcast_proto::SampleFormat;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::session::SessionServer;
use crate::state::{ServerState, StreamInfo};
use crate::{AudioFrame, StreamConfig, WireChunkData, encoder};

/// Creates encoders and starts and stops the encode threads of the streams.
///
/// Cloning shares the registry of running streams.
#[derive(Clone)]
pub(crate) struct StreamStarter {
    /// The server's default codec.
    codec: String,
    /// The server's default sample format.
    sample_format: SampleFormat,
    #[cfg(feature = "encryption")]
    encryption_psk: Option<String>,
    /// Encoded chunks go here, to the sessions.
    chunk_tx: broadcast::Sender<WireChunkData>,
    /// Running streams: dropping or firing the sender stops the encode thread.
    running: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
}

impl StreamStarter {
    /// A starter with the server's defaults.
    pub(crate) fn new(
        codec: String,
        sample_format: SampleFormat,
        #[cfg(feature = "encryption")] encryption_psk: Option<String>,
        chunk_tx: broadcast::Sender<WireChunkData>,
    ) -> Self {
        Self {
            codec,
            sample_format,
            #[cfg(feature = "encryption")]
            encryption_psk,
            chunk_tx,
            running: Arc::default(),
        }
    }

    /// Create the encoder for a stream: the server's defaults, overridden by `config`.
    /// Returns the encoder and the sample format it encodes.
    pub(crate) fn create_encoder(
        &self,
        config: &StreamConfig,
    ) -> anyhow::Result<(Box<dyn encoder::Encoder>, SampleFormat)> {
        let format = match config.sample_format.as_deref() {
            Some(s) => s
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid sample format '{s}': {e}"))?,
            None => self.sample_format,
        };
        let enc = encoder::create(&encoder::EncoderConfig {
            codec: config.codec.clone().unwrap_or_else(|| self.codec.clone()),
            format,
            options: String::new(),
            #[cfg(feature = "encryption")]
            encryption_psk: self.encryption_psk.clone(),
        })?;
        Ok((enc, format))
    }

    /// Whether a stream with this name is running: its encode thread has not ended.
    pub(crate) fn is_running(&self, name: &str) -> bool {
        let running = self.running.lock().expect("stream registry poisoned");
        running.get(name).is_some_and(|stop| !stop.is_closed())
    }

    /// Start a stream: list it in the server state (unless it is listed already,
    /// e.g. from a persisted state), register its codec header with the sessions
    /// and spawn its encode thread, which reads `rx`.
    pub(crate) async fn start(
        &self,
        name: &str,
        enc: Box<dyn encoder::Encoder>,
        format: SampleFormat,
        rx: mpsc::Receiver<AudioFrame>,
        state: &tokio::sync::Mutex<ServerState>,
        session: &SessionServer,
    ) {
        {
            let mut s = state.lock().await;
            if !s.streams.iter().any(|existing| existing.id == name) {
                s.streams.push(StreamInfo {
                    id: name.to_string(),
                    status: "idle".into(),
                    uri: String::new(),
                    properties: Default::default(),
                });
            }
        }
        tracing::info!(stream = %name, codec = enc.name(), format = %format, "Stream registered");
        session
            .register_stream_codec(name, enc.name(), enc.header())
            .await;
        let (stop_tx, stop_rx) = oneshot::channel();
        self.running
            .lock()
            .expect("stream registry poisoned")
            .insert(name.to_string(), stop_tx);
        crate::spawn_stream_encoder(
            name.to_string(),
            rx,
            stop_rx,
            enc,
            self.chunk_tx.clone(),
            format.rate(),
            format.channels(),
        );
    }

    /// Stop a stream's encode thread, if it runs. Its receiver is dropped, so
    /// the embedder's sender reports the channel as closed.
    pub(crate) fn stop(&self, name: &str) {
        let stop = self
            .running
            .lock()
            .expect("stream registry poisoned")
            .remove(name);
        if let Some(stop) = stop {
            let _ = stop.send(());
        }
    }
}
