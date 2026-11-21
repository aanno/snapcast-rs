//! The server's own threads and tasks log in the embedder's tracing context.
//!
//! An embedder that installs a scoped subscriber (instead of a global one), or
//! runs the server inside a span to tag its events, must see the events of the
//! encoder thread and of the session tasks in that subscriber and span.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use snapcast_server::{ServerConfig, SnapServer};
use tracing::Instrument;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// One captured event: its message and the names of its enclosing spans.
#[derive(Debug, Clone)]
struct Captured {
    message: String,
    spans: Vec<String>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Captured>>>);

impl Capture {
    fn find(&self, message: &str) -> Option<Captured> {
        let events = self.0.lock().unwrap();
        events.iter().find(|e| e.message == message).cloned()
    }

    async fn wait_for(&self, message: &str) -> Captured {
        for _ in 0..200 {
            if let Some(event) = self.find(message) {
                return event;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "event {message:?} not captured: {:?}",
            self.0.lock().unwrap()
        );
    }
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S> tracing_subscriber::Layer<S> for Capture
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        let spans = ctx
            .event_scope(event)
            .map(|scope| scope.map(|span| span.name().to_string()).collect())
            .unwrap_or_default();
        self.0.lock().unwrap().push(Captured {
            message: visitor.0,
            spans,
        });
    }
}

#[tokio::test]
async fn server_threads_and_tasks_log_in_the_callers_context() {
    let capture = Capture::default();
    // Scoped, not global: the server's own threads only see it if the
    // server hands its context over.
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));

    let (mut server, _events) = SnapServer::new(ServerConfig::default());
    let _audio = server.add_stream("default");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let span = tracing::info_span!("embedder");
    tokio::spawn(
        async move {
            let _ = server.serve(listener).await;
        }
        .instrument(span),
    );

    // Encoder thread.
    let started = capture.wait_for("Stream encoder started").await;
    assert_eq!(started.spans, ["embedder"]);

    // Session server task (accept loop) and per-client task: a client that
    // hangs up before its Hello ends its session with an error.
    drop(tokio::net::TcpStream::connect(addr).await.unwrap());
    let connecting = capture.wait_for("Client connecting").await;
    assert_eq!(connecting.spans, ["embedder"]);
    let ended = capture.wait_for("Client session ended").await;
    assert_eq!(ended.spans, ["embedder"]);
}
