//! End-to-end tests of the JSON-RPC control API (`snapcast-control`) against an
//! embedded server: real sockets for the control transports, a real streaming
//! client, and the server's events turned into notifications.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use snapcast_client::ClientEvent;
use snapcast_control::ControlServer;
use snapcast_server::{ServerCommand, ServerEvent, SnapServer};
use snapcast_tests::{connect_client_with_id, expect_event, spawn_serving};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const TIMEOUT: Duration = Duration::from_secs(5);

/// A serving server with a control server forwarding its events.
struct Harness {
    control: ControlServer<mpsc::Sender<ServerCommand>>,
    /// Events passed through by `forward_events`.
    passthrough: mpsc::Receiver<ServerEvent>,
    stream_port: u16,
}

async fn start() -> Harness {
    let (mut server, events) = SnapServer::new(Default::default());
    let _audio = server.add_stream("default");
    let control = ControlServer::new(server.command_sender());
    let (pass_tx, passthrough) = mpsc::channel(64);
    tokio::spawn({
        let control = control.clone();
        async move { control.forward_events(events, Some(pass_tx)).await }
    });
    let stream_port = spawn_serving(server).await;
    Harness {
        control,
        passthrough,
        stream_port,
    }
}

async fn ephemeral_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// Newline-delimited JSON-RPC over TCP.
struct TcpControl {
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TcpControl {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (reader, writer) = stream.into_split();
        Self {
            lines: BufReader::new(reader).lines(),
            writer,
        }
    }

    async fn send(&mut self, request: Value) {
        let line = format!("{request}\n");
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    /// The next message for which `pred` holds; others (e.g. notifications) are skipped.
    async fn next_matching(&mut self, pred: impl Fn(&Value) -> bool) -> Value {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let line = self
                    .lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("connection closed");
                let msg: Value = serde_json::from_str(&line).unwrap();
                if pred(&msg) {
                    return msg;
                }
            }
        })
        .await
        .expect("timed out waiting for a control message")
    }

    async fn call(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        self.next_matching(|m| m["id"] == id).await
    }
}

fn is_notification(method: &'static str) -> impl Fn(&Value) -> bool {
    move |m| m["method"] == method && m.get("id").is_none()
}

#[tokio::test]
async fn tcp_control_sees_the_client_and_changes_its_volume() {
    let mut h = start().await;
    let (listener, control_port) = ephemeral_listener().await;
    tokio::spawn({
        let control = h.control.clone();
        async move { control.serve_tcp(listener).await }
    });
    let mut ctl = TcpControl::connect(control_port).await;

    // A streaming client connects: the controller is told, and the embedder
    // still gets the event through the passthrough.
    let mut client = connect_client_with_id(h.stream_port, "control-test").await;
    let connected = ctl.next_matching(is_notification("Client.OnConnect")).await;
    let client_id = connected["params"]["id"].as_str().unwrap().to_string();
    assert_eq!(connected["params"]["client"]["id"], client_id.as_str());
    let passed = tokio::time::timeout(TIMEOUT, h.passthrough.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(passed, ServerEvent::ClientConnected { ref id, .. } if *id == client_id));

    // Server.GetStatus lists it.
    let status = ctl.call(1, "Server.GetStatus", json!({})).await;
    let clients: Vec<&str> = status["result"]["server"]["groups"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|g| g["clients"].as_array().unwrap())
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert!(clients.contains(&client_id.as_str()), "{clients:?}");

    // Client.SetVolume reaches the streaming client.
    let reply = ctl
        .call(
            2,
            "Client.SetVolume",
            json!({"id": client_id, "volume": {"percent": 30, "muted": false}}),
        )
        .await;
    assert_eq!(reply["result"]["volume"]["percent"], 30);
    let volume = expect_event(&mut client.events, 5000, |e| match e {
        ClientEvent::VolumeChanged { volume, .. } if volume == 30 => Some(volume),
        _ => None,
    })
    .await;
    assert_eq!(volume, 30);

    // Unknown methods are answered, not dropped.
    let unknown = ctl.call(3, "Foo.Bar", json!({})).await;
    assert_eq!(unknown["error"]["code"], -32601);
}

#[tokio::test]
async fn http_serves_websocket_post_and_static_files() {
    let h = start().await;

    let doc_root =
        std::env::temp_dir().join(format!("snapcast-control-test-{}", std::process::id()));
    std::fs::create_dir_all(&doc_root).unwrap();
    std::fs::write(
        doc_root.join("index.html"),
        "<title>snapweb stand-in</title>",
    )
    .unwrap();

    let (listener, http_port) = ephemeral_listener().await;
    tokio::spawn({
        let control = h.control.clone();
        let doc_root = doc_root.clone();
        async move { control.serve_http(listener, Some(doc_root)).await }
    });

    // WebSocket: a request is answered, and the notification it causes is
    // broadcast to the controllers, including this one.
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{http_port}/jsonrpc"))
            .await
            .unwrap();
    let request = json!({"jsonrpc": "2.0", "id": 1, "method": "Client.SetName",
                         "params": {"id": "nobody", "name": "Kitchen"}});
    ws.send(Message::Text(request.to_string().into()))
        .await
        .unwrap();
    let mut got_reply = false;
    let mut got_notification = false;
    tokio::time::timeout(TIMEOUT, async {
        while !(got_reply && got_notification) {
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                panic!("websocket closed")
            };
            let msg: Value = serde_json::from_str(&text).unwrap();
            if msg["id"] == 1 {
                assert_eq!(msg["result"]["name"], "Kitchen");
                got_reply = true;
            } else if msg["method"] == "Client.OnNameChanged" {
                assert_eq!(msg["params"]["name"], "Kitchen");
                got_notification = true;
            }
        }
    })
    .await
    .expect("timed out waiting for reply and notification");

    // HTTP POST /jsonrpc.
    let body = json!({"jsonrpc": "2.0", "id": 7, "method": "Server.GetRPCVersion"}).to_string();
    let response = http_request(
        http_port,
        &format!(
            "POST /jsonrpc HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    let json_start = response.find('{').expect("JSON body");
    let reply: Value = serde_json::from_str(&response[json_start..]).unwrap();
    assert_eq!(reply["id"], 7);
    assert_eq!(reply["result"]["major"], 2);

    // Static files (Snapweb) for all other paths.
    let page = http_request(
        http_port,
        "GET /index.html HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    assert!(page.contains("snapweb stand-in"));

    std::fs::remove_dir_all(&doc_root).ok();
}

async fn http_request(port: u16, request: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(TIMEOUT, stream.read_to_string(&mut response))
        .await
        .expect("timed out waiting for the HTTP response")
        .unwrap();
    response
}
