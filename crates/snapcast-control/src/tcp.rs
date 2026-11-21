//! Control server — JSON-RPC over TCP for Snapcast control clients.

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::jsonrpc::{self, RpcResult};
use crate::{ControlEvent, ControlServer, SnapControl};

/// Runs the JSON-RPC control server on a TCP listener.
pub(crate) async fn serve<C: SnapControl + Clone + 'static>(
    server: ControlServer<C>,
    listener: TcpListener,
) -> Result<()> {
    if let Ok(addr) = listener.local_addr() {
        tracing::info!(%addr, "Control server (TCP) listening");
    }

    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::debug!(%peer, "Control client connected");

        let server = server.clone();
        let mut notify_rx = server.notify_tx.subscribe();

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            let client_id = peer.to_string();
            let mut authenticated = !server.auth.enabled;

            loop {
                tokio::select! {
                    line = lines.next_line() => {
                        let Ok(Some(line)) = line else { break };
                        if line.trim().is_empty() { continue; }

                        let Ok(request) = serde_json::from_str::<Value>(&line) else {
                            let err = serde_json::json!({
                                "jsonrpc": "2.0", "id": null,
                                "error": {"code": -32700, "message": "Parse error"}
                            });
                            let _ = send_json(&mut writer, &err).await;
                            continue;
                        };

                        // Auth gate: allow Server.GetToken and Server.Authenticate without auth
                        let method = request["method"].as_str().unwrap_or("");
                        if !authenticated
                            && method != "Server.GetToken"
                            && method != "Server.Authenticate"
                        {
                            let err = serde_json::json!({
                                "jsonrpc": "2.0", "id": request["id"],
                                "error": {"code": -32000, "message": "Unauthorized — call Server.Authenticate first"}
                            });
                            let _ = send_json(&mut writer, &err).await;
                            continue;
                        }

                        match jsonrpc::handle_request(&request, &server.auth, &server.control).await {
                            RpcResult::Response { response, notification } => {
                                // Mark as authenticated if Server.Authenticate succeeded
                                if method == "Server.Authenticate" && response["result"]["ok"] == true {
                                    authenticated = true;
                                }
                                let _ = send_json(&mut writer, &response).await;
                                if let Some(n) = notification {
                                    server.notify(n);
                                }
                            }
                            RpcResult::Unknown => {
                                let extensions = server.extensions.as_deref();
                                if let Some(ext) = extensions.filter(|e| e.methods.contains(method)) {
                                    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                                    let _ = ext.event_tx.send(ControlEvent::JsonRpc {
                                        client_id: client_id.clone(),
                                        request,
                                        response_tx: Some(resp_tx),
                                    }).await;
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(5),
                                        resp_rx,
                                    ).await {
                                        Ok(Ok(response)) => {
                                            let _ = send_json(&mut writer, &response).await;
                                        }
                                        _ => {
                                            let err = serde_json::json!({
                                                "jsonrpc": "2.0", "id": null,
                                                "error": {"code": -32603, "message": "Handler timeout"}
                                            });
                                            let _ = send_json(&mut writer, &err).await;
                                        }
                                    }
                                } else if let Some(ext) = extensions.filter(|e| e.notifications.contains(method)) {
                                    let _ = ext.event_tx.send(ControlEvent::JsonRpc {
                                        client_id: client_id.clone(),
                                        request,
                                        response_tx: None,
                                    }).await;
                                } else {
                                    let err = serde_json::json!({
                                        "jsonrpc": "2.0", "id": request["id"],
                                        "error": {"code": -32601, "message": "Method not found"}
                                    });
                                    let _ = send_json(&mut writer, &err).await;
                                }
                            }
                        }
                    }
                    notification = notify_rx.recv() => {
                        if let Ok(n) = notification
                            && send_json(&mut writer, &n).await.is_err()
                        {
                            break;
                        }
                    }
                }
            }
            tracing::debug!(%peer, "Control client disconnected");
        });
    }
}

async fn send_json<W: AsyncWriteExt + Unpin>(writer: &mut W, value: &Value) -> Result<()> {
    let mut msg = serde_json::to_string(value)?;
    msg.push('\n');
    writer.write_all(msg.as_bytes()).await.context("write json")
}

#[cfg(test)]
mod tests {
    //! Unit tests for the control-server wire framing.
    //!
    //! The bulk of this module is the TCP accept loop in [`serve`] (accept / `tokio::select!` over a `BufReader` line stream and a broadcast
    //! receiver). That is genuine socket I/O and the request-routing / auth-gate
    //! logic is written inline inside the accept loop rather than as callable
    //! functions, so it is only reachable through a live TCP connection and is
    //! covered by integration tests (`snapcast-tests/tests/control.rs`), not here. The one pure, cheaply-testable
    //! seam is [`send_json`], which serialises a JSON value and frames it with a
    //! trailing newline onto any `AsyncWrite`. We exercise it against an
    //! in-memory `Vec<u8>` buffer — no real sockets, fully deterministic.

    use super::*;

    /// A single value is serialised and terminated with exactly one newline.
    #[tokio::test]
    async fn send_json_appends_single_newline() {
        let mut buf: Vec<u8> = Vec::new();
        let value = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {"ok": true}});

        send_json(&mut buf, &value).await.expect("send_json ok");

        let out = String::from_utf8(buf).expect("utf8");
        assert!(out.ends_with('\n'), "must be newline-terminated: {out:?}");
        assert_eq!(
            out.matches('\n').count(),
            1,
            "exactly one framing newline, got: {out:?}"
        );
    }

    /// The bytes written are exactly `serde_json::to_string(value)` + `\n`, so a
    /// client parsing a line back gets the same value.
    #[tokio::test]
    async fn send_json_matches_serde_serialization_plus_newline() {
        let mut buf: Vec<u8> = Vec::new();
        let value = serde_json::json!({"a": 1, "b": [true, null, "x"]});

        send_json(&mut buf, &value).await.expect("send_json ok");

        let out = String::from_utf8(buf).expect("utf8");
        let expected = format!("{}\n", serde_json::to_string(&value).unwrap());
        assert_eq!(out, expected);

        // And the framed line round-trips back to the original value.
        let line = out.strip_suffix('\n').unwrap();
        let reparsed: Value = serde_json::from_str(line).expect("reparse");
        assert_eq!(reparsed, value);
    }

    /// Two sequential sends append onto the same writer, producing two
    /// independently parseable newline-delimited frames (the on-wire protocol).
    #[tokio::test]
    async fn send_json_frames_multiple_messages() {
        let mut buf: Vec<u8> = Vec::new();
        let first = serde_json::json!({"id": 1});
        let second = serde_json::json!({"id": 2});

        send_json(&mut buf, &first).await.expect("first");
        send_json(&mut buf, &second).await.expect("second");

        let out = String::from_utf8(buf).expect("utf8");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2, "two frames expected: {out:?}");
        assert_eq!(serde_json::from_str::<Value>(lines[0]).unwrap(), first);
        assert_eq!(serde_json::from_str::<Value>(lines[1]).unwrap(), second);
    }

    /// A JSON `null` value is still framed (it is a valid whole message, e.g. a
    /// bare notification body); it must not be dropped or produce empty output.
    #[tokio::test]
    async fn send_json_handles_null_value() {
        let mut buf: Vec<u8> = Vec::new();

        send_json(&mut buf, &Value::Null)
            .await
            .expect("send_json ok");

        let out = String::from_utf8(buf).expect("utf8");
        assert_eq!(out, "null\n");
    }

    /// The parse-error envelope that the accept loop emits on malformed input
    /// serialises to a well-formed, newline-framed JSON-RPC error line.
    #[tokio::test]
    async fn send_json_frames_parse_error_envelope() {
        let mut buf: Vec<u8> = Vec::new();
        let err = serde_json::json!({
            "jsonrpc": "2.0", "id": null,
            "error": {"code": -32700, "message": "Parse error"}
        });

        send_json(&mut buf, &err).await.expect("send_json ok");

        let out = String::from_utf8(buf).expect("utf8");
        let line = out.strip_suffix('\n').expect("trailing newline");
        let parsed: Value = serde_json::from_str(line).expect("valid json line");
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["error"]["code"], -32700);
        assert_eq!(parsed["error"]["message"], "Parse error");
    }

    /// Non-ASCII content survives serialisation and round-trips unchanged, so
    /// the newline framing is byte-safe for the whole message.
    #[tokio::test]
    async fn send_json_preserves_unicode_and_stays_parseable() {
        let mut buf: Vec<u8> = Vec::new();
        let value = serde_json::json!({"name": "Wohnzimmer — Über", "emoji": "🎵"});

        send_json(&mut buf, &value).await.expect("send_json ok");

        let out = String::from_utf8(buf).expect("utf8");
        // Exactly one framing newline even with embedded multi-byte chars.
        assert_eq!(out.matches('\n').count(), 1);
        let line = out.strip_suffix('\n').unwrap();
        let reparsed: Value = serde_json::from_str(line).expect("reparse");
        assert_eq!(reparsed, value);
    }
}
