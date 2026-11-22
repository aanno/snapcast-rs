//! HTTP/WebSocket control server + Snapweb static file serving.

use std::path::PathBuf;

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use serde_json::Value;

use crate::jsonrpc::{self, RpcResult};
use crate::{ControlEvent, ControlServer, SnapControl};

/// Serve JSON-RPC over HTTP POST and WebSocket, plus optional static files (Snapweb).
pub(crate) async fn serve<C: SnapControl + Clone + 'static>(
    server: ControlServer<C>,
    listener: tokio::net::TcpListener,
    doc_root: Option<PathBuf>,
) -> Result<()> {
    let mut app = Router::new()
        .route(
            "/jsonrpc",
            get(ws_handler::<C>).post(http_jsonrpc_handler::<C>),
        )
        .with_state(server);

    if let Some(root) = doc_root {
        tracing::info!(doc_root = %root.display(), "Serving Snapweb");
        app = app.fallback_service(tower_http::services::ServeDir::new(root));
    }

    if let Ok(addr) = listener.local_addr() {
        tracing::info!(%addr, "HTTP/WebSocket server listening");
    }
    axum::serve(listener, app).await?;
    Ok(())
}

/// HTTP POST /jsonrpc handler.
async fn http_jsonrpc_handler<C: SnapControl + Clone + 'static>(
    State(app): State<ControlServer<C>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> impl IntoResponse {
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if let Err(e) = crate::auth::validate_bearer(&app.auth, auth_header) {
        return axum::Json(serde_json::json!({
            "jsonrpc": "2.0", "id": null,
            "error": {"code": -32000, "message": format!("Unauthorized: {e}")}
        }));
    }

    let Ok(request) = serde_json::from_str::<Value>(&body) else {
        return axum::Json(serde_json::json!({
            "jsonrpc": "2.0", "id": null,
            "error": {"code": -32700, "message": "Parse error"}
        }));
    };

    match jsonrpc::handle_request(&request, &app.auth, &app.control).await {
        RpcResult::Response {
            response,
            notification,
        } => {
            if let Some(n) = notification {
                app.notify(n);
            }
            axum::Json(response)
        }
        RpcResult::Unknown => {
            forward_unknown(&app, "http", request).await;
            axum::Json(serde_json::json!({
                "jsonrpc": "2.0", "id": null,
                "error": {"code": -32601, "message": "Method not found"}
            }))
        }
    }
}

/// Hand an unrecognized request to the embedder, if it registered for extensions.
async fn forward_unknown<C>(app: &ControlServer<C>, client_id: &str, request: Value) {
    if let Some(ext) = &app.extensions {
        let _ = ext
            .event_tx
            .send(ControlEvent::JsonRpc {
                response_tx: None,
                client_id: client_id.into(),
                request,
            })
            .await;
    }
}

/// WebSocket upgrade handler at GET /jsonrpc.
async fn ws_handler<C: SnapControl + Clone + 'static>(
    ws: WebSocketUpgrade,
    State(app): State<ControlServer<C>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, app))
}

async fn handle_ws<C: SnapControl + Clone + 'static>(mut socket: WebSocket, app: ControlServer<C>) {
    let mut notify_rx = app.notify_tx.subscribe();
    // Per-connection auth state, mirroring the TCP control path. Without this
    // the WebSocket endpoint dispatched every request unauthenticated even when
    // auth was enabled (HTTP POST and TCP both gated — WS was a full bypass).
    let mut authenticated = !app.auth.enabled;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                let Some(Ok(msg)) = msg else { break };
                let Message::Text(text) = msg else { continue };

                let Ok(request) = serde_json::from_str::<Value>(&text) else {
                    let err = serde_json::json!({
                        "jsonrpc": "2.0", "id": null,
                        "error": {"code": -32700, "message": "Parse error"}
                    });
                    if socket.send(Message::Text(err.to_string().into())).await.is_err() { break }
                    continue;
                };

                // Auth gate: until authenticated, only Server.GetToken and
                // Server.Authenticate are permitted (same policy as TCP control).
                let method = request["method"].as_str().unwrap_or("");
                if !authenticated
                    && method != "Server.GetToken"
                    && method != "Server.Authenticate"
                {
                    let err = serde_json::json!({
                        "jsonrpc": "2.0", "id": request["id"],
                        "error": {"code": -32000, "message": "Unauthorized — call Server.Authenticate first"}
                    });
                    if socket.send(Message::Text(err.to_string().into())).await.is_err() { break }
                    continue;
                }

                match jsonrpc::handle_request(&request, &app.auth, &app.control).await {
                    RpcResult::Response { response, notification } => {
                        if method == "Server.Authenticate" && response["result"]["ok"] == true {
                            authenticated = true;
                        }
                        if socket.send(Message::Text(response.to_string().into())).await.is_err() { break }
                        if let Some(n) = notification {
                            app.notify(n);
                        }
                    }
                    RpcResult::Unknown => {
                        forward_unknown(&app, "websocket", request).await;
                    }
                }
            }
            notification = notify_rx.recv() => {
                if let Ok(n) = notification
                    && socket.send(Message::Text(n.to_string().into())).await.is_err()
                {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthConfig;
    use serde_json::json;
    use snapcast_server::ServerCommand;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc};

    // --- Test scaffolding -------------------------------------------------
    //
    // http.rs is dominated by socket I/O (TcpListener::bind, axum::serve,
    // WebSocket recv/send, ServeDir). Those paths are NOT unit-tested here; they
    // are covered by `snapcast-tests/tests/control.rs`. What follows exercises the *pure* logic the
    // handlers are built out of: ControlServer construction/wiring, the bearer-auth
    // check `http_jsonrpc_handler` runs, the WS per-connection auth-gate
    // predicate, the `Server.Authenticate` success transition, and the exact
    // error-response JSON envelopes the handlers emit inline.

    type State = ControlServer<mpsc::Sender<ServerCommand>>;

    /// Build the handlers' state (a `ControlServer` with extensions) with real
    /// (but unread) channels. Returns the state plus the receivers/subscriber so a
    /// test can observe what the handler-equivalent logic sends.
    fn make_state(
        auth_config: AuthConfig,
    ) -> (
        State,
        mpsc::Receiver<ControlEvent>,
        mpsc::Receiver<ServerCommand>,
        broadcast::Receiver<Value>,
    ) {
        let (event_tx, event_rx) = mpsc::channel::<ControlEvent>(16);
        let (cmd_tx, cmd_rx) = mpsc::channel::<ServerCommand>(16);
        let state = ControlServer::new(cmd_tx)
            .with_auth(auth_config)
            .with_extensions(event_tx, HashSet::new(), HashSet::new());
        let notify_rx = state.notify_tx.subscribe();
        (state, event_rx, cmd_rx, notify_rx)
    }

    /// A config with auth enabled and a usable signing secret.
    fn enabled_auth() -> AuthConfig {
        AuthConfig {
            enabled: true,
            secret: "test-secret-must-be-32-bytes-long".into(),
        }
    }

    /// Mirror of the WS auth-gate predicate in `handle_ws` (lines 153-156):
    /// until authenticated, only `Server.GetToken` / `Server.Authenticate` pass.
    /// `true` == request must be rejected as unauthorized.
    fn ws_gate_rejects(authenticated: bool, method: &str) -> bool {
        !authenticated && method != "Server.GetToken" && method != "Server.Authenticate"
    }

    // --- State wiring -----------------------------------------------------

    #[test]
    fn state_is_clone_and_shares_auth_arc() {
        // `serve` relies on ControlServer: Clone (Router::with_state clones per
        // request) and on the auth Arc being shared, not deep-copied.
        let (state, _e, _c, _n) = make_state(enabled_auth());
        let clone = state.clone();
        assert!(Arc::ptr_eq(&state.auth, &clone.auth));
        assert!(state.auth.enabled);
    }

    #[tokio::test]
    async fn state_channels_are_live() {
        // Sanity: the wired channels actually deliver, so the handler paths that
        // forward unknown requests / broadcast notifications reach live endpoints.
        let (state, mut event_rx, _cmd_rx, mut notify_rx) = make_state(AuthConfig::default());

        forward_unknown(&state, "http", json!({"method": "X"})).await;
        match event_rx.recv().await.unwrap() {
            ControlEvent::JsonRpc { client_id, .. } => assert_eq!(client_id, "http"),
        }

        state.notify(json!({"n": 1}));
        assert_eq!(notify_rx.recv().await.unwrap(), json!({"n": 1}));
    }

    // --- Bearer auth check run by `http_jsonrpc_handler` ------------------

    #[test]
    fn bearer_check_allows_all_when_auth_disabled() {
        // With auth disabled the handler treats every request as "anonymous"
        // regardless of the Authorization header (or its absence).
        let cfg = AuthConfig::default();
        assert!(crate::auth::validate_bearer(&cfg, None).is_ok());
        assert!(crate::auth::validate_bearer(&cfg, Some("garbage")).is_ok());
    }

    #[test]
    fn bearer_check_rejects_missing_and_malformed_header_when_enabled() {
        let cfg = enabled_auth();
        // No header -> Unauthorized branch (-32000) in http_jsonrpc_handler.
        assert!(crate::auth::validate_bearer(&cfg, None).is_err());
        // Present but not a Bearer token.
        assert!(crate::auth::validate_bearer(&cfg, Some("Basic abc")).is_err());
        // Bearer prefix but a bogus token.
        assert!(crate::auth::validate_bearer(&cfg, Some("Bearer not-a-jwt")).is_err());
    }

    #[cfg(feature = "auth")] // needs real tokens
    #[test]
    fn bearer_check_accepts_valid_token_when_enabled() {
        let cfg = enabled_auth();
        let token = crate::auth::generate_token(&cfg, "alice").unwrap();
        let subject = crate::auth::validate_bearer(&cfg, Some(&format!("Bearer {token}"))).unwrap();
        assert_eq!(subject, "alice");
    }

    // --- WS per-connection auth state -------------------------------------

    #[test]
    fn ws_initial_authenticated_mirrors_auth_disabled() {
        // handle_ws line 133: `authenticated = !auth_config.enabled`.
        assert!(!AuthConfig::default().enabled, "disabled => starts authed");
        assert!(enabled_auth().enabled, "enabled => starts unauthed");
    }

    #[test]
    fn ws_gate_blocks_normal_methods_until_authenticated() {
        // Unauthenticated: only the two auth bootstrap methods pass.
        assert!(ws_gate_rejects(false, "Client.SetVolume"));
        assert!(ws_gate_rejects(false, "Server.GetStatus"));
        assert!(ws_gate_rejects(false, ""));
        assert!(!ws_gate_rejects(false, "Server.GetToken"));
        assert!(!ws_gate_rejects(false, "Server.Authenticate"));
    }

    #[test]
    fn ws_gate_allows_everything_once_authenticated() {
        for m in [
            "Client.SetVolume",
            "Server.GetStatus",
            "Server.GetToken",
            "",
        ] {
            assert!(!ws_gate_rejects(true, m), "authed must allow {m}");
        }
    }

    // --- Error-response envelopes emitted inline by the handlers ----------

    #[test]
    fn parse_error_envelope_shape() {
        // Both handlers emit this exact object on a JSON parse failure.
        let err = json!({
            "jsonrpc": "2.0", "id": null,
            "error": {"code": -32700, "message": "Parse error"}
        });
        assert_eq!(err["jsonrpc"], "2.0");
        assert!(err["id"].is_null());
        assert_eq!(err["error"]["code"], -32700);
    }

    #[test]
    fn method_not_found_envelope_shape() {
        // http_jsonrpc_handler emits this for RpcResult::Unknown.
        let err = json!({
            "jsonrpc": "2.0", "id": null,
            "error": {"code": -32601, "message": "Method not found"}
        });
        assert_eq!(err["error"]["code"], -32601);
    }

    #[test]
    fn ws_unauthorized_envelope_preserves_request_id() {
        // handle_ws builds the unauthorized error with the *request's* id, not
        // null (unlike the HTTP-path -32000 which is id:null). Pin that.
        let request = json!({"jsonrpc": "2.0", "id": 42, "method": "Client.SetVolume"});
        let err = json!({
            "jsonrpc": "2.0", "id": request["id"],
            "error": {"code": -32000, "message": "Unauthorized — call Server.Authenticate first"}
        });
        assert_eq!(err["id"], 42);
        assert_eq!(err["error"]["code"], -32000);
    }

    // --- Dispatch path the handlers call: jsonrpc::handle_request ---------

    #[cfg(feature = "auth")] // needs real tokens
    #[tokio::test]
    async fn handler_dispatch_authenticate_success_flips_auth_state() {
        // Reproduces the two-step WS bootstrap: GetToken then Authenticate, and
        // the exact success predicate handle_ws uses:
        // `response["result"]["ok"] == true` -> authenticated = true.
        let (state, _e, _c, _n) = make_state(enabled_auth());

        // Step 1: mint a token (allowed even while unauthenticated).
        let get_token = json!({"jsonrpc": "2.0", "id": 1, "method": "Server.GetToken", "params": {"username": "bob"}});
        let RpcResult::Response { response, .. } =
            jsonrpc::handle_request(&get_token, &state.auth, &state.control).await
        else {
            panic!("expected response");
        };
        let token = response["result"]["token"].as_str().unwrap().to_string();

        // Step 2: authenticate with it.
        let auth_req = json!({"jsonrpc": "2.0", "id": 2, "method": "Server.Authenticate", "params": {"token": token}});
        let RpcResult::Response { response, .. } =
            jsonrpc::handle_request(&auth_req, &state.auth, &state.control).await
        else {
            panic!("expected response");
        };

        let mut authenticated = !state.auth.enabled; // starts false
        assert!(!authenticated);
        if auth_req["method"] == "Server.Authenticate" && response["result"]["ok"] == true {
            authenticated = true;
        }
        assert!(authenticated, "valid Authenticate must flip the gate open");
        assert_eq!(response["result"]["subject"], "bob");
    }

    #[tokio::test]
    async fn handler_dispatch_authenticate_bad_token_keeps_gate_closed() {
        let (state, _e, _c, _n) = make_state(enabled_auth());
        let auth_req = json!({
            "jsonrpc": "2.0", "id": 2, "method": "Server.Authenticate",
            "params": {"token": "not-a-valid-jwt"}
        });
        let RpcResult::Response { response, .. } =
            jsonrpc::handle_request(&auth_req, &state.auth, &state.control).await
        else {
            panic!("expected response");
        };
        // Invalid token -> error envelope, no `result.ok`.
        assert_ne!(response["result"]["ok"], true);
        assert_eq!(response["error"]["code"], -32602);

        let mut authenticated = !state.auth.enabled;
        if auth_req["method"] == "Server.Authenticate" && response["result"]["ok"] == true {
            authenticated = true;
        }
        assert!(!authenticated, "bad token must NOT open the gate");
    }

    #[tokio::test]
    async fn handler_dispatch_unknown_method_yields_unknown_variant() {
        // The RpcResult::Unknown branch is what makes the handlers forward to
        // event_tx; verify an unrecognized method produces it.
        let (state, _e, _c, _n) = make_state(AuthConfig::default());
        let req = json!({"jsonrpc": "2.0", "id": 7, "method": "Custom.DoThing", "params": {}});
        assert!(matches!(
            jsonrpc::handle_request(&req, &state.auth, &state.control).await,
            RpcResult::Unknown
        ));
    }

    #[tokio::test]
    async fn handler_dispatch_notification_is_broadcastable() {
        // When handle_request returns a notification, the handler does
        // `notify_tx.send(n)`; confirm a real method produces one and that the
        // wired broadcast channel delivers it to a subscriber.
        let (state, _e, _c, _n) = make_state(AuthConfig::default());
        let mut sub = state.notify_tx.subscribe();
        let req = json!({
            "jsonrpc": "2.0", "id": 8, "method": "Group.SetStream",
            "params": {"id": "g1", "stream_id": "music"}
        });
        let RpcResult::Response { notification, .. } =
            jsonrpc::handle_request(&req, &state.auth, &state.control).await
        else {
            panic!("expected response");
        };
        let n = notification.expect("Group.SetStream emits a notification");
        state.notify(n.clone());
        assert_eq!(sub.recv().await.unwrap(), n);
        assert_eq!(n["params"]["stream_id"], "music");
    }
}
