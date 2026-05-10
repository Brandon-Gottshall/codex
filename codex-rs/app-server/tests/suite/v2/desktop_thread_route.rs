use super::connection_handling_websocket::DEFAULT_READ_TIMEOUT;
use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::create_config_toml;
use super::connection_handling_websocket::read_response_for_id;
use super::connection_handling_websocket::read_server_request;
use super::connection_handling_websocket::send_initialize_request;
use super::connection_handling_websocket::send_request;
use super::connection_handling_websocket::send_response;
use super::connection_handling_websocket::spawn_websocket_server;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use app_test_support::create_fake_rollout_with_text_elements;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::to_response;
use codex_app_server_protocol::DesktopThreadRouteAuthority;
use codex_app_server_protocol::DesktopThreadRouteParams;
use codex_app_server_protocol::DesktopThreadRouteResponse;
use codex_app_server_protocol::DesktopThreadSelection;
use codex_app_server_protocol::DesktopThreadSelectionReadParams;
use codex_app_server_protocol::DesktopThreadSelectionReadResponse;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::ServerRequest;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

#[tokio::test]
async fn desktop_thread_route_forwards_to_registered_desktop_host_and_accepts_readback()
-> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let thread_id = create_rollout(codex_home.path(), "2025-01-05T12-10-00")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;

    let result = async {
        let mut desktop = connect_websocket(bind_addr).await?;
        let mut agent = connect_websocket(bind_addr).await?;
        initialize_client(&mut desktop, /*id*/ 1, "desktop-client").await?;
        initialize_client(&mut agent, /*id*/ 2, "agent-link-test").await?;

        send_request(
            &mut agent,
            "desktop/thread/route",
            /*id*/ 10,
            Some(serde_json::to_value(DesktopThreadRouteParams {
                thread_id: thread_id.clone(),
                focus: false,
            })?),
        )
        .await?;

        let (request_id, params) = read_desktop_route_request(&mut desktop).await?;
        assert_eq!(
            params,
            DesktopThreadRouteParams {
                thread_id: thread_id.clone(),
                focus: false,
            }
        );
        send_response(
            &mut desktop,
            request_id,
            serde_json::to_value(DesktopThreadRouteResponse {
                thread_id: thread_id.clone(),
                focus: false,
                routed: true,
                authority: DesktopThreadRouteAuthority::NativeQuietRoute,
                selection: Some(DesktopThreadSelection {
                    thread_id: thread_id.clone(),
                    focused: false,
                }),
                reason: None,
            })?,
        )
        .await?;

        let response: DesktopThreadRouteResponse =
            to_response(read_response_for_id(&mut agent, /*id*/ 10).await?)?;
        assert_eq!(
            response.authority,
            DesktopThreadRouteAuthority::NativeQuietRoute
        );
        assert!(response.routed);
        assert_eq!(
            response.selection,
            Some(DesktopThreadSelection {
                thread_id: thread_id.clone(),
                focused: false,
            })
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    result
}

#[tokio::test]
async fn desktop_thread_route_downgrades_host_response_that_focuses_quiet_route() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let thread_id = create_rollout(codex_home.path(), "2025-01-05T12-15-00")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;

    let result = async {
        let mut desktop = connect_websocket(bind_addr).await?;
        let mut agent = connect_websocket(bind_addr).await?;
        initialize_client(&mut desktop, /*id*/ 1, "desktop-client").await?;
        initialize_client(&mut agent, /*id*/ 2, "agent-link-test").await?;

        send_request(
            &mut agent,
            "desktop/thread/route",
            /*id*/ 10,
            Some(serde_json::to_value(DesktopThreadRouteParams {
                thread_id: thread_id.clone(),
                focus: false,
            })?),
        )
        .await?;

        let (request_id, _params) = read_desktop_route_request(&mut desktop).await?;
        send_response(
            &mut desktop,
            request_id,
            serde_json::to_value(DesktopThreadRouteResponse {
                thread_id: thread_id.clone(),
                focus: false,
                routed: true,
                authority: DesktopThreadRouteAuthority::NativeQuietRoute,
                selection: Some(DesktopThreadSelection {
                    thread_id: thread_id.clone(),
                    focused: true,
                }),
                reason: None,
            })?,
        )
        .await?;

        let response: DesktopThreadRouteResponse =
            to_response(read_response_for_id(&mut agent, /*id*/ 10).await?)?;
        assert_eq!(
            response.authority,
            DesktopThreadRouteAuthority::ValidatedOnly
        );
        assert!(!response.routed);
        assert_eq!(response.selection, None);
        assert!(
            response
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("focused selection"))
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    result
}

#[tokio::test]
async fn desktop_thread_selection_read_forwards_to_registered_desktop_host() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let thread_id = create_rollout(codex_home.path(), "2025-01-05T12-20-00")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;

    let result = async {
        let mut desktop = connect_websocket(bind_addr).await?;
        let mut agent = connect_websocket(bind_addr).await?;
        initialize_client(&mut desktop, /*id*/ 1, "desktop-client").await?;
        initialize_client(&mut agent, /*id*/ 2, "agent-link-test").await?;

        send_request(
            &mut agent,
            "desktop/thread/selection/read",
            /*id*/ 10,
            Some(serde_json::to_value(DesktopThreadSelectionReadParams {})?),
        )
        .await?;

        let (request_id, _params) = read_desktop_selection_read_request(&mut desktop).await?;
        send_response(
            &mut desktop,
            request_id,
            serde_json::to_value(DesktopThreadSelectionReadResponse {
                selection: Some(DesktopThreadSelection {
                    thread_id: thread_id.clone(),
                    focused: false,
                }),
                authority: DesktopThreadRouteAuthority::NativeQuietRoute,
                reason: None,
            })?,
        )
        .await?;

        let response: DesktopThreadSelectionReadResponse =
            to_response(read_response_for_id(&mut agent, /*id*/ 10).await?)?;
        assert_eq!(
            response.authority,
            DesktopThreadRouteAuthority::NativeQuietRoute
        );
        assert_eq!(
            response.selection,
            Some(DesktopThreadSelection {
                thread_id: thread_id.clone(),
                focused: false,
            })
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    result
}

async fn initialize_client(
    ws: &mut super::connection_handling_websocket::WsClient,
    id: i64,
    client_name: &str,
) -> Result<JSONRPCResponse> {
    send_initialize_request(ws, id, client_name).await?;
    timeout(DEFAULT_READ_TIMEOUT, read_response_for_id(ws, id)).await?
}

async fn read_desktop_route_request(
    ws: &mut super::connection_handling_websocket::WsClient,
) -> Result<(
    codex_app_server_protocol::RequestId,
    DesktopThreadRouteParams,
)> {
    match read_server_request(ws).await? {
        ServerRequest::DesktopThreadRoute { request_id, params } => Ok((request_id, params)),
        request => bail!("expected desktop thread route server request, got {request:?}"),
    }
}

async fn read_desktop_selection_read_request(
    ws: &mut super::connection_handling_websocket::WsClient,
) -> Result<(
    codex_app_server_protocol::RequestId,
    DesktopThreadSelectionReadParams,
)> {
    match read_server_request(ws).await? {
        ServerRequest::DesktopThreadSelectionRead { request_id, params } => {
            Ok((request_id, params))
        }
        request => bail!("expected desktop thread selection read server request, got {request:?}"),
    }
}

fn create_rollout(codex_home: &std::path::Path, filename_ts: &str) -> Result<String> {
    create_fake_rollout_with_text_elements(
        codex_home,
        filename_ts,
        "2025-01-05T12:00:00Z",
        "Saved user message",
        Vec::new(),
        Some("mock_provider"),
        /*git_info*/ None,
    )
}
