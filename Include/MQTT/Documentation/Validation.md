# Validation Guide

This page is part of the repo-local documentation set. Start at [Documentation Home](README.md) if you want the full map.

If you want the shortest clone-to-green walkthrough before reading the detailed validation options, start with [Getting Started](GettingStarted.md).

The supported validation path is intentionally small and stable. If the checks below pass, you are on the intended public green path.

The only supported include for public consumers is `#include <MQTT\MQTT.mqh>`.

Direct includes of any other header under `Include/MQTT/` are outside the documented public API surface.

## Compile-First Smoke Path

Compile these files in MetaEditor from the MT5 `MQL5` root:

1. `Scripts/MQTT/Tests/Unit/Protocol/TEST_MQTT.mq5`
2. `Scripts/MQTT/Tests/Unit/Session/TEST_MqttClient.mq5`
3. `Scripts/MQTT/Tests/Unit/Transport/TEST_Transport.mq5`
4. `Scripts/MQTT/Tests/Unit/Queue/TEST_PublishQueue.mq5`
5. `Experts/MQTT/Harnesses/PublishQueueTestHarness.mq5`

Expected result: each file compiles with `0 errors, 0 warnings`.

`Unit/Queue/TEST_PublishQueue.mq5` and `PublishQueueTestHarness.mq5` share the same suite code through `PublishQueueTestSuite.mqh`, so they are the fastest way to verify the publish queue and coordinator runtime path together.

The compile-first path is intentionally offline. `Scripts/MQTT/Tests/LiveBrokerConfig.mqh` ships with live broker testing disabled so a fresh public clone does not require network credentials or local broker setup just to compile.

For local Windows automation, run `Tools/compile-public-validation.ps1` from the repository root. By default it compiles the curated public validation set against the current working tree when the repository itself is your MT5 `MQL5` folder.

For one-off local compiles against a different active MT5 data root, `Scripts/MQTT/Tools/compile-mql5.ps1` also supports `-SyncMqttTrees` plus `-TargetMql5Root`. That path mirrors `Include/MQTT`, `Scripts/MQTT`, and `Experts/MQTT` into the target root first, then compiles the remapped source file there so angle-bracket includes resolve against the synced tree instead of a stale terminal copy.

Example:

```powershell
.\Scripts\MQTT\Tools\compile-mql5.ps1 \
	-SourcePath '.\Scripts\MQTT\Tests\Unit\Transport\TEST_Transport.mq5' \
	-LogPath "$env:TEMP\TEST_Transport.log" \
	-TargetMql5Root 'D:\MT5\MQL5' \
	-SyncMqttTrees
```

If MetaEditor is not installed in the default Windows location, pass `-MetaEditorPath` explicitly or set `METAEDITOR_PATH` before running the helper.

For repository automation, `.github/workflows/windows-mql5-compile.yml` is provided for a self-hosted Windows runner. Set runner environment variables `MQL5_ROOT` and `METAEDITOR_PATH`, then the workflow will sync `Include/MQTT`, `Scripts/MQTT`, and `Experts/MQTT` into that MT5 root before compiling.

## Optional Live Broker Test Setup

If you want to run the tracked live CONNECT integration test after cloning into your MT5 `MQL5` folder, edit only this file:

1. `Scripts/MQTT/Tests/LiveBrokerConfig.mqh`

Set these values for your environment:

1. `MQTT_TEST_LIVE_BROKER_ENABLED` -> `true`
2. `MQTT_TEST_LIVE_BROKER_HOST` -> your broker hostname
3. `MQTT_TEST_LIVE_BROKER_PORT` -> your broker port

Then compile and run `Scripts/MQTT/Tests/Unit/Protocol/TEST_Connect.mq5`. When live testing is left disabled, that integration case is logged as skipped and the rest of the public compile-first path remains offline.

This tracked live CONNECT case is a raw socket smoke check. It does not exercise the full `CMqttClient` timer or `Poll()` path. If it fails immediately with `SocketConnect()` or MT5 error `4014`, first verify the broker host is allowlisted in `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL`, then rerun it. Treat that case as an optional environment-sensitive supplement, not as the supported public green path.

## Optional Client-Level Live Smoke

If you want a checked-in client-level runtime check that exercises `CMqttClient`, use:

1. `Experts/MQTT/Harnesses/LiveBrokerSmoke.mq5`
2. `Scripts/MQTT/Tools/run-mt5-live-broker-smoke.ps1`
3. `Scripts/MQTT/Tools/invoke-remote-mosquitto-publish.ps1` when you want a broker-originated remote publish instead of loopback-only validation.

The helper compiles the harness, writes a temporary MT5 start config, launches the terminal, waits for a single `SUMMARY status=...` line, and then stops the terminal again.

Example native TLS run:

```powershell
.\Scripts\MQTT\Tools\run-mt5-live-broker-smoke.ps1 \
	-ScenarioName live-smoke-tls8883 \
	-BrokerHost broker.example.com \
	-BrokerPort 8883 \
	-UseTLS $true \
	-RequireTLS $true \
	-Username your-user \
	-Password your-password
```

Example WSS run:

```powershell
.\Scripts\MQTT\Tools\run-mt5-live-broker-smoke.ps1 \
	-ScenarioName live-smoke-wss443 \
	-BrokerHost broker.example.com \
	-BrokerPort 443 \
	-UseTLS $true \
	-UseWebSocket $true \
	-WebSocketPath /mqtt \
	-RequireTLS $true \
	-Username your-user \
	-Password your-password
```

When your MetaTrader terminal executable uses a different MT5 data root than the repository copy, pass that `MQL5` directory explicitly and sync the repo tree into it before launching:

```powershell
.\Scripts\MQTT\Tools\run-mt5-live-broker-smoke.ps1 \
	-TargetMql5Root 'D:\MT5\MQL5' \
	-SyncRepoToTarget
```

Expected pass signal: the summary line reports `status=PASS`, `connect_seen=true`, `suback_seen=true`, `publish_accepted=true`, `publish_ack_seen=true`, and `loopback_seen=true`.

For a single-command remote QoS2 proof against a broker you can reach over SSH, enable the built-in remote inject path on the same runner. The helper waits for the remote subscription to appear in broker logs before publishing, so the QoS2 publish overlaps the live smoke run without a second manual terminal.

```powershell
.\Scripts\MQTT\Tools\run-mt5-live-broker-smoke.ps1 \
	-ScenarioName live-smoke-tls8883-remote-qos2 \
	-BrokerHost broker.example.com \
	-BrokerPort 8883 \
	-UseTLS $true \
	-RequireTLS $true \
	-Username your-user \
	-Password your-password \
	-ExpectRemoteMessage $true \
	-RemoteInject $true \
	-RemoteInjectSshDestination ssh-destination-placeholder \
	-RemoteInjectSshKeyPath "$HOME\.ssh\id_ed25519" \
	-RemoteInjectComposeDir /srv/mosquitto
```

When you want to publish from the broker side outside the MT5 runner, call `Scripts/MQTT/Tools/invoke-remote-mosquitto-publish.ps1` directly. It publishes via `mosquitto_pub` inside the configured Docker Compose project and can optionally wait until the target subscription appears in broker logs before sending the message.

## Manual Runtime Checklist

Use the minimal client example from the [repository README](../../../README.md), [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5), or the publish-queue harness as your runtime starting point.

Before connecting:

1. Add the target broker host to `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL`.
2. Drive the client from `OnTimer()` and call `Poll()` regularly. `EventSetMillisecondTimer(250)` is the recommended default. See the [OnTimer Guide](OnTimerGuide.md) for timing guidance.
3. Keep tracked files on example values only. Put real hosts, credentials, certificates, or workstation-specific paths into ignored local files or local terminal settings.
4. If you use the checked-in runtime helpers, pass `-TerminalPath` explicitly or set `MT5_TERMINAL_PATH` when your local terminal is not installed in the standard MetaTrader 5 location.

During runtime validation:

1. For TLS or WSS, call `SetTLS(true)` and `SetRequireTLS(true)` before `Connect()`.
2. For WebSocket or WSS, use `SetHostWS(host, port, path)` instead of `SetHost()`.
3. For plaintext transport on a trusted private network only, call `SetRequireTLS(false)` and explicitly opt in with `SetAllowInsecurePlaintextTransport(true)`.
4. Guard publish activity with `IsSafeToPublish()` so your EA does not publish during reconnect or TLS setup.

For public release confidence, prefer the compile-first path plus a client-level runtime check over TLS, WSS, or another production-intended transport. The raw plaintext CONNECT smoke is useful for local diagnostics, but it is not the main publish-readiness signal.

Recommended runtime checks:

1. Initial connection reaches the connected state.
2. A basic publish succeeds.
3. A subscribed callback receives a message on the expected topic.
4. A forced disconnect or broker restart is followed by a successful reconnect.
5. Persistent-session behavior matches your broker settings if you use session resume or queued QoS traffic.

## What Is Intentionally Out Of Scope

The supported public validation path does not require remote inject helpers, VPS orchestration, or project-specific compliance harnesses.

## See Also

- [Documentation Home](README.md)
- [Getting Started](GettingStarted.md)
- [Examples Guide](Examples.md)
- [IDE Automation Guide](AutomationGuide.md)
- [Troubleshooting](Troubleshooting.md)
- [OnTimer Guide](OnTimerGuide.md)
- [Repository README](../../../README.md)
