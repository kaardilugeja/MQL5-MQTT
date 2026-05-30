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

The compile-first path is intentionally offline. A fresh public clone does not require network credentials or local broker setup just to compile.

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

For repository automation, `.github/workflows/windows-mql5-compile.yml` runs on GitHub-hosted Windows and validates that the curated compile helper, supporting scripts, and tracked validation targets are present and syntactically sound. Full MetaEditor compilation remains a local Windows step through `Tools/compile-public-validation.ps1`, because GitHub-hosted runners do not ship with MetaEditor or an MT5 `MQL5` data root.

## Runtime Follow-Up Outside The Shipped Public Path

The tracked repository does not currently ship a checked-in broker runtime harness source file, a one-command broker launcher, or a public broker-connect test target. Any broker validation after the compile gate is therefore local and environment-specific rather than part of the shipped public green path.

If you choose to do that follow-up locally, start from [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5) and keep broker hosts, credentials, certificates, and MT5 launch details in local or ignored state. Add the broker host to `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL` before testing.

Use [Troubleshooting](Troubleshooting.md) for `SocketConnect()` and MT5 error `4014` failures before changing library code.

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
4. Guard publish activity with `IsSafeToPublish()` so your EA does not publish during reconnect, TLS setup, subscription replay, resumed QoS retransmission, or durable queue drain. `OnConnect()` now fires at the same ready boundary.

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
