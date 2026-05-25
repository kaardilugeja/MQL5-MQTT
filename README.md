# MQTT 5 Client for MQL5

Pure MQL5 MQTT 5.0 client library for MetaTrader 5.

> Pre-release notice
> This repository is a pre-1.0 evaluation release. Public APIs, file layout, and validation workflow may still change before the first stable release.

This repository is a public pre-1.0 release track for a reusable MQTT 5 client library for MQL5.

The only supported public include is `Include/MQTT/MQTT.mqh`.

Everything else under `Include/MQTT/` should be treated as implementation detail and is not part of the public release surface.

## Status

- Pre-1.0 public release track.
- Source-only repository: no `.ex5` binaries, logs, or generated runtime artifacts are part of the public snapshot.
- MT5 drop-in layout is preserved under `Include/MQTT`, `Scripts/MQTT`, and `Experts/MQTT`.
- Curated compile-first test coverage is included under `Scripts/MQTT/Tests`.

## Highlights

- MQTT 5.0 protocol coverage for core control packets and packet properties.
- High-level `CMqttClient` facade for TCP, TLS, WebSocket, and WSS transports.
- Session persistence, retransmission management, reconnect policy, topic alias handling, and queued publish support.
- Curated unit, integration, regression, and compile-first public validation suites.
- Public-safe harnesses and GitHub-facing documentation for compile and runtime validation.

## Repository Layout

- `Include/MQTT`: canonical include and internalized implementation modules.
- `Include/MQTT/Documentation`: repo-local documentation hub for setup, validation, and runtime guidance.
- `Scripts/MQTT/Tests`: curated MT5 test entry points and subsystem-organized suites.
- `Experts/MQTT/Harnesses`: runnable experts intended for public-safe validation flows.

## Documentation Map

- [Documentation Home](Include/MQTT/Documentation/README.md)
- [Getting Started](Include/MQTT/Documentation/GettingStarted.md)
- [Validation Guide](Include/MQTT/Documentation/Validation.md)
- [Examples Guide](Include/MQTT/Documentation/Examples.md)
- [IDE Automation Guide](Include/MQTT/Documentation/AutomationGuide.md)
- [Troubleshooting](Include/MQTT/Documentation/Troubleshooting.md)
- [OnTimer Guide](Include/MQTT/Documentation/OnTimerGuide.md)

## Installation

1. Place this repository so that it becomes your MT5 `MQL5` folder, or copy the `Include/MQTT`, `Scripts/MQTT`, and `Experts/MQTT` subtrees into an existing MT5 `MQL5` folder.
2. Include the only supported public entry point from your EA or script:

```mql5
#include <MQTT\MQTT.mqh>
```

3. In MetaTrader 5, add your broker host to `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL`.
   Even though the library uses socket APIs, MT5 still requires the target host to be allowlisted before `SocketConnect()` is permitted.
4. Compile your script or EA in MetaEditor. The public repository is source-only and does not ship `.ex5` artifacts.

Use placeholder or locally overridden values such as `broker.example.com` in tracked files. Do not commit real hosts, credentials, certificates, or workstation-specific paths.

## Quick Verification

After cloning into your MT5 `MQL5` folder, the fastest supported green path is:

1. Run the curated compile helper from the repository root:

```powershell
.\Tools\compile-public-validation.ps1
```

2. If your active MetaTrader installation uses a different `MQL5` folder than the repository checkout, sync into that target root while validating:

```powershell
.\Tools\compile-public-validation.ps1 `
  -TargetMql5Root 'D:\MT5\MQL5' `
  -SyncRepoToTarget
```

3. Expected result: the helper reports `PASS` for the curated public targets and MetaEditor reports `0 errors, 0 warnings` for each compile target.

For the full human walkthrough, use [Getting Started](Include/MQTT/Documentation/GettingStarted.md). For the broker-backed path, use [Validation Guide](Include/MQTT/Documentation/Validation.md).

## Minimal Client Example

`Connect()` starts the transport attempt. `Poll()` completes the handshake and processes incoming packets, so call it from `OnTimer()` or another regular event loop.

```mql5
#include <MQTT\MQTT.mqh>

CMqttClient mqtt;

int OnInit() {
  EventSetMillisecondTimer(250);

  mqtt.SetHost("broker.example.com", 1883);
  mqtt.SetClientId("mt5-example-client");
  mqtt.SetCleanStart(true);
  mqtt.SetKeepAlive(30);

  // For TLS, enable both of these before Connect().
  // mqtt.SetTLS(true);
  // mqtt.SetRequireTLS(true);

  return INIT_SUCCEEDED;
}

void OnTimer() {
  if (!mqtt.IsConnected() && !mqtt.IsConnecting()) {
    ENUM_TRANSPORT_ERROR err = mqtt.Connect();
    if (err != TRANSPORT_OK && err != TRANSPORT_CONNECTING) {
      Print("Connect start failed: ", (int)err);
      return;
    }
  }

  mqtt.Poll();

  if (mqtt.IsSafeToPublish()) {
    mqtt.Publish("mt5/example/heartbeat", "hello from mql5");
  }
}

void OnDeinit(const int reason) {
  EventKillTimer();
  mqtt.Disconnect();
}
```

For WebSocket or WSS transport, use `SetHostWS(host, port, path)` instead of `SetHost()`.

If you want a checked-in runnable EA instead of copying the snippet manually, start with `Experts/MQTT/Harnesses/MinimalClientExample.mq5` and replace the placeholder inputs with your broker settings.

## Examples

| Harness | Start here when you want | Notes |
| --- | --- | --- |
| `Experts/MQTT/Harnesses/MinimalClientExample.mq5` | Basic publish or subscribe flow with timer-driven polling | Smallest runnable consumer example |
| `Experts/MQTT/Harnesses/PublishQueueTestHarness.mq5` | Queue, expiry, retransmit, and offline-buffer behaviour | Broker-free runtime harness |
| `Experts/MQTT/Harnesses/LiveBrokerSmoke.mq5` | End-to-end broker validation through `CMqttClient` | Pair with `Scripts/MQTT/Tools/run-mt5-live-broker-smoke.ps1` |

For extra snippets and use-case guidance, use [Examples Guide](Include/MQTT/Documentation/Examples.md).

## Validation

The supported public validation path stays intentionally small:

1. Run the offline compile gate with `Tools/compile-public-validation.ps1`.
2. Optionally enable the tracked raw CONNECT integration case through `Scripts/MQTT/Tests/LiveBrokerConfig.mqh`.
3. Optionally run the checked-in client-level smoke with `Scripts/MQTT/Tools/run-mt5-live-broker-smoke.ps1` and `Experts/MQTT/Harnesses/LiveBrokerSmoke.mq5`.

If you want the detailed command lines and expected results, use [Validation Guide](Include/MQTT/Documentation/Validation.md). If you want the shortest clone-to-green path, use [Getting Started](Include/MQTT/Documentation/GettingStarted.md).

## IDE Agent Validation

This repository is checked in so that an IDE agent can validate a fresh clone without editing tracked machine-specific paths:

- root `.editorconfig` and `.vscode/settings.json` provide portable editor and whitespace defaults
- [AGENTS.md](AGENTS.md) points agents at the public include boundary and the checked-in validation scripts
- `Tools/compile-public-validation.ps1` handles compile validation for both clone-local and synced-to-target MT5 roots
- `Scripts/MQTT/Tools/run-mt5-live-broker-smoke.ps1` provides the optional broker-backed end-to-end smoke path

Give the agent your broker host, broker port, TLS or WSS requirements, optional credentials, and any non-default `METAEDITOR_PATH` or `MT5_TERMINAL_PATH`. For ready-to-use prompt templates, use [IDE Automation Guide](Include/MQTT/Documentation/AutomationGuide.md).

## Troubleshooting

- `SocketConnect()` failure or MT5 error `4014`: add the broker host to `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL`.
- `Connect()` fails before the MQTT handshake: enable TLS with `SetTLS(true)` and `SetRequireTLS(true)`, or explicitly opt in to plaintext only on a trusted private test network.
- Checked-in helpers cannot find MetaEditor or the terminal: pass `-MetaEditorPath` or `-TerminalPath`, or set `METAEDITOR_PATH` or `MT5_TERMINAL_PATH`.

For the MT5-specific failure modes and queue-state explanations, use [Troubleshooting](Include/MQTT/Documentation/Troubleshooting.md).

## Public API Boundary

- `Include/MQTT/MQTT.mqh` is the only documented include for public consumers.
- Every other header under `Include/MQTT/`, including non-`Internal` paths, is outside the public API boundary.

If you include deeper files directly, you should expect pre-1.0 refactors to move or rename them without a compatibility layer.

## License

This project is licensed under Apache-2.0. See `LICENSE`.
