# Getting Started

This page is the shortest path from a fresh clone to a validated MQTT client checkout.

Start at [Documentation Home](README.md) if you want the full documentation map.

## 1. Put The Repository Where MetaTrader Can See It

You have two supported layouts:

1. Clone the repository so that the repository root is your MetaTrader 5 `MQL5` folder.
2. Clone the repository anywhere, then let the checked-in helpers sync `Include/MQTT`, `Scripts/MQTT`, and `Experts/MQTT` into another `MQL5` folder when you validate.

The public library surface is still the same in both layouts:

```mql5
#include <MQTT\MQTT.mqh>
```

## 2. Confirm The Public Entry Point

A healthy clone should contain these paths:

- `Include/MQTT/MQTT.mqh`
- `Include/MQTT/Documentation/README.md`
- `Experts/MQTT/Harnesses/MinimalClientExample.mq5`
- `Tools/compile-public-validation.ps1`

If those files are present, you have the intended public checkout.

## 3. Compile The Curated Public Validation Set

From the repository root, run:

```powershell
.\Tools\compile-public-validation.ps1
```

If MetaEditor is installed outside the default Windows location, either pass it explicitly:

```powershell
.\Tools\compile-public-validation.ps1 -MetaEditorPath 'C:\Program Files\MetaTrader 5\MetaEditor64.exe'
```

or set `METAEDITOR_PATH` first.

If your active terminal uses a different MT5 data root than the repository checkout, sync into that target root during validation:

```powershell
.\Tools\compile-public-validation.ps1 `
  -TargetMql5Root 'D:\MT5\MQL5' `
  -SyncRepoToTarget
```

Expected result: the helper reports `PASS` for the curated compile targets and each target compiles with `0 errors, 0 warnings`.

## 4. Optional Live Broker Checks

The compile-first path is the supported offline green path. After that, you have two public live-broker options.

### Raw CONNECT Smoke

If you want the smallest tracked live integration case, edit only [LiveBrokerConfig.mqh](../../../Scripts/MQTT/Tests/LiveBrokerConfig.mqh), enable the live test, set your broker host and port, then compile and run `Scripts/MQTT/Tests/Unit/Protocol/TEST_Connect.mq5`.

Use this when you want a narrow socket-level proof that MT5 can reach the broker.

### Full CMqttClient Smoke

If you want an end-to-end client proof with subscriptions, publish flow, reconnect-safe polling, and summary output, use the checked-in smoke harness helper:

```powershell
.\Scripts\MQTT\Tools\run-mt5-live-broker-smoke.ps1 `
  -ScenarioName live-smoke-tls8883 `
  -BrokerHost broker.example.com `
  -BrokerPort 8883 `
  -UseTLS $true `
  -RequireTLS $true `
  -Username your-user `
  -Password your-password
```

If you use WSS instead of native MQTT/TLS, switch to `-UseWebSocket $true` and provide `-WebSocketPath`.

Before any runtime test, add the broker host to `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL`. MT5 still requires that allowlist entry before socket connections are permitted.

Expected pass signal: the smoke harness prints and writes a `SUMMARY status=PASS` line.

## 5. Know Which Checked-In Example To Start With

- [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5): smallest timer-driven client example.
- [PublishQueueTestHarness.mq5](../../../Experts/MQTT/Harnesses/PublishQueueTestHarness.mq5): queue durability and retransmit-oriented runtime harness.
- [LiveBrokerSmoke.mq5](../../../Experts/MQTT/Harnesses/LiveBrokerSmoke.mq5): end-to-end broker validation harness.

For a side-by-side comparison and extra snippets, use [Examples Guide](Examples.md).

## 6. If You Want An IDE Agent To Drive Validation

Use [IDE Automation Guide](AutomationGuide.md). It provides the exact inputs an IDE agent needs and prompt templates that work against the checked-in scripts without editing tracked secrets into the repository.

## See Also

- [Validation Guide](Validation.md)
- [Examples Guide](Examples.md)
- [IDE Automation Guide](AutomationGuide.md)
- [Troubleshooting](Troubleshooting.md)
- [OnTimer Guide](OnTimerGuide.md)
