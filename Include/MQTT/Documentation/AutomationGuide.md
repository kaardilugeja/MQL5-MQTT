# IDE Automation Guide

This page is for users who want an IDE agent to validate the repository after cloning it.

The checked-in scripts already cover the supported public flow. A capable agent in VS Code, Cursor, Antigravity, or another repo-aware IDE only needs the broker and local-tool details that you provide.

Start at [Documentation Home](README.md) if you want the broader docs map.

## What The Agent Needs From You

Provide these values up front:

- broker host
- broker port
- whether the broker expects native MQTT/TLS, WSS, or plaintext for a trusted private test network
- WebSocket path when you use WSS or `ws://`
- username and password if the broker requires them
- non-default `METAEDITOR_PATH` when MetaEditor is not installed in the standard location
- non-default `MT5_TERMINAL_PATH` when the MetaTrader terminal is not installed in the standard location
- `TargetMql5Root` when the repository checkout is not itself the active MT5 `MQL5` directory

## Safe Validation Flow

The recommended agent workflow is:

1. Run the offline compile gate first.
2. Stop if the compile gate fails.
3. Run the live smoke helper only after compile validation passes and you have provided broker access.
4. Report the compile summary plus the smoke harness `SUMMARY status=...` line.

That sequence uses the checked-in public scripts and avoids inventing custom local validation steps.

## Compile-Only Prompt Template

Use a prompt like this with your IDE agent:

```text
Validate this MQL5 MQTT repository from a fresh clone. First run .\Tools\compile-public-validation.ps1 from the repo root. If MetaEditor is not in the default location, use METAEDITOR_PATH or the explicit path I provide. Summarize which targets passed or failed and do not edit tracked files unless a real repository issue is found.
```

## Live Broker Prompt Template

Use a prompt like this when you want end-to-end broker proof after the compile gate passes:

```text
Validate this MQL5 MQTT repository against my broker. First run .\Tools\compile-public-validation.ps1. If that passes, run .\Scripts\MQTT\Tools\run-mt5-live-broker-smoke.ps1 with these inputs: broker host=<HOST>, broker port=<PORT>, use TLS=<true|false>, use WebSocket=<true|false>, WebSocket path=<PATH>, username=<USER>, password=<PASSWORD>. If the repository is not my active MT5 MQL5 root, also use -TargetMql5Root and -SyncRepoToTarget. Report the compile summary and the final SUMMARY status line.
```

## Example Live Smoke Commands

Native MQTT/TLS:

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

WSS:

```powershell
.\Scripts\MQTT\Tools\run-mt5-live-broker-smoke.ps1 `
  -ScenarioName live-smoke-wss443 `
  -BrokerHost broker.example.com `
  -BrokerPort 443 `
  -UseTLS $true `
  -UseWebSocket $true `
  -WebSocketPath /mqtt `
  -RequireTLS $true `
  -Username your-user `
  -Password your-password
```

## Secret Handling

Do not ask the agent to save your broker secrets into tracked files.

Prefer one of these approaches instead:

- pass credentials directly on the command line to the live smoke helper
- keep credentials in ignored local shell history or local terminal environment variables
- use locally edited ignored files, not tracked repository files

Tracked files such as [LiveBrokerConfig.mqh](../../../Scripts/MQTT/Tests/LiveBrokerConfig.mqh) should stay on placeholder values in commits.

## Expected Success Signals

- Compile gate: the curated targets report `PASS`.
- Live smoke: the result line reports `SUMMARY status=PASS`.

If compile succeeds but runtime fails immediately with `SocketConnect()` or MT5 error `4014`, use [Troubleshooting](Troubleshooting.md) before changing library code.

## See Also

- [Getting Started](GettingStarted.md)
- [Validation Guide](Validation.md)
- [Troubleshooting](Troubleshooting.md)
