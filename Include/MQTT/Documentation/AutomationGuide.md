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
3. For the shipped public validation path, report the compile summary and stop there.
4. Treat any broker-specific runtime check as a local manual follow-up. Do not assume extra checked-in broker runtime automation or harness source files exist.

That sequence stays within the tracked public surface and avoids inventing missing repository tooling.

## Compile-Only Prompt Template

Use a prompt like this with your IDE agent:

```text
Validate this MQL5 MQTT repository from a fresh clone. First run .\Tools\compile-public-validation.ps1 from the repo root. If MetaEditor is not in the default location, use METAEDITOR_PATH or the explicit path I provide. Summarize which targets passed or failed and do not edit tracked files unless a real repository issue is found.
```

## Runtime Follow-Up Prompt Template

Use a prompt like this only after the compile gate passes and you want help with a local broker test that you control:

```text
The compile gate passed for this MQL5 MQTT repository. Help me adapt Experts/MQTT/Harnesses/MinimalClientExample.mq5 for a local broker test without assuming any extra shipped broker-runtime helper exists. Keep secrets out of tracked files and tell me which local values or terminal settings I still need to provide.
```

## Secret Handling

Do not ask the agent to save your broker secrets into tracked files.

Prefer one of these approaches instead:

- pass credentials directly to your local runtime command
- keep credentials in ignored local shell history or local terminal environment variables
- use locally edited ignored files, not tracked repository files

Tracked example files should stay on placeholder values in commits.

## Expected Success Signals

- Compile gate: the curated targets report `PASS`.
- Any runtime follow-up is local and environment-specific; define the success condition in the prompt instead of assuming a shipped `SUMMARY` line.

If compile succeeds but runtime fails immediately with `SocketConnect()` or MT5 error `4014`, use [Troubleshooting](Troubleshooting.md) before changing library code.

## See Also

- [Getting Started](GettingStarted.md)
- [Validation Guide](Validation.md)
- [Troubleshooting](Troubleshooting.md)
