# MQTT Documentation

This directory is the repo-local documentation hub for the public MQTT 5 client library for MQL5.

## Start Here

- Public consumers should include only `Include/MQTT/MQTT.mqh`.
- Use the [repository README](../../../README.md) for the public overview and installation notes.
- Use [Getting Started](GettingStarted.md) for the shortest clone-to-green path.
- Use [IDE Automation Guide](AutomationGuide.md) if you want an IDE agent to run the public validation flow.

## Guides

- [Getting Started](GettingStarted.md): clone and run the compile-first public validation path.
- [Validation Guide](Validation.md): the curated compile-first path and the boundary around local runtime follow-up.
- [Examples Guide](Examples.md): checked-in harness matrix and copy-paste configuration snippets.
- [Troubleshooting](Troubleshooting.md): MT5-specific failure modes before you debug library code.
- [OnTimer Guide](OnTimerGuide.md): how often to call `Poll()` and what timing tradeoffs matter in MT5.

## Public Surface

- `../MQTT.mqh`: the only documented include for public consumers.
- `../Internal/*`: implementation detail, not public API.
- Any other header under `../`: implementation detail unless it is later promoted into the documented public surface.

## Checked-In Harnesses

- [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5)
- [PublishQueueTestHarness.mq5](../../../Experts/MQTT/Harnesses/PublishQueueTestHarness.mq5)

## Release Hygiene

- Keep tracked files on placeholder hosts, credentials, certificates, and paths only.
- Public validation is source-only. Do not commit `.ex5`, logs, generated results, or machine-specific state.
- For GitHub automation, see `Tools/compile-public-validation.ps1` and `.github/workflows/windows-mql5-compile.yml` from the repository root.
- For repo-aware IDE automation, use [IDE Automation Guide](AutomationGuide.md).
