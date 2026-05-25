# Changelog

All notable changes to this repository will be documented in this file.

## Unreleased

- Replaced machine-specific VS Code settings with portable workspace defaults and added root `.editorconfig` whitespace hygiene for public clones.
- Rewrote `AGENTS.md` around the checked-in public validation scripts instead of private development paths.
- Added repo-wiki-style public docs for getting started, examples, IDE automation, and troubleshooting.
- Added GitHub issue and pull request templates for public contribution hygiene.
- Simplified the public include boundary so `Include/MQTT/MQTT.mqh` is the only documented public entry point.
- Moved the `CMqttClient` implementation behind `Include/MQTT/Internal/Client/MqttClient.mqh` and rewired the client unit test through `MQTT.mqh`.
- Reworked the public docs into a repo-local documentation hub and converted `Include/MQTT/Documentation/OnTimerGuide.md` into GitHub-friendly Markdown.
- Added `Scripts/MQTT/Tests/LiveBrokerConfig.mqh` so public clones can enable the tracked live CONNECT test by editing one file.
- Clarified the offline-by-default validation path in `README.md` and `Include/MQTT/Documentation/Validation.md`.
- Documented that the tracked live CONNECT test is a raw, environment-sensitive socket smoke check and not the primary public release gate.
- Added `Experts/MQTT/Harnesses/MinimalClientExample.mq5` as a checked-in runnable consumer example.
- Added `Tools/compile-public-validation.ps1` and `.github/workflows/windows-mql5-compile.yml` for checked-in Windows compile automation on self-hosted MT5 runners.
- Corrected the publish queue test path in `README.md`.

## 0.1.0 - 2026-05-07

- First public pre-1.0 snapshot of the pure MQL5 MQTT 5 client library.
- Kept the stable public entry surface intentionally small.
- Added a curated public validation path built around `TEST_MQTT.mq5`, `TEST_MqttClient.mq5`, `TEST_Transport.mq5`, `TEST_PublishQueue.mq5`, and `PublishQueueTestHarness.mq5`.
- Removed generated binaries, logs, and private infrastructure details from the public release set.
- Excluded private compliance, operations, and environment-specific assets from the public snapshot.
