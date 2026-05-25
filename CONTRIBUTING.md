# Contributing

Contributions should improve the public MQTT library, keep the MT5 drop-in layout intact, and avoid leaking private deployment details.

## Ground Rules

- Preserve the repository roots used by MetaTrader 5: `Include/MQTT`, `Scripts/MQTT`, and `Experts/MQTT`.
- Keep `Include/MQTT/MQTT.mqh` as the canonical documented include. Treat `Include/MQTT/Internal/*` as implementation detail unless a change is explicitly documented as public API expansion.
- Do not commit generated artifacts such as `.ex5`, logs, result files, or machine-specific runtime state.
- Replace real broker hosts, credentials, certificates, account identifiers, and workstation-specific paths with public-safe examples or ignored local overrides.
- Keep `Scripts/MQTT/Tests/LiveBrokerConfig.mqh` on placeholder values in commits. Public users may edit it locally after cloning to enable the optional live CONNECT test.
- Keep documentation updated when public behavior, structure, or release assumptions change.

## Suggested Workflow

1. Make the smallest focused change that solves the problem.
2. Add or update the most targeted test coverage practical for that change.
3. Compile the most focused affected test in MetaEditor, or run `Tools/compile-public-validation.ps1` when you need the curated public smoke set.
4. For broader library changes, also compile `Scripts/MQTT/Tests/Unit/Protocol/TEST_MQTT.mq5` and `Scripts/MQTT/Tests/Unit/Session/TEST_MqttClient.mq5`.
5. Update `README.md` or the relevant docs if consumers need to change how they use the library.

## Pull Request Checklist

- The change is limited to the intended slice.
- No real infrastructure details or personal paths were introduced.
- No generated artifacts were added.
- Public API impact is documented when applicable.
- Validation steps and any limitations are described in the PR.

## Scope Notes

- This repository is for the reusable MQTT library and public-safe examples/tests.
- Trading-specific, operations-specific, or environment-bound assets should stay private unless they have been generalized for public release.
