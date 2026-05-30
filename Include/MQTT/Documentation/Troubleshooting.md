# Troubleshooting

This page collects the most common public-clone problems before you start debugging library code.

Start at [Documentation Home](README.md) if you want the broader docs map.

## `SocketConnect()` Fails Or MT5 Reports `4014`

MT5 blocks socket access unless the broker host is allowlisted.

Fix:

1. Open `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL`.
2. Add the broker host you are trying to reach.
3. Re-run the test.

This requirement still applies even though the library uses sockets rather than HTTP requests.

## `Connect()` Fails Before The MQTT Handshake Starts

The most common cause is a transport-policy mismatch.

- For native MQTT/TLS or WSS, call `SetTLS(true)` and `SetRequireTLS(true)` before `Connect()`.
- For `ws://` or plaintext TCP on a trusted private network only, call `SetRequireTLS(false)` and explicitly opt in with `SetAllowInsecurePlaintextTransport(true)`.
- If you send username/password or enhanced AUTH data without TLS, also opt in explicitly with `SetAllowInsecurePlaintextAuth(true)`.

Treat plaintext transport as a private-network debugging path, not the default public validation path.

## The Compile Helper Cannot Find MetaEditor

`Tools/compile-public-validation.ps1` looks for MetaEditor in the standard MetaTrader 5 install locations first.

If your installation lives somewhere else, either:

- pass `-MetaEditorPath` explicitly, or
- set `METAEDITOR_PATH` before running the helper.

## Compile Validation Uses The Wrong MT5 Data Root

When the repository checkout is not the same `MQL5` folder that your active terminal actually uses, compile validation may run against the wrong tree.

Fix:

```powershell
.\Tools\compile-public-validation.ps1 `
  -TargetMql5Root 'D:\MT5\MQL5' `
  -SyncRepoToTarget
```

## `Publish()` Returns `MQTT_PUB_RECONNECTING` Or `MQTT_PUB_QUEUED`

Those are state signals, not parser failures.

- `MQTT_PUB_RECONNECTING`: reconnect or queue drain is in progress. Wait for the session to settle.
- `MQTT_PUB_QUEUED`: the publish was accepted for deferred delivery while offline.

The intended public pattern is to call `Poll()` regularly and guard normal publish traffic with `IsSafeToPublish()`.

## `Poll()` Appears To Freeze The Chart

TLS and WSS handshakes use blocking MQL5 socket APIs. A short chart pause during initial connect or reconnect is expected.

Fix or mitigation:

- keep MQTT on a dedicated chart when latency matters
- use the default `EventSetMillisecondTimer(250)` cadence unless you have a measured reason to go faster
- lower connect timeouts carefully if your environment is fast and stable

For the timing model, see [OnTimer Guide](OnTimerGuide.md).

## Incoming Messages Never Arrive

Check the full chain:

1. `OnTimer()` is firing.
2. `Poll()` is called every timer tick.
3. `Connect()` is allowed to return `TRANSPORT_CONNECTING` without being treated as a failure.
4. The subscription reaches `SUBACK`.
5. The broker topic and QoS match what your harness expects.

Start with the compile-first path, then reproduce the issue with [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5) or your own local harness so broker-specific settings stay under your control.

## See Also

- [Getting Started](GettingStarted.md)
- [Examples Guide](Examples.md)
- [Validation Guide](Validation.md)
- [OnTimer Guide](OnTimerGuide.md)
