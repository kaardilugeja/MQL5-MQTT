# OnTimer Guide

This page explains how often to call `CMqttClient::Poll()` from `OnTimer()` in MetaTrader 5.

Start at [Documentation Home](README.md) if you want the broader docs map.

## Why Timer Cadence Matters

`Poll()` is the client's event loop. It is responsible for:

1. reading incoming data from the socket
2. framing and dispatching MQTT packets
3. sending `PINGREQ` to keep the connection alive
4. triggering retransmissions for stalled QoS 1 and QoS 2 messages
5. advancing the auto-reconnect state machine
6. checking CONNACK timeout
7. flushing dirty session state to disk

If you use persistent sessions and the local terminal storage contains sensitive routing or payload data, configure `SetSessionEncryptionPassphrase(...)` before `Connect()` so the flushed session file is protected at rest with AES-256 using a SHA-256-derived key and SHA-256 integrity envelope.

## Recommended Intervals

| Interval | Use case | Notes |
| --- | --- | --- |
| 100 ms | Low-latency trading signals | Best responsiveness, higher CPU cost |
| 250 ms | General-purpose MQTT | Recommended default |
| 500 ms | Moderate update rates | Fine for most non-critical data |
| 1000 ms | Low-frequency monitoring | Safe for status-style workloads |

## Practical Rules

- Use `EventSetMillisecondTimer(250)` as the default.
- Avoid going below `100 ms` unless you also reduced the underlying transport read timeout.
- Keep the timer interval comfortably below the keep-alive period.
- MQTT 5 requires at least one control packet within `1.5 x keep_alive`, but running closer than that to the limit is not a good operating target.
- If keep-alive is `10` seconds, aim for roughly `1000-2000 ms`, not `10000 ms`.

## Example Setup

```mql5
#include <MQTT\MQTT.mqh>

CMqttClient mqtt;

int OnInit() {
	EventSetMillisecondTimer(250);

	mqtt.SetHost("broker.example.com", 8883);
	mqtt.SetTLS(true);
	mqtt.SetRequireTLS(true);
	mqtt.SetSessionEncryptionPassphrase("terminal-secret");
	mqtt.SetTofuPinning(true);
	mqtt.SetTofuThumbprint("001122334...DDEEFF00112233");
	mqtt.AddRedirectAllowHost("broker.example.com");
	mqtt.SetKeepAlive(60);
	mqtt.SetOnMessage(OnMqttMessage);
	mqtt.Connect();
	return INIT_SUCCEEDED;
}

void OnTimer() {
	mqtt.Poll();
}

void OnDeinit(const int reason) {
	EventKillTimer();
	mqtt.Disconnect();
}
```

## Performance Notes

Each `Poll()` call typically involves:

- one `SocketRead()` call using the transport read timeout
- packet framing and dispatch
- keep-alive checks
- occasional session database flush checks

The read timeout is usually the dominant cost. If no data is available, `SocketRead()` can block up to the configured read timeout. That means an `OnTimer()` interval lower than the read timeout often provides diminishing returns.

If you deliberately want lower latency, reduce the transport read timeout first and then tighten the timer.

## Retransmission And Reconnect Timing

QoS 1 and QoS 2 retransmission checks run on every `Poll()` call while the client is connected. The retransmission timeout decides when a message is considered stalled; the timer interval only decides how quickly the client notices that deadline has passed.

Auto-reconnect backoff is also checked on each `Poll()` call while disconnected. A slower timer means reconnect attempts may land slightly after the scheduled backoff target, which is usually acceptable.

The default reconnect circuit breaker is `12` consecutive attempts. Use `SetMaxReconnectAttempts(0)` only when unlimited retry is an intentional operator choice.

## TLS And WSS Blocking Limitation

When connecting to a TLS broker such as port `8883` or to WSS, MQL5 calls `SocketTlsHandshake()` synchronously. There is no asynchronous TLS API. The chart thread is paused until the TLS negotiation completes.

Treat this as an operational constraint for production trading EAs, not just a transport detail.

Key facts:

- The freeze occurs only during TLS handshake on initial connect and reconnect.
- Steady-state `Poll()` calls are not blocked by this specific handshake path.
- Typical duration is often `50-300 ms` on nearby networks.
- Worst case is bounded by `connect_timeout_ms`, which defaults to `5000 ms`.
- Lowering the timeout can create spurious failures on higher-latency links without improving steady-state behavior.

Recommended patterns:

1. Run MQTT on a separate chart from latency-sensitive trading logic.
2. Use plaintext TCP only on trusted LAN or VPN paths and only with explicit `SetAllowInsecurePlaintextTransport(true)` opt-in.
3. Accept the reconnect freeze only for dashboards, monitors, or other non-latency-critical EAs.

MetaTrader port behavior also matters: implicit TLS on port `443` does not require an explicit TLS handshake call, and this codebase treats explicit `443` handshakes as harmful. For other secure ports such as `8883` and `8884`, the library uses MT5's normal TLS handshake path.

## WebSocket Notes

When using `CWebSocketTransport` with async connect helpers, the TCP connect phase is non-blocking. After TCP succeeds, two synchronous steps still occur inside a single `Poll()` call:

1. `SocketTlsHandshake()` for WSS
2. `_DoHandshake()` for the HTTP `101` WebSocket upgrade

On high-latency links this can still block `OnTimer()` for several hundred milliseconds.

Plain `ws://` is fail-closed by default and must be explicitly enabled with `SetAllowInsecurePlaintextTransport(true)` on a trusted private network. Prefer `wss://` for production use.

If you enable auto-redirect, add approved hosts first. Redirects fail closed unless the destination hostname is present in the explicit allowlist.

WebSocket masking keys are a protocol-compliance mechanism for intermediaries, not a confidentiality or integrity boundary. Production deployments should still require TLS or WSS with `SetRequireTLS(true)`.

## Summary

- Most EAs: `EventSetMillisecondTimer(250)`
- Signal-critical EAs: `EventSetMillisecondTimer(100)`
- Monitoring EAs: `EventSetMillisecondTimer(1000)`
- Do not let the timer drift near the keep-alive ceiling

## See Also

- [Validation Guide](Validation.md)
- [Repository README](../../../README.md)
