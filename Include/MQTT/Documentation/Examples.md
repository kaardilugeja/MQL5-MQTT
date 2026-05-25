# Examples Guide

This page maps the checked-in examples to common public use cases.

Start at [Documentation Home](README.md) if you want the broader docs map.

## Harness Matrix

| Harness | Use it for | Notes |
| --- | --- | --- |
| [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5) | Basic publish flow, timer-driven polling, TLS or plaintext bring-up | Smallest runnable example |
| [PublishQueueTestHarness.mq5](../../../Experts/MQTT/Harnesses/PublishQueueTestHarness.mq5) | Offline queue, resend, expiry, and queue coordinator behaviour | Broker-free runtime harness |
| [LiveBrokerSmoke.mq5](../../../Experts/MQTT/Harnesses/LiveBrokerSmoke.mq5) | End-to-end broker proof with summary output | Best public broker validation harness |

## Minimal TLS Client

Use this when you want the smallest production-shaped pattern: start connect attempts in `OnTimer()`, call `Poll()` every timer tick, and publish only after `IsSafeToPublish()` says the session is fully usable.

```mql5
#include <MQTT\MQTT.mqh>

CMqttClient mqtt;
datetime    last_publish_at = 0;

int OnInit() {
  EventSetMillisecondTimer(250);

  mqtt.SetHost("broker.example.com", 8883);
  mqtt.SetTLS(true);
  mqtt.SetRequireTLS(true);
  mqtt.SetClientId("mt5-example-client");
  mqtt.SetKeepAlive(30);
  mqtt.Subscribe("mt5/example/in", QoS_1);
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

  if (!mqtt.IsSafeToPublish()) {
    return;
  }

  datetime now = TimeCurrent();
  if (last_publish_at != 0 && (now - last_publish_at) < 30) {
    return;
  }

  if (mqtt.Publish("mt5/example/out", "hello from mql5") == MQTT_PUB_OK) {
    last_publish_at = now;
  }
}

void OnDeinit(const int reason) {
  EventKillTimer();
  mqtt.Disconnect();
}
```

## WSS Client Setup

Use this when your broker exposes MQTT over WebSocket or secure WebSocket.

```mql5
CMqttClient mqtt;

mqtt.SetHostWS("broker.example.com", 443, "/mqtt");
mqtt.SetTLS(true);
mqtt.SetRequireTLS(true);
mqtt.SetClientId("mt5-wss-client");
mqtt.SetKeepAlive(30);
```

For `ws://` on a private test network only, call `SetRequireTLS(false)` and explicitly opt in with `SetAllowInsecurePlaintextTransport(true)`.

## Queue-Focused Configuration

Use this when you want reconnect-friendly publishing and bounded offline buffering.

```mql5
CMqttClient mqtt;

mqtt.SetAutoReconnect(true, 1000, 60000);
mqtt.SetSessionExpiry(3600);
mqtt.SetMaxQueuedMessages(500);
mqtt.SetMaxQueuedPayloadBytes(262144);
mqtt.SetMaxQueuedPropertyBytes(32768);
mqtt.SetQueueQoS0WhenDisconnected(false);
```

This is the configuration family exercised by [PublishQueueTestHarness.mq5](../../../Experts/MQTT/Harnesses/PublishQueueTestHarness.mq5) and the unit tests behind it.

## Broker Validation Harness

Use [LiveBrokerSmoke.mq5](../../../Experts/MQTT/Harnesses/LiveBrokerSmoke.mq5) together with `Scripts/MQTT/Tools/run-mt5-live-broker-smoke.ps1` when you want a single command that compiles the harness, launches MT5, waits for a summary line, and shuts the terminal back down.

That harness is the best public example when you need evidence that the full `CMqttClient` lifecycle works against a real broker.

## See Also

- [Getting Started](GettingStarted.md)
- [Validation Guide](Validation.md)
- [IDE Automation Guide](AutomationGuide.md)
- [OnTimer Guide](OnTimerGuide.md)
