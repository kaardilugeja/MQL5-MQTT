//+------------------------------------------------------------------+
//|                                         MinimalClientExample.mq5 |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Minimal Expert Advisor example that connects to a broker, polls  |
//| on a timer, and publishes a periodic heartbeat message.          |
//+------------------------------------------------------------------+
#property strict

#include <MQTT\MQTT.mqh>

input string InpBrokerHost                      = "broker.example.com";
input uint   InpBrokerPort                      = 8883;
input bool   InpUseTLS                          = true;
input bool   InpRequireTLS                      = true;
input bool   InpAllowInsecurePlaintextTransport = false;
input string InpClientId                        = "";
input string InpUsername                        = "";
input string InpPassword                        = "";
input string InpPublishTopic                    = "mt5/example/heartbeat";
input string InpPublishPayload                  = "hello from mql5";
input uint   InpPublishIntervalSeconds          = 30;

CMqttClient  g_mqtt;
datetime     g_last_publish_at = 0;

int OnInit() {
  EventSetMillisecondTimer(250);

  string client_id = InpClientId;
  if (StringLen(client_id) == 0) {
    client_id = "mt5-example-" + (string)ChartID();
  }

  g_mqtt.SetHost(InpBrokerHost, InpBrokerPort);
  g_mqtt.SetClientId(client_id);
  g_mqtt.SetCleanStart(true);
  g_mqtt.SetKeepAlive(30);

  if (InpUseTLS) {
    g_mqtt.SetTLS(true);
    g_mqtt.SetRequireTLS(InpRequireTLS);
  } else {
    g_mqtt.SetRequireTLS(false);
    if (InpAllowInsecurePlaintextTransport) {
      g_mqtt.SetAllowInsecurePlaintextTransport(true);
    }
  }

  if (StringLen(InpUsername) > 0 || StringLen(InpPassword) > 0) {
    g_mqtt.SetCredentials(InpUsername, InpPassword);
  }

  Print("[MQTT Example] Configure Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL with ",
        InpBrokerHost);
  return INIT_SUCCEEDED;
}

void OnTimer() {
  if (!g_mqtt.IsConnected() && !g_mqtt.IsConnecting()) {
    ENUM_TRANSPORT_ERROR err = g_mqtt.Connect();
    if (err != TRANSPORT_OK && err != TRANSPORT_CONNECTING) {
      Print("[MQTT Example] Connect start failed: ", (int)err);
      return;
    }
  }

  g_mqtt.Poll();

  if (!g_mqtt.IsSafeToPublish()) {
    return;
  }

  datetime now = TimeCurrent();
  if (g_last_publish_at != 0 && (now - g_last_publish_at) < (int)InpPublishIntervalSeconds) {
    return;
  }

  ENUM_MQTT_PUBLISH_ERROR pub_err = g_mqtt.Publish(InpPublishTopic, InpPublishPayload);
  if (pub_err == MQTT_PUB_OK || pub_err == MQTT_PUB_QUEUED) {
    g_last_publish_at = now;
  } else {
    Print("[MQTT Example] Publish failed: ", (int)pub_err);
  }
}

void OnDeinit(const int reason) {
  EventKillTimer();
  g_mqtt.Disconnect();
}
