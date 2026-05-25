//+------------------------------------------------------------------+
//|                                            LiveBrokerConfig.mqh  |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| User-editable settings for optional live broker tests.           |
//|                                                                  |
//| Keep MQTT_TEST_LIVE_BROKER_ENABLED set to false for the default  |
//| public compile-first path. To run the live CONNECT integration   |
//| test, set it to true and replace the placeholder host and port   |
//| with a broker you control or a public test broker you trust.     |
//+------------------------------------------------------------------+
#ifndef MQTT_TEST_LIVE_BROKER_CONFIG_MQH
#define MQTT_TEST_LIVE_BROKER_CONFIG_MQH

#define MQTT_TEST_LIVE_BROKER_ENABLED false
#define MQTT_TEST_LIVE_BROKER_HOST    "broker.example.com"
#define MQTT_TEST_LIVE_BROKER_PORT    1883

#endif  // MQTT_TEST_LIVE_BROKER_CONFIG_MQH
