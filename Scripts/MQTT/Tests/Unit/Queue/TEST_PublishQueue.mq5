//+------------------------------------------------------------------+
//|                                            TEST_PublishQueue.mq5 |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Entry-point wrapper for the shared publish queue test harness.   |
//| All case execution and standardized result output live in        |
//| PublishQueueTestSuite.mqh.                                       |
//+------------------------------------------------------------------+
#include "..\..\PublishQueueTestSuite.mqh"

//+------------------------------------------------------------------+
//| OnStart - Run the publish queue test suite                       |
//+------------------------------------------------------------------+
void OnStart() { RunPublishQueueTestSuite(); }
