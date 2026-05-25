//+------------------------------------------------------------------+
//|                                      PublishQueueTestHarness.mq5 |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Expert Advisor harness for the shared publish queue runtime      |
//| suite with standardized result-file output.                      |
//+------------------------------------------------------------------+
#property strict

input string InpResultFileName = "PublishQueueTestHarness.result.txt";

#include "..\..\..\Scripts\MQTT\Tests\PublishQueueTestSuite.mqh"

int OnInit() {
  Print("[PublishQueueHarness] Starting publish-queue runtime suite");
  bool success = RunPublishQueueTestSuite(InpResultFileName);
  Print("[PublishQueueHarness] Completed publish-queue runtime suite status=", (success ? "PASS" : "FAIL"));
  ExpertRemove();
  return INIT_SUCCEEDED;
}

void OnTick() {}
