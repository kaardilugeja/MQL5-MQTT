//+------------------------------------------------------------------+
//|                                                     TestUtil.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Shared test infrastructure for the mql5-mqtt-cli test suite.     |
//| Provides ASSERT_* macros, global pass/fail counters, and network |
//| utility helpers used across all TEST_*.mq5 test scripts.         |
//+------------------------------------------------------------------+
#ifndef TESTUTIL_SKIP_MQTT_INCLUDE
#include "../../../Include/MQTT/MQTT.mqh"
#endif
#include "LiveBrokerConfig.mqh"

#define TESTUTIL_PKT_TYPE_CONNACK 0x02
#define TEST_CASE_START()         TestUtilRecordCaseStart(__FUNCTION__)

//+------------------------------------------------------------------+
//| Test Global State                                                |
//+------------------------------------------------------------------+
int          g_tests_passed                      = 0;  // Counter for passed assertions
int          g_tests_failed                      = 0;  // Counter for failed assertions

//--- Nested helpers such as array comparators do not know the caller line that
//--- matters during triage. They stage mismatch detail here so the outer ASSERT_*
//--- macro can emit one precise MT5 Journal line against the assertion site.
string       g_testutil_pending_failure_expected = "";
string       g_testutil_pending_failure_actual   = "";

const string TESTUTIL_LOG_PREFIX                 = "[MQTT-TEST]";
const uchar  TESTUTIL_REASON_CODE_SUCCESS        = 0x00;

//+------------------------------------------------------------------+
//| Logging Helpers                                                  |
//+------------------------------------------------------------------+

//--- MT5 journal scanning works best when each result stays on a single line.
//--- Escape control characters so payload snippets do not break log structure.
string       TestUtilSanitizeLogValue(const string value) {
  string sanitized = value;
  StringReplace(sanitized, "\r\n", "\\n");
  StringReplace(sanitized, "\n", "\\n");
  StringReplace(sanitized, "\r", "\\r");
  StringReplace(sanitized, "\t", "\\t");
  if (StringLen(sanitized) == 0) {
    return "<empty>";
  }
  return sanitized;
}

//--- Type-safe string conversion overloads for all primitive types
string TestUtilToString(const bool value) { return value ? "true" : "false"; }
string TestUtilToString(const string value) { return TestUtilSanitizeLogValue(value); }
string TestUtilToString(const int value) { return (string)value; }
string TestUtilToString(const uint value) { return (string)value; }
string TestUtilToString(const long value) { return (string)value; }
string TestUtilToString(const ulong value) { return (string)value; }
string TestUtilToString(const short value) { return (string)((int)value); }
string TestUtilToString(const ushort value) { return (string)((uint)value); }
string TestUtilToString(const char value) { return (string)((int)value); }
string TestUtilToString(const uchar value) { return (string)((uint)value); }
string TestUtilToString(const double value) { return DoubleToString(value, 8); }

//--- Format single byte value as standard 2-digit hex with 0x prefix
string TestUtilFormatByteHex(const uchar value) { return StringFormat("0x%02X", (int)value); }

//--- Reset pending failure buffer to clean state before each assertion
void   TestUtilClearPendingFailure() {
  g_testutil_pending_failure_expected = "";
  g_testutil_pending_failure_actual   = "";
}

//--- Check if nested helper has staged a failure detail for current assertion
bool TestUtilHasPendingFailure() {
  return StringLen(g_testutil_pending_failure_expected) > 0 || StringLen(g_testutil_pending_failure_actual) > 0;
}

//--- Stage failure details from nested comparison helpers for outer ASSERT macro
void TestUtilSetPendingFailure(const string expected, const string actual) {
  g_testutil_pending_failure_expected = expected;
  g_testutil_pending_failure_actual   = actual;
}

//--- Unified structured logging with standard test prefix and severity level
void TestUtilLogLine(const string level, const string message) {
  Print(TESTUTIL_LOG_PREFIX + "[" + level + "] " + message);
}

//--- Initialize test suite run and log suite start marker
void TestUtilRecordSuiteStart(const string suite_name) {
  TestUtilClearPendingFailure();
  TestUtilLogLine("SUITE", TestUtilSanitizeLogValue(suite_name));
}

//--- Initialize test case run and log case start marker
void TestUtilRecordCaseStart(const string case_name) {
  TestUtilClearPendingFailure();
  TestUtilLogLine("CASE", "name=" + TestUtilSanitizeLogValue(case_name));
}

//--- Log informational message associated with a specific test case
void TestUtilRecordInfo(const string case_name, const string detail) {
  TestUtilLogLine("INFO", "case=" + TestUtilSanitizeLogValue(case_name) + " " + TestUtilSanitizeLogValue(detail));
}

//--- Log operational progress event for network/IO operations
void TestUtilRecordOperationInfo(const string operation, const string detail) {
  TestUtilLogLine("INFO", "op=" + TestUtilSanitizeLogValue(operation) + " " + TestUtilSanitizeLogValue(detail));
}

//--- Log operational error event for failed network/IO operations
void TestUtilRecordOperationError(const string operation, const string detail) {
  TestUtilLogLine("ERROR", "op=" + TestUtilSanitizeLogValue(operation) + " " + TestUtilSanitizeLogValue(detail));
}

//--- Record assertion failure with precise line number and value comparison
void TestUtilRecordAssertionFailure(const string case_name, const int line_number, const string fallback_expected,
                                    const string fallback_actual) {
  string expected_value = fallback_expected;  // Use caller provided values by default
  string actual_value   = fallback_actual;    // Fallback if no pending failure was staged

  //--- If nested helper staged detailed failure override fallback values
  if (TestUtilHasPendingFailure()) {
    expected_value = g_testutil_pending_failure_expected;  // Use detailed expected value from comparator
    actual_value   = g_testutil_pending_failure_actual;    // Use detailed actual value from comparator
  }

  //--- Emit standardized failure log line parsable by CI scripts
  TestUtilLogLine("FAIL", StringFormat("case=%s line=%d expected=%s actual=%s", TestUtilSanitizeLogValue(case_name),
                                       line_number, TestUtilSanitizeLogValue(expected_value),
                                       TestUtilSanitizeLogValue(actual_value)));
  TestUtilClearPendingFailure();
}

//--- Record successful test case completion with assertion count
void TestUtilRecordCasePass(const string case_name, const int assertion_count) {
  TestUtilLogLine("PASS", StringFormat("case=%s assertions=%d", TestUtilSanitizeLogValue(case_name), assertion_count));
}

//--- Record failed test case completion with failure count
void TestUtilRecordCaseFail(const string case_name, const int failure_count) {
  int normalized_failures = (failure_count > 0) ? failure_count : 1;
  TestUtilLogLine("FAIL",
                  StringFormat("case=%s failures=%d", TestUtilSanitizeLogValue(case_name), normalized_failures));
}

//--- Record final test suite summary statistics
void TestUtilRecordSuiteSummary(const string suite_name, const int passed_tests, const int total_tests,
                                const int total_assertions) {
  int failed_tests = total_tests - passed_tests;  // Calculate failed test count

  //--- Guard against negative failure count due to counter overflow
  if (failed_tests < 0) {
    failed_tests = 0;
  }

  //--- Emit standardized summary line parsable by CI scripts
  TestUtilLogLine("SUMMARY",
                  StringFormat("suite=%s passed=%d failed=%d assertions=%d", TestUtilSanitizeLogValue(suite_name),
                               passed_tests, failed_tests, total_assertions));
}

//--- Record final suite pass/fail status for CI exit code determination
void TestUtilRecordSuiteStatus(const string suite_name, const bool passed) {
  TestUtilLogLine("STATUS",
                  StringFormat("suite=%s result=%s", TestUtilSanitizeLogValue(suite_name), passed ? "PASS" : "FAIL"));
}

//--- Emit the canonical suite-level summary and final status markers.
void TestUtilFinalizeSuite(const string suite_name, const int passed_tests, const int total_tests,
                           const int total_assertions) {
  TestUtilRecordSuiteSummary(suite_name, passed_tests, total_tests, total_assertions);
  TestUtilRecordSuiteStatus(suite_name, passed_tests == total_tests);
}

//--- Stage array size mismatch failure for outer ASSERT macro
void TestUtilSetArraySizeFailure(const string array_type, const int expected_size, const int actual_size) {
  TestUtilSetPendingFailure(StringFormat("%s size=%d", array_type, expected_size),
                            StringFormat("%s size=%d", array_type, actual_size));
}

//--- Stage array element mismatch failure with index position for outer ASSERT macro
void TestUtilSetArrayMismatch(const string array_type, const int expected_size, const int actual_size, const int index,
                              const string expected_value, const string actual_value) {
  TestUtilSetPendingFailure(
    StringFormat("%s size=%d index=%d value=%s", array_type, expected_size, index, expected_value),
    StringFormat("%s size=%d index=%d value=%s", array_type, actual_size, index, actual_value));
}

//+------------------------------------------------------------------+
//| Test Result Macros                                               |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| ASSERT_TRUE - Assert expression is true                          |
//+------------------------------------------------------------------+
#define ASSERT_TRUE(expr)                                                    \
  if (!(expr)) {                                                             \
    TestUtilRecordAssertionFailure(__FUNCTION__, __LINE__, "true", "false"); \
    g_tests_failed++;                                                        \
    return false;                                                            \
  } else {                                                                   \
    TestUtilClearPendingFailure();                                           \
    g_tests_passed++;                                                        \
  }

//+------------------------------------------------------------------+
//| ASSERT_FALSE - Assert expression is false                        |
//+------------------------------------------------------------------+
#define ASSERT_FALSE(expr)                                                   \
  if (expr) {                                                                \
    TestUtilRecordAssertionFailure(__FUNCTION__, __LINE__, "false", "true"); \
    g_tests_failed++;                                                        \
    return false;                                                            \
  } else {                                                                   \
    TestUtilClearPendingFailure();                                           \
    g_tests_passed++;                                                        \
  }

//+------------------------------------------------------------------+
//| ASSERT_EQ - Assert expected equals actual                        |
//+------------------------------------------------------------------+
#define ASSERT_EQ(expected, actual)                                                                                   \
  if ((expected) != (actual)) {                                                                                       \
    TestUtilRecordAssertionFailure(__FUNCTION__, __LINE__, TestUtilToString((expected)), TestUtilToString((actual))); \
    g_tests_failed++;                                                                                                 \
    return false;                                                                                                     \
  } else {                                                                                                            \
    TestUtilClearPendingFailure();                                                                                    \
    g_tests_passed++;                                                                                                 \
  }

//+------------------------------------------------------------------+
//| ASSERT_IN_RANGE - Assert actual value is within [lo, hi]         |
//| Use for non-deterministic values such as jittered backoff        |
//+------------------------------------------------------------------+
#define ASSERT_IN_RANGE(lo, hi, actual)                                                                     \
  if ((actual) < (lo) || (actual) > (hi)) {                                                                 \
    TestUtilRecordAssertionFailure(__FUNCTION__, __LINE__,                                                  \
                                   "[" + TestUtilToString((lo)) + "," + TestUtilToString((hi)) + "]",       \
                                   TestUtilToString((actual)));                                             \
    g_tests_failed++;                                                                                       \
    return false;                                                                                           \
  } else {                                                                                                  \
    TestUtilRecordInfo(__FUNCTION__,                                                                        \
                       StringFormat("line=%d value=%s range=[%s,%s]", __LINE__, TestUtilToString((actual)), \
                                    TestUtilToString((lo)), TestUtilToString((hi))));                       \
    TestUtilClearPendingFailure();                                                                          \
    g_tests_passed++;                                                                                       \
  }

//+------------------------------------------------------------------+
//| ASSERT_STR_EQ - Assert strings are equal                         |
//+------------------------------------------------------------------+
#define ASSERT_STR_EQ(expected, actual)                                                                               \
  if ((expected) != (actual)) {                                                                                       \
    TestUtilRecordAssertionFailure(__FUNCTION__, __LINE__, TestUtilToString((expected)), TestUtilToString((actual))); \
    g_tests_failed++;                                                                                                 \
    return false;                                                                                                     \
  } else {                                                                                                            \
    TestUtilClearPendingFailure();                                                                                    \
    g_tests_passed++;                                                                                                 \
  }

//+------------------------------------------------------------------+
//| Live Broker Test Settings                                        |
//+------------------------------------------------------------------+
bool   TestUtilLiveBrokerEnabled() { return MQTT_TEST_LIVE_BROKER_ENABLED; }

string TestUtilLiveBrokerHost() { return MQTT_TEST_LIVE_BROKER_HOST; }

int    TestUtilLiveBrokerPort() { return MQTT_TEST_LIVE_BROKER_PORT; }

bool   TestUtilSkipIfLiveBrokerDisabled(const string case_name) {
  if (TestUtilLiveBrokerEnabled()) {
    return false;
  }

  TestUtilRecordInfo(case_name, "skip=live_broker_disabled config=Scripts/MQTT/Tests/LiveBrokerConfig.mqh");
  return true;
}

//+------------------------------------------------------------------+
//| Network Utility Functions                                        |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| SendPublish - Send PUBLISH packet to broker                      |
//+------------------------------------------------------------------+
//| @param broker_host - Target broker hostname/IP                   |
//| @param broker_port - Target broker port                          |
//| @param pkt - PUBLISH packet bytes                                |
//| @return true if send successful                                  |
//+------------------------------------------------------------------+
bool SendPublish(const string broker_host, const int broker_port, uchar &pkt[]) {
  int skt = SocketCreate();  // Create blocking TCP socket

  //--- Validate socket handle was allocated successfully
  if (skt == INVALID_HANDLE) {
    TestUtilRecordOperationError("SendPublish", "stage=SocketCreate error=" + (string)GetLastError());
    return false;
  }

  //--- Establish TCP connection to broker with 5 second timeout
  if (!SocketConnect(skt, broker_host, broker_port, 5000)) {
    TestUtilRecordOperationError("SendPublish", "stage=SocketConnect host=" + broker_host + " port="
                                                  + (string)broker_port + " error=" + (string)GetLastError());
    SocketClose(skt);
    return false;
  }

  //--- Send complete packet buffer over established connection
  if (SocketSend(skt, pkt, ArraySize(pkt)) < 0) {
    TestUtilRecordOperationError("SendPublish", "stage=SocketSend error=" + (string)GetLastError());
    SocketClose(skt);
    return false;
  }

  //--- Cleanup socket immediately after send (fire and forget pattern)
  SocketClose(skt);
  return true;
}

//+------------------------------------------------------------------+
//| SendConnect - Send CONNECT and receive CONNACK                   |
//+------------------------------------------------------------------+
//| @param broker_host - Target broker hostname/IP                   |
//| @param broker_port - Target broker port                          |
//| @param pkt - CONNECT packet bytes                                |
//| @return 0 on success, -1 on failure                              |
//+------------------------------------------------------------------+
int SendConnect(const string broker_host, const int broker_port, uchar &pkt[]) {
  int skt = SocketCreate();  // Create blocking TCP socket

  //--- Validate socket handle was allocated successfully
  if (skt == INVALID_HANDLE) {
    TestUtilRecordOperationError("SendConnect", "stage=SocketCreate error=" + (string)GetLastError());
    return -1;
  }

  //--- Attempt to connect to the broker
  if (!SocketConnect(skt, broker_host, broker_port, 5000)) {
    TestUtilRecordOperationError("SendConnect", "stage=SocketConnect host=" + broker_host + " port="
                                                  + (string)broker_port + " error=" + (string)GetLastError());
    SocketClose(skt);
    return -1;
  }

  //--- Log successful connection establishment
  TestUtilRecordOperationInfo("SendConnect", "stage=SocketConnect host=" + broker_host + " port=" + (string)broker_port
                                               + " connected=true");

  //--- Send the CONNECT packet
  if (SocketSend(skt, pkt, ArraySize(pkt)) < 0) {
    TestUtilRecordOperationError("SendConnect", "stage=SocketSend error=" + (string)GetLastError());
    SocketClose(skt);
    return -1;
  }

  //--- Wait for CONNACK response (Fixed header 2 bytes + Variable header 2 bytes = 4 bytes)
  uchar rsp[];                                 // Response buffer for CONNACK packet
  int   read = SocketRead(skt, rsp, 4, 1000);  // Read exactly 4 bytes with 1 second timeout
  SocketClose(skt);                            // Always close socket before returning regardless of result

  //--- Validate we received complete CONNACK packet
  if (read < 4) {
    TestUtilRecordOperationError("SendConnect", "stage=SocketRead bytes=" + (string)read);
    return -1;
  }

  //--- Validate packet type (CONNACK = 2)
  if ((rsp[0] >> 4) != TESTUTIL_PKT_TYPE_CONNACK) {
    TestUtilRecordOperationError("SendConnect", "stage=ConnAck type=" + (string)(rsp[0] >> 4));
    return -1;
  }

  //--- Validate reason code (Byte 4 of CONNACK)
  if (rsp[3] != TESTUTIL_REASON_CODE_SUCCESS) {
    TestUtilRecordOperationError("SendConnect", "stage=ConnAck reason_code=" + (string)rsp[3]);
    return -1;
  }

  //--- All validations passed, connection accepted by broker
  TestUtilRecordOperationInfo("SendConnect", "stage=ConnAck result=accepted");
  return 0;
}

//+------------------------------------------------------------------+
//| Array Comparison Functions                                       |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| AssertEqual - Compare ushort arrays                              |
//+------------------------------------------------------------------+
bool AssertEqual(ushort &expected[], ushort &result[]) {
  int expected_size = ArraySize(expected);  // Get length of expected reference array
  int actual_size   = ArraySize(result);    // Get length of actual test result array

  //--- First validate array lengths match
  if (expected_size != actual_size) {
    TestUtilSetArraySizeFailure("ushort[]", expected_size, actual_size);
    return false;
  }

  //--- Compare each array element sequentially
  for (int i = 0; i < expected_size; i++) {
    if (expected[i] != result[i]) {
      TestUtilSetArrayMismatch("ushort[]", expected_size, actual_size, i, TestUtilToString(expected[i]),
                               TestUtilToString(result[i]));
      return false;
    }
  }

  //--- All elements match, clear any pending failure state
  TestUtilClearPendingFailure();
  return true;
}

//+------------------------------------------------------------------+
//| AssertEqual - Compare uint arrays                                |
//+------------------------------------------------------------------+
bool AssertEqual(uint &expected[], uint &result[]) {
  int expected_size = ArraySize(expected);  // Get length of expected reference array
  int actual_size   = ArraySize(result);    // Get length of actual test result array

  //--- First validate array lengths match
  if (expected_size != actual_size) {
    TestUtilSetArraySizeFailure("uint[]", expected_size, actual_size);
    return false;
  }

  //--- Compare each array element sequentially
  for (int i = 0; i < expected_size; i++) {
    if (expected[i] != result[i]) {
      TestUtilSetArrayMismatch("uint[]", expected_size, actual_size, i, TestUtilToString(expected[i]),
                               TestUtilToString(result[i]));
      return false;
    }
  }

  //--- All elements match, clear any pending failure state
  TestUtilClearPendingFailure();
  return true;
}

//+------------------------------------------------------------------+
//| AssertNotEqual - Verify uchar arrays are different               |
//+------------------------------------------------------------------+
bool AssertNotEqual(uchar &expected[], uchar &result[]) {
  int expected_size = ArraySize(expected);  // Get length of expected reference array
  int actual_size   = ArraySize(result);    // Get length of actual test result array

  //--- Different lengths automatically mean arrays are not equal
  if (expected_size != actual_size) {
    TestUtilClearPendingFailure();
    return true;  // They are indeed not equal
  }

  //--- Search for any differing element
  for (int i = 0; i < expected_size; i++) {
    if (expected[i] != result[i]) {
      TestUtilClearPendingFailure();
      return true;
    }
  }

  //--- All elements are identical - this is a failure for AssertNotEqual
  TestUtilSetPendingFailure(StringFormat("uchar[] arrays_different size=%d", expected_size),
                            StringFormat("uchar[] arrays_equal size=%d", actual_size));
  return false;
}

//+------------------------------------------------------------------+
//| AssertEqual - Compare string arrays                              |
//+------------------------------------------------------------------+
bool AssertEqual(string &expected[], string &result[]) {
  int expected_size = ArraySize(expected);  // Get length of expected reference array
  int actual_size   = ArraySize(result);    // Get length of actual test result array

  //--- First validate array lengths match
  if (expected_size != actual_size) {
    TestUtilSetArraySizeFailure("string[]", expected_size, actual_size);
    return false;
  }

  //--- Compare each array element sequentially
  for (int i = 0; i < expected_size; i++) {
    if (expected[i] != result[i]) {
      TestUtilSetArrayMismatch("string[]", expected_size, actual_size, i, expected[i], result[i]);
      return false;
    }
  }

  //--- All elements match, clear any pending failure state
  TestUtilClearPendingFailure();
  return true;
}

//+------------------------------------------------------------------+
//| AssertEqual - Compare uchar arrays (detailed error reporting)    |
//+------------------------------------------------------------------+
bool AssertEqual(uchar &expected[], uchar &result[]) {
  int expected_size = ArraySize(expected);  // Get length of expected reference array
  int actual_size   = ArraySize(result);    // Get length of actual test result array

  //--- First validate array lengths match
  if (expected_size != actual_size) {
    TestUtilSetArraySizeFailure("uchar[]", expected_size, actual_size);
    return false;
  }

  //--- Compare each array element sequentially
  for (int i = 0; i < expected_size; i++) {
    if (expected[i] != result[i]) {
      TestUtilSetArrayMismatch("uchar[]", expected_size, actual_size, i, TestUtilFormatByteHex(expected[i]),
                               TestUtilFormatByteHex(result[i]));
      return false;
    }
  }

  //--- All elements match, clear any pending failure state
  TestUtilClearPendingFailure();
  return true;
}

//+------------------------------------------------------------------+
//| Assert - Alias for AssertEqual                                   |
//+------------------------------------------------------------------+
bool   Assert(uchar &expected[], uchar &result[]) { return AssertEqual(expected, result); }

//+------------------------------------------------------------------+
//| Utility Functions                                                |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| ArrayToHex - Convert byte array to hex string                    |
//+------------------------------------------------------------------+
//| @param arr - Input byte array                                    |
//| @param count - Number of bytes to convert (-1 for all)           |
//| @return Hex string representation                                |
//+------------------------------------------------------------------+
string ArrayToHex(uchar &arr[], int count = -1) {
  string res = "";  // Output buffer for hex string result

  //--- Check bounds
  if (count < 0 || count > ArraySize(arr)) {
    count = ArraySize(arr);
  }

  //--- Transform to HEX string
  for (int i = 0; i < count; i++) {
    res += StringFormat("%.2X ", arr[i]);
  }

  //--- Return the resulting HEX string
  return (res);
}
