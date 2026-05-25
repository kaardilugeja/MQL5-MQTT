//+------------------------------------------------------------------+
//|                                        PublishQueueTestSuite.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Shared runner for the publish queue and coordinator test cases.  |
//| Keeps TEST_PublishQueue.mq5 thin while centralizing execution,   |
//| standardized suite logging, and optional result-file output.     |
//+------------------------------------------------------------------+
#ifndef MQTT_PUBLISH_QUEUE_TEST_SUITE_MQH
#define MQTT_PUBLISH_QUEUE_TEST_SUITE_MQH

#ifndef TESTUTIL_SKIP_MQTT_INCLUDE
#define TESTUTIL_SKIP_MQTT_INCLUDE
#endif

#include "TestUtil.mqh"
#include <MQTT/Internal/Storage/SessionDatabase.mqh>
#include <MQTT/Internal/Queue/PublishQueue.mqh>
#include <MQTT/Internal/Queue/PublishQueueCoordinator.mqh>

//+------------------------------------------------------------------+
//| CPublishQueueDrainSinkStub                                       |
//| Test double for coordinator drain calls. Captures both the flat  |
//| buffer slice coordinates and copied bytes so tests can assert    |
//| queue slicing, expiry propagation, and durable handoff rules.    |
//+------------------------------------------------------------------+
class CPublishQueueDrainSinkStub : public IMqttPublishQueueDrainSink {
 public:
  int    m_publish_error;                  // Injected PublishQueuedEntry() result for failure-path tests.
  int    m_publish_calls;                  // Number of drain attempts observed by this stub.
  bool   m_publish_handoff_complete;       // Scripted durable-handoff answer to snapshot on the next publish.
  bool   m_last_publish_handoff_complete;  // Durable-handoff state recorded for the most recent drain attempt.
  uint   m_last_remaining_expiry;          // Expiry seconds forwarded to the latest PublishQueuedEntry() call.
  int    m_last_error_code;                // Last queue/coordinator error code reported through ReportQueueError().
  string m_last_error_description;         // Human-readable description paired with m_last_error_code.
  string m_last_publish_topic;             // Topic captured from the most recent drained queue entry.
  uint   m_last_payload_offset;            // Original payload slice offset inside the queue's flat payload buffer.
  uint   m_last_payload_length;            // Original payload slice length inside the queue's flat payload buffer.
  uint   m_last_prop_offset;               // Original property slice offset inside the flat property buffer.
  uint   m_last_prop_length;               // Original property slice length inside the flat property buffer.
  uchar  m_last_payload[];     // Owning payload copy for assertions that should not depend on queue storage.
  uchar  m_last_properties[];  // Owning MQTT 5 PUBLISH property copy for slice/replay assertions.

  CPublishQueueDrainSinkStub() {
    m_publish_error                 = 0;
    m_publish_calls                 = 0;
    m_publish_handoff_complete      = false;
    m_last_publish_handoff_complete = false;
    m_last_remaining_expiry         = 0;
    m_last_error_code               = 0;
    m_last_error_description        = "";
    m_last_publish_topic            = "";
    m_last_payload_offset           = 0;
    m_last_payload_length           = 0;
    m_last_prop_offset              = 0;
    m_last_prop_length              = 0;
    ArrayResize(m_last_payload, 0);
    ArrayResize(m_last_properties, 0);
  }

  virtual uint RemainingExpirySecondsFromDeadlineUs(ulong expiry_deadline_us, ulong now_us) const override {
    if (expiry_deadline_us == 0 || expiry_deadline_us <= now_us) {
      return 0;
    }

    return (uint)((expiry_deadline_us - now_us + 999999ULL) / 1000000ULL);
  }

  virtual int PublishQueuedEntry(const string topic, const uchar &payload_buffer[], uint payload_offset,
                                 uint payload_length, uchar qos, bool retain, const uchar &encoded_props_buffer[],
                                 uint prop_offset, uint prop_length, uint remaining_expiry,
                                 bool allow_outgoing_sub_id) override {
    //--- Record the original offsets as well as copied bytes so slice-oriented tests
    //--- can prove the coordinator streamed the right window from the queue buffers.
    m_publish_calls++;
    m_last_publish_handoff_complete = m_publish_handoff_complete;
    m_last_remaining_expiry         = remaining_expiry;
    m_last_publish_topic            = topic;
    m_last_payload_offset           = payload_offset;
    m_last_payload_length           = payload_length;
    m_last_prop_offset              = prop_offset;
    m_last_prop_length              = prop_length;
    ArrayResize(m_last_payload, (int)payload_length);
    if (payload_length > 0) {
      ArrayCopy(m_last_payload, payload_buffer, 0, (int)payload_offset, (int)payload_length);
    }
    ArrayResize(m_last_properties, (int)prop_length);
    if (prop_length > 0) {
      ArrayCopy(m_last_properties, encoded_props_buffer, 0, (int)prop_offset, (int)prop_length);
    }
    return m_publish_error;
  }

  virtual bool LastQueuedPublishDurablyHandedOff() const override { return m_last_publish_handoff_complete; }

  virtual void ReportQueueError(int code, const string description) override {
    m_last_error_code        = code;
    m_last_error_description = description;
  }
};

bool TEST_PublishQueue_AppendAndReadBack() {
  TEST_CASE_START();

  CMqttPublishQueue      queue;
  MqttQueuedPublishEntry entry;
  uchar                  payload[] = {0x41, 0x42, 0x43};
  uchar                  props[]   = {0x26, 0x00, 0x01, 0x6B, 0x00, 0x01, 0x76};

  ASSERT_TRUE(queue.Append("queue/topic", payload, (uint)ArraySize(payload), 1, true, props,
                           GetMicrosecondCount() + 5000000ULL, true, 7));
  ASSERT_EQ(1, (int)queue.GetQueuedMessageCount());
  ASSERT_EQ(3, (int)queue.GetPayloadBytes());
  ASSERT_EQ(ArraySize(props), (int)queue.GetPropertyBytes());

  ASSERT_TRUE(queue.ReadEntry(0, entry));
  ASSERT_STR_EQ("queue/topic", entry.topic);
  ASSERT_EQ(1, (int)entry.qos);
  ASSERT_TRUE(entry.retain);
  ASSERT_TRUE(entry.allow_outgoing_sub_id);
  ASSERT_EQ(7, (int)entry.durable_store_id);
  ASSERT_TRUE(AssertEqual(payload, entry.payload));
  ASSERT_TRUE(AssertEqual(props, entry.properties));

  return true;
}

bool TEST_PublishQueue_AdmissionBudgets() {
  TEST_CASE_START();

  CMqttPublishQueue queue;

  queue.SetMaxMessages(1);
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_OK, (int)queue.EvaluateAdmission(2, 1));
  uchar payload[] = {0x01, 0x02};
  uchar props[]   = {0x11};
  ASSERT_TRUE(queue.Append("queue/a", payload, 2, 1, false, props, 0, false, 0));
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_COUNT_LIMIT, (int)queue.EvaluateAdmission(1, 0));

  CMqttPublishQueue bytes_queue;
  bytes_queue.SetMaxMessages(10);
  bytes_queue.SetMaxSingleBytes(4);
  bytes_queue.SetMaxPayloadBytes(3);
  bytes_queue.SetMaxPropertyBytes(2);
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_SINGLE_MESSAGE_LIMIT, (int)bytes_queue.EvaluateAdmission(3, 2));
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_OK, (int)bytes_queue.EvaluateAdmission(2, 1));
  ASSERT_TRUE(bytes_queue.Append("queue/b", payload, 2, 1, false, props, 0, false, 0));
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_PAYLOAD_BYTES_LIMIT, (int)bytes_queue.EvaluateAdmission(2, 0));
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_PROPERTY_BYTES_LIMIT, (int)bytes_queue.EvaluateAdmission(0, 2));

  return true;
}

bool TEST_PublishQueue_RollbackTail() {
  TEST_CASE_START();

  CMqttPublishQueue queue;
  uchar             payload[] = {0x51, 0x52, 0x53};
  uchar             props[]   = {0x01, 0x02};

  ASSERT_TRUE(queue.Append("queue/rollback", payload, 3, 1, false, props, 0, false, 0));
  ASSERT_EQ(1, (int)queue.GetQueuedMessageCount());
  queue.RollbackTail(3, 2);
  ASSERT_EQ(0, (int)queue.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)queue.GetPayloadBytes());
  ASSERT_EQ(0, (int)queue.GetPropertyBytes());

  return true;
}

bool TEST_PublishQueue_CompactAfterDrain() {
  TEST_CASE_START();

  CMqttPublishQueue      queue;
  MqttQueuedPublishEntry entry;
  uchar                  payload_a[] = {0x61};
  uchar                  payload_b[] = {0x62, 0x62};
  uchar                  payload_c[] = {0x63, 0x63, 0x63};
  uchar                  no_props[];

  ASSERT_TRUE(queue.Append("queue/a", payload_a, 1, 1, false, no_props, 0, false, 0));
  ASSERT_TRUE(queue.Append("queue/b", payload_b, 2, 1, false, no_props, 0, false, 0));
  ASSERT_TRUE(queue.Append("queue/c", payload_c, 3, 1, false, no_props, 0, false, 0));

  queue.AdvanceDrainHead();
  queue.AdvanceDrainHead();
  ASSERT_TRUE(queue.ShouldCompactAfterDrain());
  queue.Compact();

  ASSERT_EQ(1, (int)queue.GetQueuedMessageCount());
  ASSERT_EQ(1, (int)queue.GetTotalCount());
  ASSERT_TRUE(queue.ReadDrainEntry(entry));
  ASSERT_STR_EQ("queue/c", entry.topic);
  ASSERT_TRUE(AssertEqual(payload_c, entry.payload));
  ASSERT_EQ(3, (int)queue.GetPayloadBytes());

  return true;
}

bool TEST_PublishQueue_PurgeExpiredReturnsStoreIds() {
  TEST_CASE_START();

  CMqttPublishQueue queue;
  uchar             payload[] = {0x41};
  uchar             no_props[];
  ulong             removed_store_ids[];
  ulong             now_us = GetMicrosecondCount();

  ASSERT_TRUE(queue.Append("queue/live", payload, 1, 1, false, no_props, now_us + 1000000ULL, false, 0));
  ASSERT_TRUE(queue.Append("queue/expired", payload, 1, 1, false, no_props, now_us - 1ULL, false, 99));

  ASSERT_EQ(1, (int)queue.PurgeExpired(now_us, removed_store_ids));
  ASSERT_EQ(1, ArraySize(removed_store_ids));
  ASSERT_EQ(99, (int)removed_store_ids[0]);
  ASSERT_EQ(1, (int)queue.GetQueuedMessageCount());

  MqttQueuedPublishEntry entry;
  ASSERT_TRUE(queue.ReadEntry(0, entry));
  ASSERT_STR_EQ("queue/live", entry.topic);

  return true;
}

bool TEST_PublishQueue_OldestAgeHonorsDrainHead() {
  TEST_CASE_START();

  CMqttPublishQueue queue;
  uchar             payload[] = {0x41};
  uchar             no_props[];
  ulong             now_us = 6000000ULL;

  ASSERT_TRUE(queue.Append("queue/a", payload, 1, 1, false, no_props, 0, false, 0));
  ASSERT_TRUE(queue.Append("queue/b", payload, 1, 1, false, no_props, 0, false, 0));
  queue.SetEnqueuedAtUs(0, now_us - 5000000ULL);
  queue.SetEnqueuedAtUs(1, now_us - 1500000ULL);

  ASSERT_TRUE(queue.GetOldestQueuedMessageAgeMs(now_us, TimeLocal()) >= 5000ULL);
  queue.AdvanceDrainHead();
  ASSERT_TRUE(queue.GetOldestQueuedMessageAgeMs(now_us, TimeLocal()) >= 1500ULL);
  ASSERT_TRUE(queue.GetOldestQueuedMessageAgeMs(now_us, TimeLocal()) < 5000ULL);

  return true;
}

bool TEST_PublishQueue_BytesTrackDrainHead() {
  TEST_CASE_START();

  //--- Regression: m_payload_bytes and m_property_bytes must reflect only the live
  //--- (not-yet-drained) entries so EvaluateAdmission is accurate between drain and
  //--- compaction.  Before the fix, AdvanceDrainHead() did not decrement the counters,
  //--- causing false QUEUE_FULL rejections and O(n²) PurgeExpired work.
  CMqttPublishQueue queue;
  uchar             p1[] = {0x41};              // 1 byte
  uchar             p2[] = {0x42, 0x43};        // 2 bytes
  uchar             p3[] = {0x44, 0x45, 0x46};  // 3 bytes
  uchar             no_props[];

  ASSERT_TRUE(queue.Append("q/a", p1, 1, 1, false, no_props, 0, false, 0));
  ASSERT_TRUE(queue.Append("q/b", p2, 2, 1, false, no_props, 0, false, 0));
  ASSERT_TRUE(queue.Append("q/c", p3, 3, 1, false, no_props, 0, false, 0));
  ASSERT_EQ(6, (int)queue.GetPayloadBytes());

  queue.AdvanceDrainHead();                    // drain entry 0 (1 byte)
  ASSERT_EQ(5, (int)queue.GetPayloadBytes());  // live: p2 + p3

  queue.AdvanceDrainHead();                    // drain entry 1 (2 bytes)
  ASSERT_EQ(3, (int)queue.GetPayloadBytes());  // live: p3 only

  //--- After compaction the recalculated value must still be 3.
  ASSERT_TRUE(queue.ShouldCompactAfterDrain());
  queue.Compact();
  ASSERT_EQ(3, (int)queue.GetPayloadBytes());

  return true;
}

bool TEST_PublishQueue_AdmissionRespectsDrainedBytes() {
  TEST_CASE_START();

  //--- Regression: EvaluateAdmission must allow new messages that would fit in the
  //--- remaining byte budget once drained entries are excluded.  Before the fix,
  //--- m_payload_bytes included drained bytes, causing false PAYLOAD_BYTES_LIMIT
  //--- rejections when the live queue had room.
  CMqttPublishQueue queue;
  uchar             p[] = {0x01, 0x02, 0x03};  // 3 bytes
  uchar             no_props[];

  queue.SetMaxPayloadBytes(4);
  ASSERT_TRUE(queue.Append("q/a", p, 3, 1, false, no_props, 0, false, 0));
  //--- At 3/4 bytes: trying to add 2 more exceeds budget.
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_PAYLOAD_BYTES_LIMIT, (int)queue.EvaluateAdmission(2, 0));

  //--- Drain entry 0 → live bytes drop to 0; now 2 fits.
  queue.AdvanceDrainHead();
  ASSERT_EQ(0, (int)queue.GetPayloadBytes());
  ASSERT_EQ((int)MQTT_QUEUE_ADMIT_OK, (int)queue.EvaluateAdmission(2, 0));

  return true;
}

bool TEST_PublishQueue_PurgeExpiredSkipsCompactionForDrainedPrefix() {
  TEST_CASE_START();

  //--- Regression: PurgeExpired must return 0 without any array copies when
  //--- no entries are expired, even if m_drain_head > 0.  The previous guard
  //--- (dropped == 0 && start == 0) fell through and did O(n) work, turning the
  //--- mid-drain-disconnect + offline-queue enqueue loop from O(n) to O(n²).
  CMqttPublishQueue queue;
  uchar             p[] = {0x01};
  uchar             no_props[];
  ulong             far_future_us = GetMicrosecondCount() + 3600000000ULL;
  ulong             removed_store_ids[];

  //--- Append three non-expiring entries.
  ASSERT_TRUE(queue.Append("q/a", p, 1, 1, false, no_props, far_future_us, false, 10));
  ASSERT_TRUE(queue.Append("q/b", p, 1, 1, false, no_props, far_future_us, false, 20));
  ASSERT_TRUE(queue.Append("q/c", p, 1, 1, false, no_props, far_future_us, false, 30));

  //--- Simulate partial drain (drain_head > 0, simulating mid-drain-disconnect).
  queue.AdvanceDrainHead();
  ASSERT_EQ(1, (int)queue.GetDrainHead());
  ASSERT_EQ(2, (int)queue.GetPayloadBytes());  // live: entries 1 and 2

  //--- PurgeExpired with nothing expired must return 0 and must NOT compact.
  uint dropped = queue.PurgeExpired(GetMicrosecondCount(), removed_store_ids);
  ASSERT_EQ(0, (int)dropped);
  ASSERT_EQ(0, ArraySize(removed_store_ids));
  //--- drain_head and m_count must be unchanged (no compaction happened).
  ASSERT_EQ(3, (int)queue.GetTotalCount());
  ASSERT_EQ(1, (int)queue.GetDrainHead());
  //--- byte counters must still be correct.
  ASSERT_EQ(2, (int)queue.GetPayloadBytes());

  return true;
}

bool TEST_PublishQueueCoordinator_ClassifyDrainResult() {
  TEST_CASE_START();

  CMqttPublishQueueCoordinator coordinator;

  ASSERT_EQ((int)MQTT_QUEUED_DRAIN_ACTION_CONSUME, (int)coordinator.ClassifyDrainResult(0));
  ASSERT_EQ((int)MQTT_QUEUED_DRAIN_ACTION_DROP, (int)coordinator.ClassifyDrainResult(-10));
  ASSERT_EQ((int)MQTT_QUEUED_DRAIN_ACTION_DROP, (int)coordinator.ClassifyDrainResult(-5));
  ASSERT_EQ((int)MQTT_QUEUED_DRAIN_ACTION_DROP, (int)coordinator.ClassifyDrainResult(-6));
  ASSERT_EQ((int)MQTT_QUEUED_DRAIN_ACTION_RETRY, (int)coordinator.ClassifyDrainResult(-1));

  return true;
}

bool TEST_PublishQueueCoordinator_RestoreDurableRoundTrip() {
  TEST_CASE_START();

  string                       session_id = "test-publish-queue-coordinator-" + (string)GetTickCount();
  CSessionDatabase             writer;
  CSessionDatabase             reader;
  CMqttPublishQueue            queue;
  CMqttPublishQueue            restored_queue;
  CMqttPublishQueueCoordinator coordinator;
  MqttQueuedPublishEntry       entry;
  uchar                        payload[]      = {0x61, 0x62, 0x63};
  uchar                        props[]        = {0x26, 0x00, 0x01, 0x6B, 0x00, 0x01, 0x76};
  string                       error_text     = "";
  uint                         restored_count = 0;

  //--- Persist through one session DB instance, then reload through a fresh reader
  //--- instance so the test exercises the same restore path used after EA restart.
  ASSERT_TRUE(writer.Init(session_id, true));
  ASSERT_TRUE(queue.Append("durable/topic", payload, 3, 1, true, props, GetMicrosecondCount() + 1000000ULL, true, 0));
  ASSERT_TRUE(coordinator.PersistAcceptedQueueTail(writer, queue, 1, "durable/topic", payload, 3, true, 30, props, true,
                                                   (uint)(ArraySize(props) + 5), error_text));
  ASSERT_TRUE(queue.GetStoreId(0) > 0);

  ASSERT_TRUE(reader.Init(session_id, false));
  reader.SetPersistence(true);
  ASSERT_TRUE(reader.LoadFromFile());
  ASSERT_TRUE(coordinator.RestorePersistedQueue(reader, restored_queue, GetMicrosecondCount(), TimeLocal(),
                                                restored_count, error_text));
  ASSERT_EQ(1, (int)restored_count);
  ASSERT_TRUE(restored_queue.ReadEntry(0, entry));
  ASSERT_STR_EQ("durable/topic", entry.topic);
  ASSERT_TRUE(entry.retain);
  ASSERT_TRUE(entry.allow_outgoing_sub_id);
  ASSERT_TRUE(entry.durable_store_id > 0);
  ASSERT_TRUE(AssertEqual(payload, entry.payload));
  ASSERT_TRUE(AssertEqual(props, entry.properties));
  ASSERT_TRUE(coordinator.RemoveDurableMessage(reader, entry.durable_store_id, error_text));
  ASSERT_EQ(0, (int)reader.GetOfflineQueuedMessageCount());

  reader.Clear();
  return true;
}

bool TEST_PublishQueueCoordinator_RestorePreservesQueuedAge() {
  TEST_CASE_START();

  string                       session_id = "test-publish-queue-restore-age-" + (string)GetTickCount();
  CSessionDatabase             writer;
  CSessionDatabase             reader;
  CMqttPublishQueue            restored_queue;
  CMqttPublishQueueCoordinator coordinator;
  uchar                        payload[] = {0x70, 0x71};
  uchar                        no_props[];
  string                       error_text     = "";
  uint                         restored_count = 0;

  ASSERT_TRUE(writer.Init(session_id, true));
  ASSERT_TRUE(
    writer.StoreOfflineQueuedMessage(1, "restore/age", payload, ArraySize(payload), false, 30, no_props, false) > 0);
  ASSERT_TRUE(writer.SaveToFile());

  Sleep(2100);

  ASSERT_TRUE(reader.Init(session_id, false));
  reader.SetPersistence(true);
  ASSERT_TRUE(reader.LoadFromFile());
  ASSERT_TRUE(coordinator.RestorePersistedQueue(reader, restored_queue, GetMicrosecondCount(), TimeLocal(),
                                                restored_count, error_text));
  ASSERT_EQ(1, (int)restored_count);
  ASSERT_TRUE(restored_queue.GetOldestQueuedMessageAgeMs(GetMicrosecondCount(), TimeLocal()) >= 1000ULL);

  reader.Clear();
  return true;
}

bool TEST_PublishQueueCoordinator_DrainQueueFlow() {
  TEST_CASE_START();

  string                       session_id = "test-publish-queue-drain-" + (string)GetTickCount();
  CSessionDatabase             session_db;
  CMqttPublishQueue            queue;
  CMqttPublishQueueCoordinator coordinator;
  CPublishQueueDrainSinkStub   sink;
  uchar                        live_payload[]    = {0x41};
  uchar                        expired_payload[] = {0x42};
  uchar                        no_props[];
  uint                         sent_count    = 0;
  uint                         dropped_count = 0;
  ulong                        now_us        = GetMicrosecondCount();

  ASSERT_TRUE(session_db.Init(session_id, true));

  ulong live_store_id =
    session_db.StoreOfflineQueuedMessage(1, "queue/live", live_payload, 1, false, 30, no_props, false);
  ulong expired_store_id =
    session_db.StoreOfflineQueuedMessage(1, "queue/expired", expired_payload, 1, false, 30, no_props, false);
  ASSERT_TRUE(live_store_id > 0);
  ASSERT_TRUE(expired_store_id > 0);
  ASSERT_TRUE(session_db.FlushIfDirty(0));

  ASSERT_TRUE(
    queue.Append("queue/live", live_payload, 1, 1, false, no_props, now_us + 2000000ULL, false, live_store_id));
  ASSERT_TRUE(
    queue.Append("queue/expired", expired_payload, 1, 1, false, no_props, now_us - 1ULL, false, expired_store_id));

  coordinator.DrainQueue(session_db, queue, sink, sent_count, dropped_count);

  ASSERT_EQ(1, (int)sent_count);
  ASSERT_EQ(1, (int)dropped_count);
  ASSERT_EQ(1, sink.m_publish_calls);
  ASSERT_STR_EQ("queue/live", sink.m_last_publish_topic);
  ASSERT_TRUE(sink.m_last_remaining_expiry > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, sink.m_last_error_code);
  ASSERT_TRUE(StringFind(sink.m_last_error_description, "expired queued publish") >= 0);
  ASSERT_EQ(0, (int)session_db.GetOfflineQueuedMessageCount());
  ASSERT_EQ(0, (int)queue.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)queue.GetTotalCount());

  session_db.Clear();
  return true;
}

bool TEST_PublishQueueCoordinator_DrainQueueUsesSlices() {
  TEST_CASE_START();

  CSessionDatabase             session_db;
  CMqttPublishQueue            queue;
  CMqttPublishQueueCoordinator coordinator;
  CPublishQueueDrainSinkStub   sink;
  uchar                        prefix_payload[] = {0x10, 0x11};
  uchar                        prefix_props[]   = {0x01};
  uchar                        target_payload[] = {0x21, 0x22, 0x23};
  uchar                        target_props[]   = {0x26, 0x00, 0x01, 0x6B, 0x00, 0x01, 0x76};
  uint                         sent_count       = 0;
  uint                         dropped_count    = 0;

  ASSERT_TRUE(session_db.Init("test-publish-queue-slice-" + (string)GetTickCount(), false));
  ASSERT_TRUE(
    queue.Append("queue/prefix", prefix_payload, ArraySize(prefix_payload), 1, false, prefix_props, 0, false, 0));
  ASSERT_TRUE(
    queue.Append("queue/target", target_payload, ArraySize(target_payload), 1, true, target_props, 0, true, 0));

  queue.AdvanceDrainHead();
  coordinator.DrainQueue(session_db, queue, sink, sent_count, dropped_count);

  ASSERT_EQ(1, (int)sent_count);
  ASSERT_EQ(0, (int)dropped_count);
  ASSERT_EQ(1, sink.m_publish_calls);
  ASSERT_STR_EQ("queue/target", sink.m_last_publish_topic);
  ASSERT_TRUE(sink.m_last_payload_offset > 0);
  ASSERT_TRUE(sink.m_last_prop_offset > 0);
  ASSERT_EQ(ArraySize(target_payload), (int)sink.m_last_payload_length);
  ASSERT_EQ(ArraySize(target_props), (int)sink.m_last_prop_length);
  ASSERT_TRUE(AssertEqual(target_payload, sink.m_last_payload));
  ASSERT_TRUE(AssertEqual(target_props, sink.m_last_properties));

  session_db.Clear();
  return true;
}

bool TEST_PublishQueueCoordinator_PurgeExpiredFlow() {
  TEST_CASE_START();

  string                       session_id = "test-publish-queue-purge-" + (string)GetTickCount();
  CSessionDatabase             session_db;
  CMqttPublishQueue            queue;
  CMqttPublishQueueCoordinator coordinator;
  CPublishQueueDrainSinkStub   sink;
  uchar                        live_payload[]    = {0x51};
  uchar                        expired_payload[] = {0x52};
  uchar                        no_props[];
  MqttQueuedPublishEntry       entry;
  ulong                        now_us = GetMicrosecondCount();

  ASSERT_TRUE(session_db.Init(session_id, true));

  ulong live_store_id =
    session_db.StoreOfflineQueuedMessage(1, "purge/live", live_payload, 1, false, 30, no_props, false);
  ulong expired_store_id =
    session_db.StoreOfflineQueuedMessage(1, "purge/expired", expired_payload, 1, false, 30, no_props, false);
  ASSERT_TRUE(live_store_id > 0);
  ASSERT_TRUE(expired_store_id > 0);
  ASSERT_TRUE(session_db.FlushIfDirty(0));

  ASSERT_TRUE(
    queue.Append("purge/live", live_payload, 1, 1, false, no_props, now_us + 1000000ULL, false, live_store_id));
  ASSERT_TRUE(
    queue.Append("purge/expired", expired_payload, 1, 1, false, no_props, now_us - 1ULL, false, expired_store_id));

  ASSERT_EQ(1, (int)coordinator.PurgeExpiredQueue(session_db, queue, sink));
  ASSERT_EQ(0, sink.m_last_error_code);
  ASSERT_EQ(1, (int)session_db.GetOfflineQueuedMessageCount());
  ASSERT_EQ(1, (int)queue.GetQueuedMessageCount());
  ASSERT_TRUE(queue.ReadEntry(0, entry));
  ASSERT_STR_EQ("purge/live", entry.topic);

  session_db.Clear();
  return true;
}

bool TEST_PublishQueueCoordinator_RetryStopsDrain() {
  TEST_CASE_START();

  string                       session_id = "test-publish-queue-retry-" + (string)GetTickCount();
  CSessionDatabase             session_db;
  CMqttPublishQueue            queue;
  CMqttPublishQueueCoordinator coordinator;
  CPublishQueueDrainSinkStub   sink;
  uchar                        first_payload[]  = {0x61};
  uchar                        second_payload[] = {0x62};
  uchar                        no_props[];
  bool                         is_draining   = false;
  uint                         sent_count    = 0;
  uint                         dropped_count = 0;
  ulong                        now_us        = GetMicrosecondCount();

  ASSERT_TRUE(session_db.Init(session_id, true));

  ulong first_store_id =
    session_db.StoreOfflineQueuedMessage(1, "retry/one", first_payload, 1, false, 30, no_props, false);
  ulong second_store_id =
    session_db.StoreOfflineQueuedMessage(1, "retry/two", second_payload, 1, false, 30, no_props, false);
  ASSERT_TRUE(first_store_id > 0);
  ASSERT_TRUE(second_store_id > 0);
  ASSERT_TRUE(session_db.FlushIfDirty(0));

  ASSERT_TRUE(
    queue.Append("retry/one", first_payload, 1, 1, false, no_props, now_us + 2000000ULL, false, first_store_id));
  ASSERT_TRUE(
    queue.Append("retry/two", second_payload, 1, 1, false, no_props, now_us + 2000000ULL, false, second_store_id));

  sink.m_publish_error = -1;

  ASSERT_TRUE(coordinator.DrainQueueIfPending(session_db, queue, sink, is_draining, sent_count, dropped_count));
  ASSERT_FALSE(is_draining);
  ASSERT_EQ(1, sink.m_publish_calls);
  ASSERT_EQ(0, (int)sent_count);
  ASSERT_EQ(0, (int)dropped_count);
  ASSERT_EQ(-1, sink.m_last_error_code);
  ASSERT_TRUE(StringFind(sink.m_last_error_description, "Failed to drain queued publish") >= 0);
  ASSERT_EQ(2, (int)session_db.GetOfflineQueuedMessageCount());
  ASSERT_EQ(2, (int)queue.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)queue.GetDrainHead());

  session_db.Clear();
  return true;
}

bool TEST_PQC_RetryAfterHandoffConsumes() {
  TEST_CASE_START();

  string                       session_id = "test-publish-queue-handoff-retry-" + (string)GetTickCount();
  CSessionDatabase             session_db;
  CSessionDatabase             reader;
  CMqttPublishQueue            queue;
  CMqttPublishQueue            restored_queue;
  CMqttPublishQueueCoordinator coordinator;
  CPublishQueueDrainSinkStub   sink;
  uchar                        payload[] = {0x71, 0x72};
  uchar                        no_props[];
  bool                         is_draining    = false;
  uint                         sent_count     = 0;
  uint                         dropped_count  = 0;
  uint                         restored_count = 0;
  string                       error_text     = "";
  OfflineQueuedMessage         queued[];
  ulong                        expiry_time_us = GetMicrosecondCount() + 2000000ULL;

  ASSERT_TRUE(session_db.Init(session_id, true));

  ulong store_id =
    session_db.StoreOfflineQueuedMessage(1, "handoff/retry", payload, ArraySize(payload), false, 30, no_props, false);
  ASSERT_TRUE(store_id > 0);
  ASSERT_TRUE(
    queue.Append("handoff/retry", payload, ArraySize(payload), 1, false, no_props, expiry_time_us, false, store_id));

  //--- The client can report a retry-style result even after the queued publish was
  //--- durably promoted into the normal in-flight QoS path. That case must consume the
  //--- queue slot so restart restore does not resurrect a ghost replay.
  sink.m_publish_error            = -1;
  sink.m_publish_handoff_complete = true;
  session_db.TestForceFinalizeOfflineQueuedFallbackOnce();

  ASSERT_TRUE(coordinator.DrainQueueIfPending(session_db, queue, sink, is_draining, sent_count, dropped_count));
  ASSERT_FALSE(is_draining);
  ASSERT_EQ(1, sink.m_publish_calls);
  ASSERT_EQ(1, (int)sent_count);
  ASSERT_EQ(0, (int)dropped_count);
  ASSERT_EQ(0, (int)queue.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)queue.GetTotalCount());
  ASSERT_EQ(0, (int)session_db.GetOfflineQueuedMessageCount());
  ASSERT_EQ(0, (int)session_db.GetOfflineQueuedMessages(queued));

  ASSERT_TRUE(reader.Init(session_id, false));
  reader.SetPersistence(true);
  ASSERT_TRUE(reader.LoadFromFile());
  ASSERT_EQ(0, (int)reader.GetOfflineQueuedMessageCount());
  ASSERT_EQ(0, (int)reader.GetOfflineQueuedMessages(queued));
  ASSERT_TRUE(coordinator.RestorePersistedQueue(reader, restored_queue, GetMicrosecondCount(), TimeLocal(),
                                                restored_count, error_text));
  ASSERT_EQ(0, (int)restored_count);
  ASSERT_STR_EQ("", error_text);

  reader.Clear();
  return true;
}

bool TEST_PublishQueueCoordinator_QueueWhileDisconnectedPersistsTail() {
  TEST_CASE_START();

  string                       session_id = "test-publish-queue-offline-accept-" + (string)GetTickCount();
  CSessionDatabase             session_db;
  CMqttPublishQueue            queue;
  CMqttPublishQueueCoordinator coordinator;
  CPublishQueueDrainSinkStub   sink;
  uchar                        expired_payload[] = {0x61};
  uchar                        live_payload[]    = {0x62, 0x63};
  uchar                        props[]           = {0x26, 0x00, 0x01, 0x6B, 0x00, 0x01, 0x76};
  uint                         purged_count      = 0;
  string                       warning_text      = "";
  string                       error_text        = "";
  ulong                        now_us            = GetMicrosecondCount();

  ASSERT_TRUE(session_db.Init(session_id, true));

  ulong expired_store_id =
    session_db.StoreOfflineQueuedMessage(1, "offline/expired", expired_payload, 1, false, 30, props, false);
  ASSERT_TRUE(expired_store_id > 0);
  ASSERT_TRUE(session_db.FlushIfDirty(0));
  ASSERT_TRUE(
    queue.Append("offline/expired", expired_payload, 1, 1, false, props, now_us - 1ULL, false, expired_store_id));

  ENUM_MQTT_OFFLINE_QUEUE_RESULT result = coordinator.QueueWhileDisconnected(
    session_db, queue, sink, false, true, "offline/live", live_payload, (uint)ArraySize(live_payload), 1, true, 30,
    props, true, purged_count, warning_text, error_text);

  ASSERT_EQ((int)MQTT_OFFLINE_QUEUE_RESULT_QUEUED, (int)result);
  ASSERT_EQ(1, (int)purged_count);
  ASSERT_STR_EQ("", warning_text);
  ASSERT_STR_EQ("", error_text);
  ASSERT_EQ(1, (int)session_db.GetOfflineQueuedMessageCount());
  ASSERT_EQ(1, (int)queue.GetQueuedMessageCount());

  MqttQueuedPublishEntry entry;
  ASSERT_TRUE(queue.ReadEntry(queue.GetDrainHead(), entry));
  ASSERT_STR_EQ("offline/live", entry.topic);
  ASSERT_TRUE(entry.retain);
  ASSERT_TRUE(entry.allow_outgoing_sub_id);
  ASSERT_TRUE(entry.durable_store_id > 0);
  ASSERT_TRUE(AssertEqual(live_payload, entry.payload));
  ASSERT_TRUE(AssertEqual(props, entry.properties));

  session_db.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| _WritePublishQueueSuiteResult                                    |
//| Optional harness result-file writer used by the script and EA    |
//| wrappers that want a machine-readable summary outside the MT5    |
//| Journal stream.                                                  |
//+------------------------------------------------------------------+
void _WritePublishQueueSuiteResult(const string result_file_name, string &result_lines[]) {
  if (StringLen(result_file_name) == 0) {
    return;
  }

  int handle = FileOpen(result_file_name, FILE_WRITE | FILE_TXT | FILE_ANSI);
  if (handle == INVALID_HANDLE) {
    TestUtilRecordOperationError("_WritePublishQueueSuiteResult",
                                 "file=" + result_file_name + " error=" + (string)GetLastError());
    return;
  }

  for (int i = 0; i < ArraySize(result_lines); i++) {
    FileWriteString(handle, result_lines[i] + "\r\n");
  }
  FileClose(handle);
}

//+------------------------------------------------------------------+
//| RunPublishQueueTestSuite                                         |
//| Executes the ordered publish queue case list, records per-case   |
//| pass/fail lines, and optionally writes a result artifact for     |
//| wrappers such as TEST_PublishQueue.mq5 and PublishQueue harness. |
//+------------------------------------------------------------------+
bool RunPublishQueueTestSuite(const string result_file_name = "") {
  const string suite_name        = "TEST_PublishQueue";
  int          total_tests       = 0;
  int          passed_tests      = 0;
  int          passed_assertions = 0;
  int          failed_test_count = 0;
  string       result_lines[];
  string       test_names[] = {"TEST_PublishQueue_AppendAndReadBack",
                               "TEST_PublishQueue_AdmissionBudgets",
                               "TEST_PublishQueue_RollbackTail",
                               "TEST_PublishQueue_CompactAfterDrain",
                               "TEST_PublishQueue_PurgeExpiredReturnsStoreIds",
                               "TEST_PublishQueue_OldestAgeHonorsDrainHead",
                               "TEST_PublishQueue_BytesTrackDrainHead",
                               "TEST_PublishQueue_AdmissionRespectsDrainedBytes",
                               "TEST_PublishQueue_PurgeExpiredSkipsCompactionForDrainedPrefix",
                               "TEST_PublishQueueCoordinator_ClassifyDrainResult",
                               "TEST_PublishQueueCoordinator_RestoreDurableRoundTrip",
                               "TEST_PublishQueueCoordinator_RestorePreservesQueuedAge",
                               "TEST_PublishQueueCoordinator_DrainQueueFlow",
                               "TEST_PublishQueueCoordinator_DrainQueueUsesSlices",
                               "TEST_PublishQueueCoordinator_PurgeExpiredFlow",
                               "TEST_PublishQueueCoordinator_RetryStopsDrain",
                               "TEST_PQC_RetryAfterHandoffConsumes",
                               "TEST_PublishQueueCoordinator_QueueWhileDisconnectedPersistsTail"};
  const int    test_count   = ArraySize(test_names);

  TestUtilRecordSuiteStart(suite_name);

  //--- Keep dispatch explicit and ordered so the MT5 Journal and optional result file
  //--- stay stable across runs and match the curated case order declared above.
  for (int i = 0; i < test_count; i++) {
    g_tests_passed = 0;
    g_tests_failed = 0;
    TestUtilClearPendingFailure();
    bool result = false;

    if (test_names[i] == "TEST_PublishQueue_AppendAndReadBack") {
      result = TEST_PublishQueue_AppendAndReadBack();
    } else if (test_names[i] == "TEST_PublishQueue_AdmissionBudgets") {
      result = TEST_PublishQueue_AdmissionBudgets();
    } else if (test_names[i] == "TEST_PublishQueue_RollbackTail") {
      result = TEST_PublishQueue_RollbackTail();
    } else if (test_names[i] == "TEST_PublishQueue_CompactAfterDrain") {
      result = TEST_PublishQueue_CompactAfterDrain();
    } else if (test_names[i] == "TEST_PublishQueue_PurgeExpiredReturnsStoreIds") {
      result = TEST_PublishQueue_PurgeExpiredReturnsStoreIds();
    } else if (test_names[i] == "TEST_PublishQueue_OldestAgeHonorsDrainHead") {
      result = TEST_PublishQueue_OldestAgeHonorsDrainHead();
    } else if (test_names[i] == "TEST_PublishQueue_BytesTrackDrainHead") {
      result = TEST_PublishQueue_BytesTrackDrainHead();
    } else if (test_names[i] == "TEST_PublishQueue_AdmissionRespectsDrainedBytes") {
      result = TEST_PublishQueue_AdmissionRespectsDrainedBytes();
    } else if (test_names[i] == "TEST_PublishQueue_PurgeExpiredSkipsCompactionForDrainedPrefix") {
      result = TEST_PublishQueue_PurgeExpiredSkipsCompactionForDrainedPrefix();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_ClassifyDrainResult") {
      result = TEST_PublishQueueCoordinator_ClassifyDrainResult();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_RestoreDurableRoundTrip") {
      result = TEST_PublishQueueCoordinator_RestoreDurableRoundTrip();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_RestorePreservesQueuedAge") {
      result = TEST_PublishQueueCoordinator_RestorePreservesQueuedAge();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_DrainQueueFlow") {
      result = TEST_PublishQueueCoordinator_DrainQueueFlow();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_DrainQueueUsesSlices") {
      result = TEST_PublishQueueCoordinator_DrainQueueUsesSlices();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_PurgeExpiredFlow") {
      result = TEST_PublishQueueCoordinator_PurgeExpiredFlow();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_RetryStopsDrain") {
      result = TEST_PublishQueueCoordinator_RetryStopsDrain();
    } else if (test_names[i] == "TEST_PQC_RetryAfterHandoffConsumes") {
      result = TEST_PQC_RetryAfterHandoffConsumes();
    } else if (test_names[i] == "TEST_PublishQueueCoordinator_QueueWhileDisconnectedPersistsTail") {
      result = TEST_PublishQueueCoordinator_QueueWhileDisconnectedPersistsTail();
    }

    total_tests       += g_tests_passed + g_tests_failed;
    passed_assertions += g_tests_passed;
    if (result && g_tests_failed == 0) {
      passed_tests++;
      TestUtilRecordCasePass(test_names[i], g_tests_passed);
      string line            = "PASS: " + test_names[i] + " (" + (string)g_tests_passed + " assertions)";
      int    result_line_idx = ArraySize(result_lines);
      ArrayResize(result_lines, result_line_idx + 1);
      result_lines[result_line_idx] = line;
    } else {
      failed_test_count++;
      TestUtilRecordCaseFail(test_names[i], g_tests_failed);
      string line            = "FAIL: " + test_names[i] + " (" + (string)g_tests_failed + " failed assertions)";
      int    result_line_idx = ArraySize(result_lines);
      ArrayResize(result_lines, result_line_idx + 1);
      result_lines[result_line_idx] = line;
    }
  }

  TestUtilFinalizeSuite(suite_name, passed_tests, test_count, total_tests);

  string summary_line = "SUMMARY status=" + ((failed_test_count == 0) ? "PASS" : "FAIL")
                      + ", cases_passed=" + (string)passed_tests + ", cases_total=" + (string)test_count
                      + ", assertions_passed=" + (string)passed_assertions + ", assertions_total=" + (string)total_tests
                      + ", failed_test_count=" + (string)failed_test_count;
  int    summary_idx  = ArraySize(result_lines);
  ArrayResize(result_lines, summary_idx + 1);
  result_lines[summary_idx] = summary_line;
  _WritePublishQueueSuiteResult(result_file_name, result_lines);
  return failed_test_count == 0;
}

#endif
