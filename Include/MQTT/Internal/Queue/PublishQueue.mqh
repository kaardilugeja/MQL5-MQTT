//+------------------------------------------------------------------+
//|                                                 PublishQueue.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Compact offline publish queue for accepted outgoing publishes.   |
//| Stores payload and property bytes in flat shared buffers so      |
//| admission checks, drain iteration, and midpoint compaction stay  |
//| cheap even when the queue churns during reconnect loops.         |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_QUEUE_PUBLISHQUEUE_MQH
#define MQTT_INTERNAL_QUEUE_PUBLISHQUEUE_MQH

#ifndef MQTT_LOG_ERROR
#define MQTT_LOG_ERROR(msg)
#endif

enum ENUM_MQTT_QUEUE_ADMISSION {
  MQTT_QUEUE_ADMIT_OK = 0,
  MQTT_QUEUE_ADMIT_COUNT_LIMIT,
  MQTT_QUEUE_ADMIT_SINGLE_MESSAGE_LIMIT,
  MQTT_QUEUE_ADMIT_PAYLOAD_BYTES_LIMIT,
  MQTT_QUEUE_ADMIT_PROPERTY_BYTES_LIMIT
};

//+------------------------------------------------------------------+
//| IMqttPublishQueueDrainSink                                       |
//| Facade implemented by CMqttClient so the queue can stream        |
//| entries back into the normal publish path without owning any     |
//| transport, session, or logging state itself.                     |
//+------------------------------------------------------------------+
class IMqttPublishQueueDrainSink {
 public:
  virtual ~IMqttPublishQueueDrainSink() {}
  virtual uint RemainingExpirySecondsFromDeadlineUs(ulong expiry_deadline_us, ulong now_us) const = 0;
  virtual int  PublishQueuedEntry(const string topic, const uchar& payload_buffer[], uint payload_offset,
                                  uint payload_length, uchar qos, bool retain, const uchar& encoded_props_buffer[],
                                  uint prop_offset, uint prop_length, uint remaining_expiry,
                                  bool allow_outgoing_sub_id)                                     = 0;
  virtual bool LastQueuedPublishDurablyHandedOff() const                                          = 0;
  virtual void ReportQueueError(int code, const string description)                               = 0;
};

//+------------------------------------------------------------------+
//| MqttQueuedPublishEntry                                           |
//| Expanded copy of one queued publish used by tests and by helper  |
//| paths that need an owning snapshot instead of flat-buffer views. |
//+------------------------------------------------------------------+
struct MqttQueuedPublishEntry {
  string topic;                  // Original publish topic stored for replay.
  uchar  qos;                    // Requested outgoing QoS level.
  bool   retain;                 // Original retain bit that must survive offline replay.
  uchar  payload[];              // Owning payload copy returned by queue snapshot helpers.
  uchar  properties[];           // Encoded MQTT 5 PUBLISH properties as queued.
  ulong  expiry_time_us;         // Absolute GetMicrosecondCount deadline; 0 means no expiry.
  bool   allow_outgoing_sub_id;  // True when Subscription Identifier properties may be replayed outbound.
  ulong  durable_store_id;       // Session DB row ID backing this entry; 0 means memory-only.
};

//+------------------------------------------------------------------+
//| CMqttPublishQueue                                                |
//| Uses parallel metadata arrays plus flat payload/property buffers |
//| so queue admission reflects only still-pending entries while     |
//| compaction can stay amortized instead of happening on every pop. |
//+------------------------------------------------------------------+
class CMqttPublishQueue {
 private:
  string   m_topic[];                   // Topic per queued entry; index aligns with every metadata array below.
  uchar    m_qos[];                     // QoS per queued entry.
  bool     m_retain[];                  // Retain flag per queued entry.
  uint     m_payload_offset[];          // Byte offset into m_payload_buffer for each entry.
  uint     m_payload_length[];          // Payload byte length for each entry.
  uchar    m_payload_buffer[];          // Flat payload arena shared by all queued entries.
  uint     m_property_offset[];         // Byte offset into m_property_buffer for each entry.
  uint     m_property_length[];         // Actual encoded property length stored for each entry.
  uint     m_property_budget_length[];  // Property bytes charged against admission even if stored bytes are shorter.
  uchar    m_property_buffer[];         // Flat MQTT 5 property arena shared by all queued entries.
  ulong    m_enqueued_at_us[];          // Monotonic enqueue timestamps for age and expiry calculations.
  datetime m_enqueued_at_time[];        // Wall-clock enqueue timestamps kept for diagnostics and tests.
  ulong    m_expiry_time_us[];          // Absolute monotonic expiry deadlines; 0 means no expiry.
  bool     m_allow_outgoing_sub_id[];   // Whether queued Subscription Identifier properties may be replayed.
  ulong    m_durable_store_id[];        // Session DB row IDs so drain/purge can finalize durable rows.

  uint     m_count;                     // Total slots allocated, including drained prefix entries before compaction.
  uint     m_max_count;                 // Maximum live queued publishes allowed.
  uint     m_payload_bytes;             // Live payload budget from m_drain_head to m_count - 1.
  uint     m_property_bytes;            // Live property budget from m_drain_head to m_count - 1.
  uint     m_max_payload_bytes;         // Hard cap for live payload bytes; 0 means unlimited.
  uint     m_max_property_bytes;        // Hard cap for live property bytes; 0 means unlimited.
  uint     m_max_single_bytes;          // Hard cap for one queued publish's payload + properties; 0 means unlimited.
  uint     m_drain_head;                // First live entry; indices below this were drained and await compaction.

  bool _TryComputeArrayAppendSize(uint current_size, uint append_size, int& new_size, const string buffer_name) const {
    const uint max_array_size = 2147483647u;
    if (current_size > max_array_size || append_size > max_array_size || current_size > max_array_size - append_size) {
      MQTT_LOG_ERROR(buffer_name + " exceeds 32-bit array capacity.");
      return false;
    }

    new_size = (int)(current_size + append_size);
    return true;
  }

  void _ResizeEntryArrays(int new_size, int reserve = 0) {
    ArrayResize(m_topic, new_size, reserve);
    ArrayResize(m_qos, new_size, reserve);
    ArrayResize(m_retain, new_size, reserve);
    ArrayResize(m_payload_offset, new_size, reserve);
    ArrayResize(m_payload_length, new_size, reserve);
    ArrayResize(m_property_offset, new_size, reserve);
    ArrayResize(m_property_length, new_size, reserve);
    ArrayResize(m_property_budget_length, new_size, reserve);
    ArrayResize(m_enqueued_at_us, new_size, reserve);
    ArrayResize(m_enqueued_at_time, new_size, reserve);
    ArrayResize(m_expiry_time_us, new_size, reserve);
    ArrayResize(m_allow_outgoing_sub_id, new_size, reserve);
    ArrayResize(m_durable_store_id, new_size, reserve);
  }

 public:
  CMqttPublishQueue()
      : m_count(0)
      , m_max_count(500)
      , m_payload_bytes(0)
      , m_property_bytes(0)
      , m_max_payload_bytes(0)
      , m_max_property_bytes(0)
      , m_max_single_bytes(0)
      , m_drain_head(0) {}

  void SetMaxMessages(uint count) { m_max_count = count; }
  uint GetMaxMessages() const { return m_max_count; }

  void SetMaxPayloadBytes(uint bytes) { m_max_payload_bytes = bytes; }
  void SetMaxPropertyBytes(uint bytes) { m_max_property_bytes = bytes; }
  void SetMaxSingleBytes(uint bytes) { m_max_single_bytes = bytes; }

  uint GetTotalCount() const { return m_count; }
  uint GetDrainHead() const { return m_drain_head; }
  uint GetQueuedMessageCount() const { return (m_drain_head <= m_count) ? (m_count - m_drain_head) : 0; }
  bool HasPendingDrain() const { return m_drain_head < m_count; }
  bool IsCompletelyEmpty() const { return m_count == 0 && m_drain_head == 0; }

  uint GetPayloadBytes() const { return m_payload_bytes; }
  uint GetPropertyBytes() const { return m_property_bytes; }

  ENUM_MQTT_QUEUE_ADMISSION EvaluateAdmission(uint payload_len, uint prop_len) const {
    if (GetQueuedMessageCount() >= m_max_count) {
      return MQTT_QUEUE_ADMIT_COUNT_LIMIT;
    }
    if (m_max_single_bytes > 0 && ((ulong)payload_len + (ulong)prop_len) > (ulong)m_max_single_bytes) {
      return MQTT_QUEUE_ADMIT_SINGLE_MESSAGE_LIMIT;
    }
    if (m_max_payload_bytes > 0 && ((ulong)m_payload_bytes + (ulong)payload_len) > (ulong)m_max_payload_bytes) {
      return MQTT_QUEUE_ADMIT_PAYLOAD_BYTES_LIMIT;
    }
    if (m_max_property_bytes > 0 && ((ulong)m_property_bytes + (ulong)prop_len) > (ulong)m_max_property_bytes) {
      return MQTT_QUEUE_ADMIT_PROPERTY_BYTES_LIMIT;
    }
    return MQTT_QUEUE_ADMIT_OK;
  }

  bool ExceedsSingleMessageBudget(uint payload_len, uint prop_len) const {
    return m_max_single_bytes > 0 && ((ulong)payload_len + (ulong)prop_len) > (ulong)m_max_single_bytes;
  }

  bool ExceedsPayloadBudget(uint payload_len) const {
    return m_max_payload_bytes > 0 && ((ulong)m_payload_bytes + (ulong)payload_len) > (ulong)m_max_payload_bytes;
  }

  bool ExceedsPropertyBudget(uint prop_len) const {
    return m_max_property_bytes > 0 && ((ulong)m_property_bytes + (ulong)prop_len) > (ulong)m_max_property_bytes;
  }

  bool NeedsMidpointCompaction() const { return m_drain_head > 0 && m_drain_head * 2 >= m_count; }

  bool ShouldCompactAfterDrain() const {
    return m_drain_head > 0 && (m_count == m_drain_head || m_drain_head * 2 >= m_count);
  }

  //--- Append stores metadata in parallel arrays and appends bytes into flat shared
  //--- buffers. That layout keeps queued entries stable while avoiding one dynamic
  //--- payload/property array allocation per message.
  bool Append(const string topic, const uchar& payload[], uint payload_len, uchar qos, bool retain,
              const uchar& persisted_props[], ulong expiry_deadline_us, bool allow_outgoing_sub_id,
              ulong durable_store_id) {
    return Append(topic, payload, payload_len, qos, retain, persisted_props, expiry_deadline_us, allow_outgoing_sub_id,
                  durable_store_id, (uint)ArraySize(persisted_props));
  }

  bool Append(const string topic, const uchar& payload[], uint payload_len, uchar qos, bool retain,
              const uchar& persisted_props[], ulong expiry_deadline_us, bool allow_outgoing_sub_id,
              ulong durable_store_id, uint property_budget_len) {
    uint flat_off             = (uint)ArraySize(m_payload_buffer);
    uint prop_off             = (uint)ArraySize(m_property_buffer);
    uint prop_len             = (uint)ArraySize(persisted_props);
    int  new_payload_buf_size = (int)flat_off;
    int  new_prop_buf_size    = (int)prop_off;

    if (property_budget_len < prop_len) {
      property_budget_len = prop_len;
    }

    if (payload_len > 0
        && !_TryComputeArrayAppendSize(flat_off, payload_len, new_payload_buf_size, "Queued publish payload buffer")) {
      return false;
    }
    if (prop_len > 0
        && !_TryComputeArrayAppendSize(prop_off, prop_len, new_prop_buf_size, "Queued publish property buffer")) {
      return false;
    }

    _ResizeEntryArrays((int)(m_count + 1), 16);
    m_topic[m_count]                  = topic;
    m_qos[m_count]                    = qos;
    m_retain[m_count]                 = retain;
    m_payload_offset[m_count]         = flat_off;
    m_payload_length[m_count]         = payload_len;
    m_property_offset[m_count]        = prop_off;
    m_property_length[m_count]        = prop_len;
    m_property_budget_length[m_count] = property_budget_len;
    m_enqueued_at_us[m_count]         = GetMicrosecondCount();
    m_enqueued_at_time[m_count]       = TimeLocal();
    m_expiry_time_us[m_count]         = expiry_deadline_us;
    m_allow_outgoing_sub_id[m_count]  = allow_outgoing_sub_id;
    m_durable_store_id[m_count]       = durable_store_id;

    if (payload_len > 0) {
      ArrayResize(m_payload_buffer, new_payload_buf_size);
      ArrayCopy(m_payload_buffer, payload, (int)flat_off, 0, (int)payload_len);
    }
    if (prop_len > 0) {
      ArrayResize(m_property_buffer, new_prop_buf_size);
      ArrayCopy(m_property_buffer, persisted_props, (int)prop_off, 0, (int)prop_len);
    }

    //--- Increment live-only byte counters so EvaluateAdmission sees only the queued
    //--- (not-yet-compacted drained) bytes.  AdvanceDrainHead() decrements them when each
    //--- entry is consumed, keeping the counters accurate without relying on compaction.
    m_payload_bytes  += payload_len;
    m_property_bytes += property_budget_len;
    m_count++;
    return true;
  }

  //--- Roll back only the newest entry after a durable write-through failure. The
  //--- property budget may be larger than the encoded property bytes because queue
  //--- accounting reserves room for a synthesized expiry property during replay.
  void RollbackTail(uint payload_len, uint prop_len) {
    if (m_count == 0) {
      return;
    }

    uint tail_idx          = m_count - 1;
    uint tail_payload_len  = (tail_idx < (uint)ArraySize(m_payload_length)) ? m_payload_length[tail_idx] : payload_len;
    uint tail_property_len = (tail_idx < (uint)ArraySize(m_property_length)) ? m_property_length[tail_idx] : prop_len;
    uint tail_property_budget_len =
      (tail_idx < (uint)ArraySize(m_property_budget_length)) ? m_property_budget_length[tail_idx] : prop_len;

    m_count--;
    _ResizeEntryArrays((int)m_count);

    if (tail_payload_len > 0) {
      int new_payload_size = ArraySize(m_payload_buffer) - (int)tail_payload_len;
      if (new_payload_size < 0) {
        new_payload_size = 0;
      }
      ArrayResize(m_payload_buffer, new_payload_size);
      //--- Decrement live-only counter; guard against underflow on corrupt state.
      m_payload_bytes = (m_payload_bytes >= tail_payload_len) ? (m_payload_bytes - tail_payload_len) : 0;
    }
    if (tail_property_len > 0) {
      int new_prop_size = ArraySize(m_property_buffer) - (int)tail_property_len;
      if (new_prop_size < 0) {
        new_prop_size = 0;
      }
      ArrayResize(m_property_buffer, new_prop_size);
      m_property_bytes =
        (m_property_bytes >= tail_property_budget_len) ? (m_property_bytes - tail_property_budget_len) : 0;
    }
    if (m_drain_head > m_count) {
      m_drain_head = m_count;
    }
  }

  string GetTopic(uint idx) const { return (idx < m_count) ? m_topic[idx] : ""; }
  uchar  GetQoS(uint idx) const { return (idx < m_count) ? m_qos[idx] : 0; }
  bool   GetRetain(uint idx) const { return (idx < m_count) ? m_retain[idx] : false; }
  ulong  GetExpiryTimeUs(uint idx) const { return (idx < m_count) ? m_expiry_time_us[idx] : 0; }
  bool   GetAllowOutgoingSubId(uint idx) const { return (idx < m_count) ? m_allow_outgoing_sub_id[idx] : false; }
  ulong  GetStoreId(uint idx) const { return (idx < m_count) ? m_durable_store_id[idx] : 0; }

  void   SetStoreId(uint idx, ulong durable_store_id) {
    if (idx < m_count) {
      m_durable_store_id[idx] = durable_store_id;
    }
  }

  void SetEnqueuedAtUs(uint idx, ulong enqueue_time_us) {
    if (idx < m_count) {
      m_enqueued_at_us[idx] = enqueue_time_us;
    }
  }

  void SetEnqueuedAtTime(uint idx, datetime enqueue_time) {
    if (idx < m_count) {
      m_enqueued_at_time[idx] = enqueue_time;
    }
  }

  bool CopyPayload(uint idx, uchar& dest[]) const {
    ArrayResize(dest, 0);
    if (idx >= m_count) {
      return false;
    }

    uint payload_len = m_payload_length[idx];
    if (payload_len == 0) {
      return true;
    }

    ArrayResize(dest, (int)payload_len);
    ArrayCopy(dest, m_payload_buffer, 0, (int)m_payload_offset[idx], (int)payload_len);
    return true;
  }

  bool CopyProperties(uint idx, uchar& dest[]) const {
    ArrayResize(dest, 0);
    if (idx >= m_count) {
      return false;
    }

    uint prop_len = m_property_length[idx];
    if (prop_len == 0) {
      return true;
    }

    ArrayResize(dest, (int)prop_len);
    ArrayCopy(dest, m_property_buffer, 0, (int)m_property_offset[idx], (int)prop_len);
    return true;
  }

  bool ReadEntry(uint idx, MqttQueuedPublishEntry& entry) const {
    if (idx >= m_count) {
      ArrayResize(entry.payload, 0);
      ArrayResize(entry.properties, 0);
      return false;
    }

    entry.topic                 = m_topic[idx];
    entry.qos                   = m_qos[idx];
    entry.retain                = m_retain[idx];
    entry.expiry_time_us        = m_expiry_time_us[idx];
    entry.allow_outgoing_sub_id = m_allow_outgoing_sub_id[idx];
    entry.durable_store_id      = m_durable_store_id[idx];

    return CopyPayload(idx, entry.payload) && CopyProperties(idx, entry.properties);
  }

  bool ReadDrainEntry(MqttQueuedPublishEntry& entry) const { return ReadEntry(m_drain_head, entry); }

  bool DrainEntryToSink(uint idx, IMqttPublishQueueDrainSink& sink, uint remaining_expiry, int& publish_error) const {
    if (idx >= m_count) {
      return false;
    }

    publish_error =
      sink.PublishQueuedEntry(m_topic[idx], m_payload_buffer, m_payload_offset[idx], m_payload_length[idx], m_qos[idx],
                              m_retain[idx], m_property_buffer, m_property_offset[idx], m_property_length[idx],
                              remaining_expiry, m_allow_outgoing_sub_id[idx]);
    return true;
  }

  bool IsEntryExpired(uint idx, ulong now_us) const {
    return idx < m_count && m_expiry_time_us[idx] > 0 && now_us >= m_expiry_time_us[idx];
  }

  void AdvanceDrainHead() {
    if (m_drain_head < m_count) {
      //--- Deduct the drained entry from the live-only byte counters so that
      //--- EvaluateAdmission remains accurate without requiring a full compaction.
      m_payload_bytes =
        (m_payload_bytes >= m_payload_length[m_drain_head]) ? (m_payload_bytes - m_payload_length[m_drain_head]) : 0;
      m_property_bytes = (m_property_bytes >= m_property_budget_length[m_drain_head]) ?
                           (m_property_bytes - m_property_budget_length[m_drain_head]) :
                           0;
      m_drain_head++;
    }
  }

  uint PurgeExpired(ulong now_us, ulong& removed_store_ids[]) {
    ArrayResize(removed_store_ids, 0);
    if (m_count == 0) {
      return 0;
    }

    //--- Only the not-yet-drained suffix participates here. Anything below
    //--- m_drain_head has already been accounted out of the live byte budgets.
    uint start              = (m_drain_head <= m_count) ? m_drain_head : 0;
    uint remain             = 0;
    uint dropped            = 0;
    uint new_payload_size   = 0;
    uint new_property_size  = 0;
    uint new_property_bytes = 0;

    for (uint i = start; i < m_count; i++) {
      if (m_expiry_time_us[i] > 0 && now_us >= m_expiry_time_us[i]) {
        dropped++;
        continue;
      }
      remain++;
      new_payload_size   += m_payload_length[i];
      new_property_size  += m_property_length[i];
      new_property_bytes += m_property_budget_length[i];
    }

    //--- When nothing expired there is nothing to do: byte counters are already
    //--- kept accurate by AdvanceDrainHead(), so the drained prefix does not
    //--- inflate admission checks.  Skipping the O(n) copy here turns the
    //--- mid-drain-disconnect + offline-queue scenario from O(n²) to O(n).
    if (dropped == 0) {
      return 0;
    }

    uchar new_payload_buffer[];
    uchar new_property_buffer[];
    ArrayResize(new_payload_buffer, (int)new_payload_size);
    ArrayResize(new_property_buffer, (int)new_property_size);
    ArrayResize(removed_store_ids, (int)dropped);

    uint write_idx           = 0;
    uint payload_off         = 0;
    uint property_off        = 0;
    uint removed_store_count = 0;

    //--- Rewrite the surviving suffix densely into fresh flat buffers so the queue
    //--- shape after expiry purge matches the normal post-compaction layout.
    for (uint i = start; i < m_count; i++) {
      if (m_expiry_time_us[i] > 0 && now_us >= m_expiry_time_us[i]) {
        if (m_durable_store_id[i] > 0) {
          removed_store_ids[removed_store_count++] = m_durable_store_id[i];
        }
        continue;
      }

      m_topic[write_idx]                  = m_topic[i];
      m_qos[write_idx]                    = m_qos[i];
      m_retain[write_idx]                 = m_retain[i];
      m_payload_offset[write_idx]         = payload_off;
      m_payload_length[write_idx]         = m_payload_length[i];
      m_property_offset[write_idx]        = property_off;
      m_property_length[write_idx]        = m_property_length[i];
      m_property_budget_length[write_idx] = m_property_budget_length[i];
      m_enqueued_at_us[write_idx]         = m_enqueued_at_us[i];
      m_enqueued_at_time[write_idx]       = m_enqueued_at_time[i];
      m_expiry_time_us[write_idx]         = m_expiry_time_us[i];
      m_allow_outgoing_sub_id[write_idx]  = m_allow_outgoing_sub_id[i];
      m_durable_store_id[write_idx]       = m_durable_store_id[i];

      if (m_payload_length[i] > 0) {
        ArrayCopy(new_payload_buffer, m_payload_buffer, (int)payload_off, (int)m_payload_offset[i],
                  (int)m_payload_length[i]);
        payload_off += m_payload_length[i];
      }
      if (m_property_length[i] > 0) {
        ArrayCopy(new_property_buffer, m_property_buffer, (int)property_off, (int)m_property_offset[i],
                  (int)m_property_length[i]);
        property_off += m_property_length[i];
      }
      write_idx++;
    }

    ArrayCopy(m_payload_buffer, new_payload_buffer, 0, 0, (int)new_payload_size);
    ArrayResize(m_payload_buffer, (int)new_payload_size);
    ArrayCopy(m_property_buffer, new_property_buffer, 0, 0, (int)new_property_size);
    ArrayResize(m_property_buffer, (int)new_property_size);
    m_payload_bytes  = new_payload_size;
    m_property_bytes = new_property_bytes;
    m_count          = remain;
    m_drain_head     = 0;
    _ResizeEntryArrays((int)remain);
    ArrayResize(removed_store_ids, (int)removed_store_count);
    return dropped;
  }

  //--- Compact discards the drained prefix in one pass and rewrites the remaining
  //--- suffix to the front of the metadata arrays and flat buffers.
  void Compact() {
    uint remain = GetQueuedMessageCount();
    if (remain > 0) {
      uchar new_payload_buffer[];
      uchar new_property_buffer[];
      uint  new_payload_size   = 0;
      uint  new_property_size  = 0;
      uint  new_property_bytes = 0;

      for (uint qi = 0; qi < remain; qi++) {
        new_payload_size   += m_payload_length[m_drain_head + qi];
        new_property_size  += m_property_length[m_drain_head + qi];
        new_property_bytes += m_property_budget_length[m_drain_head + qi];
      }

      ArrayResize(new_payload_buffer, (int)new_payload_size);
      ArrayResize(new_property_buffer, (int)new_property_size);
      uint payload_off  = 0;
      uint property_off = 0;
      for (uint qi = 0; qi < remain; qi++) {
        uint src_idx                 = m_drain_head + qi;
        m_topic[qi]                  = m_topic[src_idx];
        m_qos[qi]                    = m_qos[src_idx];
        m_retain[qi]                 = m_retain[src_idx];
        m_payload_length[qi]         = m_payload_length[src_idx];
        m_payload_offset[qi]         = payload_off;
        m_property_length[qi]        = m_property_length[src_idx];
        m_property_offset[qi]        = property_off;
        m_property_budget_length[qi] = m_property_budget_length[src_idx];
        m_enqueued_at_us[qi]         = m_enqueued_at_us[src_idx];
        m_enqueued_at_time[qi]       = m_enqueued_at_time[src_idx];
        m_expiry_time_us[qi]         = m_expiry_time_us[src_idx];
        m_allow_outgoing_sub_id[qi]  = m_allow_outgoing_sub_id[src_idx];
        m_durable_store_id[qi]       = m_durable_store_id[src_idx];

        if (m_payload_length[qi] > 0) {
          ArrayCopy(new_payload_buffer, m_payload_buffer, (int)payload_off, (int)m_payload_offset[src_idx],
                    (int)m_payload_length[qi]);
          payload_off += m_payload_length[qi];
        }
        if (m_property_length[qi] > 0) {
          ArrayCopy(new_property_buffer, m_property_buffer, (int)property_off, (int)m_property_offset[src_idx],
                    (int)m_property_length[qi]);
          property_off += m_property_length[qi];
        }
      }

      ArrayCopy(m_payload_buffer, new_payload_buffer, 0, 0, (int)new_payload_size);
      ArrayResize(m_payload_buffer, (int)new_payload_size);
      ArrayCopy(m_property_buffer, new_property_buffer, 0, 0, (int)new_property_size);
      ArrayResize(m_property_buffer, (int)new_property_size);
      m_payload_bytes  = new_payload_size;
      m_property_bytes = new_property_bytes;
    } else {
      ArrayResize(m_payload_buffer, 0);
      ArrayResize(m_property_buffer, 0);
      m_payload_bytes  = 0;
      m_property_bytes = 0;
    }

    m_count      = remain;
    m_drain_head = 0;
    _ResizeEntryArrays((int)m_count);
  }

  ulong GetOldestQueuedMessageAgeMs(ulong now_us, datetime now_time) const {
    if (m_count == 0 || m_drain_head >= m_count) {
      return 0;
    }

    ulong oldest_age_ms = 0;
    for (uint i = m_drain_head; i < m_count; i++) {
      ulong age_ms          = 0;
      ulong enqueue_time_us = m_enqueued_at_us[i];
      if (enqueue_time_us > 0 && now_us > enqueue_time_us) {
        age_ms = (now_us - enqueue_time_us) / 1000ULL;
      } else if (m_enqueued_at_time[i] > 0 && now_time >= m_enqueued_at_time[i]) {
        age_ms = (ulong)(now_time - m_enqueued_at_time[i]) * 1000ULL;
      }
      if (age_ms > oldest_age_ms) {
        oldest_age_ms = age_ms;
      }
    }

    return oldest_age_ms;
  }
};

#endif
