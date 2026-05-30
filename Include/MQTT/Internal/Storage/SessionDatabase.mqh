//+------------------------------------------------------------------+
//|                                              SessionDatabase.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Session database for MQTT 5.0 session state management per §4.1. |
//| Tracks in-flight messages, QoS 2 states, and packet IDs.         |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_STORAGE_SESSIONDATABASE_MQH
#define MQTT_INTERNAL_STORAGE_SESSIONDATABASE_MQH

//--- Fallback logging keeps SessionDatabase usable when it is included through
//--- MQTT.mqh before CMqttClient installs the chart-scoped MQTT_LOG_* macros.
#ifndef MQTT_LOG_ERROR
#define MQTT_LOG_ERROR(msg)    \
  do {                         \
    Print("[MQTT][ERROR] ", (msg)); \
  } while (0)
#endif
#ifndef MQTT_LOG_WARN
#define MQTT_LOG_WARN(msg)   \
  do {                       \
    Print("[MQTT][WARN] ", (msg)); \
  } while (0)
#endif
#ifndef MQTT_LOG_INFO
#define MQTT_LOG_INFO(msg)   \
  do {                       \
    Print("[MQTT][INFO] ", (msg)); \
  } while (0)
#endif
#ifndef MQTT_LOG_DEBUG
#define MQTT_LOG_DEBUG(msg)   \
  do {                        \
    Print("[MQTT][DEBUG] ", (msg)); \
  } while (0)
#endif

//--- Binary session file magic: 'M','Q','T','T' (0x4D515454)
//--- Any file whose first 4 bytes do not match is rejected as corrupt/incompatible.
#define MQTT_SESSION_FILE_MAGIC          0x4D515454
//--- Session file format version — increment whenever the layout changes.
//--- Extend LoadFromFile with a staged reader before bumping so older
//--- durable sessions are migrated forward instead of discarded.
#define MQTT_SESSION_FILE_VERSION        7
#define MQTT_SESSION_FILE_FLAG_ENCRYPTED 0x01
#define MQTT_SESSION_ENVELOPE_MAGIC      0x53444245
#define MQTT_SESSION_MAX_BODY_BYTES      16777216

#include <Generic\HashMap.mqh>

//+------------------------------------------------------------------+
//| QoS 2 State Machine States per spec §4.3.3                       |
//+------------------------------------------------------------------+
enum ENUM_QOS2_STATE {
  QOS2_STATE_NONE = 0,          // Not a QoS 2 message
  QOS2_STATE_PUBLISH_SENT,      // Outgoing step 1: PUBLISH sent, waiting for PUBREC
  QOS2_STATE_PUBREC_RECEIVED,   // Outgoing step 2: PUBREC received, PUBREL sent, waiting for PUBCOMP
  QOS2_STATE_PUBCOMP_RECEIVED,  // Outgoing step 3: Complete
  QOS2_STATE_PUBLISH_RECEIVED   // Incoming: PUBLISH received, PUBREC sent, waiting for PUBREL
};

//+------------------------------------------------------------------+
//| Struct SessionMessage                                            |
//| Purpose: Stores message state for session persistence            |
//+------------------------------------------------------------------+
struct SessionMessage {
  ushort          packet_id;
  uchar           qos_level;
  ENUM_QOS2_STATE qos2_state;
  string          topic;
  uchar           payload[];
  datetime        timestamp;
  ulong           mono_timestamp_us;             // Monotonic timestamp (GetMicrosecondCount) for retransmission
  uint            payload_size;                  // Cached payload length used by persistence and retransmit budgeting.
  bool            is_outgoing;                   // true = client→broker state, false = broker→client QoS state.
  uint            retransmit_count;              // Number of retransmit attempts already consumed for this message.
  datetime        expiry_time;                   // Set to > 0 if there's an expiry limit
  uchar           priority;                      // 0 is lowest priority
  bool            retain;                        // Original retain flag from PUBLISH per §3.3.1.3
  bool  allow_outgoing_subscription_identifier;  // True when a stored Subscription Identifier may be replayed.
  uchar publish_properties[];                    // Encoded MQTT 5 PUBLISH properties persisted with the message.
};

//+------------------------------------------------------------------+
//| Struct OfflineQueuedMessage                                      |
//| Purpose: Stores accepted offline QoS publishes until they are    |
//|          promoted into the in-flight outgoing QoS path.          |
//+------------------------------------------------------------------+
struct OfflineQueuedMessage {
  ulong    queued_id;
  uchar    qos_level;
  string   topic;
  uchar    payload[];
  uint     payload_size;
  bool     retain;
  datetime timestamp;
  datetime expiry_time;
  ulong    mono_timestamp_us;
  uint     remaining_expiry_seconds;                // Expiry budget captured at queue time for reconnect replay.
  bool     allow_outgoing_subscription_identifier;  // True when stored Subscription Identifier props may be replayed.
  uchar    publish_properties[];                    // Encoded MQTT 5 PUBLISH properties preserved for offline replay.
};

//+------------------------------------------------------------------+
//| Class CSessionDatabase                                           |
//| Purpose: Manages MQTT 5.0 Session State per spec §4.1            |
//| Usage:   Tracks in-flight messages, QoS 2 states, packet IDs     |
//|          Supports session persistence across reconnects          |
//+------------------------------------------------------------------+
class CSessionDatabase {
 private:
  //--- Session identification
  string               m_session_id;     // File-system/session key shared across reconnects.
  bool                 m_is_persistent;  // true keeps state on disk between terminal restarts.

  //--- Packet ID management
  ushort               m_next_packet_id;          // Next candidate for round-robin packet ID allocation.
  uint                 m_id_bitfield[];           // Bitfield for O(1) in-use lookup
  uint                 m_in_use_packet_id_count;  // Count of currently allocated packet IDs

  //--- Bitfield helper
  bool                 _IdBitTest(ushort id) const { return (m_id_bitfield[id >> 5] & ((uint)1 << (id & 0x1F))) != 0; }
  void                 _IdBitSet(ushort id) { m_id_bitfield[id >> 5] |= ((uint)1 << (id & 0x1F)); }
  void                 _IdBitClear(ushort id) { m_id_bitfield[id >> 5] &= ~((uint)1 << (id & 0x1F)); }

  //--- Message storage
  SessionMessage       m_messages[];
  uint                 m_message_count;  // Number of populated SessionMessage slots.
  CHashMap<uint, int>  m_id_index;       // (direction, packet_id) → m_messages[] index for O(1) lookups

  //--- Offline accepted QoS publish storage (durable before packet ID allocation)
  OfflineQueuedMessage m_offline_messages[];
  uint                 m_offline_message_count;    // Number of visible offline queue rows currently stored.
  ulong                m_next_offline_message_id;  // Monotonic durable row ID so restarts never reuse queue IDs.

  //--- Offline queued publishes that have already been consumed (sent, promoted,
  //--- or intentionally dropped) but whose durable offline row could not be
  //--- removed immediately. These IDs are persisted so restart restore skips
  //--- them instead of creating ghost replays.
  ulong                m_consumed_offline_message_ids[];  // Tombstones for durable rows awaiting eventual cleanup.
  uint                 m_consumed_offline_message_count;  // Number of active tombstones in the array above.

  //--- Statistics
  uint                 m_total_allocated_ids;
  uint                 m_total_released_ids;
  uint                 m_total_messages_stored;
  uint                 m_total_messages_removed;

  //--- Deferred persistence
  bool                 m_dirty;            // True when in-memory state differs from disk
  datetime             m_last_flush_time;  // TimeLocal() of last successful SaveToFile()

  //--- Cached QoS counts (O(1) instead of O(n) scan)
  uint                 m_qos1_count;
  uint                 m_qos2_count;

  //--- Persisted circuit-breaker state
  //--- Counts consecutive reconnect failures across EA restarts.
  //--- Saved in the session file header so broker-outage IP-ban protection
  //--- survives a terminal restart that occurs during the outage.
  uint                 m_reconnect_failure_count;
  uint   m_incoming_storage_error_count;  // Consecutive incoming QoS2 persistence failures restored on load.
  uchar  m_session_encryption_key[];      // Single-pass SHA-256 passphrase hash used as the AES-256 key; empty = plaintext.
  bool   m_test_force_finalize_offline_fallback_once;  // Test hook that forces the finalize-fallback path once.

  //--- Private helper methods
  uint   MakeMessageIndexKey(const ushort packet_id, const bool is_outgoing) const;
  int    FindMessageByPacketId(const ushort packet_id);
  int    FindMessageByPacketId(const ushort packet_id, const bool is_outgoing);
  int    FindOfflineQueuedMessageById(const ulong queued_id) const;
  int    FindConsumedOfflineQueuedMessageById(const ulong queued_id) const;
  void   RebuildMessageIndex();
  ushort FindAvailablePacketId();
  bool   IsValidPacketId(const ushort packet_id) const;
  void   _PurgeExpiredMessages();  // Shared expiry cleanup
  void   _PurgeExpiredOfflineQueuedMessages();
  bool   _PurgeConsumedOfflineQueuedMessages();
  uint   _GetVisibleOfflineQueuedMessageCount() const;
  bool   _AppendBufferByte(uchar& dest[], uchar value) const;
  bool   _AppendBufferUInt16(uchar& dest[], ushort value) const;
  bool   _AppendBufferUInt32(uchar& dest[], uint value) const;
  bool   _AppendBufferUInt64(uchar& dest[], ulong value) const;
  bool   _AppendBufferBytes(uchar& dest[], const uchar& src[], uint count) const;
  bool   _AppendBufferUtf8String(uchar& dest[], const string value) const;
  bool   _ReadBufferByte(const uchar& src[], int& offset, uchar& value) const;
  bool   _ReadBufferUInt16(const uchar& src[], int& offset, ushort& value) const;
  bool   _ReadBufferUInt32(const uchar& src[], int& offset, uint& value) const;
  bool   _ReadBufferUInt64(const uchar& src[], int& offset, ulong& value) const;
  bool   _ReadBufferBytes(const uchar& src[], int& offset, uint count, uchar& dest[]) const;
  bool   _ReadBufferUtf8String(const uchar& src[], int& offset, uint max_len, string& value) const;
  bool   _ReadFileBuffer(int handle, uint bytes_to_read, uchar& dest[]) const;
  uint   _GetOfflineRemainingExpirySeconds(const OfflineQueuedMessage& msg, datetime now_time = 0,
                                           ulong now_us = 0) const;
  bool   _IsOfflineQueuedMessageExpired(const OfflineQueuedMessage& msg, datetime now_time = 0, ulong now_us = 0) const;
  bool   _SerializeStateV7(uchar& body[]) const;
  bool   _DeserializeStateV7(const uchar& body[], int file_version);
  bool   _ConstantTimeEqual(const uchar& lhs[], const uchar& rhs[], uint count) const;
  bool   _EncryptSerializedState(const uchar& plain_body[], uchar& stored_body[]) const;
  bool   _DecryptSerializedState(const uchar& stored_body[], uchar& plain_body[]) const;
  bool   _WriteThroughMutation();

 public:
  //--- Constructor/Destructor
  CSessionDatabase();
  ~CSessionDatabase();

  //--- Session management
  bool   Init(const string session_id, const bool persistent = false);
  void   ResetSession();
  string GetSessionId() const;
  bool   IsPersistent() const;
  bool   SaveToFile();
  bool   LoadFromFile();
  bool   FlushIfDirty(uint interval_seconds = 5);  // Deferred flush: write only if dirty and interval elapsed
  bool   IsDirty() const { return m_dirty; }
  void   SetPersistence(bool persistent);
  void   SetEncryptionPassphrase(const string passphrase);
  bool   IsEncryptionEnabled() const { return ArraySize(m_session_encryption_key) > 0; }

  //--- Packet ID Management
  ushort AllocatePacketId();
  bool   ReleasePacketId(const ushort packet_id);
  bool   IsPacketIdInUse(const ushort packet_id) const;
  uint   GetAvailablePacketIdCount() const;
  uint   GetInUsePacketIdCount() const;
  void   ResetPacketIds();

  //--- Message Storage
  bool   StoreOutgoingMessage(const ushort packet_id, const uchar qos_level, const string topic, const uchar& payload[],
                              const uint payload_size, const uchar priority = 0, const uint expiry_interval = 0);
  bool   StoreOutgoingMessage(const ushort packet_id, const uchar qos_level, const string topic, const uchar& payload[],
                              const uint payload_size, const bool retain, const uchar priority,
                              const uint expiry_interval, const uchar& publish_properties[],
                              const bool allow_outgoing_subscription_identifier = false);
  bool   StoreOutgoingMessageRange(const ushort packet_id, const uchar qos_level, const string topic,
                                   const uchar& payload[], const uint payload_offset, const uint payload_size,
                                   const bool retain, const uchar priority, const uint expiry_interval,
                                   const uchar& publish_properties[],
                                   const bool   allow_outgoing_subscription_identifier = false);
  bool   StoreIncomingMessage(const ushort packet_id, const uchar qos_level, const string topic, const uchar& payload[],
                              const uint payload_size, const bool retain = false);
  bool   StoreIncomingMessage(const ushort packet_id, const uchar qos_level, const string topic, const uchar& payload[],
                              const uint payload_size, const bool retain, const uchar& publish_properties[]);
  ulong  StoreOfflineQueuedMessage(const uchar qos_level, const string topic, const uchar& payload[],
                                   const uint payload_size, const bool retain = false, const uint expiry_interval = 0);
  ulong  StoreOfflineQueuedMessage(const uchar qos_level, const string topic, const uchar& payload[],
                                   const uint payload_size, const bool retain, const uint expiry_interval,
                                   const uchar& publish_properties[],
                                   const bool   allow_outgoing_subscription_identifier = false);
  bool   UpdateQoS2State(const ushort packet_id, const ENUM_QOS2_STATE new_state);
  bool   TouchMessage(const ushort packet_id);
  bool   RemoveMessage(const ushort packet_id);
  bool   RemoveMessage(const ushort packet_id, const bool is_outgoing);
  bool   RemoveOfflineQueuedMessage(const ulong queued_id);
  bool   FinalizeOfflineQueuedMessage(const ulong queued_id);
  bool   GetMessage(const ushort packet_id, SessionMessage& out_msg);
  bool   GetMessage(const ushort packet_id, SessionMessage& out_msg, const bool is_outgoing);

  //--- Bulk operations
  uint   GetPendingMessages(SessionMessage& dest[], const bool outgoing_only = true);
  uint   GetIncomingMessages(SessionMessage& dest[]);  // Returns all non-outgoing messages (for flow-control rebuild)
  uint   GetOfflineQueuedMessages(OfflineQueuedMessage& dest[]);
  uint   GetQoS1Messages(SessionMessage& dest[]);
  uint   GetQoS2Messages(SessionMessage& dest[]);
  uint   GetMessagesByQoS2State(SessionMessage& dest[], const ENUM_QOS2_STATE state);
  uint   GetStalledMessages(SessionMessage& dest[], const uint timeout_seconds);

  //--- State queries
  bool   HasPendingMessages() const;
  bool   HasOfflineQueuedMessages() const;
  uint   GetPendingMessageCount() const;
  uint   GetOfflineQueuedMessageCount() const;
  uint   GetPendingQoS1Count() const;
  uint   GetPendingQoS2Count() const;
  ENUM_QOS2_STATE GetQoS2State(const ushort packet_id);
  ENUM_QOS2_STATE GetQoS2State(const ushort packet_id, const bool is_outgoing);

  //--- Cleanup
  void            ClearOfflineQueuedMessages();
  void            ClearAllMessages();
  void            ClearCompletedQoS2();
  void            Clear();

  //--- Debug/Info
  void            PrintStatistics();
  uint            GetTotalAllocatedIds() const { return m_total_allocated_ids; }
  uint            GetTotalReleasedIds() const { return m_total_released_ids; }
  void            TestForceFinalizeOfflineQueuedFallbackOnce() { m_test_force_finalize_offline_fallback_once = true; }

  //--- Persisted circuit-breaker state
  uint            GetReconnectFailureCount() const { return m_reconnect_failure_count; }
  void            SetReconnectFailureCount(uint count) {
    m_reconnect_failure_count = count;
    if (m_is_persistent && !_WriteThroughMutation()) {
      MQTT_LOG_ERROR("Failed to persist reconnect failure counter for session " + m_session_id);
    }
  }
  uint GetIncomingStorageErrorCount() const { return m_incoming_storage_error_count; }
  void SetIncomingStorageErrorCount(uint count) {
    m_incoming_storage_error_count = count;
    if (m_is_persistent && !_WriteThroughMutation()) {
      MQTT_LOG_ERROR("Failed to persist incoming storage error counter for session " + m_session_id);
    }
  }
};

//+------------------------------------------------------------------+
//| Constructor                                                      |
//+------------------------------------------------------------------+
CSessionDatabase::CSessionDatabase()
    : m_qos1_count(0)
    , m_qos2_count(0)
    , m_session_id("")
    , m_is_persistent(false)
    , m_next_packet_id(1)
    , m_message_count(0)
    , m_offline_message_count(0)
    , m_next_offline_message_id(1)
    , m_consumed_offline_message_count(0)
    , m_in_use_packet_id_count(0)
    , m_total_allocated_ids(0)
    , m_total_released_ids(0)
    , m_total_messages_stored(0)
    , m_total_messages_removed(0)
    , m_dirty(false)
    , m_last_flush_time(0)
    , m_reconnect_failure_count(0)
    , m_incoming_storage_error_count(0)
    , m_test_force_finalize_offline_fallback_once(false) {
  ArrayResize(m_messages, 0);
  ArrayResize(m_id_bitfield, 2048);
  ArrayInitialize(m_id_bitfield, 0);
  ArrayResize(m_session_encryption_key, 0);
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CSessionDatabase::~CSessionDatabase() {}

//+------------------------------------------------------------------+
//| SetEncryptionPassphrase                                          |
//| Purpose: Enable or disable AES-256 encryption for persisted      |
//|          session files using a single-pass SHA-256 passphrase    |
//|          hash as the AES-256 key plus a SHA-256 integrity        |
//|          envelope.                                               |
//+------------------------------------------------------------------+
void CSessionDatabase::SetEncryptionPassphrase(const string passphrase) {
  ArrayResize(m_session_encryption_key, 0);
  if (StringLen(passphrase) == 0) {
    return;
  }

  uchar passphrase_utf8[];
  int   passphrase_len = StringToCharArray(passphrase, passphrase_utf8, 0, -1, CP_UTF8) - 1;
  if (passphrase_len <= 0) {
    return;
  }

  ArrayResize(passphrase_utf8, passphrase_len);
  uchar empty_key[];
  uchar derived_key[];
  int   derived_len = CryptEncode(CRYPT_HASH_SHA256, passphrase_utf8, empty_key, derived_key);
  if (derived_len <= 0) {
    MQTT_LOG_ERROR("Failed to derive session encryption key");
    ArrayResize(m_session_encryption_key, 0);
    return;
  }
  ArrayResize(derived_key, derived_len);

  ArrayResize(m_session_encryption_key, ArraySize(derived_key));
  ArrayCopy(m_session_encryption_key, derived_key);
}

bool CSessionDatabase::_ConstantTimeEqual(const uchar& lhs[], const uchar& rhs[], uint count) const {
  if ((uint)ArraySize(lhs) < count || (uint)ArraySize(rhs) < count) {
    return false;
  }

  uint diff = 0;
  for (uint i = 0; i < count; i++) {
    diff |= (uint)(lhs[(int)i] ^ rhs[(int)i]);
  }
  return diff == 0;
}

//+------------------------------------------------------------------+
//| Buffer serialization helpers                                     |
//+------------------------------------------------------------------+
bool CSessionDatabase::_AppendBufferByte(uchar& dest[], uchar value) const {
  int old_size = ArraySize(dest);
  if (ArrayResize(dest, old_size + 1) != old_size + 1) {
    return false;
  }
  dest[old_size] = value;
  return true;
}

bool CSessionDatabase::_AppendBufferUInt16(uchar& dest[], ushort value) const {
  int old_size = ArraySize(dest);
  if (ArrayResize(dest, old_size + 2) != old_size + 2) {
    return false;
  }
  dest[old_size]     = (uchar)(value & 0xFF);
  dest[old_size + 1] = (uchar)((value >> 8) & 0xFF);
  return true;
}

bool CSessionDatabase::_AppendBufferUInt32(uchar& dest[], uint value) const {
  int old_size = ArraySize(dest);
  if (ArrayResize(dest, old_size + 4) != old_size + 4) {
    return false;
  }
  dest[old_size]     = (uchar)(value & 0xFF);
  dest[old_size + 1] = (uchar)((value >> 8) & 0xFF);
  dest[old_size + 2] = (uchar)((value >> 16) & 0xFF);
  dest[old_size + 3] = (uchar)((value >> 24) & 0xFF);
  return true;
}

bool CSessionDatabase::_AppendBufferUInt64(uchar& dest[], ulong value) const {
  int old_size = ArraySize(dest);
  if (ArrayResize(dest, old_size + 8) != old_size + 8) {
    return false;
  }
  for (int i = 0; i < 8; i++) {
    dest[old_size + i] = (uchar)((value >> (8 * i)) & 0xFF);
  }
  return true;
}

bool CSessionDatabase::_AppendBufferBytes(uchar& dest[], const uchar& src[], uint count) const {
  if (count == 0) {
    return true;
  }

  int old_size = ArraySize(dest);
  int add_size = (int)count;
  if (add_size < 0 || ArrayResize(dest, old_size + add_size) != old_size + add_size) {
    return false;
  }
  ArrayCopy(dest, src, old_size, 0, add_size);
  return true;
}

bool CSessionDatabase::_AppendBufferUtf8String(uchar& dest[], const string value) const {
  uchar utf8[];
  int   utf8_len = StringToCharArray(value, utf8, 0, -1, CP_UTF8) - 1;
  if (utf8_len < 0) {
    utf8_len = 0;
  }

  if (!_AppendBufferUInt32(dest, (uint)utf8_len)) {
    return false;
  }
  if (utf8_len == 0) {
    return true;
  }

  ArrayResize(utf8, utf8_len);
  return _AppendBufferBytes(dest, utf8, (uint)utf8_len);
}

bool CSessionDatabase::_ReadBufferByte(const uchar& src[], int& offset, uchar& value) const {
  if (offset < 0 || offset >= ArraySize(src)) {
    return false;
  }
  value = src[offset++];
  return true;
}

bool CSessionDatabase::_ReadBufferUInt16(const uchar& src[], int& offset, ushort& value) const {
  if (offset < 0 || offset + 2 > ArraySize(src)) {
    return false;
  }
  value   = (ushort)((uint)src[offset] | ((uint)src[offset + 1] << 8));
  offset += 2;
  return true;
}

bool CSessionDatabase::_ReadBufferUInt32(const uchar& src[], int& offset, uint& value) const {
  if (offset < 0 || offset + 4 > ArraySize(src)) {
    return false;
  }
  value =
    (uint)src[offset] | ((uint)src[offset + 1] << 8) | ((uint)src[offset + 2] << 16) | ((uint)src[offset + 3] << 24);
  offset += 4;
  return true;
}

bool CSessionDatabase::_ReadBufferUInt64(const uchar& src[], int& offset, ulong& value) const {
  if (offset < 0 || offset + 8 > ArraySize(src)) {
    return false;
  }
  value = 0;
  for (int i = 0; i < 8; i++) {
    value |= ((ulong)src[offset + i] << (8 * i));
  }
  offset += 8;
  return true;
}

bool CSessionDatabase::_ReadBufferBytes(const uchar& src[], int& offset, uint count, uchar& dest[]) const {
  int count_i = (int)count;
  if (count_i < 0 || offset < 0 || offset + count_i > ArraySize(src)) {
    return false;
  }
  ArrayResize(dest, count_i);
  if (count_i > 0) {
    ArrayCopy(dest, src, 0, offset, count_i);
  }
  offset += count_i;
  return true;
}

bool CSessionDatabase::_ReadBufferUtf8String(const uchar& src[], int& offset, uint max_len, string& value) const {
  uint len = 0;
  if (!_ReadBufferUInt32(src, offset, len) || len > max_len) {
    return false;
  }
  if (len == 0) {
    value = "";
    return true;
  }
  if (offset < 0 || offset + (int)len > ArraySize(src)) {
    return false;
  }
  value   = CharArrayToString(src, offset, (int)len, CP_UTF8);
  offset += (int)len;
  return true;
}

bool CSessionDatabase::_ReadFileBuffer(int handle, uint bytes_to_read, uchar& dest[]) const {
  int bytes_i = (int)bytes_to_read;
  if (bytes_i < 0) {
    return false;
  }
  ArrayResize(dest, bytes_i);
  if (bytes_i == 0) {
    return true;
  }
  return (uint)FileReadArray(handle, dest, 0, bytes_i) == bytes_to_read;
}

uint CSessionDatabase::_GetOfflineRemainingExpirySeconds(const OfflineQueuedMessage& msg, datetime now_time,
                                                         ulong now_us) const {
  if (msg.remaining_expiry_seconds > 0 && msg.mono_timestamp_us > 0) {
    if (now_us == 0) {
      now_us = GetMicrosecondCount();
    }

    if (now_us <= msg.mono_timestamp_us) {
      return msg.remaining_expiry_seconds;
    }

    ulong elapsed_us  = now_us - msg.mono_timestamp_us;
    ulong lifetime_us = (ulong)msg.remaining_expiry_seconds * 1000000ULL;
    if (elapsed_us >= lifetime_us) {
      return 0;
    }

    return (uint)((lifetime_us - elapsed_us + 999999ULL) / 1000000ULL);
  }

  if (msg.expiry_time <= 0) {
    return 0;
  }

  if (now_time == 0) {
    now_time = TimeLocal();
  }
  if (now_time >= msg.expiry_time) {
    return 0;
  }

  return (uint)(msg.expiry_time - now_time);
}

bool CSessionDatabase::_IsOfflineQueuedMessageExpired(const OfflineQueuedMessage& msg, datetime now_time,
                                                      ulong now_us) const {
  if (msg.remaining_expiry_seconds > 0 && msg.mono_timestamp_us > 0) {
    if (now_us == 0) {
      now_us = GetMicrosecondCount();
    }

    if (now_us <= msg.mono_timestamp_us) {
      return false;
    }

    ulong elapsed_us  = now_us - msg.mono_timestamp_us;
    ulong lifetime_us = (ulong)msg.remaining_expiry_seconds * 1000000ULL;
    return elapsed_us >= lifetime_us;
  }

  if (msg.expiry_time <= 0) {
    return false;
  }

  if (now_time == 0) {
    now_time = TimeLocal();
  }
  return now_time >= msg.expiry_time;
}

bool CSessionDatabase::_SerializeStateV7(uchar& body[]) const {
  ArrayResize(body, 0);

  if (!_AppendBufferUInt32(body, m_reconnect_failure_count) || !_AppendBufferUInt16(body, m_next_packet_id)
      || !_AppendBufferUInt32(body, m_in_use_packet_id_count)) {
    return false;
  }

  for (uint word_idx = 0; word_idx < 2048; word_idx++) {
    uint word = m_id_bitfield[word_idx];
    if (word == 0) {
      continue;
    }
    for (uint bit = 0; bit < 32; bit++) {
      if ((word & ((uint)1 << bit)) != 0) {
        ushort id = (ushort)(word_idx * 32 + bit);
        if (id > 0 && !_AppendBufferUInt16(body, id)) {
          return false;
        }
      }
    }
  }

  if (!_AppendBufferUInt32(body, m_message_count)) {
    return false;
  }

  for (uint i = 0; i < m_message_count; i++) {
    uint prop_len = (uint)ArraySize(m_messages[i].publish_properties);
    if (!_AppendBufferUInt16(body, m_messages[i].packet_id) || !_AppendBufferByte(body, m_messages[i].qos_level)
        || !_AppendBufferUInt32(body, (uint)m_messages[i].qos2_state)
        || !_AppendBufferUtf8String(body, m_messages[i].topic)
        || !_AppendBufferUInt64(body, (ulong)m_messages[i].timestamp)
        || !_AppendBufferUInt32(body, m_messages[i].payload_size)
        || !_AppendBufferByte(body, (uchar)(m_messages[i].is_outgoing ? 1 : 0))
        || !_AppendBufferUInt32(body, m_messages[i].retransmit_count)
        || !_AppendBufferUInt64(body, (ulong)m_messages[i].expiry_time)
        || !_AppendBufferByte(body, m_messages[i].priority)
        || !_AppendBufferByte(body, (uchar)(m_messages[i].retain ? 1 : 0))
        || !_AppendBufferByte(body, (uchar)(m_messages[i].allow_outgoing_subscription_identifier ? 1 : 0))
        || !_AppendBufferUInt64(body, m_messages[i].mono_timestamp_us)
        || !_AppendBufferBytes(body, m_messages[i].payload, m_messages[i].payload_size)
        || !_AppendBufferUInt32(body, prop_len)
        || !_AppendBufferBytes(body, m_messages[i].publish_properties, prop_len)) {
      return false;
    }
  }

  uint     serializable_offline_count = 0;
  datetime now_time                   = TimeLocal();
  ulong    now_us                     = GetMicrosecondCount();
  for (uint i = 0; i < m_offline_message_count; i++) {
    if (!_IsOfflineQueuedMessageExpired(m_offline_messages[i], now_time, now_us)) {
      serializable_offline_count++;
    }
  }

  if (!_AppendBufferUInt64(body, m_next_offline_message_id) || !_AppendBufferUInt32(body, serializable_offline_count)) {
    return false;
  }

  for (uint i = 0; i < m_offline_message_count; i++) {
    if (_IsOfflineQueuedMessageExpired(m_offline_messages[i], now_time, now_us)) {
      continue;
    }

    uint     prop_len              = (uint)ArraySize(m_offline_messages[i].publish_properties);
    uint     remaining_expiry      = _GetOfflineRemainingExpirySeconds(m_offline_messages[i], now_time, now_us);
    datetime persisted_expiry_time = (remaining_expiry > 0) ? (now_time + (datetime)remaining_expiry) : 0;
    if (!_AppendBufferUInt64(body, m_offline_messages[i].queued_id)
        || !_AppendBufferByte(body, m_offline_messages[i].qos_level)
        || !_AppendBufferByte(body, (uchar)(m_offline_messages[i].retain ? 1 : 0))
        || !_AppendBufferByte(body, (uchar)(m_offline_messages[i].allow_outgoing_subscription_identifier ? 1 : 0))
        || !_AppendBufferUtf8String(body, m_offline_messages[i].topic)
        || !_AppendBufferUInt64(body, (ulong)m_offline_messages[i].timestamp)
        || !_AppendBufferUInt64(body, (ulong)persisted_expiry_time) || !_AppendBufferUInt32(body, remaining_expiry)
        || !_AppendBufferUInt32(body, m_offline_messages[i].payload_size)
        || !_AppendBufferBytes(body, m_offline_messages[i].payload, m_offline_messages[i].payload_size)
        || !_AppendBufferUInt32(body, prop_len)
        || !_AppendBufferBytes(body, m_offline_messages[i].publish_properties, prop_len)) {
      return false;
    }
  }

  if (!_AppendBufferUInt32(body, m_consumed_offline_message_count)) {
    return false;
  }
  for (uint i = 0; i < m_consumed_offline_message_count; i++) {
    if (!_AppendBufferUInt64(body, m_consumed_offline_message_ids[i])) {
      return false;
    }
  }

  if (!_AppendBufferUInt32(body, m_incoming_storage_error_count)) {
    return false;
  }

  return true;
}

bool CSessionDatabase::_DeserializeStateV7(const uchar& body[], int file_version) {
  ClearAllMessages();
  ClearOfflineQueuedMessages();
  ResetPacketIds();

  int  offset                    = 0;
  uint id_count                  = 0;
  uint msg_count                 = 0;
  m_incoming_storage_error_count = 0;

  if (!_ReadBufferUInt32(body, offset, m_reconnect_failure_count)
      || !_ReadBufferUInt16(body, offset, m_next_packet_id)) {
    return false;
  }
  if (m_next_packet_id == 0) {
    m_next_packet_id = 1;
  }

  if (!_ReadBufferUInt32(body, offset, id_count) || id_count > 65535) {
    return false;
  }
  ArrayInitialize(m_id_bitfield, 0);
  m_in_use_packet_id_count = 0;
  for (uint i = 0; i < id_count; i++) {
    ushort packet_id = 0;
    if (!_ReadBufferUInt16(body, offset, packet_id) || packet_id == 0 || _IdBitTest(packet_id)) {
      return false;
    }
    _IdBitSet(packet_id);
    m_in_use_packet_id_count++;
  }

  if (!_ReadBufferUInt32(body, offset, msg_count) || msg_count > 65535) {
    return false;
  }
  m_message_count = msg_count;
  ArrayResize(m_messages, (int)m_message_count);
  m_id_index.Clear();
  m_qos1_count       = 0;
  m_qos2_count       = 0;
  ulong    base_mono = GetMicrosecondCount();
  datetime load_now  = TimeLocal();

  for (uint i = 0; i < m_message_count; i++) {
    uchar qos_level                   = 0;
    uint  qos2_state                  = 0;
    ulong timestamp                   = 0;
    ulong expiry_time                 = 0;
    uchar is_outgoing                 = 0;
    uchar retain                      = 0;
    uchar allow_outgoing_sub_id       = 0;
    ulong persisted_mono_timestamp_us = 0;
    uint  payload_size                = 0;
    uint  prop_len                    = 0;

    if (!_ReadBufferUInt16(body, offset, m_messages[i].packet_id) || !_ReadBufferByte(body, offset, qos_level)
        || !_ReadBufferUInt32(body, offset, qos2_state)
        || !_ReadBufferUtf8String(body, offset, 65535, m_messages[i].topic)
        || !_ReadBufferUInt64(body, offset, timestamp) || !_ReadBufferUInt32(body, offset, payload_size)
        || payload_size > 1048576 || !_ReadBufferByte(body, offset, is_outgoing)
        || !_ReadBufferUInt32(body, offset, m_messages[i].retransmit_count)
        || !_ReadBufferUInt64(body, offset, expiry_time) || !_ReadBufferByte(body, offset, m_messages[i].priority)
        || !_ReadBufferByte(body, offset, retain) || !_ReadBufferByte(body, offset, allow_outgoing_sub_id)
        || !_ReadBufferUInt64(body, offset, persisted_mono_timestamp_us)
        || !_ReadBufferBytes(body, offset, payload_size, m_messages[i].payload)
        || !_ReadBufferUInt32(body, offset, prop_len) || prop_len > 65535
        || !_ReadBufferBytes(body, offset, prop_len, m_messages[i].publish_properties)) {
      return false;
    }

    m_messages[i].qos_level                              = qos_level;
    m_messages[i].qos2_state                             = (ENUM_QOS2_STATE)qos2_state;
    m_messages[i].timestamp                              = (datetime)timestamp;
    m_messages[i].payload_size                           = payload_size;
    m_messages[i].is_outgoing                            = (is_outgoing != 0);
    m_messages[i].expiry_time                            = (datetime)expiry_time;
    m_messages[i].retain                                 = (retain != 0);
    m_messages[i].allow_outgoing_subscription_identifier = (allow_outgoing_sub_id != 0);
    m_messages[i].mono_timestamp_us                      = base_mono + (ulong)i * 100000;

    m_id_index.Add(m_messages[i].packet_id, (int)i);
    if (m_messages[i].qos_level == 1) {
      m_qos1_count++;
    } else if (m_messages[i].qos_level == 2) {
      m_qos2_count++;
    }
  }

  ulong next_offline_id = 0;
  uint  offline_count   = 0;
  if (!_ReadBufferUInt64(body, offset, next_offline_id)) {
    return false;
  }
  m_next_offline_message_id = (next_offline_id > 0) ? next_offline_id : 1;
  if (!_ReadBufferUInt32(body, offset, offline_count) || offline_count > 65535) {
    return false;
  }
  m_offline_message_count = offline_count;
  ArrayResize(m_offline_messages, (int)m_offline_message_count);

  for (uint i = 0; i < m_offline_message_count; i++) {
    uchar retain                   = 0;
    uchar allow_outgoing_sub_id    = 0;
    ulong timestamp                = 0;
    ulong expiry_time              = 0;
    uint  remaining_expiry_seconds = 0;
    uint  payload_size             = 0;
    uint  prop_len                 = 0;

    if (!_ReadBufferUInt64(body, offset, m_offline_messages[i].queued_id) || m_offline_messages[i].queued_id == 0
        || !_ReadBufferByte(body, offset, m_offline_messages[i].qos_level) || !_ReadBufferByte(body, offset, retain)
        || !_ReadBufferByte(body, offset, allow_outgoing_sub_id)
        || !_ReadBufferUtf8String(body, offset, 65535, m_offline_messages[i].topic)
        || !_ReadBufferUInt64(body, offset, timestamp) || !_ReadBufferUInt64(body, offset, expiry_time)
        || (file_version >= 7 && !_ReadBufferUInt32(body, offset, remaining_expiry_seconds))
        || !_ReadBufferUInt32(body, offset, payload_size) || payload_size > 1048576
        || !_ReadBufferBytes(body, offset, payload_size, m_offline_messages[i].payload)
        || !_ReadBufferUInt32(body, offset, prop_len) || prop_len > 65535
        || !_ReadBufferBytes(body, offset, prop_len, m_offline_messages[i].publish_properties)) {
      return false;
    }

    m_offline_messages[i].retain                                 = (retain != 0);
    m_offline_messages[i].allow_outgoing_subscription_identifier = (allow_outgoing_sub_id != 0);
    m_offline_messages[i].timestamp                              = (datetime)timestamp;
    m_offline_messages[i].expiry_time                            = (datetime)expiry_time;
    if (file_version < 7 && m_offline_messages[i].expiry_time > 0 && load_now < m_offline_messages[i].expiry_time) {
      remaining_expiry_seconds = (uint)(m_offline_messages[i].expiry_time - load_now);
    }
    m_offline_messages[i].remaining_expiry_seconds = remaining_expiry_seconds;
    m_offline_messages[i].mono_timestamp_us = (remaining_expiry_seconds > 0) ? (base_mono + (ulong)i * 100000ULL) : 0;
    m_offline_messages[i].payload_size      = payload_size;
  }

  if (file_version >= 6) {
    uint consumed_count = 0;
    if (!_ReadBufferUInt32(body, offset, consumed_count) || consumed_count > 65535) {
      return false;
    }
    m_consumed_offline_message_count = consumed_count;
    ArrayResize(m_consumed_offline_message_ids, (int)m_consumed_offline_message_count);
    for (uint i = 0; i < m_consumed_offline_message_count; i++) {
      if (!_ReadBufferUInt64(body, offset, m_consumed_offline_message_ids[i]) || m_consumed_offline_message_ids[i] == 0
          || FindConsumedOfflineQueuedMessageById(m_consumed_offline_message_ids[i]) != (int)i) {
        return false;
      }
    }
  }

  if (offset < ArraySize(body) && !_ReadBufferUInt32(body, offset, m_incoming_storage_error_count)) {
    return false;
  }

  return offset == ArraySize(body);
}

bool CSessionDatabase::_EncryptSerializedState(const uchar& plain_body[], uchar& stored_body[]) const {
  uchar empty_key[];
  uchar hash[];
  int   hash_len = CryptEncode(CRYPT_HASH_SHA256, plain_body, empty_key, hash);
  if (hash_len <= 0) {
    MQTT_LOG_ERROR("Failed to hash serialized session state");
    return false;
  }
  ArrayResize(hash, hash_len);
  if (ArraySize(hash) != 32) {
    MQTT_LOG_ERROR("Serialized session state hash length mismatch");
    return false;
  }

  uchar envelope[];
  if (!_AppendBufferUInt32(envelope, MQTT_SESSION_ENVELOPE_MAGIC)
      || !_AppendBufferUInt32(envelope, (uint)ArraySize(plain_body))
      || !_AppendBufferBytes(envelope, hash, (uint)ArraySize(hash))
      || !_AppendBufferBytes(envelope, plain_body, (uint)ArraySize(plain_body))) {
    return false;
  }

  ArrayResize(stored_body, 0);
  int encoded_len = CryptEncode(CRYPT_AES256, envelope, m_session_encryption_key, stored_body);
  if (encoded_len <= 0) {
    MQTT_LOG_ERROR("Failed to encrypt serialized session state");
    return false;
  }
  ArrayResize(stored_body, encoded_len);
  if (ArraySize(stored_body) == 0) {
    MQTT_LOG_ERROR("Encrypted session state is empty");
    return false;
  }
  return true;
}

bool CSessionDatabase::_DecryptSerializedState(const uchar& stored_body[], uchar& plain_body[]) const {
  if (!IsEncryptionEnabled()) {
    MQTT_LOG_ERROR("Encrypted session file requires SetEncryptionPassphrase() before loading");
    return false;
  }

  uchar envelope[];
  int   decoded_len = CryptDecode(CRYPT_AES256, stored_body, m_session_encryption_key, envelope);
  if (decoded_len <= 0) {
    MQTT_LOG_ERROR("Failed to decrypt session file body");
    return false;
  }
  ArrayResize(envelope, decoded_len);
  if (ArraySize(envelope) < 40) {
    MQTT_LOG_ERROR("Decrypted session file body is shorter than the integrity envelope");
    return false;
  }

  int   offset           = 0;
  uint  envelope_magic   = 0;
  uint  expected_body_sz = 0;
  int   padding_bytes    = 0;
  uchar expected_hash[];
  if (!_ReadBufferUInt32(envelope, offset, envelope_magic) || !_ReadBufferUInt32(envelope, offset, expected_body_sz)
      || envelope_magic != MQTT_SESSION_ENVELOPE_MAGIC || expected_body_sz > MQTT_SESSION_MAX_BODY_BYTES
      || !_ReadBufferBytes(envelope, offset, 32, expected_hash)
      || !_ReadBufferBytes(envelope, offset, expected_body_sz, plain_body)) {
    MQTT_LOG_ERROR("Encrypted session file failed integrity envelope validation");
    return false;
  }

  padding_bytes = ArraySize(envelope) - offset;
  if (padding_bytes < 0 || padding_bytes > 16) {
    MQTT_LOG_ERROR("Encrypted session file envelope has invalid trailing padding");
    return false;
  }

  uchar empty_key[];
  uchar actual_hash[];
  int   actual_hash_len = CryptEncode(CRYPT_HASH_SHA256, plain_body, empty_key, actual_hash);
  if (actual_hash_len <= 0) {
    MQTT_LOG_ERROR("Failed to hash decrypted session state");
    return false;
  }
  ArrayResize(actual_hash, actual_hash_len);
  if (ArraySize(actual_hash) != 32) {
    MQTT_LOG_ERROR("Decrypted session state hash length mismatch");
    return false;
  }

  if (!_ConstantTimeEqual(actual_hash, expected_hash, 32)) {
    MQTT_LOG_ERROR("Encrypted session file hash mismatch — wrong passphrase or tampered file");
    return false;
  }

  return true;
}

//+------------------------------------------------------------------+
//| Init                                                             |
//| Purpose: Initialize database for a session                       |
//| Parameters: session_id - unique session identifier               |
//|             persistent - true if session should persist          |
//| Return: true if initialization successful                        |
//+------------------------------------------------------------------+
bool CSessionDatabase::Init(const string session_id, const bool persistent) {
  if (StringLen(session_id) == 0) {
    MQTT_LOG_ERROR("Session ID cannot be empty");
    return false;
  }

  m_session_id                   = session_id;
  m_is_persistent                = persistent;
  m_next_packet_id               = 1;
  m_reconnect_failure_count      = 0;
  m_incoming_storage_error_count = 0;

  if (m_is_persistent) {
    LoadFromFile();
    //--- Clean up any stale .tmp file left by a previous crash.
    //--- A large .tmp that was never renamed (crash mid-save + antivirus lock)
    //--- can grow unbounded. Delete it on init when the primary .bin is readable.
    string tmp_name = "MQTT_Sessions\\" + m_session_id + ".bin.tmp";
    if (FileIsExist(tmp_name)) {
      FileDelete(tmp_name);
      MQTT_LOG_DEBUG("MQL5-4: Cleaned up stale session temp file: " + tmp_name);
    }
  }

  return true;
}

//+------------------------------------------------------------------+
//| SaveToFile                                                       |
//| Purpose: Save persistent session state to disk                   |
//| Files are stored in the terminal-local MQL5/Files/               |
//| directory (no FILE_COMMON) which is unique per terminal          |
//| instance, preventing cross-terminal session collisions.          |
//| Uses write-to-temp-then-rename to avoid partial-write            |
//| corruption if the process crashes mid-save.                      |
//+------------------------------------------------------------------+
bool CSessionDatabase::SaveToFile() {
  if (!m_is_persistent || StringLen(m_session_id) == 0) {
    return false;
  }

  //--- Store in terminal-local MQL5/Files/
  string db_dir = "MQTT_Sessions";
  if (!FolderCreate(db_dir)) {
    // It's ok if folder already exists
  }

  //--- Write to a temporary file first; rename on success so that
  //--- a crash mid-write never produces a partially-written .bin file.
  string file_name = db_dir + "\\" + m_session_id + ".bin";
  string tmp_name  = db_dir + "\\" + m_session_id + ".bin.tmp";

  uchar  plain_body[];
  if (!_SerializeStateV7(plain_body)) {
    MQTT_LOG_ERROR("Failed to serialize session state");
    return false;
  }

  uchar stored_body[];
  uchar file_flags = 0;
  if (IsEncryptionEnabled()) {
    file_flags = MQTT_SESSION_FILE_FLAG_ENCRYPTED;
    if (!_EncryptSerializedState(plain_body, stored_body)) {
      return false;
    }
  } else {
    ArrayResize(stored_body, ArraySize(plain_body));
    if (ArraySize(plain_body) > 0) {
      ArrayCopy(stored_body, plain_body);
    }
  }

  int handle = FileOpen(tmp_name, FILE_WRITE | FILE_BIN);
  if (handle == INVALID_HANDLE) {
    MQTT_LOG_ERROR("Failed to open tmp file " + tmp_name);
    return false;
  }

  //--- Magic header — used to detect corrupt or incompatible files.
  FileWriteInteger(handle, MQTT_SESSION_FILE_MAGIC, INT_VALUE);
  //--- File format version — enables future migration paths.
  FileWriteInteger(handle, MQTT_SESSION_FILE_VERSION, CHAR_VALUE);
  FileWriteInteger(handle, file_flags, CHAR_VALUE);
  FileWriteInteger(handle, (int)ArraySize(stored_body), INT_VALUE);
  if (ArraySize(stored_body) > 0
      && (uint)FileWriteArray(handle, stored_body, 0, ArraySize(stored_body)) != (uint)ArraySize(stored_body)) {
    FileClose(handle);
    FileDelete(tmp_name);
    MQTT_LOG_ERROR("Failed to write serialized session body to tmp file " + tmp_name);
    return false;
  }
  FileClose(handle);

  //--- Atomically commit by renaming .tmp → .bin
  //--- FileMove overwrites an existing .bin with FILE_REWRITE.
  ResetLastError();
  if (!FileMove(tmp_name, 0, file_name, FILE_REWRITE)) {
    int move_err = GetLastError();

    //--- Some MT5/Windows environments sporadically fail the rewrite rename
    //--- even though both files are local and closed. Fall back once by
    //--- deleting the previous committed file and retrying the rename.
    if (FileIsExist(file_name)) {
      ResetLastError();
      if (!FileDelete(file_name)) {
        MQTT_LOG_ERROR("FileMove failed (err=" + (string)move_err + ") and FileDelete fallback failed (err="
                       + (string)GetLastError() + "). Temp file preserved at " + tmp_name);
        return false;
      }

      ResetLastError();
      if (!FileMove(tmp_name, 0, file_name, 0)) {
        MQTT_LOG_ERROR("FileMove failed (err=" + (string)move_err
                       + ") and retry after deleting destination failed (err=" + (string)GetLastError()
                       + "). Temp file preserved at " + tmp_name);
        return false;
      }

      MQTT_LOG_WARN("Recovered session DB commit after FileMove(FILE_REWRITE) failure by deleting the previous file "
                    "and retrying rename.");
    } else {
      MQTT_LOG_ERROR("FileMove failed (err=" + (string)move_err + "). Temp file preserved at " + tmp_name);
      return false;
    }
  }

  m_dirty           = false;
  m_last_flush_time = TimeLocal();
  return true;
}

//+------------------------------------------------------------------+
//| FlushIfDirty                                                     |
//| Purpose: Write session to disk only when dirty and the flush     |
//|          interval has elapsed. Call periodically (e.g. OnTimer). |
//| Parameters: interval_seconds - minimum seconds between flushes   |
//| Return: true if a flush was performed                            |
//+------------------------------------------------------------------+
bool CSessionDatabase::FlushIfDirty(uint interval_seconds) {
  if (!m_dirty) {
    return false;
  }
  if ((TimeLocal() - m_last_flush_time) < (datetime)interval_seconds) {
    return false;
  }
  return SaveToFile();
}

bool CSessionDatabase::_WriteThroughMutation() {
  if (!m_is_persistent) {
    return true;
  }

  m_dirty = true;
  return SaveToFile();
}

//+------------------------------------------------------------------+
//| SetPersistence                                                   |
//| Purpose: Toggle on-disk session persistence while preserving the |
//|          in-memory QoS/session state.                            |
//+------------------------------------------------------------------+
void CSessionDatabase::SetPersistence(bool persistent) {
  if (persistent == m_is_persistent) {
    return;
  }

  if (!persistent) {
    if (StringLen(m_session_id) > 0) {
      string file_name = "MQTT_Sessions\\" + m_session_id + ".bin";
      string tmp_name  = "MQTT_Sessions\\" + m_session_id + ".bin.tmp";
      FileDelete(file_name);
      FileDelete(tmp_name);
    }
    m_is_persistent   = false;
    m_dirty           = false;
    m_last_flush_time = 0;
    return;
  }

  if (StringLen(m_session_id) == 0) {
    return;
  }

  m_is_persistent   = true;
  m_dirty           = true;
  m_last_flush_time = 0;
}

//+------------------------------------------------------------------+
//| LoadFromFile                                                     |
//| Purpose: Load persistent session state from disk                 |
//+------------------------------------------------------------------+
bool CSessionDatabase::LoadFromFile() {
  if (!m_is_persistent || StringLen(m_session_id) == 0) {
    return false;
  }

  //--- Terminal-local storage
  string file_name = "MQTT_Sessions\\" + m_session_id + ".bin";
  string tmp_name  = "MQTT_Sessions\\" + m_session_id + ".bin.tmp";
  int    handle    = FileOpen(file_name, FILE_READ | FILE_BIN);
  if (handle == INVALID_HANDLE) {
    //--- Try the temp file as a recovery fallback (crash during last save)
    handle = FileOpen(tmp_name, FILE_READ | FILE_BIN);
    if (handle != INVALID_HANDLE) {
      MQTT_LOG_WARN(".bin missing; recovering from " + tmp_name);
    } else {
      return false;  // Valid case if file does not exist yet
    }
  }

  ClearAllMessages();
  ClearOfflineQueuedMessages();
  ResetPacketIds();

  int magic = FileReadInteger(handle, INT_VALUE);
  if (magic != MQTT_SESSION_FILE_MAGIC) {
    MQTT_LOG_ERROR("Session file magic mismatch (got 0x" + StringFormat("%08X", magic) + "); expected 0x"
                   + StringFormat("%08X", MQTT_SESSION_FILE_MAGIC) + ". Discarding corrupt session file.");
    FileClose(handle);
    //--- Delete the corrupt file so the next save produces a clean file.
    FileDelete(file_name);
    return false;
  }
  //--- Read and validate file format version.
  //--- EA authors: add a staged reader before bumping the version so older
  //--- durable sessions can be migrated forward instead of discarded.
  int file_version = FileReadInteger(handle, CHAR_VALUE);
  //--- Accept only the exact versions this code was written to parse. An unknown higher
  //--- version would cause all subsequent FileReadInteger calls to read fields at wrong
  //--- offsets, silently producing corrupt in-memory state.
  switch (file_version) {
    case 1:
    case 2:
      MQTT_LOG_WARN("Session file version " + (string)file_version + " is older than current format — "
                    + "discarding for clean-start semantics.");
      FileClose(handle);
      FileDelete(file_name);
      return false;
    case 3:
    case 4:
      break;
    case 5:
    case 6:
    case 7:
      break;
    default:
      MQTT_LOG_ERROR("Session file version " + (string)file_version + " is unknown (expected "
                     + (string)MQTT_SESSION_FILE_VERSION + ") — discarding to prevent wrong-offset reads.");
      FileClose(handle);
      FileDelete(file_name);
      return false;
  }
  if (file_version >= 5) {
    int  file_flags = FileReadInteger(handle, CHAR_VALUE);
    uint body_len   = (uint)FileReadInteger(handle, INT_VALUE);
    if (body_len > MQTT_SESSION_MAX_BODY_BYTES) {
      MQTT_LOG_ERROR("Session file body_len corrupt (" + (string)body_len + ") — discarding session");
      FileClose(handle);
      FileDelete(file_name);
      return false;
    }

    uchar stored_body[];
    if (!_ReadFileBuffer(handle, body_len, stored_body)) {
      MQTT_LOG_ERROR("Session file body truncated — discarding session");
      FileClose(handle);
      FileDelete(file_name);
      return false;
    }
    FileClose(handle);

    uchar plain_body[];
    if ((file_flags & MQTT_SESSION_FILE_FLAG_ENCRYPTED) != 0) {
      if (!_DecryptSerializedState(stored_body, plain_body)) {
        return false;
      }
    } else {
      ArrayResize(plain_body, ArraySize(stored_body));
      if (ArraySize(stored_body) > 0) {
        ArrayCopy(plain_body, stored_body);
      }
    }

    if (!_DeserializeStateV7(plain_body, file_version)) {
      MQTT_LOG_ERROR("Session file body failed validation — discarding session");
      FileDelete(file_name);
      ClearAllMessages();
      ClearOfflineQueuedMessages();
      ResetPacketIds();
      return false;
    }

    bool should_rewrite = (file_version != MQTT_SESSION_FILE_VERSION);
    m_dirty             = false;
    m_last_flush_time   = TimeLocal();
    if (should_rewrite && !SaveToFile()) {
      MQTT_LOG_WARN("Loaded session file version " + (string)file_version
                    + " but failed to rewrite migrated session file to version " + (string)MQTT_SESSION_FILE_VERSION);
      m_dirty = true;
    }
    return true;
  }
  //--- Restore persisted circuit-breaker counter.
  m_reconnect_failure_count = (uint)FileReadInteger(handle, INT_VALUE);
  m_next_packet_id          = (ushort)FileReadInteger(handle, SHORT_VALUE);
  //--- Packet ID 0 is reserved; a zero value indicates file corruption or a
  //--- zero-padded truncation. Clamp to 1 so AllocatePacketId() stays functional.
  if (m_next_packet_id == 0) {
    m_next_packet_id = 1;
  }

  uint id_count = (uint)FileReadInteger(handle, INT_VALUE);
  //--- MQTT packet identifiers are in [1, 65535]; a larger count indicates file corruption.
  if (id_count > 65535) {
    MQTT_LOG_ERROR("Session file id_count corrupt (" + (string)id_count + ") — discarding session");
    FileClose(handle);
    FileDelete(file_name);
    return false;
  }
  ArrayInitialize(m_id_bitfield, 0);
  m_in_use_packet_id_count = 0;
  //--- Compact list of in-use packet IDs
  for (uint i = 0; i < id_count; i++) {
    ushort id = (ushort)FileReadInteger(handle, SHORT_VALUE);
    if (id >= 1) {
      _IdBitSet(id);
      m_in_use_packet_id_count++;
    }
  }

  m_message_count = (uint)FileReadInteger(handle, INT_VALUE);
  //--- A corrupt count here would drive ArrayResize to allocate gigabytes and OOM-crash the terminal.
  if (m_message_count > 65535) {
    MQTT_LOG_ERROR("Session file message_count corrupt (" + (string)m_message_count + ") — discarding session");
    FileClose(handle);
    FileDelete(file_name);
    return false;
  }
  ArrayResize(m_messages, m_message_count);
  m_id_index.Clear();      // Reset HashMap index before rebuilding
  m_qos1_count       = 0;  // Reset cached QoS counts before rebuilding
  m_qos2_count       = 0;
  ulong    base_mono = GetMicrosecondCount();
  datetime load_now  = TimeLocal();
  for (uint i = 0; i < m_message_count; i++) {
    //--- Detect truncated file before reading next message
    if (FileIsEnding(handle)) {
      MQTT_LOG_ERROR("Session file truncated at message " + (string)i + "/" + (string)m_message_count
                     + " — discarding session");
      FileClose(handle);
      FileDelete(file_name);
      ClearAllMessages();
      ClearOfflineQueuedMessages();
      ResetPacketIds();
      return false;
    }
    m_messages[i].packet_id = (ushort)FileReadInteger(handle, SHORT_VALUE);
    m_messages[i].qos_level = (uchar)FileReadInteger(handle, CHAR_VALUE);
    //--- Detect truncation within a message (fields return 0/garbage after EOF).
    //--- The check intentionally applies to every message including the last one;
    //--- excluding it would allow a file truncated exactly at the last record boundary
    //--- to load a zeroed ghost message (empty topic, zero payload) into the retransmit queue.
    if (FileIsEnding(handle)) {
      MQTT_LOG_ERROR("Session file truncated mid-message at message " + (string)i + "/" + (string)m_message_count
                     + " — discarding session");
      FileClose(handle);
      FileDelete(file_name);
      ClearAllMessages();
      ClearOfflineQueuedMessages();
      ResetPacketIds();
      return false;
    }
    m_messages[i].qos2_state = (ENUM_QOS2_STATE)FileReadInteger(handle, INT_VALUE);
    int topic_len            = FileReadInteger(handle, INT_VALUE);
    //--- MQTT topic max length is 65535 bytes
    if (topic_len < 0 || topic_len > 65535) {
      MQTT_LOG_ERROR("Session file topic_len corrupt (" + (string)topic_len + ") — discarding session");
      FileClose(handle);
      FileDelete(file_name);
      ClearAllMessages();
      ClearOfflineQueuedMessages();
      ResetPacketIds();
      return false;
    }
    if (file_version >= 3) {
      //--- Version 3+: topic stored as raw UTF-8 bytes — safe for all Unicode characters.
      if (topic_len > 0) {
        uchar _topic_utf8[];
        int   topic_len_i = (int)topic_len;
        ArrayResize(_topic_utf8, topic_len_i);
        uint topic_bytes_read = (uint)FileReadArray(handle, _topic_utf8, 0, topic_len_i);
        if (topic_bytes_read != topic_len) {
          MQTT_LOG_ERROR("Session file topic bytes truncated at message " + (string)i + "/" + (string)m_message_count
                         + " — discarding session");
          FileClose(handle);
          FileDelete(file_name);
          ClearAllMessages();
          ClearOfflineQueuedMessages();
          ResetPacketIds();
          return false;
        }
        m_messages[i].topic = CharArrayToString(_topic_utf8, 0, topic_len, CP_UTF8);
      } else {
        m_messages[i].topic = "";
      }
    } else {
      //--- Version 1-2: topic stored as ANSI string via FileWriteString.
      m_messages[i].topic = (topic_len > 0) ? FileReadString(handle, topic_len) : "";
      if (topic_len > 0 && StringLen(m_messages[i].topic) != topic_len) {
        MQTT_LOG_ERROR("Session file legacy topic bytes truncated at message " + (string)i + "/"
                       + (string)m_message_count + " — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
    }
    m_messages[i].timestamp    = (datetime)FileReadLong(handle);
    m_messages[i].payload_size = (uint)FileReadInteger(handle, INT_VALUE);
    //--- Cap payload size to prevent OOM from corrupt values
    if (m_messages[i].payload_size > 1048576) {
      MQTT_LOG_ERROR("Session file payload_size corrupt (" + (string)m_messages[i].payload_size
                     + ") — discarding session");
      FileClose(handle);
      FileDelete(file_name);
      ClearAllMessages();
      ClearOfflineQueuedMessages();
      ResetPacketIds();
      return false;
    }
    m_messages[i].is_outgoing      = (bool)FileReadInteger(handle, CHAR_VALUE);
    m_messages[i].retransmit_count = (uint)FileReadInteger(handle, INT_VALUE);
    m_messages[i].expiry_time      = (datetime)FileReadLong(handle);
    m_messages[i].priority         = (uchar)FileReadInteger(handle, CHAR_VALUE);
    if (file_version >= 2) {
      m_messages[i].retain                                 = (bool)FileReadInteger(handle, CHAR_VALUE);
      m_messages[i].allow_outgoing_subscription_identifier = (bool)FileReadInteger(handle, CHAR_VALUE);
    } else {
      m_messages[i].retain                                 = false;
      m_messages[i].allow_outgoing_subscription_identifier = false;
    }
    FileReadLong(handle);  // skip persisted mono_timestamp_us
    //--- Stagger timestamps so retransmissions spread across multiple
    //--- Poll ticks instead of bursting simultaneously.
    m_messages[i].mono_timestamp_us = base_mono + (ulong)i * 100000;
    int payload_size_i              = (int)m_messages[i].payload_size;
    ArrayResize(m_messages[i].payload, payload_size_i);
    if (m_messages[i].payload_size > 0) {
      uint payload_bytes_read = (uint)FileReadArray(handle, m_messages[i].payload, 0, payload_size_i);
      if (payload_bytes_read != m_messages[i].payload_size) {
        MQTT_LOG_ERROR("Session file payload truncated at message " + (string)i + "/" + (string)m_message_count
                       + " — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
    }
    if (file_version >= 2) {
      uint prop_len = (uint)FileReadInteger(handle, INT_VALUE);
      //--- Cap property length to prevent OOM from corrupt values
      if (prop_len > 65535) {
        MQTT_LOG_ERROR("Session file prop_len corrupt (" + (string)prop_len + ") — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
      int prop_len_i = (int)prop_len;
      ArrayResize(m_messages[i].publish_properties, prop_len_i);
      if (prop_len > 0) {
        uint prop_bytes_read = (uint)FileReadArray(handle, m_messages[i].publish_properties, 0, prop_len_i);
        if (prop_bytes_read != prop_len) {
          MQTT_LOG_ERROR("Session file properties truncated at message " + (string)i + "/" + (string)m_message_count
                         + " — discarding session");
          FileClose(handle);
          FileDelete(file_name);
          ClearAllMessages();
          ClearOfflineQueuedMessages();
          ResetPacketIds();
          return false;
        }
      }
    } else {
      ArrayResize(m_messages[i].publish_properties, 0);
    }
    //--- Rebuild HashMap index and cached QoS counts for this message
    m_id_index.Add(m_messages[i].packet_id, (int)i);
    if (m_messages[i].qos_level == 1) {
      m_qos1_count++;
    } else if (m_messages[i].qos_level == 2) {
      m_qos2_count++;
    }
  }
  ArrayResize(m_offline_messages, 0);
  m_offline_message_count   = 0;
  m_next_offline_message_id = 1;
  if (file_version >= 4) {
    m_next_offline_message_id = (ulong)FileReadLong(handle);
    if (m_next_offline_message_id == 0) {
      m_next_offline_message_id = 1;
    }
    m_offline_message_count = (uint)FileReadInteger(handle, INT_VALUE);
    if (m_offline_message_count > 65535) {
      MQTT_LOG_ERROR("Session file offline_message_count corrupt (" + (string)m_offline_message_count
                     + ") — discarding session");
      FileClose(handle);
      FileDelete(file_name);
      ClearAllMessages();
      ClearOfflineQueuedMessages();
      ResetPacketIds();
      return false;
    }
    ArrayResize(m_offline_messages, (int)m_offline_message_count);
    for (uint i = 0; i < m_offline_message_count; i++) {
      if (FileIsEnding(handle)) {
        MQTT_LOG_ERROR("Session file truncated at offline queued message " + (string)i + "/"
                       + (string)m_offline_message_count + " — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
      m_offline_messages[i].queued_id = (ulong)FileReadLong(handle);
      if (m_offline_messages[i].queued_id == 0) {
        MQTT_LOG_ERROR("Session file queued_id corrupt at offline message " + (string)i + " — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
      m_offline_messages[i].qos_level = (uchar)FileReadInteger(handle, CHAR_VALUE);
      if (m_offline_messages[i].qos_level < 1 || m_offline_messages[i].qos_level > 2) {
        MQTT_LOG_ERROR("Session file offline qos_level corrupt at message " + (string)i + " — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
      m_offline_messages[i].retain                                 = (bool)FileReadInteger(handle, CHAR_VALUE);
      m_offline_messages[i].allow_outgoing_subscription_identifier = (bool)FileReadInteger(handle, CHAR_VALUE);
      int offline_topic_len                                        = FileReadInteger(handle, INT_VALUE);
      if (offline_topic_len < 0 || offline_topic_len > 65535) {
        MQTT_LOG_ERROR("Session file offline topic_len corrupt (" + (string)offline_topic_len
                       + ") — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
      if (offline_topic_len > 0) {
        uchar offline_topic_utf8[];
        ArrayResize(offline_topic_utf8, offline_topic_len);
        uint offline_topic_bytes_read = (uint)FileReadArray(handle, offline_topic_utf8, 0, offline_topic_len);
        if (offline_topic_bytes_read != (uint)offline_topic_len) {
          MQTT_LOG_ERROR("Session file offline topic bytes truncated at message " + (string)i + "/"
                         + (string)m_offline_message_count + " — discarding session");
          FileClose(handle);
          FileDelete(file_name);
          ClearAllMessages();
          ClearOfflineQueuedMessages();
          ResetPacketIds();
          return false;
        }
        m_offline_messages[i].topic = CharArrayToString(offline_topic_utf8, 0, offline_topic_len, CP_UTF8);
      } else {
        m_offline_messages[i].topic = "";
      }
      m_offline_messages[i].timestamp   = (datetime)FileReadLong(handle);
      m_offline_messages[i].expiry_time = (datetime)FileReadLong(handle);
      if (m_offline_messages[i].expiry_time > 0 && load_now < m_offline_messages[i].expiry_time) {
        m_offline_messages[i].remaining_expiry_seconds = (uint)(m_offline_messages[i].expiry_time - load_now);
        m_offline_messages[i].mono_timestamp_us        = base_mono + (ulong)i * 100000ULL;
      } else {
        m_offline_messages[i].remaining_expiry_seconds = 0;
        m_offline_messages[i].mono_timestamp_us        = 0;
      }
      m_offline_messages[i].payload_size = (uint)FileReadInteger(handle, INT_VALUE);
      if (m_offline_messages[i].payload_size > 1048576) {
        MQTT_LOG_ERROR("Session file offline payload_size corrupt (" + (string)m_offline_messages[i].payload_size
                       + ") — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
      ArrayResize(m_offline_messages[i].payload, (int)m_offline_messages[i].payload_size);
      if (m_offline_messages[i].payload_size > 0) {
        uint offline_payload_bytes_read =
          (uint)FileReadArray(handle, m_offline_messages[i].payload, 0, (int)m_offline_messages[i].payload_size);
        if (offline_payload_bytes_read != m_offline_messages[i].payload_size) {
          MQTT_LOG_ERROR("Session file offline payload truncated at message " + (string)i + "/"
                         + (string)m_offline_message_count + " — discarding session");
          FileClose(handle);
          FileDelete(file_name);
          ClearAllMessages();
          ClearOfflineQueuedMessages();
          ResetPacketIds();
          return false;
        }
      }
      uint offline_prop_len = (uint)FileReadInteger(handle, INT_VALUE);
      if (offline_prop_len > 65535) {
        MQTT_LOG_ERROR("Session file offline prop_len corrupt (" + (string)offline_prop_len + ") — discarding session");
        FileClose(handle);
        FileDelete(file_name);
        ClearAllMessages();
        ClearOfflineQueuedMessages();
        ResetPacketIds();
        return false;
      }
      ArrayResize(m_offline_messages[i].publish_properties, (int)offline_prop_len);
      if (offline_prop_len > 0) {
        uint offline_prop_bytes_read =
          (uint)FileReadArray(handle, m_offline_messages[i].publish_properties, 0, (int)offline_prop_len);
        if (offline_prop_bytes_read != offline_prop_len) {
          MQTT_LOG_ERROR("Session file offline properties truncated at message " + (string)i + "/"
                         + (string)m_offline_message_count + " — discarding session");
          FileClose(handle);
          FileDelete(file_name);
          ClearAllMessages();
          ClearOfflineQueuedMessages();
          ResetPacketIds();
          return false;
        }
      }
    }
  }
  FileClose(handle);
  if (file_version != MQTT_SESSION_FILE_VERSION) {
    m_dirty           = false;
    m_last_flush_time = TimeLocal();
    if (!SaveToFile()) {
      MQTT_LOG_WARN("Loaded session file version " + (string)file_version
                    + " but failed to rewrite migrated session file to version " + (string)MQTT_SESSION_FILE_VERSION);
      m_dirty = true;
      return true;
    }
  }
  return true;
}

//+------------------------------------------------------------------+
//| ResetSession                                                     |
//| Purpose: Clear all session state (for Clean Start = 1)           |
//+------------------------------------------------------------------+
void CSessionDatabase::ResetSession() {
  Clear();
  m_next_packet_id = 1;
}

//+------------------------------------------------------------------+
//| GetSessionId                                                     |
//| Return: Current session ID                                       |
//+------------------------------------------------------------------+
string CSessionDatabase::GetSessionId() const { return m_session_id; }

//+------------------------------------------------------------------+
//| IsPersistent                                                     |
//| Return: true if session is configured as persistent              |
//+------------------------------------------------------------------+
bool   CSessionDatabase::IsPersistent() const { return m_is_persistent; }

//+------------------------------------------------------------------+
//| IsValidPacketId                                                  |
//| Purpose: Check if packet ID is in valid range (1-65535)          |
//| Parameters: packet_id - packet ID to validate                    |
//| Return: true if valid                                            |
//+------------------------------------------------------------------+
bool   CSessionDatabase::IsValidPacketId(const ushort packet_id) const { return (packet_id >= 1); }

//+------------------------------------------------------------------+
//| _PurgeExpiredMessages                                            |
//| Purpose: Remove all messages whose expiry_time has passed.       |
//|          Two-pass: collect IDs first, then remove (safe during   |
//|          iteration since RemoveMessage modifies m_messages[]).   |
//+------------------------------------------------------------------+
void   CSessionDatabase::_PurgeExpiredMessages() {
  const datetime now = TimeLocal();
  ushort         expired_ids[];
  uint           expired_count = 0;
  for (uint i = 0; i < m_message_count; i++) {
    if (m_messages[i].expiry_time > 0 && now >= m_messages[i].expiry_time) {
      ArrayResize(expired_ids, (int)(expired_count + 1), 16);
      expired_ids[expired_count++] = m_messages[i].packet_id;
    }
  }
  for (uint e = 0; e < expired_count; e++) {
    RemoveMessage(expired_ids[e]);
  }
}

//+------------------------------------------------------------------+
//| MakeMessageIndexKey                                              |
//| Purpose: Compose a direction-aware key for packet-id lookups     |
//+------------------------------------------------------------------+
uint CSessionDatabase::MakeMessageIndexKey(const ushort packet_id, const bool is_outgoing) const {
  return (uint)packet_id + (is_outgoing ? 65536 : 0);
}

//+------------------------------------------------------------------+
//| FindMessageByPacketId                                            |
//| Purpose: Find outgoing message index by packet ID                |
//| Parameters: packet_id - packet ID to search for                  |
//| Return: Array index or -1 if not found                           |
//+------------------------------------------------------------------+
int CSessionDatabase::FindMessageByPacketId(const ushort packet_id) { return FindMessageByPacketId(packet_id, true); }

//+------------------------------------------------------------------+
//| FindMessageByPacketId                                            |
//| Purpose: Find message index by packet ID and direction           |
//| Parameters: packet_id - packet ID to search for                  |
//|             is_outgoing - true for client-originated messages    |
//| Return: Array index or -1 if not found                           |
//+------------------------------------------------------------------+
int CSessionDatabase::FindMessageByPacketId(const ushort packet_id, const bool is_outgoing) {
  int idx = -1;
  if (m_id_index.TryGetValue(MakeMessageIndexKey(packet_id, is_outgoing), idx)) {
    return idx;
  }
  return -1;
}

//+------------------------------------------------------------------+
//| FindOfflineQueuedMessageById                                     |
//+------------------------------------------------------------------+
int CSessionDatabase::FindOfflineQueuedMessageById(const ulong queued_id) const {
  if (queued_id == 0) {
    return -1;
  }
  for (uint i = 0; i < m_offline_message_count; i++) {
    if (m_offline_messages[i].queued_id == queued_id) {
      return (int)i;
    }
  }
  return -1;
}

//+------------------------------------------------------------------+
//| FindConsumedOfflineQueuedMessageById                             |
//+------------------------------------------------------------------+
int CSessionDatabase::FindConsumedOfflineQueuedMessageById(const ulong queued_id) const {
  if (queued_id == 0) {
    return -1;
  }
  for (uint i = 0; i < m_consumed_offline_message_count; i++) {
    if (m_consumed_offline_message_ids[i] == queued_id) {
      return (int)i;
    }
  }
  return -1;
}

//+------------------------------------------------------------------+
//| RebuildMessageIndex                                              |
//+------------------------------------------------------------------+
void CSessionDatabase::RebuildMessageIndex() {
  m_id_index.Clear();
  for (uint i = 0; i < m_message_count; i++) {
    m_id_index.Add(MakeMessageIndexKey(m_messages[i].packet_id, m_messages[i].is_outgoing), (int)i);
  }
}

//+------------------------------------------------------------------+
//| FindAvailablePacketId                                            |
//| Purpose: Find an unused packet ID                                |
//| Return: Available packet ID or 0 if none found                   |
//+------------------------------------------------------------------+
ushort CSessionDatabase::FindAvailablePacketId() {
  //--- Round-robin O(1)-typical search using bitmap, starting from m_next_packet_id
  ushort start = m_next_packet_id;
  ushort id    = start;
  do {
    if (!_IdBitTest(id)) {
      return id;
    }
    id++;
    if (id == 0) {
      id = 1;  // ushort wraps 65535->0; skip reserved ID 0
    }
  } while (id != start);
  return 0;    // All 65535 IDs in use
}

//+------------------------------------------------------------------+
//| AllocatePacketId                                                 |
//| Purpose: Allocate next available packet ID                       |
//| Return: Allocated packet ID (1..65535) or 0 if exhausted         |
//| Note: Uses O(1)-typical round-robin search via bitfield.         |
//+------------------------------------------------------------------+
ushort CSessionDatabase::AllocatePacketId() {
  //--- Find an available packet ID
  ushort available_id = FindAvailablePacketId();
  if (available_id == 0) {
    MQTT_LOG_ERROR("No available packet IDs");
    return 0;
  }

  const ushort packet_id = (ushort)available_id;

  //--- Mark in bitmap and advance round-robin cursor
  _IdBitSet(packet_id);
  m_next_packet_id = (packet_id == 65535) ? 1 : (ushort)(packet_id + 1);
  m_in_use_packet_id_count++;

  m_total_allocated_ids++;
  return packet_id;
}

//+------------------------------------------------------------------+
//| ReleasePacketId                                                  |
//| Purpose: Release a packet ID for reuse                           |
//| Parameters: packet_id - packet ID to release (1..65535)          |
//| Return: true if released successfully                            |
//+------------------------------------------------------------------+
bool CSessionDatabase::ReleasePacketId(const ushort packet_id) {
  if (!IsValidPacketId(packet_id) || !_IdBitTest(packet_id)) {
    return false;
  }

  //--- Clear bitmap entry so this ID is available again
  _IdBitClear(packet_id);
  m_in_use_packet_id_count--;

  m_total_released_ids++;
  return true;
}

//+------------------------------------------------------------------+
//| IsPacketIdInUse                                                  |
//| Purpose: Check if a packet ID is currently in use                |
//| Parameters: packet_id - packet ID to check                       |
//| Return: true if in use                                           |
//+------------------------------------------------------------------+
bool CSessionDatabase::IsPacketIdInUse(const ushort packet_id) const {
  if (packet_id == 0) {
    return false;
  }
  return _IdBitTest(packet_id);  // O(1) bitfield lookup
}

//+------------------------------------------------------------------+
//| GetAvailablePacketIdCount                                        |
//| Purpose: Get count of available packet IDs                       |
//| Return: Number of available IDs                                  |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetAvailablePacketIdCount() const {
  uint in_use = GetInUsePacketIdCount();
  return (65535 - in_use);
}

//+------------------------------------------------------------------+
//| GetInUsePacketIdCount                                            |
//| Purpose: Get count of packet IDs in use                          |
//| Return: Number of IDs in use                                     |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetInUsePacketIdCount() const { return m_in_use_packet_id_count; }

//+------------------------------------------------------------------+
//| ResetPacketIds                                                   |
//| Purpose: Release all packet IDs                                  |
//+------------------------------------------------------------------+
void CSessionDatabase::ResetPacketIds() {
  ArrayInitialize(m_id_bitfield, 0);
  m_in_use_packet_id_count = 0;
  m_next_packet_id         = 1;
}

//+------------------------------------------------------------------+
//| StoreOutgoingMessage                                             |
//| Purpose: Store outgoing QoS 1/2 message                          |
//| Parameters: packet_id - packet identifier                        |
//|             qos_level - QoS level (1 or 2)                       |
//|             topic - message topic                                |
//|             payload - message payload                            |
//|             payload_size - size of payload                       |
//|             priority - priority for queuing (0 low, 255 high)    |
//|             expiry_interval - expiry interval in seconds (0 none)|
//| Return: true if stored successfully                              |
//+------------------------------------------------------------------+
bool CSessionDatabase::StoreOutgoingMessage(const ushort packet_id, const uchar qos_level, const string topic,
                                            const uchar& payload[], const uint payload_size, const uchar priority = 0,
                                            const uint expiry_interval = 0) {
  uchar publish_properties[];
  return StoreOutgoingMessage(packet_id, qos_level, topic, payload, payload_size, false, priority, expiry_interval,
                              publish_properties, false);
}

//+------------------------------------------------------------------+
//| StoreOutgoingMessage                                             |
//| Purpose: Store outgoing QoS 1/2 message including retain state   |
//|          and encoded MQTT 5 PUBLISH properties for retransmit.   |
//+------------------------------------------------------------------+
bool CSessionDatabase::StoreOutgoingMessage(const ushort packet_id, const uchar qos_level, const string topic,
                                            const uchar& payload[], const uint payload_size, const bool retain,
                                            const uchar priority, const uint expiry_interval,
                                            const uchar& publish_properties[],
                                            const bool   allow_outgoing_subscription_identifier) {
  return StoreOutgoingMessageRange(packet_id, qos_level, topic, payload, 0, payload_size, retain, priority,
                                   expiry_interval, publish_properties, allow_outgoing_subscription_identifier);
}

//+------------------------------------------------------------------+
//| StoreOutgoingMessageRange                                        |
//| Purpose: Store outgoing QoS 1/2 message from a payload slice     |
//+------------------------------------------------------------------+
bool CSessionDatabase::StoreOutgoingMessageRange(const ushort packet_id, const uchar qos_level, const string topic,
                                                 const uchar& payload[], const uint payload_offset,
                                                 const uint payload_size, const bool retain, const uchar priority,
                                                 const uint expiry_interval, const uchar& publish_properties[],
                                                 const bool allow_outgoing_subscription_identifier) {
  //--- Only store QoS 1 and 2 messages
  if (qos_level < 1 || qos_level > 2) {
    return true;  // QoS 0 doesn't need storage
  }

  //--- Validate packet ID
  if (!IsValidPacketId(packet_id)) {
    MQTT_LOG_ERROR("Invalid packet ID " + (string)(int)packet_id);
    return false;
  }

  //--- Check if message already exists
  const int existing_idx = FindMessageByPacketId(packet_id, true);
  if (existing_idx >= 0) {
    //--- Adjust cached QoS counts when the QoS level changes on update
    uchar old_qos = m_messages[existing_idx].qos_level;
    if (old_qos != qos_level) {
      if (old_qos == 1) {
        m_qos1_count--;
      } else if (old_qos == 2) {
        m_qos2_count--;
      }
      if (qos_level == 1) {
        m_qos1_count++;
      } else if (qos_level == 2) {
        m_qos2_count++;
      }
    }
    //--- Update existing message
    //--- Use TimeLocal() for timestamps - it updates during Sleep() unlike TimeCurrent()
    datetime now                               = TimeLocal();
    m_messages[existing_idx].qos_level         = qos_level;
    //--- Reset QoS 2 state to initial state on update to prevent stale
    //--- state from a prior handshake corrupting the new message flow
    m_messages[existing_idx].qos2_state        = QOS2_STATE_PUBLISH_SENT;
    m_messages[existing_idx].topic             = topic;
    m_messages[existing_idx].timestamp         = now;
    m_messages[existing_idx].mono_timestamp_us = GetMicrosecondCount();  // Monotonic timestamp for retransmission
    m_messages[existing_idx].payload_size      = payload_size;
    m_messages[existing_idx].is_outgoing       = true;
    m_messages[existing_idx].priority          = priority;
    m_messages[existing_idx].expiry_time       = (expiry_interval > 0) ? now + expiry_interval : 0;
    m_messages[existing_idx].retain            = retain;
    m_messages[existing_idx].allow_outgoing_subscription_identifier = allow_outgoing_subscription_identifier;

    ArrayResize(m_messages[existing_idx].payload, payload_size);
    if (payload_size > 0) {
      ArrayCopy(m_messages[existing_idx].payload, payload, 0, (int)payload_offset, (int)payload_size);
    }
    ArrayResize(m_messages[existing_idx].publish_properties, ArraySize(publish_properties));
    if (ArraySize(publish_properties) > 0) {
      ArrayCopy(m_messages[existing_idx].publish_properties, publish_properties);
    }
    return _WriteThroughMutation();
  }

  //--- Add new message
  const uint new_idx = m_message_count;
  ArrayResize(m_messages, new_idx + 1, 64);  // Reserve 64 slots to reduce reallocations under high throughput
  m_message_count++;

  //--- Use TimeLocal() for timestamps - it updates during Sleep() unlike TimeCurrent()
  datetime now                          = TimeLocal();
  m_messages[new_idx].packet_id         = packet_id;
  m_messages[new_idx].qos_level         = qos_level;
  m_messages[new_idx].qos2_state        = QOS2_STATE_PUBLISH_SENT;
  m_messages[new_idx].topic             = topic;
  m_messages[new_idx].timestamp         = now;
  m_messages[new_idx].mono_timestamp_us = GetMicrosecondCount();  // Monotonic timestamp for retransmission
  m_messages[new_idx].payload_size      = payload_size;
  m_messages[new_idx].is_outgoing       = true;
  m_messages[new_idx].retransmit_count  = 0;
  m_messages[new_idx].priority          = priority;
  m_messages[new_idx].expiry_time       = (expiry_interval > 0) ? now + expiry_interval : 0;
  m_messages[new_idx].retain            = retain;
  m_messages[new_idx].allow_outgoing_subscription_identifier = allow_outgoing_subscription_identifier;

  ArrayResize(m_messages[new_idx].payload, payload_size);
  if (payload_size > 0) {
    ArrayCopy(m_messages[new_idx].payload, payload, 0, (int)payload_offset, (int)payload_size);
  }
  ArrayResize(m_messages[new_idx].publish_properties, ArraySize(publish_properties));
  if (ArraySize(publish_properties) > 0) {
    ArrayCopy(m_messages[new_idx].publish_properties, publish_properties);
  }

  //--- Register in HashMap index
  //--- INVARIANT: m_id_index must always map packet_id → the
  //--- current index of that message in m_messages[]. Any operation that
  //--- moves or removes elements (e.g. swap-with-last in RemoveMessage)
  //--- MUST update m_id_index accordingly. Failure to do so causes silent
  //--- data corruption — lookups will return the wrong message.
  m_id_index.Add(MakeMessageIndexKey(packet_id, true), (int)new_idx);

  //--- Update cached QoS counts
  if (qos_level == 1) {
    m_qos1_count++;
  } else if (qos_level == 2) {
    m_qos2_count++;
  }

  m_total_messages_stored++;
  return _WriteThroughMutation();
}

//+------------------------------------------------------------------+
//| StoreIncomingMessage                                             |
//| Purpose: Store incoming QoS 2 message                            |
//| Parameters: packet_id - packet identifier                        |
//|             qos_level - QoS level (1 or 2)                       |
//|             topic - message topic                                |
//|             payload - message payload                            |
//|             payload_size - size of payload                       |
//| Return: true if stored successfully                              |
//+------------------------------------------------------------------+
bool CSessionDatabase::StoreIncomingMessage(const ushort packet_id, const uchar qos_level, const string topic,
                                            const uchar& payload[], const uint payload_size, const bool retain) {
  uchar publish_properties[];
  return StoreIncomingMessage(packet_id, qos_level, topic, payload, payload_size, retain, publish_properties);
}

//+------------------------------------------------------------------+
//| StoreIncomingMessage                                             |
//| Purpose: Store incoming QoS 2 message with raw PUBLISH props     |
//+------------------------------------------------------------------+
bool CSessionDatabase::StoreIncomingMessage(const ushort packet_id, const uchar qos_level, const string topic,
                                            const uchar& payload[], const uint payload_size, const bool retain,
                                            const uchar& publish_properties[]) {
  //--- Only track incoming QoS 2 (for PUBREC/PUBCOMP flow)
  if (qos_level != 2) {
    return true;
  }

  if (!IsValidPacketId(packet_id)) {
    MQTT_LOG_ERROR("Invalid packet ID " + (string)(int)packet_id);
    return false;
  }

  //--- Check if message already exists
  const int existing_idx = FindMessageByPacketId(packet_id, false);
  if (existing_idx >= 0) {
    m_messages[existing_idx].timestamp         = TimeLocal();
    m_messages[existing_idx].mono_timestamp_us = GetMicrosecondCount();  // Monotonic timestamp for retransmission
    m_messages[existing_idx].retain            = retain;
    ArrayResize(m_messages[existing_idx].publish_properties, ArraySize(publish_properties));
    if (ArraySize(publish_properties) > 0) {
      ArrayCopy(m_messages[existing_idx].publish_properties, publish_properties);
    }
    return _WriteThroughMutation();
  }

  //--- Add new message
  const uint new_idx = m_message_count;
  ArrayResize(m_messages, new_idx + 1, 64);  // Reserve 64 slots to reduce reallocations under high throughput
  m_message_count++;

  m_messages[new_idx].packet_id  = packet_id;
  m_messages[new_idx].qos_level  = qos_level;
  m_messages[new_idx].qos2_state = QOS2_STATE_PUBLISH_RECEIVED;   // Incoming QoS 2, PUBREC sent, waiting for PUBREL
  m_messages[new_idx].topic      = topic;
  m_messages[new_idx].timestamp  = TimeLocal();
  m_messages[new_idx].mono_timestamp_us = GetMicrosecondCount();  // Monotonic timestamp for retransmission
  m_messages[new_idx].payload_size      = payload_size;
  m_messages[new_idx].is_outgoing       = false;
  m_messages[new_idx].retransmit_count  = 0;
  m_messages[new_idx].expiry_time       = 0;
  m_messages[new_idx].priority          = 0;
  m_messages[new_idx].retain            = retain;
  m_messages[new_idx].allow_outgoing_subscription_identifier = false;

  ArrayResize(m_messages[new_idx].payload, payload_size);
  ArrayCopy(m_messages[new_idx].payload, payload, 0, 0, payload_size);
  ArrayResize(m_messages[new_idx].publish_properties, ArraySize(publish_properties));
  if (ArraySize(publish_properties) > 0) {
    ArrayCopy(m_messages[new_idx].publish_properties, publish_properties);
  }

  //--- Register in HashMap index
  m_id_index.Add(MakeMessageIndexKey(packet_id, false), (int)new_idx);

  //--- Update cached QoS counts
  if (qos_level == 1) {
    m_qos1_count++;
  } else if (qos_level == 2) {
    m_qos2_count++;
  }

  return _WriteThroughMutation();
}

//+------------------------------------------------------------------+
//| StoreOfflineQueuedMessage                                        |
//| Purpose: Persist an accepted offline QoS 1/2 publish before it   |
//|          has a packet identifier or enters the in-flight store.  |
//+------------------------------------------------------------------+
ulong CSessionDatabase::StoreOfflineQueuedMessage(const uchar qos_level, const string topic, const uchar& payload[],
                                                  const uint payload_size, const bool retain,
                                                  const uint expiry_interval) {
  uchar publish_properties[];
  return StoreOfflineQueuedMessage(qos_level, topic, payload, payload_size, retain, expiry_interval, publish_properties,
                                   false);
}

//+------------------------------------------------------------------+
//| StoreOfflineQueuedMessage                                        |
//+------------------------------------------------------------------+
ulong CSessionDatabase::StoreOfflineQueuedMessage(const uchar qos_level, const string topic, const uchar& payload[],
                                                  const uint payload_size, const bool retain,
                                                  const uint expiry_interval, const uchar& publish_properties[],
                                                  const bool allow_outgoing_subscription_identifier) {
  if (qos_level < 1 || qos_level > 2) {
    return 0;
  }

  const uint new_idx = m_offline_message_count;
  ArrayResize(m_offline_messages, (int)(new_idx + 1), 32);
  m_offline_message_count++;

  datetime now       = TimeLocal();
  ulong    queued_id = m_next_offline_message_id;
  if (queued_id == 0) {
    queued_id = 1;
  }
  m_next_offline_message_id = queued_id + 1;
  if (m_next_offline_message_id == 0) {
    m_next_offline_message_id = 1;
  }

  m_offline_messages[new_idx].queued_id                = queued_id;
  m_offline_messages[new_idx].qos_level                = qos_level;
  m_offline_messages[new_idx].topic                    = topic;
  m_offline_messages[new_idx].payload_size             = payload_size;
  m_offline_messages[new_idx].retain                   = retain;
  m_offline_messages[new_idx].timestamp                = now;
  m_offline_messages[new_idx].expiry_time              = (expiry_interval > 0) ? now + expiry_interval : 0;
  m_offline_messages[new_idx].mono_timestamp_us        = (expiry_interval > 0) ? GetMicrosecondCount() : 0;
  m_offline_messages[new_idx].remaining_expiry_seconds = expiry_interval;
  m_offline_messages[new_idx].allow_outgoing_subscription_identifier = allow_outgoing_subscription_identifier;

  ArrayResize(m_offline_messages[new_idx].payload, (int)payload_size);
  if (payload_size > 0) {
    ArrayCopy(m_offline_messages[new_idx].payload, payload, 0, 0, (int)payload_size);
  }
  ArrayResize(m_offline_messages[new_idx].publish_properties, ArraySize(publish_properties));
  if (ArraySize(publish_properties) > 0) {
    ArrayCopy(m_offline_messages[new_idx].publish_properties, publish_properties);
  }

  if (m_is_persistent) {
    m_dirty = true;
  }
  return queued_id;
}

//+------------------------------------------------------------------+
//| UpdateQoS2State                                                  |
//| Purpose: Update QoS 2 state for a message                        |
//| Parameters: packet_id - packet identifier                        |
//|             new_state - new QoS 2 state                          |
//| Return: true if updated successfully                             |
//+------------------------------------------------------------------+
bool CSessionDatabase::UpdateQoS2State(const ushort packet_id, const ENUM_QOS2_STATE new_state) {
  const int idx = FindMessageByPacketId(packet_id, true);
  if (idx < 0) {
    return false;
  }

  //--- Can only update QoS 2 messages
  if (m_messages[idx].qos_level != 2) {
    return false;
  }

  //--- Validate state transition per MQTT §4.3.3
  //--- Valid transitions:
  //---   QOS2_STATE_NONE            → QOS2_STATE_PUBLISH_SENT
  //---   QOS2_STATE_PUBLISH_SENT    → QOS2_STATE_PUBREC_RECEIVED
  //---   QOS2_STATE_PUBREC_RECEIVED → QOS2_STATE_PUBCOMP_RECEIVED
  ENUM_QOS2_STATE current_state = m_messages[idx].qos2_state;
  bool            valid         = false;

  switch (new_state) {
    case QOS2_STATE_PUBLISH_SENT:
      valid = (current_state == QOS2_STATE_NONE);
      break;
    case QOS2_STATE_PUBREC_RECEIVED:
      valid = (current_state == QOS2_STATE_PUBLISH_SENT);
      break;
    case QOS2_STATE_PUBCOMP_RECEIVED:
      valid = (current_state == QOS2_STATE_PUBREC_RECEIVED);
      break;
    default:
      valid = false;
      break;
  }

  if (!valid) {
    MQTT_LOG_ERROR("Invalid QoS 2 state transition from " + EnumToString(current_state) + " to "
                   + EnumToString(new_state) + " for packet ID " + (string)(int)packet_id + " per MQTT §4.3.3");
    return false;
  }

  m_messages[idx].qos2_state = new_state;
  m_messages[idx].timestamp  = TimeLocal();
  return _WriteThroughMutation();
}

//+------------------------------------------------------------------+
//| TouchMessage                                                     |
//| Purpose: Update timestamp of a message (reset retransmission)    |
//| Parameters: packet_id - packet ID of message to touch            |
//| Return: true if message found and touched                        |
//+------------------------------------------------------------------+
bool CSessionDatabase::TouchMessage(const ushort packet_id) {
  const int idx = FindMessageByPacketId(packet_id, true);
  if (idx < 0) {
    return false;
  }

  //--- Use TimeLocal() for timestamps - it updates during Sleep() unlike TimeCurrent()
  //--- and must match the time source used in GetStalledMessages/GetPendingMessages
  m_messages[idx].timestamp         = TimeLocal();
  m_messages[idx].mono_timestamp_us = GetMicrosecondCount();  // Monotonic timestamp for retransmission
  m_messages[idx].retransmit_count++;
  return _WriteThroughMutation();
}

//+------------------------------------------------------------------+
//| RemoveMessage                                                    |
//| Purpose: Remove a message from storage                           |
//| Parameters: packet_id - packet identifier of message to remove   |
//| Return: true if message was found and removed                    |
//+------------------------------------------------------------------+
bool CSessionDatabase::RemoveMessage(const ushort packet_id) { return RemoveMessage(packet_id, true); }

//+------------------------------------------------------------------+
//| RemoveMessage                                                    |
//| Purpose: Remove a message from storage by direction and packet ID|
//+------------------------------------------------------------------+
bool CSessionDatabase::RemoveMessage(const ushort packet_id, const bool is_outgoing) {
  const int idx = FindMessageByPacketId(packet_id, is_outgoing);
  if (idx < 0) {
    return false;
  }

  bool           was_dirty                     = m_dirty;
  uint           backup_message_count          = m_message_count;
  uint           backup_qos1_count             = m_qos1_count;
  uint           backup_qos2_count             = m_qos2_count;
  uint           backup_total_messages_removed = m_total_messages_removed;
  uint           backup_total_released_ids     = m_total_released_ids;
  uint           backup_in_use_packet_id_count = m_in_use_packet_id_count;
  SessionMessage backup_messages[];
  uint           backup_id_bitfield[];

  if (m_is_persistent) {
    ArrayResize(backup_messages, (int)m_message_count);
    for (uint i = 0; i < m_message_count; i++) {
      backup_messages[i] = m_messages[i];
    }

    ArrayResize(backup_id_bitfield, ArraySize(m_id_bitfield));
    ArrayCopy(backup_id_bitfield, m_id_bitfield);
  }

  bool removed_is_outgoing = m_messages[idx].is_outgoing;

  //--- Remove from HashMap index
  m_id_index.Remove(MakeMessageIndexKey(packet_id, is_outgoing));

  //--- Update cached QoS counts BEFORE the swap (idx still refers to original message)
  if (m_messages[idx].qos_level == 1 && m_qos1_count > 0) {
    m_qos1_count--;
  } else if (m_messages[idx].qos_level == 2 && m_qos2_count > 0) {
    m_qos2_count--;
  }

  //--- Swap-with-last O(1) removal: move the last element into the vacated slot
  //--- instead of shifting all subsequent elements, eliminating the O(n) shift.
  const uint last = m_message_count - 1;
  if ((uint)idx < last) {
    ushort moved_id          = m_messages[last].packet_id;
    bool   moved_is_outgoing = m_messages[last].is_outgoing;
    m_messages[idx]          = m_messages[last];
    //--- Update the index entry for the moved element to reflect its new position
    m_id_index.Remove(MakeMessageIndexKey(moved_id, moved_is_outgoing));
    m_id_index.Add(MakeMessageIndexKey(moved_id, moved_is_outgoing), idx);
  }

  m_message_count--;
  ArrayResize(m_messages, m_message_count);

  //--- Only locally allocated outgoing packet IDs participate in the allocator bitmap.
  if (removed_is_outgoing) {
    ReleasePacketId(packet_id);
  }

  m_total_messages_removed++;
  if (_WriteThroughMutation()) {
    return true;
  }

  if (m_is_persistent) {
    ArrayResize(m_messages, (int)backup_message_count);
    for (uint i = 0; i < backup_message_count; i++) {
      m_messages[i] = backup_messages[i];
    }

    ArrayCopy(m_id_bitfield, backup_id_bitfield);
    m_message_count          = backup_message_count;
    m_qos1_count             = backup_qos1_count;
    m_qos2_count             = backup_qos2_count;
    m_total_messages_removed = backup_total_messages_removed;
    m_total_released_ids     = backup_total_released_ids;
    m_in_use_packet_id_count = backup_in_use_packet_id_count;
    m_dirty                  = was_dirty;
    RebuildMessageIndex();
  }

  return false;
}

//+------------------------------------------------------------------+
//| RemoveOfflineQueuedMessage                                       |
//+------------------------------------------------------------------+
bool CSessionDatabase::RemoveOfflineQueuedMessage(const ulong queued_id) {
  const int idx = FindOfflineQueuedMessageById(queued_id);
  if (idx < 0) {
    return false;
  }

  bool                 was_dirty = m_dirty;
  OfflineQueuedMessage backup[];
  if (m_is_persistent) {
    ArrayResize(backup, (int)m_offline_message_count);
    for (uint i = 0; i < m_offline_message_count; i++) {
      backup[i] = m_offline_messages[i];
    }
  }

  for (uint i = (uint)idx + 1; i < m_offline_message_count; i++) {
    m_offline_messages[i - 1] = m_offline_messages[i];
  }
  m_offline_message_count--;
  ArrayResize(m_offline_messages, (int)m_offline_message_count);

  if (m_is_persistent) {
    m_dirty = true;
    if (!SaveToFile()) {
      ArrayResize(m_offline_messages, ArraySize(backup));
      m_offline_message_count = (uint)ArraySize(backup);
      for (uint i = 0; i < m_offline_message_count; i++) {
        m_offline_messages[i] = backup[i];
      }
      m_dirty = was_dirty;
      return false;
    }
  }
  return true;
}

//+------------------------------------------------------------------+
//| FinalizeOfflineQueuedMessage                                     |
//| Persist that an accepted offline queued publish has been         |
//| consumed and must never be restored again, even if the durable   |
//| offline row cannot be deleted immediately.                       |
//+------------------------------------------------------------------+
bool CSessionDatabase::FinalizeOfflineQueuedMessage(const ulong queued_id) {
  if (queued_id == 0) {
    return true;
  }

  if (FindOfflineQueuedMessageById(queued_id) < 0) {
    return true;
  }

  if (!m_is_persistent) {
    int idx = FindOfflineQueuedMessageById(queued_id);
    return (idx < 0) ? true : RemoveOfflineQueuedMessage(queued_id);
  }

  bool force_fallback                         = m_test_force_finalize_offline_fallback_once;
  m_test_force_finalize_offline_fallback_once = false;

  if (!force_fallback && RemoveOfflineQueuedMessage(queued_id)) {
    return true;
  }

  if (FindConsumedOfflineQueuedMessageById(queued_id) >= 0) {
    MQTT_LOG_WARN("Offline queued publish id=" + (string)queued_id
                  + " is already marked consumed; restore will continue skipping it until cleanup succeeds.");
    return true;
  }

  bool  was_dirty = m_dirty;
  ulong backup_ids[];
  ArrayResize(backup_ids, (int)m_consumed_offline_message_count);
  for (uint i = 0; i < m_consumed_offline_message_count; i++) {
    backup_ids[i] = m_consumed_offline_message_ids[i];
  }

  ArrayResize(m_consumed_offline_message_ids, (int)(m_consumed_offline_message_count + 1), 8);
  m_consumed_offline_message_ids[m_consumed_offline_message_count] = queued_id;
  m_consumed_offline_message_count++;
  m_dirty = true;
  if (!SaveToFile()) {
    ArrayResize(m_consumed_offline_message_ids, ArraySize(backup_ids));
    m_consumed_offline_message_count = (uint)ArraySize(backup_ids);
    for (uint i = 0; i < m_consumed_offline_message_count; i++) {
      m_consumed_offline_message_ids[i] = backup_ids[i];
    }
    m_dirty = was_dirty;
    return false;
  }

  MQTT_LOG_WARN("Persisted consumed fallback marker for offline queued publish id=" + (string)queued_id
                + "; restore will skip it until durable cleanup can be committed.");
  return true;
}

//+------------------------------------------------------------------+
//| GetMessage                                                       |
//| Purpose: Get message by packet ID                                |
//| Parameters: packet_id - packet identifier                        |
//|             out_msg - output parameter for message               |
//| Return: true if message found                                    |
//+------------------------------------------------------------------+
bool CSessionDatabase::GetMessage(const ushort packet_id, SessionMessage& out_msg) {
  return GetMessage(packet_id, out_msg, true);
}

//+------------------------------------------------------------------+
//| GetMessage                                                       |
//| Purpose: Get message by packet ID and direction                  |
//+------------------------------------------------------------------+
bool CSessionDatabase::GetMessage(const ushort packet_id, SessionMessage& out_msg, const bool is_outgoing) {
  const int idx = FindMessageByPacketId(packet_id, is_outgoing);
  if (idx < 0) {
    return false;
  }

  out_msg = m_messages[idx];
  return true;
}

//+------------------------------------------------------------------+
//| GetPendingMessages                                               |
//| Purpose: Get all pending messages                                |
//| Parameters: dest - output array                                  |
//|             outgoing_only - if true, only return outgoing msgs   |
//| Return: Count of messages copied                                 |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetPendingMessages(SessionMessage& dest[], const bool outgoing_only) {
  ArrayResize(dest, 0);
  uint count = 0;

  //--- Purge expired messages before collecting
  _PurgeExpiredMessages();

  //--- Collect pending messages from the now-clean collection
  for (uint i = 0; i < m_message_count; i++) {
    if (outgoing_only && !m_messages[i].is_outgoing) {
      continue;
    }

    //--- Use reserve to avoid frequent reallocations in output array
    ArrayResize(dest, count + 1, 16);
    dest[count] = m_messages[i];
    count++;
  }

  //--- Priority sort is now opt-in. Only sort when at least one message
  //--- has a non-default priority, preserving MQTT §4.4 message ordering for
  //--- same-QoS messages when no custom priority is set.
  bool has_custom_priority = false;
  for (uint i = 0; i < count && !has_custom_priority; i++) {
    if (dest[i].priority != 0) {
      has_custom_priority = true;
    }
  }
  if (has_custom_priority) {
    //--- Log runtime warning when priority sort overrides §4.4 ordering
    MQTT_LOG_WARN("Custom priority sort active — §4.4 message ordering not guaranteed");
    //--- Sort by priority descending (higher priority first) using insertion sort
    //--- O(n) best-case for nearly-sorted data; stable sort preserving arrival order.
    //--- WARNING: Custom priority reorders messages within the same QoS level,
    //--- which may violate MQTT §4.4 ordering guarantees.
    for (uint i = 1; i < count; i++) {
      SessionMessage key = dest[i];
      int            j   = (int)i - 1;
      while (j >= 0 && dest[j].priority < key.priority) {
        dest[j + 1] = dest[j];
        j--;
      }
      dest[j + 1] = key;
    }
  }

  return count;
}

//+------------------------------------------------------------------+
//| GetIncomingMessages                                              |
//| Purpose: Get all non-outgoing messages (incoming from broker).   |
//|          Used to rebuild CFlowControl incoming state after an    |
//|          EA restart so that PUBREL for loaded QoS-2 messages     |
//|          does not underflow the incoming-inflight counter        |
//| Parameters: dest - output array                                  |
//| Return: Count of incoming messages copied                        |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetIncomingMessages(SessionMessage& dest[]) {
  ArrayResize(dest, 0);
  uint count = 0;
  for (uint i = 0; i < m_message_count; i++) {
    if (!m_messages[i].is_outgoing) {
      ArrayResize(dest, count + 1, 16);
      dest[count] = m_messages[i];
      count++;
    }
  }
  return count;
}

//+------------------------------------------------------------------+
//| GetOfflineQueuedMessages                                         |
//| Purpose: Return accepted offline QoS messages in enqueue order.  |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetOfflineQueuedMessages(OfflineQueuedMessage& dest[]) {
  ArrayResize(dest, 0);
  uint     count    = 0;
  datetime now_time = TimeLocal();
  ulong    now_us   = GetMicrosecondCount();

  _PurgeExpiredOfflineQueuedMessages();
  _PurgeConsumedOfflineQueuedMessages();

  for (uint i = 0; i < m_offline_message_count; i++) {
    if (FindConsumedOfflineQueuedMessageById(m_offline_messages[i].queued_id) >= 0) {
      continue;
    }
    ArrayResize(dest, (int)(count + 1), 16);
    dest[count]                          = m_offline_messages[i];
    dest[count].remaining_expiry_seconds = _GetOfflineRemainingExpirySeconds(m_offline_messages[i], now_time, now_us);
    dest[count].mono_timestamp_us        = (dest[count].remaining_expiry_seconds > 0) ? now_us : 0;
    if (dest[count].remaining_expiry_seconds > 0) {
      dest[count].expiry_time = now_time + (datetime)dest[count].remaining_expiry_seconds;
    }
    count++;
  }

  return count;
}

//+------------------------------------------------------------------+
//| GetQoS1Messages                                                  |
//| Purpose: Get all QoS 1 messages                                  |
//| Parameters: dest - output array                                  |
//| Return: Count of messages copied                                 |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetQoS1Messages(SessionMessage& dest[]) {
  ArrayResize(dest, 0);
  uint count = 0;

  for (uint i = 0; i < m_message_count; i++) {
    if (m_messages[i].qos_level == 1) {
      ArrayResize(dest, count + 1, 16);
      dest[count] = m_messages[i];
      count++;
    }
  }

  return count;
}

//+------------------------------------------------------------------+
//| GetQoS2Messages                                                  |
//| Purpose: Get all QoS 2 messages                                  |
//| Parameters: dest - output array                                  |
//| Return: Count of messages copied                                 |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetQoS2Messages(SessionMessage& dest[]) {
  ArrayResize(dest, 0);
  uint count = 0;

  for (uint i = 0; i < m_message_count; i++) {
    if (m_messages[i].qos_level == 2) {
      ArrayResize(dest, count + 1, 16);
      dest[count] = m_messages[i];
      count++;
    }
  }

  return count;
}

//+------------------------------------------------------------------+
//| GetMessagesByQoS2State                                           |
//| Purpose: Get messages by QoS 2 state                             |
//| Parameters: dest - output array                                  |
//|             state - QoS 2 state to filter by                     |
//| Return: Count of messages copied                                 |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetMessagesByQoS2State(SessionMessage& dest[], const ENUM_QOS2_STATE state) {
  ArrayResize(dest, 0);
  uint count = 0;

  for (uint i = 0; i < m_message_count; i++) {
    if (m_messages[i].qos_level == 2 && m_messages[i].qos2_state == state) {
      ArrayResize(dest, count + 1, 16);
      dest[count] = m_messages[i];
      count++;
    }
  }

  return count;
}

//+------------------------------------------------------------------+
//| GetStalledMessages                                               |
//| Purpose: Get messages that need retransmission (timeout reached) |
//| Parameters: dest - output array                                  |
//|             timeout_seconds - timeout threshold                  |
//| Return: Count of stalled messages                                |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetStalledMessages(SessionMessage& dest[], const uint timeout_seconds) {
  ArrayResize(dest, 0);
  uint        count       = 0;
  //--- Use monotonic GetMicrosecondCount() for retransmission timeout
  //--- to avoid clock-skew issues (NTP sync, DST transitions, VM drift).
  const ulong mono_now_us = GetMicrosecondCount();

  //--- Scan the now-clean collection for stalled messages
  for (uint i = 0; i < m_message_count; i++) {
    //--- Only outgoing messages are subject to retransmission
    if (!m_messages[i].is_outgoing) {
      continue;
    }

    //--- Use monotonic elapsed time for retransmission timeout check
    ulong elapsed_us = mono_now_us - m_messages[i].mono_timestamp_us;
    ulong timeout_us = (ulong)timeout_seconds * 1000000;
    if (elapsed_us >= timeout_us) {
      //--- For QoS 2, we only retransmit if not completed
      if (m_messages[i].qos_level == 2 && m_messages[i].qos2_state == QOS2_STATE_PUBCOMP_RECEIVED) {
        continue;
      }

      ArrayResize(dest, count + 1, 16);
      dest[count] = m_messages[i];
      count++;
    }
  }

  return count;
}

//+------------------------------------------------------------------+
//| HasPendingMessages                                               |
//| Purpose: Check if there are pending messages                     |
//| Return: true if messages pending                                 |
//+------------------------------------------------------------------+
bool CSessionDatabase::HasPendingMessages() const { return (m_message_count > 0); }

//+------------------------------------------------------------------+
//| HasOfflineQueuedMessages                                         |
//+------------------------------------------------------------------+
bool CSessionDatabase::HasOfflineQueuedMessages() const { return (_GetVisibleOfflineQueuedMessageCount() > 0); }

//+------------------------------------------------------------------+
//| GetPendingMessageCount                                           |
//| Purpose: Get total pending message count                         |
//| Return: Count of pending messages                                |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetPendingMessageCount() const { return m_message_count; }

//+------------------------------------------------------------------+
//| GetOfflineQueuedMessageCount                                     |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetOfflineQueuedMessageCount() const { return _GetVisibleOfflineQueuedMessageCount(); }

//+------------------------------------------------------------------+
//| GetPendingQoS1Count                                              |
//| Purpose: Get pending QoS 1 message count                         |
//| Return: Count of QoS 1 messages                                  |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetPendingQoS1Count() const {
  return m_qos1_count;  // O(1) cached count
}

//+------------------------------------------------------------------+
//| GetPendingQoS2Count                                              |
//| Purpose: Get pending QoS 2 message count                         |
//| Return: Count of QoS 2 messages                                  |
//+------------------------------------------------------------------+
uint CSessionDatabase::GetPendingQoS2Count() const {
  return m_qos2_count;  // O(1) cached count
}

//+------------------------------------------------------------------+
//| GetQoS2State                                                     |
//| Purpose: Get QoS 2 state for a message                           |
//| Parameters: packet_id - packet identifier                        |
//| Return: QoS 2 state (QOS2_STATE_NONE if not found)               |
//+------------------------------------------------------------------+
ENUM_QOS2_STATE CSessionDatabase::GetQoS2State(const ushort packet_id) { return GetQoS2State(packet_id, true); }

//+------------------------------------------------------------------+
//| GetQoS2State                                                     |
//| Purpose: Get QoS 2 state for a message by direction              |
//+------------------------------------------------------------------+
ENUM_QOS2_STATE CSessionDatabase::GetQoS2State(const ushort packet_id, const bool is_outgoing) {
  //--- O(1) HashMap lookup instead of O(n) linear scan
  const int idx = FindMessageByPacketId(packet_id, is_outgoing);
  if (idx >= 0 && (uint)idx < m_message_count) {
    return m_messages[idx].qos2_state;
  }
  return QOS2_STATE_NONE;
}

//+------------------------------------------------------------------+
//| ClearAllMessages                                                 |
//| Purpose: Remove all messages                                     |
//+------------------------------------------------------------------+
void CSessionDatabase::ClearAllMessages() {
  for (uint i = 0; i < m_message_count; i++) {
    ArrayResize(m_messages[i].payload, 0);
  }
  ArrayResize(m_messages, 0);
  m_message_count = 0;
  m_id_index.Clear();  // Reset HashMap index
  m_qos1_count = 0;
  m_qos2_count = 0;
}

//+------------------------------------------------------------------+
//| ClearOfflineQueuedMessages                                       |
//+------------------------------------------------------------------+
void CSessionDatabase::ClearOfflineQueuedMessages() {
  for (uint i = 0; i < m_offline_message_count; i++) {
    ArrayResize(m_offline_messages[i].payload, 0);
    ArrayResize(m_offline_messages[i].publish_properties, 0);
  }
  ArrayResize(m_offline_messages, 0);
  m_offline_message_count   = 0;
  m_next_offline_message_id = 1;
  ArrayResize(m_consumed_offline_message_ids, 0);
  m_consumed_offline_message_count = 0;
}

//+------------------------------------------------------------------+
//| ClearCompletedQoS2                                               |
//| Purpose: Remove completed QoS 2 messages                         |
//+------------------------------------------------------------------+
void CSessionDatabase::ClearCompletedQoS2() {
  //--- Collect-then-remove pattern to avoid skipping elements
  //--- when swap-and-remove reorders during reverse iteration.
  ushort completed_ids[];
  uint   completed_count = 0;
  for (uint i = 0; i < m_message_count; i++) {
    if (m_messages[i].qos_level == 2 && m_messages[i].qos2_state == QOS2_STATE_PUBCOMP_RECEIVED) {
      ArrayResize(completed_ids, completed_count + 1, 16);
      completed_ids[completed_count++] = m_messages[i].packet_id;
    }
  }
  for (uint e = 0; e < completed_count; e++) {
    RemoveMessage(completed_ids[e]);
  }
}

//+------------------------------------------------------------------+
//| Clear                                                            |
//| Purpose: Clear all state                                         |
//+------------------------------------------------------------------+
void CSessionDatabase::Clear() {
  //--- Delete session file if persistent (clean up persisted state)
  //--- Terminal-local storage
  if (m_is_persistent && StringLen(m_session_id) > 0) {
    string file_name = "MQTT_Sessions\\" + m_session_id + ".bin";
    FileDelete(file_name);
    //--- Also clean up any leftover temp file
    string tmp_name = "MQTT_Sessions\\" + m_session_id + ".bin.tmp";
    FileDelete(tmp_name);
  }

  ClearAllMessages();
  ClearOfflineQueuedMessages();
  ResetPacketIds();
  m_reconnect_failure_count                   = 0;
  m_incoming_storage_error_count              = 0;
  m_session_id                                = "";
  m_is_persistent                             = false;
  m_test_force_finalize_offline_fallback_once = false;
}

//+------------------------------------------------------------------+
//| PrintStatistics                                                  |
//| Purpose: Print database statistics to log                        |
//+------------------------------------------------------------------+
void CSessionDatabase::PrintStatistics() {
  MQTT_LOG_DEBUG("=== Session Database Statistics ===");
  MQTT_LOG_DEBUG("Session ID: " + m_session_id);
  MQTT_LOG_DEBUG("Persistent: " + (m_is_persistent ? "Yes" : "No"));
  MQTT_LOG_DEBUG("Packet IDs in use: " + (string)GetInUsePacketIdCount());
  MQTT_LOG_DEBUG("Packet IDs available: " + (string)GetAvailablePacketIdCount());
  MQTT_LOG_DEBUG("Total allocated IDs: " + (string)m_total_allocated_ids);
  MQTT_LOG_DEBUG("Total released IDs: " + (string)m_total_released_ids);
  MQTT_LOG_DEBUG("Pending messages: " + (string)m_message_count);
  MQTT_LOG_DEBUG("Offline queued publishes: " + (string)GetOfflineQueuedMessageCount());
  if (m_consumed_offline_message_count > 0) {
    MQTT_LOG_DEBUG("Consumed offline queued cleanup markers: " + (string)m_consumed_offline_message_count);
  }
  MQTT_LOG_DEBUG("  QoS 1: " + (string)GetPendingQoS1Count());
  MQTT_LOG_DEBUG("  QoS 2: " + (string)GetPendingQoS2Count());
  MQTT_LOG_DEBUG("Total messages stored: " + (string)m_total_messages_stored);
  MQTT_LOG_DEBUG("Total messages removed: " + (string)m_total_messages_removed);
  MQTT_LOG_DEBUG("===================================");
}

//+------------------------------------------------------------------+
//| _PurgeExpiredOfflineQueuedMessages                               |
//+------------------------------------------------------------------+
void CSessionDatabase::_PurgeExpiredOfflineQueuedMessages() {
  if (m_offline_message_count == 0) {
    return;
  }

  const datetime now       = TimeLocal();
  const ulong    now_us    = GetMicrosecondCount();
  uint           write_idx = 0;
  bool           dropped   = false;
  for (uint i = 0; i < m_offline_message_count; i++) {
    if (_IsOfflineQueuedMessageExpired(m_offline_messages[i], now, now_us)) {
      dropped = true;
      continue;
    }
    if (write_idx != i) {
      m_offline_messages[write_idx] = m_offline_messages[i];
    }
    write_idx++;
  }

  if (dropped) {
    m_offline_message_count = write_idx;
    ArrayResize(m_offline_messages, (int)m_offline_message_count);
    if (m_is_persistent && !_WriteThroughMutation()) {
      MQTT_LOG_ERROR("Failed to persist expired offline queued publish purge for session " + m_session_id);
    }
  }
}

//+------------------------------------------------------------------+
//| _GetVisibleOfflineQueuedMessageCount                             |
//+------------------------------------------------------------------+
uint CSessionDatabase::_GetVisibleOfflineQueuedMessageCount() const {
  uint     visible = 0;
  datetime now     = TimeLocal();
  ulong    now_us  = GetMicrosecondCount();
  for (uint i = 0; i < m_offline_message_count; i++) {
    if (FindConsumedOfflineQueuedMessageById(m_offline_messages[i].queued_id) < 0
        && !_IsOfflineQueuedMessageExpired(m_offline_messages[i], now, now_us)) {
      visible++;
    }
  }
  return visible;
}

//+------------------------------------------------------------------+
//| _PurgeConsumedOfflineQueuedMessages                              |
//+------------------------------------------------------------------+
bool CSessionDatabase::_PurgeConsumedOfflineQueuedMessages() {
  if (m_consumed_offline_message_count == 0) {
    return true;
  }

  bool                 changed   = true;
  bool                 was_dirty = m_dirty;

  OfflineQueuedMessage backup_messages[];
  ulong                backup_consumed_ids[];
  if (m_is_persistent) {
    ArrayResize(backup_messages, (int)m_offline_message_count);
    for (uint i = 0; i < m_offline_message_count; i++) {
      backup_messages[i] = m_offline_messages[i];
    }
    ArrayResize(backup_consumed_ids, (int)m_consumed_offline_message_count);
    for (uint i = 0; i < m_consumed_offline_message_count; i++) {
      backup_consumed_ids[i] = m_consumed_offline_message_ids[i];
    }
  }

  uint write_idx = 0;
  for (uint i = 0; i < m_offline_message_count; i++) {
    if (FindConsumedOfflineQueuedMessageById(m_offline_messages[i].queued_id) >= 0) {
      continue;
    }
    if (write_idx != i) {
      m_offline_messages[write_idx] = m_offline_messages[i];
    }
    write_idx++;
  }

  m_offline_message_count = write_idx;
  ArrayResize(m_offline_messages, (int)m_offline_message_count);
  ArrayResize(m_consumed_offline_message_ids, 0);
  m_consumed_offline_message_count = 0;

  if (m_is_persistent) {
    m_dirty = true;
    if (!SaveToFile()) {
      ArrayResize(m_offline_messages, ArraySize(backup_messages));
      m_offline_message_count = (uint)ArraySize(backup_messages);
      for (uint i = 0; i < m_offline_message_count; i++) {
        m_offline_messages[i] = backup_messages[i];
      }
      ArrayResize(m_consumed_offline_message_ids, ArraySize(backup_consumed_ids));
      m_consumed_offline_message_count = (uint)ArraySize(backup_consumed_ids);
      for (uint i = 0; i < m_consumed_offline_message_count; i++) {
        m_consumed_offline_message_ids[i] = backup_consumed_ids[i];
      }
      m_dirty = was_dirty;
      return false;
    }
  }

  return changed;
}

#endif  // MQTT_DB_MQH
