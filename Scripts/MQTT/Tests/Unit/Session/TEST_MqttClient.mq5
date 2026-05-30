//+------------------------------------------------------------------+
//|                                              TEST_MqttClient.mq5 |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Unit tests for the CMqttClient facade.                           |
//| Tests are offline (no broker required) — they exercise           |
//| configuration, state transitions, subscription registry,         |
//| callback registration, and publish queue without actually        |
//| connecting to a network.                                         |
//+------------------------------------------------------------------+
#define MQTT_UNIT_TESTS
#define TESTUTIL_SKIP_MQTT_INCLUDE
#include "..\..\..\..\..\Include\MQTT\MQTT.mqh"
#include "..\..\TestUtil.mqh"
#undef TESTUTIL_SKIP_MQTT_INCLUDE
#undef MQTT_UNIT_TESTS

//+------------------------------------------------------------------+
//| Test transport mock                                              |
//+------------------------------------------------------------------+
class CTestTransport : public IMqttTransport {
 public:
  bool         m_connected;
  int          m_send_calls;
  int          m_fail_on_send_call;  // 0 = never fail
  int          m_poll_calls;
  uint         m_keepalive_seconds;
  uint         m_blocking_warn_threshold_ms;
  ulong        m_last_blocking_duration_us;
  PacketBuffer m_incoming[];
  uint         m_incoming_count;
  PacketBuffer m_sent[];
  uint         m_sent_count;

  CTestTransport() {
    m_connected                  = true;
    m_send_calls                 = 0;
    m_fail_on_send_call          = 0;
    m_poll_calls                 = 0;
    m_keepalive_seconds          = 0;
    m_blocking_warn_threshold_ms = 0;
    m_last_blocking_duration_us  = 0;
    m_incoming_count             = 0;
    m_sent_count                 = 0;
  }

  void EnqueueIncoming(const uchar &pkt[]) {
    ArrayResize(m_incoming, m_incoming_count + 1);
    int len = ArraySize(pkt);
    ArrayResize(m_incoming[m_incoming_count].data, len);
    if (len > 0) {
      ArrayCopy(m_incoming[m_incoming_count].data, pkt);
    }
    m_incoming_count++;
  }

  void ClearSentPackets() {
    ArrayResize(m_sent, 0);
    m_sent_count = 0;
  }

  virtual void                         Disconnect() override { m_connected = false; }

  virtual bool                         IsConnected() const override { return m_connected; }

  virtual bool                         IsConnecting() const override { return false; }

  virtual ENUM_TRANSPORT_CONNECT_PHASE GetConnectPhase() const override {
    return m_connected ? TRANSPORT_PHASE_CONNECTED : TRANSPORT_PHASE_IDLE;
  }

  virtual ENUM_TRANSPORT_ERROR Send(const uchar &pkt[], int len = -1) override {
    m_send_calls++;
    if (m_fail_on_send_call > 0 && m_send_calls >= m_fail_on_send_call) {
      m_connected = false;
      return TRANSPORT_ERROR_SEND;
    }

    int send_len = (len < 0) ? ArraySize(pkt) : len;
    ArrayResize(m_sent, m_sent_count + 1);
    ArrayResize(m_sent[m_sent_count].data, send_len);
    if (send_len > 0) {
      ArrayCopy(m_sent[m_sent_count].data, pkt, 0, 0, send_len);
    }
    m_sent_count++;
    return TRANSPORT_OK;
  }

  virtual ENUM_TRANSPORT_ERROR Poll(PacketBuffer &out_packets[], uint &out_count) override {
    m_poll_calls++;
    if (!m_connected) {
      out_count = 0;
      ArrayResize(out_packets, 0);
      return TRANSPORT_ERROR_SOCKET;
    }

    out_count = m_incoming_count;
    ArrayResize(out_packets, out_count);
    for (uint i = 0; i < out_count; i++) {
      int len = ArraySize(m_incoming[i].data);
      ArrayResize(out_packets[i].data, len);
      if (len > 0) {
        ArrayCopy(out_packets[i].data, m_incoming[i].data);
      }
    }

    m_incoming_count = 0;
    ArrayResize(m_incoming, 0);
    return TRANSPORT_OK;
  }

  virtual void  SetMaxPacketSize(uint max_size) override {}
  virtual void  SetMaxBufferSize(uint max_size) override {}
  virtual void  SetKeepAlive(uint seconds) override { m_keepalive_seconds = seconds; }
  virtual void  SetPingRespTimeout(uint sec) override {}
  virtual void  SetReadTimeout(uint ms) override {}
  virtual void  SetBlockingOperationWarnThreshold(uint ms) override { m_blocking_warn_threshold_ms = ms; }
  virtual int   GetSocket() const override { return -1; }
  virtual ulong GetLastPingRTT_us() const override { return 0; }
  virtual ulong GetLastBlockingOperationDuration_us() const override { return m_last_blocking_duration_us; }
};

class CConnectingThenReadyTransport : public CTestTransport {
 public:
  bool m_report_connecting_once;

  CConnectingThenReadyTransport() { m_report_connecting_once = true; }

  virtual ENUM_TRANSPORT_ERROR Poll(PacketBuffer &out_packets[], uint &out_count) override {
    if (!m_connected) {
      out_count = 0;
      ArrayResize(out_packets, 0);
      return TRANSPORT_ERROR_SOCKET;
    }

    m_poll_calls++;
    if (m_report_connecting_once) {
      m_report_connecting_once = false;
      out_count                = 0;
      ArrayResize(out_packets, 0);
      return TRANSPORT_CONNECTING;
    }

    return CTestTransport::Poll(out_packets, out_count);
  }
};

class CDeferredFirstSendTransport : public CTestTransport {
 public:
  bool m_defer_first_send;

  CDeferredFirstSendTransport() { m_defer_first_send = true; }

  virtual ENUM_TRANSPORT_ERROR Send(const uchar &pkt[], int len = -1) override {
    if (m_defer_first_send) {
      m_defer_first_send = false;
      return TRANSPORT_CONNECTING;
    }

    return CTestTransport::Send(pkt, len);
  }
};

void InitPublishProperties(MqttPublishProperties &props) {
  props.has_payload_format      = false;
  props.payload_format          = RAW_BYTES;
  props.has_message_expiry      = false;
  props.message_expiry_interval = 0;
  props.has_topic_alias         = false;
  props.topic_alias             = 0;
  props.response_topic          = "";
  ArrayResize(props.correlation_data, 0);
  props.content_type                           = "";
  props.has_subscription_identifier            = false;
  props.subscription_identifier                = 0;
  props.allow_outgoing_subscription_identifier = false;
  ArrayResize(props.user_property_keys, 0);
  ArrayResize(props.user_property_vals, 0);
}

void ResetPersistentSessionStore(const string session_id) {
  CSessionDatabase db;

  db.Init(session_id, true);
  db.Clear();
}

string GetPersistentSessionStorePath(const string session_id) {
  return "MQTT_Sessions\\" + session_id + ".bin";
}

bool ReadPersistentSessionStoreBytes(const string session_id, uchar& data[]) {
  ArrayResize(data, 0);

  int file_handle = FileOpen(GetPersistentSessionStorePath(session_id), FILE_READ | FILE_BIN);
  if (file_handle == INVALID_HANDLE) {
    return false;
  }

  int file_size = (int)FileSize(file_handle);
  if (file_size < 0) {
    FileClose(file_handle);
    return false;
  }

  if (ArrayResize(data, file_size) != file_size) {
    FileClose(file_handle);
    return false;
  }

  uint bytes_read = 0;
  if (file_size > 0) {
    bytes_read = (uint)FileReadArray(file_handle, data, 0, file_size);
  }
  FileClose(file_handle);

  return file_size == 0 || bytes_read == (uint)file_size;
}

bool WritePersistentSessionStoreBytes(const string session_id, const uchar& data[]) {
  string file_name = GetPersistentSessionStorePath(session_id);
  if (FileIsExist(file_name) && !FileDelete(file_name)) {
    return false;
  }

  int file_handle = FileOpen(file_name, FILE_WRITE | FILE_BIN);
  if (file_handle == INVALID_HANDLE) {
    return false;
  }

  uint bytes_written = 0;
  if (ArraySize(data) > 0) {
    bytes_written = (uint)FileWriteArray(file_handle, data, 0, ArraySize(data));
  }
  FileClose(file_handle);

  return ArraySize(data) == 0 || bytes_written == (uint)ArraySize(data);
}

bool TamperPersistentSessionStoreByte(const string session_id, const int offset, const uchar delta) {
  uchar file_bytes[];
  if (!ReadPersistentSessionStoreBytes(session_id, file_bytes)) {
    return false;
  }
  if (offset < 0 || offset >= ArraySize(file_bytes)) {
    return false;
  }

  file_bytes[offset] = (uchar)(file_bytes[offset] ^ delta);
  return WritePersistentSessionStoreBytes(session_id, file_bytes);
}

bool PacketContainsByte(const uchar &pkt[], const uchar needle) {
  int len = ArraySize(pkt);
  for (int i = 0; i < len; i++) {
    if (pkt[i] == needle) {
      return true;
    }
  }
  return false;
}

ushort ExtractConnectKeepAlive(const uchar &pkt[]) {
  if (ArraySize(pkt) < 10 || pkt[0] != 0x10) {
    return 0;
  }

  uint idx        = 1;
  uint multiplier = 1;
  while (idx < (uint)ArraySize(pkt)) {
    uchar encoded = pkt[idx++];
    if ((encoded & 0x80) == 0) {
      break;
    }
    multiplier *= 128;
    if (multiplier > 2097152) {
      return 0;
    }
  }

  uint keepalive_idx = idx + 2 + 4 + 1 + 1;
  if (keepalive_idx + 1 >= (uint)ArraySize(pkt)) {
    return 0;
  }

  return (ushort)(((uint)pkt[keepalive_idx] << 8) | (uint)pkt[keepalive_idx + 1]);
}

//+------------------------------------------------------------------+
//| Callback state for capturing invocations                         |
//+------------------------------------------------------------------+
int                    g_cb_connect_count                  = 0;
bool                   g_cb_session_present                = false;
int                    g_cb_disconnect_count               = 0;
int                    g_cb_disconnect_code                = -1;
string                 g_cb_disconnect_reason              = "";
int                    g_cb_error_count                    = 0;
int                    g_cb_error_code                     = -1;
string                 g_cb_error_desc                     = "";
int                    g_cb_state_count                    = 0;
ENUM_MQTT_CLIENT_STATE g_cb_old_state                      = MQTT_CLIENT_DISCONNECTED;
ENUM_MQTT_CLIENT_STATE g_cb_new_state                      = MQTT_CLIENT_DISCONNECTED;
int                    g_cb_message_count                  = 0;
ushort                 g_cb_last_packet_id                 = 0;
string                 g_cb_last_topic                     = "";
uint                   g_cb_last_sub_id                    = 0;
int                    g_cb_message_ex_count               = 0;
uint                   g_cb_message_ex_matched_sub_id      = 0;
bool                   g_cb_message_ex_has_payload_format  = false;
uchar                  g_cb_message_ex_payload_format      = 0;
bool                   g_cb_message_ex_has_message_expiry  = false;
uint                   g_cb_message_ex_message_expiry      = 0;
bool                   g_cb_message_ex_has_topic_alias     = false;
ushort                 g_cb_message_ex_topic_alias         = 0;
string                 g_cb_message_ex_response_topic      = "";
string                 g_cb_message_ex_content_type        = "";
int                    g_cb_message_ex_corr_len            = 0;
uchar                  g_cb_message_ex_corr_first          = 0;
uchar                  g_cb_message_ex_corr_second         = 0;
int                    g_cb_message_ex_broker_subid_count  = 0;
uint                   g_cb_message_ex_broker_subid_first  = 0;
uint                   g_cb_message_ex_broker_subid_second = 0;
int                    g_cb_message_ex_user_prop_count     = 0;
string                 g_cb_message_ex_user_key            = "";
string                 g_cb_message_ex_user_val            = "";
int                    g_cb_ack_ex_count                   = 0;
int                    g_cb_ack_ex_packet_type             = 0;
ushort                 g_cb_ack_ex_packet_id               = 0;
int                    g_cb_ack_ex_reason_code             = -1;
string                 g_cb_ack_ex_reason                  = "";
int                    g_cb_ack_ex_user_prop_count         = 0;
string                 g_cb_ack_ex_user_key                = "";
string                 g_cb_ack_ex_user_val                = "";
int                    g_cb_suback_ex_count                = 0;
string                 g_cb_suback_ex_reason               = "";
int                    g_cb_unsuback_ex_count              = 0;
string                 g_cb_unsuback_ex_reason             = "";
int                    g_cb_disconnect_ex_count            = 0;
string                 g_cb_disconnect_ex_server_ref       = "";
int                    g_cb_disconnect_ex_user_prop_count  = 0;
string                 g_cb_disconnect_ex_user_key         = "";
string                 g_cb_disconnect_ex_user_val         = "";
int                    g_cb_auth_ex_count                  = 0;
int                    g_cb_auth_ex_reason_code            = -1;
string                 g_cb_auth_ex_reason_string          = "";
string                 g_cb_auth_ex_method                 = "";
int                    g_cb_auth_ex_data_len               = 0;
int                    g_cb_auth_ex_user_prop_count        = 0;
string                 g_cb_auth_ex_user_key               = "";
string                 g_cb_auth_ex_user_val               = "";
int                    g_reentrant_message_count           = 0;
CMqttClient           *g_reentrant_client                  = NULL;

//+------------------------------------------------------------------+
//| ResetCallbackState                                               |
//| Purpose: Zero all callback capture counters and strings before   |
//|          each test run to ensure a clean, isolated state.        |
//+------------------------------------------------------------------+
void                   ResetCallbackState() {
  g_cb_connect_count                  = 0;
  g_cb_session_present                = false;
  g_cb_disconnect_count               = 0;
  g_cb_disconnect_code                = -1;
  g_cb_disconnect_reason              = "";
  g_cb_error_count                    = 0;
  g_cb_error_code                     = -1;
  g_cb_error_desc                     = "";
  g_cb_state_count                    = 0;
  g_cb_old_state                      = MQTT_CLIENT_DISCONNECTED;
  g_cb_new_state                      = MQTT_CLIENT_DISCONNECTED;
  g_cb_message_count                  = 0;
  g_cb_last_packet_id                 = 0;
  g_cb_last_topic                     = "";
  g_cb_last_sub_id                    = 0;
  g_cb_message_ex_count               = 0;
  g_cb_message_ex_matched_sub_id      = 0;
  g_cb_message_ex_has_payload_format  = false;
  g_cb_message_ex_payload_format      = 0;
  g_cb_message_ex_has_message_expiry  = false;
  g_cb_message_ex_message_expiry      = 0;
  g_cb_message_ex_has_topic_alias     = false;
  g_cb_message_ex_topic_alias         = 0;
  g_cb_message_ex_response_topic      = "";
  g_cb_message_ex_content_type        = "";
  g_cb_message_ex_corr_len            = 0;
  g_cb_message_ex_corr_first          = 0;
  g_cb_message_ex_corr_second         = 0;
  g_cb_message_ex_broker_subid_count  = 0;
  g_cb_message_ex_broker_subid_first  = 0;
  g_cb_message_ex_broker_subid_second = 0;
  g_cb_message_ex_user_prop_count     = 0;
  g_cb_message_ex_user_key            = "";
  g_cb_message_ex_user_val            = "";
  g_cb_ack_ex_count                   = 0;
  g_cb_ack_ex_packet_type             = 0;
  g_cb_ack_ex_packet_id               = 0;
  g_cb_ack_ex_reason_code             = -1;
  g_cb_ack_ex_reason                  = "";
  g_cb_ack_ex_user_prop_count         = 0;
  g_cb_ack_ex_user_key                = "";
  g_cb_ack_ex_user_val                = "";
  g_cb_suback_ex_count                = 0;
  g_cb_suback_ex_reason               = "";
  g_cb_unsuback_ex_count              = 0;
  g_cb_unsuback_ex_reason             = "";
  g_cb_disconnect_ex_count            = 0;
  g_cb_disconnect_ex_server_ref       = "";
  g_cb_disconnect_ex_user_prop_count  = 0;
  g_cb_disconnect_ex_user_key         = "";
  g_cb_disconnect_ex_user_val         = "";
  g_cb_auth_ex_count                  = 0;
  g_cb_auth_ex_reason_code            = -1;
  g_cb_auth_ex_reason_string          = "";
  g_cb_auth_ex_method                 = "";
  g_cb_auth_ex_data_len               = 0;
  g_cb_auth_ex_user_prop_count        = 0;
  g_cb_auth_ex_user_key               = "";
  g_cb_auth_ex_user_val               = "";
  g_reentrant_message_count           = 0;
  g_reentrant_client                  = NULL;
}

//+------------------------------------------------------------------+
//| TestOnConnect                                                    |
//| Purpose: OnConnect callback stub — increments the connect        |
//|          counter and records the session_present flag received   |
//|          in the CONNACK acknowledgement packet.                  |
//+------------------------------------------------------------------+
void TestOnConnect(bool session_present) {
  g_cb_connect_count++;
  g_cb_session_present = session_present;
}

//+------------------------------------------------------------------+
//| TestOnDisconnect                                                 |
//| Purpose: OnDisconnect callback stub — captures the reason code   |
//|          and reason string from a broker-initiated DISCONNECT    |
//|          or a transport-level connection drop.                   |
//+------------------------------------------------------------------+
void TestOnDisconnect(int reason_code, const string reason_string, const string server_reference,
                      const string &user_prop_keys[], const string &user_prop_vals[], int user_prop_count) {
  g_cb_disconnect_count++;
  g_cb_disconnect_code   = reason_code;
  g_cb_disconnect_reason = reason_string;
}

void TestOnDisconnectEx(int reason_code, const string reason_string, const string server_reference,
                        const string &user_prop_keys[], const string &user_prop_vals[], int user_prop_count) {
  g_cb_disconnect_ex_count++;
  g_cb_disconnect_code               = reason_code;
  g_cb_disconnect_reason             = reason_string;
  g_cb_disconnect_ex_server_ref      = server_reference;
  g_cb_disconnect_ex_user_prop_count = user_prop_count;
  g_cb_disconnect_ex_user_key        = (user_prop_count > 0) ? user_prop_keys[0] : "";
  g_cb_disconnect_ex_user_val        = (user_prop_count > 0) ? user_prop_vals[0] : "";
}

void TestOnAuthEx(uchar reason_code, const string method, const uchar &data[], int data_len, const string reason_string,
                  const string &user_prop_keys[], const string &user_prop_vals[], int user_prop_count) {
  g_cb_auth_ex_count++;
  g_cb_auth_ex_reason_code     = (int)reason_code;
  g_cb_auth_ex_reason_string   = reason_string;
  g_cb_auth_ex_method          = method;
  g_cb_auth_ex_data_len        = data_len;
  g_cb_auth_ex_user_prop_count = user_prop_count;
  g_cb_auth_ex_user_key        = (user_prop_count > 0) ? user_prop_keys[0] : "";
  g_cb_auth_ex_user_val        = (user_prop_count > 0) ? user_prop_vals[0] : "";
}

//+------------------------------------------------------------------+
//| TestOnError                                                      |
//| Purpose: OnError callback stub — captures the numeric error code |
//|          and description string from transport or protocol error |
//|          events fired by the CMqttClient error path.             |
//+------------------------------------------------------------------+
void TestOnError(const MqttErrorContext &context) {
  g_cb_error_count++;
  g_cb_error_code = context.error_code;
  g_cb_error_desc = context.description;
}

//+------------------------------------------------------------------+
//| TestOnStateChange                                                |
//| Purpose: OnStateChange callback stub — records the previous and  |
//|          new ENUM_MQTT_CLIENT_STATE values and increments the    |
//|          state-change counter for post-test assertions.          |
//+------------------------------------------------------------------+
void TestOnStateChange(ENUM_MQTT_CLIENT_STATE old_state, ENUM_MQTT_CLIENT_STATE new_state) {
  g_cb_state_count++;
  g_cb_old_state = old_state;
  g_cb_new_state = new_state;
}

//+------------------------------------------------------------------+
//| TestOnMessage                                                    |
//| Purpose: OnMessage callback stub — increments the message counter|
//|          and records the topic string and packet ID of the last  |
//|          incoming PUBLISH delivery for post-test assertions.     |
//+------------------------------------------------------------------+
void TestOnMessage(const string topic, const uchar &payload[], int payload_len, uchar qos, bool retain,
                   ushort packet_id, const MqttIncomingMessageMetadata &metadata) {
  g_cb_message_count++;
  g_cb_last_packet_id = packet_id;
  g_cb_last_topic     = topic;
  g_cb_last_sub_id    = metadata.matched_subscription_identifier;
}

void TestOnMessageEx(const string topic, const uchar &payload[], int payload_len, uchar qos, bool retain,
                     ushort packet_id, const MqttIncomingMessageMetadata &metadata) {
  uchar correlation_data[];

  g_cb_message_ex_count              = g_cb_message_ex_count + 1;
  g_cb_last_packet_id                = packet_id;
  g_cb_last_topic                    = topic;
  g_cb_message_ex_matched_sub_id     = metadata.matched_subscription_identifier;
  g_cb_message_ex_has_payload_format = metadata.has_payload_format;
  g_cb_message_ex_payload_format     = metadata.payload_format;
  g_cb_message_ex_has_message_expiry = metadata.has_message_expiry;
  g_cb_message_ex_message_expiry     = metadata.message_expiry_interval;
  g_cb_message_ex_has_topic_alias    = metadata.has_topic_alias;
  g_cb_message_ex_topic_alias        = metadata.topic_alias;
  g_cb_message_ex_response_topic     = metadata.response_topic;
  g_cb_message_ex_content_type       = metadata.content_type;
  g_cb_message_ex_broker_subid_count = (int)metadata.broker_subscription_id_count;
  g_cb_message_ex_broker_subid_first =
    (metadata.broker_subscription_id_count > 0) ? metadata.broker_subscription_ids[0] : 0;
  g_cb_message_ex_broker_subid_second =
    (metadata.broker_subscription_id_count > 1) ? metadata.broker_subscription_ids[1] : 0;
  g_cb_message_ex_user_prop_count = (int)metadata.user_property_count;
  g_cb_message_ex_user_key        = (metadata.user_property_count > 0) ? metadata.user_property_keys[0] : "";
  g_cb_message_ex_user_val        = (metadata.user_property_count > 0) ? metadata.user_property_vals[0] : "";

  ArrayResize(correlation_data, ArraySize(metadata.correlation_data));
  if (ArraySize(metadata.correlation_data) > 0) {
    ArrayCopy(correlation_data, metadata.correlation_data);
  }
  g_cb_message_ex_corr_len    = ArraySize(correlation_data);
  g_cb_message_ex_corr_first  = (g_cb_message_ex_corr_len > 0) ? correlation_data[0] : 0;
  g_cb_message_ex_corr_second = (g_cb_message_ex_corr_len > 1) ? correlation_data[1] : 0;
}

void TestOnAckEx(uchar packet_type, ushort packet_id, uchar reason_code, const string reason_string,
                 const string &user_prop_keys[], const string &user_prop_vals[], int user_prop_count) {
  g_cb_ack_ex_count           = g_cb_ack_ex_count + 1;
  g_cb_ack_ex_packet_type     = packet_type;
  g_cb_ack_ex_packet_id       = packet_id;
  g_cb_ack_ex_reason_code     = reason_code;
  g_cb_ack_ex_reason          = reason_string;
  g_cb_ack_ex_user_prop_count = user_prop_count;
  g_cb_ack_ex_user_key        = (user_prop_count > 0) ? user_prop_keys[0] : "";
  g_cb_ack_ex_user_val        = (user_prop_count > 0) ? user_prop_vals[0] : "";
}

void TestOnSubackEx(ushort packet_id, const uchar &reason_codes[], int count, const string reason_string,
                    const string &user_prop_keys[], const string &user_prop_vals[], int user_prop_count) {
  g_cb_suback_ex_count  = g_cb_suback_ex_count + 1;
  g_cb_suback_ex_reason = reason_string;
}

void TestOnUnsubackEx(ushort packet_id, const uchar &reason_codes[], int count, const string reason_string,
                      const string &user_prop_keys[], const string &user_prop_vals[], int user_prop_count) {
  g_cb_unsuback_ex_count  = g_cb_unsuback_ex_count + 1;
  g_cb_unsuback_ex_reason = reason_string;
}

//+------------------------------------------------------------------+
//| ReentrantOnMessage                                               |
//| Purpose: Exercise Poll() re-entry from inside a user callback.   |
//+------------------------------------------------------------------+
void ReentrantOnMessage(const string topic, const uchar &payload[], int payload_len, uchar qos, bool retain,
                        ushort packet_id, const MqttIncomingMessageMetadata &metadata) {
  g_reentrant_message_count++;
  if (g_reentrant_client != NULL) {
    g_reentrant_client.Poll();
  }
}

void ReentrantOnDisconnectSetConnecting(int reason_code, const string reason_string, const string server_reference,
                                        const string &user_prop_keys[], const string &user_prop_vals[],
                                        int user_prop_count) {
  TestOnDisconnect(reason_code, reason_string, server_reference, user_prop_keys, user_prop_vals, user_prop_count);
  if (g_reentrant_client != NULL) {
    g_reentrant_client.TestSetState(MQTT_CLIENT_CONNECTING);
  }
}

//+------------------------------------------------------------------+
//| TEST_DefaultState                                                |
//| A freshly constructed client should be DISCONNECTED with         |
//| zero metrics and no callbacks registered.                        |
//+------------------------------------------------------------------+
bool TEST_DefaultState() {
  TEST_CASE_START();

  CMqttClient mqtt;

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_FALSE(mqtt.IsConnected());
  ASSERT_FALSE(mqtt.IsConnecting());
  ASSERT_FALSE(mqtt.IsSafeToPublish());
  ASSERT_FALSE(mqtt.IsReconnectInProgress());
  ASSERT_FALSE(mqtt.IsSessionEncryptionEnabled());
  ASSERT_EQ(0, (int)mqtt.GetReconnectCount());
  ASSERT_EQ(0, (int)mqtt.GetReconnectAttemptCount());
  ASSERT_EQ(12, (int)mqtt.GetMaxReconnectAttempts());
  ASSERT_EQ(0, (int)mqtt.GetMessagesSent());
  ASSERT_EQ(0, (int)mqtt.GetMessagesReceived());
  ASSERT_EQ(0, (int)mqtt.GetLastPingRTT());
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetDurableQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetQueuedPayloadBytes());
  ASSERT_EQ(0, (int)mqtt.GetQueuedPropertyBytes());
  ASSERT_EQ(0, (int)mqtt.GetInFlightCount());
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS1Count());
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS2Count());
  ASSERT_EQ(0, (int)mqtt.GetIncomingInFlightCount());
  ASSERT_EQ(0, (int)mqtt.GetOldestQueuedMessageAgeMs());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPayloadBytes());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPropertyBytes());
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogBytes());
  ASSERT_EQ(0, mqtt.GetLastFailureCode());
  ASSERT_STR_EQ("", mqtt.GetLastFailureDescription());
  ASSERT_EQ((int)MQTT_FAILURE_NONE, (int)mqtt.GetLastFailureClass());
  ASSERT_EQ(10, (int)mqtt.GetMaxRetransmitCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_IsSafeToPublishSurface                                      |
//| The facade guard should only be true when fully connected.       |
//+------------------------------------------------------------------+
bool TEST_IsSafeToPublishSurface() {
  TEST_CASE_START();

  CMqttClient mqtt;

  ASSERT_FALSE(mqtt.IsSafeToPublish());

  mqtt.TestSetState(MQTT_CLIENT_CONNECTING);
  ASSERT_FALSE(mqtt.IsSafeToPublish());

  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);
  ASSERT_FALSE(mqtt.IsSafeToPublish());

  mqtt.TestSetState(MQTT_CLIENT_TLS_HANDSHAKING);
  ASSERT_FALSE(mqtt.IsSafeToPublish());

  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  ASSERT_TRUE(mqtt.IsSafeToPublish());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_SetHost_TCP                                                 |
//| SetHost should select TCP transport and store parameters.        |
//+------------------------------------------------------------------+
bool TEST_SetHost_TCP() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.example.com", 8883);
  mqtt.SetTLS(true);

  //--- State should still be disconnected after configuration
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_FALSE(mqtt.IsConnected());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_SetHostWS                                                   |
//| SetHostWS should select WebSocket transport.                     |
//+------------------------------------------------------------------+
bool TEST_SetHostWS() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHostWS("ws.broker.com", 443, "/mqtt");
  mqtt.SetTLS(true);

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_FALSE(mqtt.IsConnected());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_FluentConfiguration                                         |
//| All fluent setters should be callable without errors.            |
//+------------------------------------------------------------------+
bool TEST_FluentConfiguration() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetTLS(false);
  mqtt.SetClientId("test-client-42");
  mqtt.SetCredentials("admin", "secret");
  mqtt.SetConnectTimeout(3000);
  mqtt.SetCleanStart(true);
  mqtt.SetSessionExpiry(7200);
  mqtt.SetSessionEncryptionPassphrase("client-config-secret");
  mqtt.SetKeepAlive(30);
  mqtt.SetDefaultQoS(QoS_1);
  mqtt.SetAutoReconnect(true, 500, 30000);
  mqtt.SetMaxRetransmitCount(5);
  mqtt.SetRetransmitTimeout(15);
  mqtt.SetMaxQueuedMessages(50);
  mqtt.SetConnackTimeout(8000);

  //--- After all configuration, state should still be disconnected
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(mqtt.IsSessionEncryptionEnabled());
  ASSERT_EQ(5, (int)mqtt.GetMaxRetransmitCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_WillConfiguration                                           |
//| LWT setters should be callable in both string and binary forms.  |
//+------------------------------------------------------------------+
bool TEST_WillConfiguration() {
  TEST_CASE_START();

  CMqttClient mqtt;

  //--- String will
  mqtt.SetWill("status/offline", "goodbye", QoS_1, true);
  mqtt.SetWillDelay(30);
  mqtt.SetWillExpiry(3600);
  mqtt.SetWillProperties("text/plain", "reply/topic");

  //--- Binary will
  uchar bin_payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
  mqtt.SetWillBytes("binary/will", bin_payload, QoS_2, false);

  //--- Should still be disconnected
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_CallbackRegistration                                        |
//| All callback setters should accept function pointers.            |
//+------------------------------------------------------------------+
bool TEST_CallbackRegistration() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetOnConnect(TestOnConnect);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnStateChange(TestOnStateChange);

  //--- Registering callbacks should not change state
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_SubscriptionRegistry                                        |
//| Subscribe/Unsubscribe should maintain a persistent registry.     |
//| When not connected, no packets are sent — just bookkeeping.      |
//+------------------------------------------------------------------+
bool TEST_SubscriptionRegistry() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);

  //--- Subscribe to several topics
  mqtt.Subscribe("sensors/+/temperature", QoS_1);
  mqtt.Subscribe("commands/#", QoS_0);
  mqtt.Subscribe("alerts/critical", QoS_2);

  //--- Duplicate topic should update QoS, not create a new entry
  mqtt.Subscribe("sensors/+/temperature", QoS_2);

  //--- Unsubscribe
  mqtt.Unsubscribe("commands/#");

  //--- Client should still be disconnected (no broker needed for registry)
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DefaultQoS                                                  |
//| SetDefaultQoS should clamp invalid values to 2.                  |
//+------------------------------------------------------------------+
bool TEST_DefaultQoS() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetDefaultQoS(QoS_0);
  mqtt.SetDefaultQoS(QoS_1);
  mqtt.SetDefaultQoS(QoS_2);

  //--- Invalid QoS 3 should be clamped
  mqtt.SetDefaultQoS(3);

  //--- No crash expected
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishQueuing                                              |
//| QoS 1/2 publishing while disconnected should queue messages.     |
//+------------------------------------------------------------------+
bool TEST_PublishQueuing() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(3);

  //--- Publish while disconnected → QoS 1 should queue
  ENUM_MQTT_PUBLISH_ERROR err1 = mqtt.Publish("topic/1", "hello", QoS_1);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)err1);
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  ENUM_MQTT_PUBLISH_ERROR err2 = mqtt.Publish("topic/2", "world", QoS_1);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)err2);
  ASSERT_EQ(2, (int)mqtt.GetQueuedMessageCount());

  ENUM_MQTT_PUBLISH_ERROR err3 = mqtt.Publish("topic/3", "foo", QoS_2);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)err3);
  ASSERT_EQ(3, (int)mqtt.GetQueuedMessageCount());

  //--- Fourth message should be rejected (queue full)
  ENUM_MQTT_PUBLISH_ERROR err4 = mqtt.Publish("topic/4", "overflow", QoS_1);
  ASSERT_EQ((int)MQTT_PUB_QUEUE_FULL, (int)err4);
  ASSERT_EQ(3, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishQueueBinary                                          |
//| Binary payloads should also be queued correctly.                 |
//+------------------------------------------------------------------+
bool TEST_PublishQueueBinary() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);

  uchar                   data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
  ENUM_MQTT_PUBLISH_ERROR err    = mqtt.Publish("binary/topic", data, ArraySize(data), QoS_1, true);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)err);
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishQueuePayloadByteBudget                               |
//| Offline queueing must enforce total payload-byte budgets.        |
//+------------------------------------------------------------------+
bool TEST_PublishQueuePayloadByteBudget() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);
  mqtt.SetMaxQueuedPayloadBytes(5);

  uchar payload1[] = {0x01, 0x02, 0x03};
  uchar payload2[] = {0x04, 0x05, 0x06};

  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("budget/payload/1", payload1, ArraySize(payload1), QoS_1, false));
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(3, (int)mqtt.GetQueuedPayloadBytes());

  ASSERT_EQ((int)MQTT_PUB_QUEUE_FULL,
            (int)mqtt.Publish("budget/payload/2", payload2, ArraySize(payload2), QoS_1, false));
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(3, (int)mqtt.GetQueuedPayloadBytes());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishQueuePropertyByteBudget                              |
//| Offline queueing must enforce total encoded-property budgets.    |
//+------------------------------------------------------------------+
bool TEST_PublishQueuePropertyByteBudget() {
  TEST_CASE_START();

  CMqttClient           mqtt;
  MqttPublishProperties props;
  uchar                 payload[] = {0x41};

  InitPublishProperties(props);
  props.has_message_expiry      = true;
  props.message_expiry_interval = 1;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);
  mqtt.SetMaxQueuedPropertyBytes(4);

  ASSERT_EQ((int)MQTT_PUB_QUEUE_FULL,
            (int)mqtt.Publish("budget/props", payload, ArraySize(payload), QoS_1, false, props));
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetQueuedPropertyBytes());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishQueueSingleMessageByteBudget                         |
//| One oversized queued publish must be rejected before enqueue.    |
//+------------------------------------------------------------------+
bool TEST_PublishQueueSingleMessageByteBudget() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);
  mqtt.SetMaxSingleQueuedPublishBytes(5);

  uchar payload[] = {0x10, 0x11, 0x12, 0x13, 0x14, 0x15};

  ASSERT_EQ((int)MQTT_PUB_QUEUE_FULL,
            (int)mqtt.Publish("budget/single", payload, ArraySize(payload), QoS_1, false));
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetQueuedPayloadBytes());
  ASSERT_EQ(0, (int)mqtt.GetQueuedPropertyBytes());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishQoS0DisconnectedDefault                              |
//| QoS 0 should not queue while disconnected unless enabled.        |
//+------------------------------------------------------------------+
bool TEST_PublishQoS0DisconnectedDefault() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);

  ENUM_MQTT_PUBLISH_ERROR err = mqtt.Publish("topic/0", "hello", QoS_0);
  ASSERT_EQ((int)MQTT_PUB_NOT_CONNECTED, (int)err);
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishQoS0QueueOptIn                                       |
//| QoS 0 queueing must require explicit opt-in.                     |
//+------------------------------------------------------------------+
bool TEST_PublishQoS0QueueOptIn() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);
  mqtt.SetQueueQoS0WhenDisconnected(true);

  ENUM_MQTT_PUBLISH_ERROR err = mqtt.Publish("topic/0", "hello", QoS_0);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)err);
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackTimeout_Config                                       |
//| Verify CONNACK timeout can be configured.                        |
//+------------------------------------------------------------------+
bool TEST_ConnackTimeout_Config() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetConnackTimeout(5000);

  //--- Verify it doesn't affect state
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackDeadlineStartsOnConnectSend                          |
//| CONNACK timeout starts only after CONNECT is transmitted.        |
//+------------------------------------------------------------------+
bool TEST_ConnackDeadlineStartsOnConnectSend() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));

  ASSERT_EQ(0, (int)mqtt.TestGetConnackDeadlineMs());
  mqtt.TestSendConnect();
  ASSERT_TRUE(mqtt.TestGetConnackDeadlineMs() > 0);
  ASSERT_EQ((int)MQTT_CLIENT_WAITING_CONNACK, (int)mqtt.GetState());
  ASSERT_EQ(1, (int)tx.m_sent_count);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectIncludesExtendedMqtt5Properties                      |
//| CMqttClient should surface extended CONNECT and Will properties. |
//+------------------------------------------------------------------+
bool TEST_ConnectIncludesExtendedMqtt5Properties() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetClientId("extended-props-client");
  mqtt.SetRequestResponseInformation(true);
  mqtt.SetRequestProblemInformation(false);
  mqtt.SetWill("status/offline", "bye", QoS_1, true);
  mqtt.SetWillPayloadFormat(UTF8);
  uchar corr_data[] = {0xCA, 0xFE};
  mqtt.SetWillCorrelationData(corr_data);
  mqtt.SetWillUserProperty("wk", "wv");
  mqtt.TestInjectTransport(GetPointer(tx));

  mqtt.TestSendConnect();

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_TRUE(PacketContainsByte(tx.m_sent[0].data, MQTT_PROP_IDENTIFIER_REQUEST_RESPONSE_INFORMATION));
  ASSERT_TRUE(PacketContainsByte(tx.m_sent[0].data, MQTT_PROP_IDENTIFIER_REQUEST_PROBLEM_INFORMATION));
  ASSERT_TRUE(PacketContainsByte(tx.m_sent[0].data, MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR));
  ASSERT_TRUE(PacketContainsByte(tx.m_sent[0].data, MQTT_PROP_IDENTIFIER_CORRELATION_DATA));
  ASSERT_TRUE(PacketContainsByte(tx.m_sent[0].data, MQTT_PROP_IDENTIFIER_USER_PROPERTY));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackResponseInformationSurface                           |
//| CMqttClient should expose CONNACK Response Information.          |
//+------------------------------------------------------------------+
bool TEST_ConnackResponseInformationSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x0D, 0x00, 0x00, 0x0A, 0x1A, 0x00, 0x07, 'r', 's', 'p', 'i', 'n', 'f', 'o'};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_STR_EQ("rspinfo", mqtt.GetConnackResponseInformation());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackAssignedClientIdentifierSurface                      |
//| CMqttClient should expose CONNACK Assigned Client Identifier.    |
//+------------------------------------------------------------------+
bool TEST_ConnackAssignedClientIdentifierSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x10, 0x00, 0x00, 0x0D, 0x12, 0x00, 0x0A, 'a',
                         'u',  't',  'o',  'c',  'l',  'i',  'e',  'n',  't'};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_STR_EQ("autoclient", mqtt.GetConnackAssignedClientIdentifier());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackMaximumQoSSurface                                    |
//| CMqttClient should expose broker Maximum QoS from CONNACK.       |
//+------------------------------------------------------------------+
bool TEST_ConnackMaximumQoSSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x05, 0x00, 0x00, 0x02, 0x24, 0x01};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(1, (int)mqtt.GetConnackMaximumQoS());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackReceiveMaximumSurface                                |
//| CMqttClient should expose broker Receive Maximum from CONNACK.   |
//+------------------------------------------------------------------+
bool TEST_ConnackReceiveMaximumSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x06, 0x00, 0x00, 0x03, 0x21, 0x00, 0x0A};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(10, (int)mqtt.GetConnackReceiveMaximum());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackServerKeepAliveSurface                               |
//| CMqttClient should expose broker Server Keep Alive from CONNACK. |
//+------------------------------------------------------------------+
bool TEST_ConnackServerKeepAliveSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x06, 0x00, 0x00, 0x03, 0x13, 0x00, 0x1E};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(30, (int)mqtt.GetConnackServerKeepAlive());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackServerKeepAliveDoesNotLeakIntoNextConnect            |
//| Server Keep Alive is connection-scoped and must not rewrite the  |
//| next CONNECT packet's configured Keep Alive value.               |
//+------------------------------------------------------------------+
bool TEST_ConnackServerKeepAliveDoesNotLeakIntoNextConnect() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetKeepAlive(45);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x06, 0x00, 0x00, 0x03, 0x13, 0x00, 0x1E};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(30, (int)mqtt.GetConnackServerKeepAlive());
  ASSERT_EQ(30, (int)tx.m_keepalive_seconds);

  tx.ClearSentPackets();
  mqtt.TestSendConnect();

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(45, (int)ExtractConnectKeepAlive(tx.m_sent[0].data));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackBrokerCapabilitySurface                              |
//| CMqttClient should expose additional CONNACK broker capability   |
//| metadata that is already parsed and applied internally.          |
//+------------------------------------------------------------------+
bool TEST_ConnackBrokerCapabilitySurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x13, 0x00, 0x00, 0x10, 0x27, 0x00, 0x00, 0x04, 0x00, 0x22,
                         0x00, 0x07, 0x25, 0x00, 0x28, 0x00, 0x29, 0x00, 0x2A, 0x00};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(1024, (int)mqtt.GetConnackMaximumPacketSize());
  ASSERT_EQ(7, (int)mqtt.GetConnackTopicAliasMaximum());
  ASSERT_FALSE(mqtt.GetConnackRetainAvailable());
  ASSERT_FALSE(mqtt.GetConnackWildcardSubscriptionAvailable());
  ASSERT_FALSE(mqtt.GetConnackSubscriptionIdentifierAvailable());
  ASSERT_FALSE(mqtt.GetConnackSharedSubscriptionAvailable());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackMaximumPacketSizeOmittedClearsPreviousLimit          |
//| Omitted CONNACK Maximum Packet Size must restore the default     |
//| no-limit semantics instead of preserving a stale prior value.    |
//+------------------------------------------------------------------+
bool TEST_ConnackMaximumPacketSizeOmittedClearsPreviousLimit() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestContext().flow_control.SetMaximumPacketSize(1024);
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x03, 0x00, 0x00, 0x00};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(0, (int)mqtt.GetConnackMaximumPacketSize());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackExtendedMetadataSurface                              |
//| CMqttClient should expose remaining parsed CONNACK metadata.     |
//+------------------------------------------------------------------+
bool TEST_ConnackExtendedMetadataSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  uchar          auth_data[];

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetAuthMethod("SCRAM");
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x21, 0x00, 0x00, 0x1E, 0x1F, 0x00, 0x02, 'o',  'k',  0x11, 0x00,
                         0x00, 0x01, 0x2C, 0x1C, 0x00, 0x03, 's',  'r',  'v',  0x15, 0x00, 0x05,
                         'S',  'C',  'R',  'A',  'M',  0x16, 0x00, 0x03, 0x01, 0x02, 0x03};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();
  mqtt.GetConnackAuthenticationData(auth_data);

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_FALSE(mqtt.GetConnackSessionPresent());
  ASSERT_EQ(0, (int)mqtt.GetConnackReasonCode());
  ASSERT_STR_EQ("ok", mqtt.GetConnackReasonString());
  ASSERT_EQ(300, (int)mqtt.GetConnackSessionExpiryInterval());
  ASSERT_STR_EQ("srv", mqtt.GetConnackServerReference());
  ASSERT_STR_EQ("SCRAM", mqtt.GetConnackAuthenticationMethod());
  ASSERT_EQ(3, ArraySize(auth_data));
  ASSERT_EQ(0x01, (int)auth_data[0]);
  ASSERT_EQ(0x02, (int)auth_data[1]);
  ASSERT_EQ(0x03, (int)auth_data[2]);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackRejectDiagnosticsSurface                             |
//| Rejected CONNACK diagnostics should remain inspectable.          |
//+------------------------------------------------------------------+
bool TEST_ConnackRejectDiagnosticsSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x0C, 0x00, 0x87, 0x09, 0x1F, 0x00, 0x06, 'd', 'e', 'n', 'i', 'e', 'd'};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_FALSE(mqtt.IsConnected());
  ASSERT_EQ(0x87, (int)mqtt.GetConnackReasonCode());
  ASSERT_STR_EQ("denied", mqtt.GetConnackReasonString());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_AckDiagnosticsCache                                         |
//| MQTT 5 PUBACK diagnostics should be queryable from the facade.   |
//+------------------------------------------------------------------+
bool TEST_AckDiagnosticsCache() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  uchar puback[] = {0x40, 0x13, 0x12, 0x34, 0x97, 0x0F, 0x1F, 0x00, 0x05, 'q', 'u',
                    'o',  't',  'a',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};
  tx.EnqueueIncoming(puback);
  mqtt.Poll();

  ASSERT_EQ((int)PUBACK, (int)mqtt.GetLastAckPacketType());
  ASSERT_EQ(0x1234, (int)mqtt.GetLastAckPacketId());
  ASSERT_EQ(0x97, (int)mqtt.GetLastAckReasonCode());
  ASSERT_STR_EQ("quota", mqtt.GetLastAckReasonString());
  ASSERT_EQ(1, (int)mqtt.GetLastAckUserPropertyCount());
  ASSERT_STR_EQ("k", mqtt.GetLastAckUserPropertyKey(0));
  ASSERT_STR_EQ("v", mqtt.GetLastAckUserPropertyValue(0));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishFacadePropertiesSurface                              |
//| CMqttClient should expose MQTT 5 outgoing PUBLISH properties.    |
//+------------------------------------------------------------------+
bool TEST_PublishFacadePropertiesSurface() {
  TEST_CASE_START();

  CTestTransport        tx;
  CMqttClient           mqtt;
  MqttPublishProperties props;

  InitPublishProperties(props);

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestContext().topic_alias_manager.SetTopicAliasMaximum(16);

  props.has_payload_format      = true;
  props.payload_format          = UTF8;
  props.has_message_expiry      = true;
  props.message_expiry_interval = 30;
  props.has_topic_alias         = true;
  props.topic_alias             = 7;
  props.response_topic          = "reply/topic";
  props.content_type            = "application/json";
  ArrayResize(props.correlation_data, 2);
  props.correlation_data[0] = 0xAA;
  props.correlation_data[1] = 0xBB;
  ArrayResize(props.user_property_keys, 1);
  ArrayResize(props.user_property_vals, 1);
  props.user_property_keys[0] = "origin";
  props.user_property_vals[0] = "ea";

  ASSERT_EQ((int)MQTT_PUB_OK, (int)mqtt.Publish("telemetry/out", "hello", QoS_1, true, props));
  ASSERT_EQ(1, (int)tx.m_sent_count);

  CPublish pub;
  ASSERT_EQ((int)MQTT_OK, pub.Read(tx.m_sent[0].data));
  ASSERT_STR_EQ("telemetry/out", pub.GetTopicName());
  ASSERT_STR_EQ("hello", pub.GetPayloadString());
  ASSERT_TRUE(pub.GetRetain());
  ASSERT_EQ((int)QoS_1, (int)pub.GetQoS());
  ASSERT_TRUE(pub.HasPayloadFormat());
  ASSERT_EQ((int)UTF8, (int)pub.GetPayloadFormatIndicator());
  ASSERT_TRUE(pub.HasMessageExpiry());
  ASSERT_EQ(30, (int)pub.GetMessageExpiryInterval());
  ASSERT_EQ(7, (int)pub.GetTopicAlias());
  ASSERT_STR_EQ("reply/topic", pub.GetResponseTopic());
  ASSERT_STR_EQ("application/json", pub.GetContentType());
  ASSERT_EQ(0, (int)pub.GetSubscriptionIdCount());
  ASSERT_EQ(1, (int)pub.GetUserPropertyCount());
  ASSERT_STR_EQ("origin", pub.GetUserPropertyKey(0));
  ASSERT_STR_EQ("ea", pub.GetUserPropertyValue(0));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_InboundPublishMetadataSurface                               |
//| Live incoming PUBLISH metadata should reach the extended facade. |
//+------------------------------------------------------------------+
bool TEST_InboundPublishMetadataSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       pub;
  uchar          payload[];
  uchar          corr[] = {0xAA, 0xBB};

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetClientTopicAliasMaximum(16);
  mqtt.Subscribe("signals/#", TestOnMessageEx, QoS_1);

  StringToCharArray("hello", payload, 0, WHOLE_ARRAY, CP_UTF8);
  if (ArraySize(payload) > 0 && payload[ArraySize(payload) - 1] == 0) {
    ArrayResize(payload, ArraySize(payload) - 1);
  }

  pub.SetTopicName("signals/eurusd");
  pub.SetPacketId(17);
  pub.SetQoS_1(true);
  pub.SetPayload(payload);
  pub.SetPayloadFormatIndicator(UTF8);
  pub.SetMessageExpiryInterval(30);
  pub.SetTopicAlias(7);
  pub.SetResponseTopic("reply/topic");
  pub.SetCorrelationData(corr);
  pub.SetContentType("application/json");
  pub.SetUserProperty("origin", "broker");
  pub.AllowOutgoingSubscriptionIdentifier(true);
  pub.SetSubscriptionIdentifier(11);
  pub.SetSubscriptionIdentifier(22);

  uchar pkt[];
  pub.Build(pkt);
  tx.EnqueueIncoming(pkt);

  mqtt.Poll();

  ASSERT_EQ(1, g_cb_message_ex_count);
  ASSERT_TRUE(g_cb_message_ex_matched_sub_id > 0);
  ASSERT_TRUE(g_cb_message_ex_has_payload_format);
  ASSERT_EQ((int)UTF8, (int)g_cb_message_ex_payload_format);
  ASSERT_TRUE(g_cb_message_ex_has_message_expiry);
  ASSERT_EQ(30, (int)g_cb_message_ex_message_expiry);
  ASSERT_TRUE(g_cb_message_ex_has_topic_alias);
  ASSERT_EQ(7, (int)g_cb_message_ex_topic_alias);
  ASSERT_STR_EQ("reply/topic", g_cb_message_ex_response_topic);
  ASSERT_STR_EQ("application/json", g_cb_message_ex_content_type);
  ASSERT_EQ(2, g_cb_message_ex_corr_len);
  ASSERT_EQ(0xAA, (int)g_cb_message_ex_corr_first);
  ASSERT_EQ(0xBB, (int)g_cb_message_ex_corr_second);
  ASSERT_EQ(2, g_cb_message_ex_broker_subid_count);
  ASSERT_EQ(11, (int)g_cb_message_ex_broker_subid_first);
  ASSERT_EQ(22, (int)g_cb_message_ex_broker_subid_second);
  ASSERT_EQ(1, g_cb_message_ex_user_prop_count);
  ASSERT_STR_EQ("origin", g_cb_message_ex_user_key);
  ASSERT_STR_EQ("broker", g_cb_message_ex_user_val);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_InboundPublishInvalidUtf8StrictDisconnect                   |
//| Strict mode should disconnect on invalid UTF-8 payloads.         |
//+------------------------------------------------------------------+
bool TEST_InboundPublishInvalidUtf8StrictDisconnect() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       pub;
  uchar          payload[] = {0xC3, 0x28};

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.Subscribe("signals/#", TestOnMessageEx, QoS_1);

  pub.SetTopicName("signals/invalid");
  pub.SetPayload(payload);
  pub.SetPayloadFormatIndicator(UTF8);

  uchar pkt[];
  pub.Build(pkt);
  tx.EnqueueIncoming(pkt);

  mqtt.Poll();

  ASSERT_EQ(0, g_cb_message_ex_count);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_PAYLOAD_FORMAT_INVALID, g_cb_error_code);
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(tx.m_sent_count > 0);
  ASSERT_EQ(0xE0, (int)tx.m_sent[tx.m_sent_count - 1].data[0]);
  ASSERT_EQ((int)MQTT_REASON_CODE_PAYLOAD_FORMAT_INVALID, (int)tx.m_sent[tx.m_sent_count - 1].data[2]);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_InboundPublishInvalidUtf8RelaxedDelivery                    |
//| Relaxed mode should warn internally and still deliver payload.   |
//+------------------------------------------------------------------+
bool TEST_InboundPublishInvalidUtf8RelaxedDelivery() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       pub;
  uchar          payload[] = {0xC3, 0x28};

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetStrictUtf8Validation(false);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.Subscribe("signals/#", TestOnMessageEx, QoS_1);
  tx.ClearSentPackets();

  pub.SetTopicName("signals/invalid");
  pub.SetPayload(payload);
  pub.SetPayloadFormatIndicator(UTF8);

  uchar pkt[];
  pub.Build(pkt);
  tx.EnqueueIncoming(pkt);

  mqtt.Poll();

  ASSERT_EQ(1, g_cb_message_ex_count);
  ASSERT_EQ(0, g_cb_disconnect_count);
  ASSERT_EQ(0, g_cb_error_count);
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(0, (int)tx.m_sent_count);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_InboundPublishMetadataQoS2ResumeSurface                     |
//| QoS 2 persisted delivery must preserve MQTT 5 metadata.          |
//+------------------------------------------------------------------+
bool TEST_InboundPublishMetadataQoS2ResumeSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       pub;
  CPubrel        rel;
  uchar          payload[];
  uchar          corr[] = {0x10, 0x20};

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetClientTopicAliasMaximum(16);
  mqtt.SetOnMessage(TestOnMessageEx);
  mqtt.Subscribe("signals/#", QoS_1);

  StringToCharArray("resume", payload, 0, WHOLE_ARRAY, CP_UTF8);
  if (ArraySize(payload) > 0 && payload[ArraySize(payload) - 1] == 0) {
    ArrayResize(payload, ArraySize(payload) - 1);
  }

  pub.SetTopicName("signals/gbpusd");
  pub.SetPacketId(42);
  pub.SetQoS_2(true);
  pub.SetPayload(payload);
  pub.SetPayloadFormatIndicator(UTF8);
  pub.SetMessageExpiryInterval(45);
  pub.SetTopicAlias(9);
  pub.SetResponseTopic("reply/qos2");
  pub.SetCorrelationData(corr);
  pub.SetContentType("text/plain");
  pub.SetUserProperty("trace", "resume");
  pub.AllowOutgoingSubscriptionIdentifier(true);
  pub.SetSubscriptionIdentifier(31);
  pub.SetSubscriptionIdentifier(32);

  uchar publish_pkt[];
  pub.Build(publish_pkt);

  mqtt.TestOnPublishReceived(publish_pkt);
  ASSERT_EQ(0, g_cb_message_ex_count);

  rel.SetPacketId(42);
  uchar pubrel_pkt[];
  rel.Build(pubrel_pkt);
  tx.EnqueueIncoming(pubrel_pkt);
  mqtt.Poll();

  ASSERT_EQ(1, g_cb_message_ex_count);
  ASSERT_TRUE(g_cb_message_ex_matched_sub_id > 0);
  ASSERT_TRUE(g_cb_message_ex_has_payload_format);
  ASSERT_EQ((int)UTF8, (int)g_cb_message_ex_payload_format);
  ASSERT_TRUE(g_cb_message_ex_has_message_expiry);
  ASSERT_EQ(45, (int)g_cb_message_ex_message_expiry);
  ASSERT_TRUE(g_cb_message_ex_has_topic_alias);
  ASSERT_EQ(9, (int)g_cb_message_ex_topic_alias);
  ASSERT_STR_EQ("reply/qos2", g_cb_message_ex_response_topic);
  ASSERT_STR_EQ("text/plain", g_cb_message_ex_content_type);
  ASSERT_EQ(2, g_cb_message_ex_corr_len);
  ASSERT_EQ(0x10, (int)g_cb_message_ex_corr_first);
  ASSERT_EQ(0x20, (int)g_cb_message_ex_corr_second);
  ASSERT_EQ(2, g_cb_message_ex_broker_subid_count);
  ASSERT_EQ(31, (int)g_cb_message_ex_broker_subid_first);
  ASSERT_EQ(32, (int)g_cb_message_ex_broker_subid_second);
  ASSERT_EQ(1, g_cb_message_ex_user_prop_count);
  ASSERT_STR_EQ("trace", g_cb_message_ex_user_key);
  ASSERT_STR_EQ("resume", g_cb_message_ex_user_val);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishRejectsOutgoingSubscriptionIdentifier                |
//| Client-originated PUBLISH Subscription Identifier must be        |
//| rejected even when explicit opt-in is requested.                 |
//+------------------------------------------------------------------+
bool TEST_PublishRejectsOutgoingSubscriptionIdentifier() {
  TEST_CASE_START();

  CTestTransport        tx;
  CMqttClient           mqtt;
  MqttPublishProperties props;

  InitPublishProperties(props);

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  props.has_subscription_identifier            = true;
  props.subscription_identifier                = 42;
  props.allow_outgoing_subscription_identifier = true;

  ASSERT_EQ((int)MQTT_PUB_SEND_FAILED, (int)mqtt.Publish("telemetry/out", "hello", QoS_1, false, props));
  ASSERT_EQ(0, (int)tx.m_sent_count);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_RetransmitStripsLegacyOutgoingSubscriptionIdentifier        |
//| Legacy persisted client-side Subscription Identifier properties  |
//| must be stripped during retransmission.                          |
//+------------------------------------------------------------------+
bool TEST_RetransmitStripsLegacyOutgoingSubscriptionIdentifier() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       builder;
  uchar          payload[] = {0x41};
  uchar          encoded_props[];

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetRetransmitTimeout(0);

  builder.SetResponseTopic("retry/reply");
  builder.SetSubscriptionIdentifier(77);
  builder.AllowOutgoingSubscriptionIdentifier(true);
  builder.GetEncodedProperties(encoded_props);

  ASSERT_TRUE(mqtt.TestContext().session_db.StoreOutgoingMessage(7, QoS_1, "retry/topic", payload, ArraySize(payload),
                                                                 false, 0, 0, encoded_props, true));

  mqtt.TestRunRetransmissions(0);

  ASSERT_EQ(1, (int)tx.m_sent_count);

  CPublish retransmit;
  ASSERT_EQ((int)MQTT_OK, retransmit.Read(tx.m_sent[0].data));
  ASSERT_STR_EQ("retry/reply", retransmit.GetResponseTopic());
  ASSERT_EQ(0, (int)retransmit.GetSubscriptionIdCount());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackSessionExpiryOverrideZeroClearsStateOnDisconnect     |
//| Broker-overridden Session Expiry = 0 must disable persistence    |
//| and clear local session state when the connection closes.        |
//+------------------------------------------------------------------+
bool TEST_ConnackSessionExpiryOverrideZeroClearsStateOnDisconnect() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetAutoReconnect(false);
  mqtt.SetCleanStart(false);
  mqtt.SetSessionExpiry(60);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestContext().session_db.Init("phase1_session_override_zero", true);

  uchar stored_payload[] = {0xAA};
  ASSERT_TRUE(mqtt.TestContext().session_db.StoreOutgoingMessage(11, QoS_1, "persist/topic", stored_payload,
                                                                 ArraySize(stored_payload)));
  ASSERT_TRUE(mqtt.TestContext().session_db.IsPersistent());

  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x08, 0x01, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x00};
  tx.EnqueueIncoming(connack_pkt);
  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(0, (int)mqtt.GetConnackSessionExpiryInterval());
  ASSERT_FALSE(mqtt.TestContext().session_db.IsPersistent());

  SessionMessage live_msg;
  ASSERT_TRUE(mqtt.TestContext().session_db.GetMessage(11, live_msg));

  tx.m_connected = false;
  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_FALSE(mqtt.TestContext().session_db.GetMessage(11, live_msg));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackSessionExpiryOmittedRestoresConfiguredPolicy         |
//| An omitted CONNACK Session Expiry must revert to the CONNECT     |
//| value instead of leaking a prior broker override.                |
//+------------------------------------------------------------------+
bool TEST_ConnackSessionExpiryOmittedRestoresConfiguredPolicy() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetAutoReconnect(false);
  mqtt.SetCleanStart(false);
  mqtt.SetSessionExpiry(0);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestContext().session_db.Init("phase1_session_restore_configured_policy", true);

  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);
  uchar connack_with_override[] = {0x20, 0x08, 0x00, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x3C};
  tx.EnqueueIncoming(connack_with_override);
  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(mqtt.TestContext().session_db.IsPersistent());

  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);
  uchar connack_without_override[] = {0x20, 0x03, 0x00, 0x00, 0x00};
  tx.EnqueueIncoming(connack_without_override);
  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(0, (int)mqtt.GetConnackSessionExpiryInterval());
  ASSERT_FALSE(mqtt.TestContext().session_db.IsPersistent());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_QueuedPublishPropertiesSurviveDrain                         |
//| Offline queueing must preserve MQTT 5 PUBLISH properties.        |
//+------------------------------------------------------------------+
bool TEST_QueuedPublishPropertiesSurviveDrain() {
  TEST_CASE_START();

  CTestTransport        tx;
  CMqttClient           mqtt;
  MqttPublishProperties props;
  uchar                 payload[] = {0x41, 0x42};

  InitPublishProperties(props);

  mqtt.SetQueueQoS0WhenDisconnected(true);
  mqtt.SetMaxQueuedMessages(4);

  props.response_topic          = "queue/reply";
  props.content_type            = "application/octet-stream";
  props.has_message_expiry      = true;
  props.message_expiry_interval = 45;

  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("queue/topic", payload, ArraySize(payload), QoS_1, false, props));
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestDrainPublishQueue();

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());

  CPublish pub;
  ASSERT_EQ((int)MQTT_OK, pub.Read(tx.m_sent[0].data));
  ASSERT_STR_EQ("queue/topic", pub.GetTopicName());
  ASSERT_STR_EQ("queue/reply", pub.GetResponseTopic());
  ASSERT_STR_EQ("application/octet-stream", pub.GetContentType());
  ASSERT_TRUE(pub.HasMessageExpiry());
  ASSERT_TRUE((int)pub.GetMessageExpiryInterval() > 0);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ExpiredQueuedPublishesArePurgedWhileOffline                 |
//| Disconnected Poll() must reclaim expired queued publishes so     |
//| they do not consume memory and queue capacity indefinitely.      |
//+------------------------------------------------------------------+
bool TEST_ExpiredQueuedPublishesArePurgedWhileOffline() {
  TEST_CASE_START();

  CMqttClient           mqtt;
  MqttPublishProperties props;
  uchar                 payload[] = {0x51};

  InitPublishProperties(props);
  props.has_message_expiry      = true;
  props.message_expiry_interval = 1;

  ASSERT_EQ((int)MQTT_PUB_QUEUED,
            (int)mqtt.Publish("queue/expire", payload, ArraySize(payload), QoS_1, false, props));
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  Sleep(1100);
  mqtt.Poll();

  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_QueuedPublishExpiryRoundsUpOnDrain                          |
//| A queued publish with less than one second remaining must still  |
//| carry Message Expiry Interval=1, not silently lose expiry.       |
//+------------------------------------------------------------------+
bool TEST_QueuedPublishExpiryRoundsUpOnDrain() {
  TEST_CASE_START();

  CTestTransport        tx;
  CMqttClient           mqtt;
  MqttPublishProperties props;
  uchar                 payload[] = {0x61, 0x62};

  InitPublishProperties(props);
  props.has_message_expiry      = true;
  props.message_expiry_interval = 1;

  ASSERT_EQ((int)MQTT_PUB_QUEUED,
            (int)mqtt.Publish("queue/round-up", payload, ArraySize(payload), QoS_1, false, props));

  Sleep(600);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestDrainPublishQueue();

  ASSERT_EQ(1, (int)tx.m_sent_count);

  CPublish pub;
  ASSERT_EQ((int)MQTT_OK, pub.Read(tx.m_sent[0].data));
  ASSERT_TRUE(pub.HasMessageExpiry());
  ASSERT_EQ(1, (int)pub.GetMessageExpiryInterval());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DurableQueuedPublishCountSurface                            |
//| Public telemetry must report durable offline queued publishes.   |
//+------------------------------------------------------------------+
bool TEST_DurableQueuedPublishCountSurface() {
  TEST_CASE_START();

  const string session_id = "mqtt_test_durable_queue_count_surface";
  ResetPersistentSessionStore(session_id);

  CTestTransport tx;
  CMqttClient    mqtt;
  uchar          payload[] = {0x31, 0x32};

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetCleanStart(false);
  mqtt.SetSessionExpiry(60);
  ASSERT_TRUE(mqtt.TestContext().session_db.Init(session_id, true));

  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("durable/count", payload, ArraySize(payload), QoS_1, false));
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(1, (int)mqtt.GetDurableQueuedMessageCount());

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestDrainPublishQueue();

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetDurableQueuedMessageCount());

  mqtt.TestContext().session_db.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_FlushSessionStateNowSucceedsWhenAlreadyDurable              |
//| FlushSessionStateNow must be idempotent once persistence has      |
//| already written the accepted offline queue entry to disk.         |
//+------------------------------------------------------------------+
bool TEST_FlushSessionStateNowSucceedsWhenAlreadyDurable() {
  TEST_CASE_START();

  const string session_id = "mqtt_test_flush_session_state_now_idempotent";
  ResetPersistentSessionStore(session_id);

  CMqttClient mqtt;
  uchar       payload[] = {0x61, 0x62, 0x63};

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetCleanStart(false);
  mqtt.SetSessionExpiry(60);
  ASSERT_TRUE(mqtt.TestContext().session_db.Init(session_id, true));

  ASSERT_EQ((int)MQTT_PUB_QUEUED,
            (int)mqtt.Publish("durable/flush", payload, ArraySize(payload), QoS_1, false));
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(1, (int)mqtt.GetDurableQueuedMessageCount());
  ASSERT_TRUE(mqtt.FlushSessionStateNow());
  ASSERT_TRUE(mqtt.FlushSessionStateNow());

  CMqttClient restored;
  ASSERT_TRUE(restored.TestContext().session_db.Init(session_id, true));
  restored.TestRestorePersistedPublishQueue();
  ASSERT_EQ(1, (int)restored.GetQueuedMessageCount());
  ASSERT_EQ(1, (int)restored.GetDurableQueuedMessageCount());

  restored.TestContext().session_db.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_OfflineQueuedPublishSurvivesClientRestart                   |
//| Persisted offline publishes must restore into a new facade       |
//| instance and drain after reconnect.                              |
//+------------------------------------------------------------------+
bool TEST_OfflineQueuedPublishSurvivesClientRestart() {
  TEST_CASE_START();

  const string session_id = "mqtt_test_offline_queue_restart_surface";
  ResetPersistentSessionStore(session_id);

  {
    CMqttClient phase1;
    uchar       payload[] = {0x44, 0x45, 0x46};

    phase1.SetHost("broker.test", 1883);
    phase1.SetCleanStart(false);
    phase1.SetSessionExpiry(60);
    ASSERT_TRUE(phase1.TestContext().session_db.Init(session_id, true));

    ASSERT_EQ((int)MQTT_PUB_QUEUED,
              (int)phase1.Publish("restart/topic", payload, ArraySize(payload), QoS_1, false));
    ASSERT_EQ(1, (int)phase1.GetQueuedMessageCount());
    ASSERT_EQ(1, (int)phase1.GetDurableQueuedMessageCount());
  }

  Sleep(2100);

  CTestTransport tx;
  CMqttClient    phase2;

  ASSERT_TRUE(phase2.TestContext().session_db.Init(session_id, true));
  phase2.TestRestorePersistedPublishQueue();

  ASSERT_EQ(1, (int)phase2.GetQueuedMessageCount());
  ASSERT_EQ(1, (int)phase2.GetDurableQueuedMessageCount());
  ASSERT_TRUE(phase2.GetOldestQueuedMessageAgeMs() >= 1000ULL);

  phase2.TestInjectTransport(GetPointer(tx));
  phase2.TestSetState(MQTT_CLIENT_CONNECTED);
  phase2.TestDrainPublishQueue();

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(0, (int)phase2.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)phase2.GetDurableQueuedMessageCount());

  CPublish pub;
  ASSERT_EQ((int)MQTT_OK, pub.Read(tx.m_sent[0].data));
  ASSERT_STR_EQ("restart/topic", pub.GetTopicName());

  phase2.TestContext().session_db.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_IncomingStorageErrorCountSurvivesClientRestart             |
//| Persisted incoming-storage failures must restore and clear only |
//| after a successful CONNACK.                                     |
//+------------------------------------------------------------------+
bool TEST_IncomingStorageErrorCountSurvivesClientRestart() {
  TEST_CASE_START();

  const string client_id  = "test_incoming_storage_restart";
  const string session_id = "mqtt_" + client_id;
  ResetPersistentSessionStore(session_id);

  CSessionDatabase writer;
  ASSERT_TRUE(writer.Init(session_id, true));
  writer.SetIncomingStorageErrorCount(2);

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetClientId(client_id);
  mqtt.SetCleanStart(false);
  mqtt.SetSessionExpiry(60);
  mqtt.TestInjectTransport(GetPointer(tx));

  ASSERT_EQ((int)TRANSPORT_CONNECTING, (int)mqtt.Connect());
  ASSERT_EQ(2, (int)mqtt.TestGetIncomingStorageErrorCount());

  uchar connack_pkt[] = {0x20, 0x03, 0x00, 0x00, 0x00};
  tx.EnqueueIncoming(connack_pkt);
  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(0, (int)mqtt.TestGetIncomingStorageErrorCount());

  CSessionDatabase reader;
  ASSERT_TRUE(reader.Init(session_id, false));
  reader.SetPersistence(true);
  ASSERT_TRUE(reader.LoadFromFile());
  ASSERT_EQ(0, (int)reader.GetIncomingStorageErrorCount());

  reader.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_EncryptedSessionRoundTrip                                  |
//| Encrypted session files must save and restore with the same     |
//| passphrase and mark the persisted file as encrypted.            |
//+------------------------------------------------------------------+
bool TEST_EncryptedSessionRoundTrip() {
  TEST_CASE_START();

  const string session_id = "mqtt_test_encrypted_session_round_trip";
  ResetPersistentSessionStore(session_id);

  CSessionDatabase writer;
  ASSERT_TRUE(writer.Init(session_id, true));
  writer.SetEncryptionPassphrase("round-trip-secret");

  uchar payload[] = {0x41, 0x42, 0x43, 0x44};
  ASSERT_TRUE(writer.StoreOutgoingMessage(7, QoS_1, "secure/topic", payload, ArraySize(payload)));
  ASSERT_TRUE(writer.SaveToFile());

  uchar file_bytes[];
  ASSERT_TRUE(ReadPersistentSessionStoreBytes(session_id, file_bytes));
  ASSERT_TRUE(ArraySize(file_bytes) > 10);
  ASSERT_EQ((int)MQTT_SESSION_FILE_FLAG_ENCRYPTED, (int)file_bytes[5]);

  CSessionDatabase reader;
  ASSERT_TRUE(reader.Init(session_id, true));
  reader.SetEncryptionPassphrase("round-trip-secret");
  ASSERT_TRUE(reader.LoadFromFile());

  SessionMessage restored;
  ASSERT_TRUE(reader.GetMessage(7, restored));
  ASSERT_TRUE(restored.is_outgoing);
  ASSERT_STR_EQ("secure/topic", restored.topic);
  ASSERT_EQ(ArraySize(payload), (int)restored.payload_size);
  ASSERT_EQ(ArraySize(payload), ArraySize(restored.payload));
  for (int payload_index = 0; payload_index < ArraySize(payload); payload_index++) {
    ASSERT_EQ((int)payload[payload_index], (int)restored.payload[payload_index]);
  }

  reader.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_EncryptedSessionWrongPassphraseFailsLoad                   |
//| Loading with the wrong passphrase must fail without restoring   |
//| persisted message state.                                        |
//+------------------------------------------------------------------+
bool TEST_EncryptedSessionWrongPassphraseFailsLoad() {
  TEST_CASE_START();

  const string session_id = "mqtt_test_encrypted_session_wrong_passphrase";
  ResetPersistentSessionStore(session_id);

  CSessionDatabase writer;
  ASSERT_TRUE(writer.Init(session_id, true));
  writer.SetEncryptionPassphrase("correct-secret");

  uchar payload[] = {0x55, 0x66, 0x77};
  ASSERT_TRUE(writer.StoreOutgoingMessage(11, QoS_1, "secure/wrong-pass", payload, ArraySize(payload)));
  ASSERT_TRUE(writer.SaveToFile());

  CSessionDatabase reader;
  ASSERT_TRUE(reader.Init(session_id, true));
  reader.SetEncryptionPassphrase("wrong-secret");
  ASSERT_FALSE(reader.LoadFromFile());

  SessionMessage restored;
  ASSERT_FALSE(reader.GetMessage(11, restored));

  reader.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_EncryptedSessionTamperFailsLoad                            |
//| Ciphertext tampering must prevent the persisted session from    |
//| loading even with the correct passphrase.                       |
//+------------------------------------------------------------------+
bool TEST_EncryptedSessionTamperFailsLoad() {
  TEST_CASE_START();

  const string session_id = "mqtt_test_encrypted_session_tamper";
  ResetPersistentSessionStore(session_id);

  CSessionDatabase writer;
  ASSERT_TRUE(writer.Init(session_id, true));
  writer.SetEncryptionPassphrase("tamper-secret");

  uchar payload[] = {0x10, 0x20, 0x30};
  ASSERT_TRUE(writer.StoreOutgoingMessage(23, QoS_1, "secure/tamper", payload, ArraySize(payload)));
  ASSERT_TRUE(writer.SaveToFile());
  ASSERT_TRUE(TamperPersistentSessionStoreByte(session_id, 10, 0x5A));

  CSessionDatabase reader;
  ASSERT_TRUE(reader.Init(session_id, true));
  reader.SetEncryptionPassphrase("tamper-secret");
  ASSERT_FALSE(reader.LoadFromFile());

  SessionMessage restored;
  ASSERT_FALSE(reader.GetMessage(23, restored));

  reader.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_FlatBufferAppendSizeRejectsOverflow                         |
//| Guard calculations must reject 32-bit array append overflow.     |
//+------------------------------------------------------------------+
bool TEST_FlatBufferAppendSizeRejectsOverflow() {
  TEST_CASE_START();

  CMqttClient mqtt;
  int         new_size = 0;

  ASSERT_TRUE(mqtt.TestTryComputeArrayAppendSize(2147483600u, 40u, new_size));
  ASSERT_EQ(2147483640, new_size);

  ASSERT_FALSE(mqtt.TestTryComputeArrayAppendSize(2147483640u, 16u, new_size));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_RetransmitPreservesPublishProperties                        |
//| QoS retransmission must rebuild retained publishes with props.   |
//+------------------------------------------------------------------+
bool TEST_RetransmitPreservesPublishProperties() {
  TEST_CASE_START();

  CTestTransport        tx;
  CMqttClient           mqtt;
  MqttPublishProperties props;

  InitPublishProperties(props);

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetRetransmitTimeout(0);

  props.response_topic          = "retry/reply";
  props.content_type            = "text/plain";
  props.has_message_expiry      = true;
  props.message_expiry_interval = 60;

  ASSERT_EQ((int)MQTT_PUB_OK, (int)mqtt.Publish("retry/topic", "again", QoS_1, true, props));
  ASSERT_EQ(1, (int)tx.m_sent_count);

  mqtt.TestRunRetransmissions(0);
  ASSERT_EQ(2, (int)tx.m_sent_count);

  CPublish retransmit;
  ASSERT_EQ((int)MQTT_OK, retransmit.Read(tx.m_sent[1].data));
  ASSERT_TRUE(retransmit.GetDup());
  ASSERT_TRUE(retransmit.GetRetain());
  ASSERT_STR_EQ("retry/reply", retransmit.GetResponseTopic());
  ASSERT_STR_EQ("text/plain", retransmit.GetContentType());
  ASSERT_TRUE(retransmit.HasMessageExpiry());
  ASSERT_TRUE((int)retransmit.GetMessageExpiryInterval() > 0);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_DiagnosticsCallbacksSurfaceMetadata                         |
//| Extended callbacks must carry MQTT 5 reason strings and props.   |
//+------------------------------------------------------------------+
bool TEST_DiagnosticsCallbacksSurfaceMetadata() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnAck(TestOnAckEx);
  mqtt.SetOnSubscribeAck(TestOnSubackEx);
  mqtt.SetOnUnsubscribeAck(TestOnUnsubackEx);
  mqtt.SetOnDisconnect(TestOnDisconnectEx);

  uchar puback[] = {0x40, 0x13, 0x12, 0x34, 0x97, 0x0F, 0x1F, 0x00, 0x05, 'q', 'u',
                    'o',  't',  'a',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};
  tx.EnqueueIncoming(puback);
  mqtt.Poll();
  ASSERT_EQ(1, g_cb_ack_ex_count);
  ASSERT_EQ((int)PUBACK, g_cb_ack_ex_packet_type);
  ASSERT_EQ(0x1234, (int)g_cb_ack_ex_packet_id);
  ASSERT_EQ(0x97, g_cb_ack_ex_reason_code);
  ASSERT_STR_EQ("quota", g_cb_ack_ex_reason);
  ASSERT_EQ(1, g_cb_ack_ex_user_prop_count);
  ASSERT_STR_EQ("k", g_cb_ack_ex_user_key);
  ASSERT_STR_EQ("v", g_cb_ack_ex_user_val);

  uchar suback[] = {0x90, 0x12, 0x00, 0x01, 0x0E, 0x1F, 0x00, 0x04, 'n', 'o',
                    'p',  'e',  0x26, 0x00, 0x01, 'a',  0x00, 0x01, 'b', 0x01};
  mqtt.TestOnSubackReceived(suback);
  ASSERT_EQ(1, g_cb_suback_ex_count);
  ASSERT_STR_EQ("nope", g_cb_suback_ex_reason);

  uchar unsuback[] = {0xB0, 0x12, 0x00, 0x02, 0x0E, 0x1F, 0x00, 0x04, 'l', 'a',
                      't',  'e',  0x26, 0x00, 0x01, 'x',  0x00, 0x01, 'y', 0x00};
  tx.EnqueueIncoming(unsuback);
  mqtt.Poll();
  ASSERT_EQ(1, g_cb_unsuback_ex_count);
  ASSERT_STR_EQ("late", g_cb_unsuback_ex_reason);

  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  uchar disconnect[] = {0xE0, 0x13, 0x9D, 0x11, 0x1F, 0x00, 0x03, 'b',  'y',  'e', 0x1C,
                        0x00, 0x01, 'h',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};
  tx.EnqueueIncoming(disconnect);
  mqtt.Poll();
  ASSERT_EQ(1, g_cb_disconnect_ex_count);
  ASSERT_STR_EQ("h", g_cb_disconnect_ex_server_ref);
  ASSERT_EQ(1, g_cb_disconnect_ex_user_prop_count);
  ASSERT_STR_EQ("k", g_cb_disconnect_ex_user_key);
  ASSERT_STR_EQ("v", g_cb_disconnect_ex_user_val);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_OutgoingAckDiagnosticsSurface                               |
//| Auto-generated PUBACK/PUBREC/PUBREL/PUBCOMP must emit the        |
//| configured MQTT 5 Reason String and User Property set.           |
//+------------------------------------------------------------------+
bool TEST_OutgoingAckDiagnosticsSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       pub;
  CPubrel        rel;
  uchar          payload[]          = {0x41};
  uchar          expected_puback[]  = {0x40, 0x12, 0x00, 0x15, 0x00, 0x0E, 0x1F, 0x00, 0x04, 'd',
                                       'i',  'a',  'g',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};
  uchar          expected_pubrec[]  = {0x50, 0x12, 0x00, 0x16, 0x00, 0x0E, 0x1F, 0x00, 0x04, 'd',
                                       'i',  'a',  'g',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};
  uchar          expected_pubrel[]  = {0x62, 0x12, 0x00, 0x01, 0x00, 0x0E, 0x1F, 0x00, 0x04, 'd',
                                       'i',  'a',  'g',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};
  uchar          expected_pubcomp[] = {0x70, 0x12, 0x00, 0x17, 0x00, 0x0E, 0x1F, 0x00, 0x04, 'd',
                                       'i',  'a',  'g',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetAckReasonString(PUBACK, "diag");
  mqtt.SetAckReasonString(PUBREC, "diag");
  mqtt.SetAckReasonString(PUBREL, "diag");
  mqtt.SetAckReasonString(PUBCOMP, "diag");
  mqtt.AddAckUserProperty(PUBACK, "k", "v");
  mqtt.AddAckUserProperty(PUBREC, "k", "v");
  mqtt.AddAckUserProperty(PUBREL, "k", "v");
  mqtt.AddAckUserProperty(PUBCOMP, "k", "v");

  pub.SetTopicName("ack/qos1");
  pub.SetPacketId(21);
  pub.SetQoS_1(true);
  pub.SetPayload(payload);
  uchar pkt_qos1[];
  pub.Build(pkt_qos1);
  mqtt.TestOnPublishReceived(pkt_qos1);
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_TRUE(AssertEqual(expected_puback, tx.m_sent[0].data));

  tx.ClearSentPackets();
  pub.Reset();
  pub.SetTopicName("ack/qos2");
  pub.SetPacketId(22);
  pub.SetQoS_2(true);
  pub.SetPayload(payload);
  uchar pkt_qos2[];
  pub.Build(pkt_qos2);
  mqtt.TestOnPublishReceived(pkt_qos2);
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_TRUE(AssertEqual(expected_pubrec, tx.m_sent[0].data));

  tx.ClearSentPackets();
  ASSERT_EQ((int)MQTT_PUB_OK, (int)mqtt.Publish("ack/outgoing", "x", QoS_2, false));
  ASSERT_EQ(1, (int)tx.m_sent_count);
  uchar pubrec_in[] = {0x50, 0x02, 0x00, 0x01};
  tx.EnqueueIncoming(pubrec_in);
  mqtt.Poll();
  ASSERT_EQ(2, (int)tx.m_sent_count);
  ASSERT_TRUE(AssertEqual(expected_pubrel, tx.m_sent[1].data));

  tx.ClearSentPackets();
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  pub.Reset();
  pub.SetTopicName("ack/pubcomp");
  pub.SetPacketId(23);
  pub.SetQoS_2(true);
  pub.SetPayload(payload);
  uchar pkt_qos2_comp[];
  pub.Build(pkt_qos2_comp);
  mqtt.TestOnPublishReceived(pkt_qos2_comp);
  ASSERT_EQ(1, (int)tx.m_sent_count);
  rel.SetPacketId(23);
  uchar pubrel_pkt[];
  rel.Build(pubrel_pkt);
  tx.EnqueueIncoming(pubrel_pkt);
  mqtt.Poll();
  ASSERT_EQ(2, (int)tx.m_sent_count);
  ASSERT_TRUE(AssertEqual(expected_pubcomp, tx.m_sent[1].data));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ImmediateConnectFailurePreservesReconnect                   |
//| Immediate setup failure during auto-reconnect must not stop the  |
//| reconnect state machine.                                         |
//+------------------------------------------------------------------+
bool TEST_ImmediateConnectFailurePreservesReconnect() {
  TEST_CASE_START();

  ResetCallbackState();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.SetAutoReconnect(true, 250, 1000);
  mqtt.TestStartReconnect();

  uint backoff_before = mqtt.TestGetReconnectBackoff();
  ASSERT_TRUE(mqtt.TestIsReconnecting());

  ENUM_TRANSPORT_ERROR err = mqtt.TestHandleConnectSetupFailure(TRANSPORT_ERROR_SOCKET, "ConnectAsync failed");

  ASSERT_EQ((int)TRANSPORT_ERROR_SOCKET, (int)err);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(mqtt.TestIsReconnecting());
  ASSERT_EQ((int)backoff_before, (int)mqtt.TestGetReconnectBackoff());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)TRANSPORT_ERROR_SOCKET, g_cb_error_code);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ReconnectTelemetrySurface                                   |
//| Public reconnect telemetry should surface operator state.        |
//+------------------------------------------------------------------+
bool TEST_ReconnectTelemetrySurface() {
  TEST_CASE_START();

  CMqttClient mqtt;

  ASSERT_FALSE(mqtt.IsReconnectInProgress());
  ASSERT_EQ(0, (int)mqtt.GetReconnectAttemptCount());
  ASSERT_EQ(12, (int)mqtt.GetMaxReconnectAttempts());

  mqtt.SetAutoReconnect(true, 250, 1000);
  mqtt.SetMaxReconnectAttempts(7);
  mqtt.TestSetReconnectAttemptCount(3);
  mqtt.TestStartReconnect();

  ASSERT_TRUE(mqtt.IsReconnectInProgress());
  ASSERT_EQ(3, (int)mqtt.GetReconnectAttemptCount());
  ASSERT_EQ(7, (int)mqtt.GetMaxReconnectAttempts());
  mqtt.SetMaxReconnectAttempts(0);
  ASSERT_EQ(0, (int)mqtt.GetMaxReconnectAttempts());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_InFlightTelemetrySurface                                    |
//| Public in-flight getters should mirror flow-control state.       |
//+------------------------------------------------------------------+
bool TEST_InFlightTelemetrySurface() {
  TEST_CASE_START();

  CMqttClient mqtt;

  ASSERT_EQ(0, (int)mqtt.GetInFlightCount());
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS1Count());
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS2Count());
  ASSERT_EQ(0, (int)mqtt.GetIncomingInFlightCount());

  ASSERT_TRUE(mqtt.TestContext().flow_control.RegisterOutgoingQoS(10, QoS_1, 12));
  ASSERT_TRUE(mqtt.TestContext().flow_control.RegisterOutgoingQoS(11, QoS_1, 12));
  ASSERT_TRUE(mqtt.TestContext().flow_control.RegisterOutgoingQoS(12, QoS_2, 12));

  ASSERT_EQ(3, (int)mqtt.GetInFlightCount());
  ASSERT_EQ(2, (int)mqtt.GetInFlightQoS1Count());
  ASSERT_EQ(1, (int)mqtt.GetInFlightQoS2Count());
  ASSERT_EQ(0, (int)mqtt.GetIncomingInFlightCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_QueuedAgeTelemetrySurface                                   |
//| Oldest queued age should reflect the oldest live in-memory item. |
//+------------------------------------------------------------------+
bool TEST_QueuedAgeTelemetrySurface() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetHost("broker.test", 1883);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("age/1", "one", QoS_1));
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("age/2", "two", QoS_1));

  ulong now_us = GetMicrosecondCount();
  mqtt.TestSetQueuedEnqueueTimeUs(0, now_us - 2500000ULL);
  mqtt.TestSetQueuedEnqueueTimeUs(1, now_us - 500000ULL);

  ulong oldest_age_ms = mqtt.GetOldestQueuedMessageAgeMs();
  ASSERT_TRUE(oldest_age_ms >= 2000ULL);
  ASSERT_TRUE(oldest_age_ms < 10000ULL);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_CallbackBacklogTelemetrySurface                             |
//| Callback backlog count should reflect deferred delivery work.    |
//+------------------------------------------------------------------+
bool TEST_CallbackBacklogTelemetrySurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       publish;
  uchar          pkt[];

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxPacketsPerPoll(1);
  mqtt.SetOnMessage(TestOnMessage);
  mqtt.Subscribe("telemetry/#", TestOnMessage, QoS_1);
  mqtt.Subscribe("telemetry/status", TestOnMessage, QoS_1);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  publish.SetTopicName("telemetry/status");
  publish.SetPayload("ok");
  publish.SetQoS_1(true);
  publish.SetPacketId(7);
  publish.Build(pkt);

  mqtt.TestOnPublishReceived(pkt);

  ASSERT_EQ(1, g_cb_message_count);
  ASSERT_EQ(1, (int)mqtt.GetCallbackBacklogCount());
  ASSERT_TRUE(mqtt.GetCallbackBacklogPayloadBytes() > 0);
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPropertyBytes());

  mqtt.TestDrainMessageCallbacks();

  ASSERT_EQ(2, g_cb_message_count);
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPayloadBytes());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPropertyBytes());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DeferredTransportBacklogTelemetrySurface                    |
//| Deferred transport telemetry should reflect packets clipped by   |
//| the per-poll dispatch budget until later Poll() calls drain them.|
//+------------------------------------------------------------------+
bool TEST_DeferredTransportBacklogTelemetrySurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       p1;
  CPublish       p2;
  CPublish       p3;
  uchar          payload1[] = {0x31};
  uchar          payload2[] = {0x32};
  uchar          payload3[] = {0x33};
  uchar          pkt1[];
  uchar          pkt2[];
  uchar          pkt3[];

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetMaxPacketsPerPoll(1);

  p1.SetTopicName("telemetry/one");
  p1.SetPayload(payload1);
  p1.Build(pkt1);

  p2.SetTopicName("telemetry/two");
  p2.SetPayload(payload2);
  p2.Build(pkt2);

  p3.SetTopicName("telemetry/three");
  p3.SetPayload(payload3);
  p3.Build(pkt3);

  tx.EnqueueIncoming(pkt1);
  tx.EnqueueIncoming(pkt2);
  tx.EnqueueIncoming(pkt3);

  mqtt.Poll();
  ASSERT_EQ(2, (int)mqtt.GetDeferredTransportBacklogCount());
  ASSERT_EQ(ArraySize(pkt2) + ArraySize(pkt3), (int)mqtt.GetDeferredTransportBacklogBytes());

  mqtt.Poll();
  ASSERT_EQ(1, (int)mqtt.GetDeferredTransportBacklogCount());
  ASSERT_EQ(ArraySize(pkt3), (int)mqtt.GetDeferredTransportBacklogBytes());

  mqtt.Poll();
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogBytes());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DeferredTransportBacklogOverflowDisconnects                 |
//| Transport backlog overflow must fail closed with reason 0x83 and |
//| clear deferred transport telemetry.                              |
//+------------------------------------------------------------------+
bool TEST_DeferredTransportBacklogOverflowDisconnects() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       p1;
  CPublish       p2;
  CPublish       p3;
  uchar          payload1[] = {0x41};
  uchar          payload2[] = {0x42};
  uchar          payload3[] = {0x43};
  uchar          pkt1[];
  uchar          pkt2[];
  uchar          pkt3[];

  ResetCallbackState();
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetMaxPacketsPerPoll(1);
  mqtt.SetMaxDeferredTransportPackets(1);

  p1.SetTopicName("overflow/one");
  p1.SetPayload(payload1);
  p1.Build(pkt1);

  p2.SetTopicName("overflow/two");
  p2.SetPayload(payload2);
  p2.Build(pkt2);

  p3.SetTopicName("overflow/three");
  p3.SetPayload(payload3);
  p3.Build(pkt3);

  tx.EnqueueIncoming(pkt1);
  tx.EnqueueIncoming(pkt2);
  tx.EnqueueIncoming(pkt3);

  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, g_cb_error_code);
  ASSERT_TRUE(StringFind(g_cb_error_desc, "Deferred transport backlog packet limit reached") >= 0);
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, g_cb_disconnect_code);
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogBytes());
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(0xE0, (int)tx.m_sent[0].data[0]);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, (int)tx.m_sent[0].data[2]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DeferredCallbackBacklogOverflowDisconnects                  |
//| Callback backlog overflow must fail closed with reason 0x83 and  |
//| clear deferred callback telemetry.                               |
//+------------------------------------------------------------------+
bool TEST_DeferredCallbackBacklogOverflowDisconnects() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       publish;
  uchar          payload[] = {0x6F, 0x6B};
  uchar          pkt[];

  ResetCallbackState();
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.SetMaxDeferredCallbackEvents(1);
  mqtt.Subscribe("telemetry/#", TestOnMessage, QoS_1);
  mqtt.Subscribe("telemetry/status", TestOnMessage, QoS_1);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  publish.SetTopicName("telemetry/status");
  publish.SetPayload(payload);
  publish.Build(pkt);

  tx.EnqueueIncoming(pkt);
  mqtt.Poll();

  ASSERT_EQ(0, g_cb_message_count);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, g_cb_error_code);
  ASSERT_TRUE(StringFind(g_cb_error_desc, "Deferred callback backlog count limit reached") >= 0);
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, g_cb_disconnect_code);
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPayloadBytes());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPropertyBytes());
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(0xE0, (int)tx.m_sent[0].data[0]);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, (int)tx.m_sent[0].data[2]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_LastFailureTelemetrySurface                                 |
//| Last-failure getters should capture classified error details.    |
//+------------------------------------------------------------------+
bool TEST_LastFailureTelemetrySurface() {
  TEST_CASE_START();

  ResetCallbackState();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);

  uchar connack_pkt[] = {0x20, 0x03, 0x00, 0x86, 0x00};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_EQ(0x86, mqtt.GetLastFailureCode());
  ASSERT_EQ((int)MQTT_FAILURE_AUTHENTICATION, (int)mqtt.GetLastFailureClass());
  ASSERT_TRUE(StringFind(mqtt.GetLastFailureDescription(), "CONNACK rejected") >= 0);
  ASSERT_EQ(0x86, g_cb_error_code);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnackTimeoutBrokerFailureSurface                         |
//| Broker-side CONNACK timeout must be tagged separately from      |
//| transport-setup timeouts.                                       |
//+------------------------------------------------------------------+
bool TEST_ConnackTimeoutBrokerFailureSurface() {
  TEST_CASE_START();

  ResetCallbackState();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetConnackTimeout(1);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));

  ASSERT_EQ((int)TRANSPORT_CONNECTING, (int)mqtt.Connect());
  ASSERT_EQ((int)MQTT_CLIENT_WAITING_CONNACK, (int)mqtt.GetState());
  ASSERT_EQ(1, (int)tx.m_sent_count);

  Sleep(20);
  mqtt.Poll();

  ASSERT_EQ(1, g_cb_error_count);
  ASSERT_EQ((int)TRANSPORT_ERROR_TIMEOUT, g_cb_error_code);
  ASSERT_EQ((int)MQTT_FAILURE_BROKER, (int)mqtt.GetLastFailureClass());
  ASSERT_TRUE(StringFind(mqtt.GetLastFailureDescription(), "MQTT_FAILURE_BROKER:") == 0);
  ASSERT_TRUE(StringFind(g_cb_error_desc, "MQTT_FAILURE_BROKER:") == 0);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_RedirectAllowlistBlocksUnapprovedHost                       |
//| Auto-redirect should refuse hosts outside the configured list.   |
//+------------------------------------------------------------------+
bool TEST_RedirectAllowlistBlocksUnapprovedHost() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetHost("primary.test", 1883);
  mqtt.SetAutoRedirect(true);
  mqtt.SetRequireRedirectAllowlist(true);

  ASSERT_FALSE(mqtt.TestHandleRedirection(MQTT_REASON_CODE_SERVER_MOVED, "backup.test:1884"));
  ASSERT_FALSE(mqtt.TestIsRedirectPending());
  ASSERT_STR_EQ("primary.test", mqtt.TestGetHost());
  ASSERT_EQ(1883, (int)mqtt.TestGetPort());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_RedirectAllowlistAllowsApprovedHost                         |
//| Auto-redirect should accept approved hosts case-insensitively.   |
//+------------------------------------------------------------------+
bool TEST_RedirectAllowlistAllowsApprovedHost() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetHost("primary.test", 1883);
  mqtt.SetAutoRedirect(true);
  mqtt.SetRequireRedirectAllowlist(true);
  mqtt.AddRedirectAllowHost("BACKUP.TEST");

  ASSERT_TRUE(mqtt.TestHandleRedirection(MQTT_REASON_CODE_USE_ANOTHER_SERVER, "backup.test:1884"));
  ASSERT_TRUE(mqtt.TestIsRedirectPending());
  ASSERT_STR_EQ("backup.test", mqtt.TestGetHost());
  ASSERT_EQ(1884, (int)mqtt.TestGetPort());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_RedirectAllowlistCannotBeDisabled                           |
//| Auto-redirect must still fail closed when callers attempt to     |
//| disable the legacy allowlist guard.                              |
//+------------------------------------------------------------------+
bool TEST_RedirectAllowlistCannotBeDisabled() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetHost("primary.test", 1883);
  mqtt.SetAutoRedirect(true);
  mqtt.SetRequireRedirectAllowlist(false);

  ASSERT_FALSE(mqtt.TestHandleRedirection(MQTT_REASON_CODE_SERVER_MOVED, "backup.test:1884"));
  ASSERT_FALSE(mqtt.TestIsRedirectPending());
  ASSERT_STR_EQ("primary.test", mqtt.TestGetHost());
  ASSERT_EQ(1883, (int)mqtt.TestGetPort());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectWithoutHost                                          |
//| Connect() without configuring a host should fail gracefully.     |
//+------------------------------------------------------------------+
bool TEST_ConnectWithoutHost() {
  TEST_CASE_START();

  CMqttClient          mqtt;

  ENUM_TRANSPORT_ERROR err = mqtt.Connect();
  ASSERT_EQ((int)TRANSPORT_ERROR_SOCKET, (int)err);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectRejectsPlaintextCredentialsByDefault                 |
//| CONNECT auth over plaintext must be denied unless explicitly     |
//| opted into by the caller.                                        |
//+------------------------------------------------------------------+
bool TEST_ConnectRejectsPlaintextCredentialsByDefault() {
  TEST_CASE_START();

  ResetCallbackState();

  CMqttClient mqtt;
  mqtt.SetHost("127.0.0.1", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.SetAllowInsecurePlaintextTransport(true);
  mqtt.SetCredentials("admin", "secret");

  ENUM_TRANSPORT_ERROR err = mqtt.Connect();

  ASSERT_EQ((int)TRANSPORT_ERROR_TLS, (int)err);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, g_cb_error_code);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectAllowsPlaintextCredentialsWhenExplicitlyPermitted    |
//| Explicit opt-in must bypass only the preflight TLS policy block  |
//| rather than being rejected before any transport attempt.         |
//+------------------------------------------------------------------+
bool TEST_ConnectAllowsPlaintextCredentialsWhenExplicitlyPermitted() {
  TEST_CASE_START();

  ResetCallbackState();

  CMqttClient mqtt;
  mqtt.SetHost("127.0.0.1", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.SetAllowInsecurePlaintextTransport(true);
  mqtt.SetCredentials("admin", "secret");
  mqtt.SetAllowInsecurePlaintextAuth(true);

  ENUM_TRANSPORT_ERROR err = mqtt.Connect();

  ASSERT_TRUE((int)err != (int)TRANSPORT_ERROR_TLS);
  ASSERT_EQ(0, g_cb_error_count);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectRejectsPlaintextTransportByDefault                   |
//| Plain TCP must fail closed unless the caller explicitly opts in. |
//+------------------------------------------------------------------+
bool TEST_ConnectRejectsPlaintextTransportByDefault() {
  TEST_CASE_START();

  ResetCallbackState();

  CMqttClient mqtt;
  mqtt.SetHost("127.0.0.1", 1883);
  mqtt.SetOnError(TestOnError);

  ENUM_TRANSPORT_ERROR err = mqtt.Connect();

  ASSERT_EQ((int)TRANSPORT_ERROR_TLS, (int)err);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, g_cb_error_code);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectAllowsPlaintextTransportWhenExplicitlyPermitted      |
//| The transport preflight block must be bypassed only when the     |
//| caller deliberately enables insecure plaintext transport.        |
//+------------------------------------------------------------------+
bool TEST_ConnectAllowsPlaintextTransportWhenExplicitlyPermitted() {
  TEST_CASE_START();

  ResetCallbackState();

  CMqttClient mqtt;
  mqtt.SetHost("127.0.0.1", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.SetAllowInsecurePlaintextTransport(true);

  ENUM_TRANSPORT_ERROR err = mqtt.Connect();

  ASSERT_TRUE((int)err != (int)TRANSPORT_ERROR_TLS);
  ASSERT_EQ(0, g_cb_error_count);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectRejectsPlainWebSocketByDefault                       |
//| Plain ws:// must follow the same fail-closed transport policy.   |
//+------------------------------------------------------------------+
bool TEST_ConnectRejectsPlainWebSocketByDefault() {
  TEST_CASE_START();

  ResetCallbackState();

  CMqttClient mqtt;
  mqtt.SetHostWS("127.0.0.1", 9001, "/mqtt");
  mqtt.SetOnError(TestOnError);

  ENUM_TRANSPORT_ERROR err = mqtt.Connect();

  ASSERT_EQ((int)TRANSPORT_ERROR_TLS, (int)err);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, g_cb_error_code);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_ReconnectUsesDedicatedConnectTimeout                        |
//| Reconnect attempts must be able to use a larger setup budget     |
//| than the first connection attempt when explicitly configured.    |
//+------------------------------------------------------------------+
bool TEST_ReconnectUsesDedicatedConnectTimeout() {
  TEST_CASE_START();

  CMqttClient mqtt;

  mqtt.SetConnectTimeout(3000);
  mqtt.SetReconnectConnectTimeout(12000);

  ASSERT_EQ(3000, (int)mqtt.TestResolveConnectTimeout(false));
  ASSERT_EQ(12000, (int)mqtt.TestResolveConnectTimeout(true));

  mqtt.TestSetHasSuccessfulConnection(true);
  ASSERT_EQ(12000, (int)mqtt.TestResolveConnectTimeout(false));

  mqtt.SetReconnectConnectTimeout(0);
  ASSERT_EQ(3000, (int)mqtt.TestResolveConnectTimeout(true));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_AsyncConnectWaitsForStableTransportTurn                     |
//| CONNECT must not be sent on the same Poll() call that still      |
//| reports transport establishment in progress.                     |
//+------------------------------------------------------------------+
bool TEST_AsyncConnectWaitsForStableTransportTurn() {
  TEST_CASE_START();

  CConnectingThenReadyTransport tx;
  CMqttClient                   mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetClientId("unit-test-connect-delay");
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTING);

  mqtt.Poll();
  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTING, (int)mqtt.GetState());

  mqtt.Poll();
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_WAITING_CONNACK, (int)mqtt.GetState());
  ASSERT_TRUE(ArraySize(tx.m_sent[0].data) > 0);
  ASSERT_EQ(0x10, (int)tx.m_sent[0].data[0]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_SendConnectDefersUntilTransportWritable                     |
//| CONNECT should wait when the first TLS application write still   |
//| reports transport establishment in progress.                     |
//+------------------------------------------------------------------+
bool TEST_SendConnectDefersUntilTransportWritable() {
  TEST_CASE_START();

  CDeferredFirstSendTransport tx;
  CMqttClient                 mqtt;

  ResetCallbackState();

  mqtt.SetHost("broker.test", 8883);
  mqtt.SetTLS(true);
  mqtt.SetClientId("unit-test-connect-send-defer");
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTING);

  mqtt.TestSendConnect();
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTING, (int)mqtt.GetState());
  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_EQ(0, g_cb_error_count);

  mqtt.TestSendConnect();
  ASSERT_EQ((int)MQTT_CLIENT_WAITING_CONNACK, (int)mqtt.GetState());
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_TRUE(ArraySize(tx.m_sent[0].data) > 0);
  ASSERT_EQ(0x10, (int)tx.m_sent[0].data[0]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_TofuRequiresProvisionedThumbprint                           |
//| Connect() must fail closed when TOFU is enabled without a        |
//| pre-provisioned or persisted MT5 certificate thumbprint.         |
//+------------------------------------------------------------------+
bool TEST_TofuRequiresProvisionedThumbprint() {
  TEST_CASE_START();

  ResetCallbackState();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 8883);
  mqtt.SetTLS(true);
  mqtt.SetTofuPinning(true);
  mqtt.SetClientId("unit-test-missing-tofu-pin-" + (string)GetTickCount());
  mqtt.SetOnError(TestOnError);

  ENUM_TRANSPORT_ERROR err = mqtt.Connect();

  ASSERT_EQ((int)TRANSPORT_ERROR_TLS, (int)err);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_TRUE(StringFind(g_cb_error_desc, "SetTofuThumbprint") >= 0);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_TofuProvisionedThumbprintPinsAndReportsPinnedTrustMode      |
//| A pre-provisioned MT5 thumbprint must verify case-insensitively  |
//| and across common separator formats.                             |
//+------------------------------------------------------------------+
bool TEST_TofuProvisionedThumbprintPinsAndReportsPinnedTrustMode() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetTLS(true);
  mqtt.SetTofuPinning(true);
  mqtt.SetTofuThumbprint("00112233445566778899AABBCCDDEEFF00112233");

  ASSERT_EQ((int)MQTT_TRUST_MODE_TOFU_PINNED, (int)mqtt.TestGetEffectiveTrustMode());
  ASSERT_TRUE(mqtt.TestEvaluateTofuCertificate(true, "00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff:00:11:22:33"));
  ASSERT_EQ((int)MQTT_TRUST_MODE_TOFU_PINNED, (int)mqtt.TestGetEffectiveTrustMode());
  ASSERT_TRUE(mqtt.TestEvaluateTofuCertificate(true, "00112233445566778899aabbccddeeff00112233"));
  ASSERT_EQ((int)MQTT_TRUST_MODE_TOFU_PINNED, (int)mqtt.TestGetEffectiveTrustMode());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_TofuReadFailureDegradesTrustMode                            |
//| Non-strict TOFU keeps the session alive but surfaces degraded    |
//| trust mode to the EA.                                            |
//+------------------------------------------------------------------+
bool TEST_TofuReadFailureDegradesTrustMode() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 8883);
  mqtt.SetTLS(true);
  mqtt.SetTofuPinning(true);
  mqtt.SetTofuThumbprint("00112233445566778899AABBCCDDEEFF00112233");
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  ASSERT_TRUE(mqtt.TestEvaluateTofuCertificate(false, ""));
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  ASSERT_EQ((int)MQTT_TRUST_MODE_TOFU_DEGRADED, (int)mqtt.TestGetEffectiveTrustMode());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_TofuStrictReadFailureDisconnects                            |
//| Strict TOFU must fail closed when certificate inspection fails.  |
//+------------------------------------------------------------------+
bool TEST_TofuStrictReadFailureDisconnects() {
  TEST_CASE_START();

  ResetCallbackState();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 8883);
  mqtt.SetTLS(true);
  mqtt.SetTofuPinning(true);
  mqtt.SetTofuThumbprint("00112233445566778899AABBCCDDEEFF00112233");
  mqtt.SetTofuStrictMode(true);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  ASSERT_FALSE(mqtt.TestEvaluateTofuCertificate(false, ""));
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_TRUE(StringFind(g_cb_error_desc, "thumbprint verification") >= 0);
  ASSERT_EQ((int)MQTT_TRUST_MODE_TOFU_DEGRADED, (int)mqtt.TestGetEffectiveTrustMode());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DisconnectWhileDisconnected                                 |
//| Disconnect() when already disconnected should be a no-op.        |
//+------------------------------------------------------------------+
bool TEST_DisconnectWhileDisconnected() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);

  mqtt.Disconnect();

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DisconnectWithReasonCode                                    |
//| Disconnect accepts a configurable reason code.                   |
//+------------------------------------------------------------------+
bool TEST_DisconnectWithReasonCode() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);

  //--- Disconnect with administrative reason
  mqtt.Disconnect(0x04);  // 0x04 = "Disconnect with Will Message"

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_DisconnectWithExtendedMetadata                              |
//| Client-initiated Disconnect must surface MQTT 5 metadata.        |
//+------------------------------------------------------------------+
bool TEST_DisconnectWithExtendedMetadata() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  string         user_prop_keys[];
  string         user_prop_vals[];

  ResetCallbackState();
  ArrayResize(user_prop_keys, 1);
  ArrayResize(user_prop_vals, 1);
  user_prop_keys[0] = "k";
  user_prop_vals[0] = "v";

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetSessionExpiry(120);
  mqtt.SetOnDisconnect(TestOnDisconnectEx);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  mqtt.Disconnect(MQTT_REASON_CODE_NORMAL_DISCONNECTION, 120, "planned maintenance", "backup.broker.test",
                  user_prop_keys, user_prop_vals);

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(1, g_cb_disconnect_ex_count);
  ASSERT_STR_EQ("planned maintenance", g_cb_disconnect_reason);
  ASSERT_STR_EQ("backup.broker.test", g_cb_disconnect_ex_server_ref);
  ASSERT_EQ(1, g_cb_disconnect_ex_user_prop_count);
  ASSERT_STR_EQ("k", g_cb_disconnect_ex_user_key);
  ASSERT_STR_EQ("v", g_cb_disconnect_ex_user_val);
  ASSERT_STR_EQ("planned maintenance", mqtt.GetLastDisconnectReasonString());
  ASSERT_STR_EQ("backup.broker.test", mqtt.GetLastDisconnectServerReference());
  ASSERT_EQ(1, (int)mqtt.GetLastDisconnectUserPropertyCount());
  ASSERT_STR_EQ("k", mqtt.GetLastDisconnectUserPropertyKey(0));
  ASSERT_STR_EQ("v", mqtt.GetLastDisconnectUserPropertyValue(0));

  CDisconnect disc;
  ASSERT_EQ((int)MQTT_OK, disc.Read(tx.m_sent[0].data));
  ASSERT_EQ((int)MQTT_REASON_CODE_NORMAL_DISCONNECTION, (int)disc.GetReasonCode());
  ASSERT_TRUE(disc.HasSessionExpiry());
  ASSERT_EQ(120, (int)disc.GetSessionExpiryInterval());
  ASSERT_STR_EQ("planned maintenance", disc.GetReasonString());
  ASSERT_STR_EQ("backup.broker.test", disc.GetServerReference());
  ASSERT_EQ(1, (int)disc.GetUserPropertyCount());
  ASSERT_STR_EQ("k", disc.GetUserPropertyKey(0));
  ASSERT_STR_EQ("v", disc.GetUserPropertyValue(0));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PollWhileDisconnected                                       |
//| Poll() while disconnected without auto-reconnect should be safe. |
//+------------------------------------------------------------------+
bool TEST_PollWhileDisconnected() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetAutoReconnect(false);

  //--- Should not crash
  mqtt.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_MetricsInitialization                                       |
//| All counters should start at zero.                               |
//+------------------------------------------------------------------+
bool TEST_MetricsInitialization() {
  TEST_CASE_START();

  CMqttClient mqtt;

  ASSERT_EQ(0, (int)mqtt.GetReconnectCount());
  ASSERT_FALSE(mqtt.IsReconnectInProgress());
  ASSERT_EQ(0, (int)mqtt.GetReconnectAttemptCount());
  ASSERT_EQ(12, (int)mqtt.GetMaxReconnectAttempts());
  ASSERT_EQ(0, (int)mqtt.GetMessagesSent());
  ASSERT_EQ(0, (int)mqtt.GetMessagesReceived());
  ASSERT_EQ(0, (int)mqtt.GetLastPingRTT());
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetDurableQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetInFlightCount());
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS1Count());
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS2Count());
  ASSERT_EQ(0, (int)mqtt.GetIncomingInFlightCount());
  ASSERT_EQ(0, (int)mqtt.GetOldestQueuedMessageAgeMs());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPayloadBytes());
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogPropertyBytes());
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogCount());
  ASSERT_EQ(0, (int)mqtt.GetDeferredTransportBacklogBytes());
  ASSERT_EQ(0, mqtt.GetLastFailureCode());
  ASSERT_STR_EQ("", mqtt.GetLastFailureDescription());
  ASSERT_EQ((int)MQTT_FAILURE_NONE, (int)mqtt.GetLastFailureClass());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_MaxRetransmitConfig                                         |
//| SetMaxRetransmitCount should update the value.                   |
//+------------------------------------------------------------------+
bool TEST_MaxRetransmitConfig() {
  TEST_CASE_START();

  CMqttClient mqtt;

  ASSERT_EQ(10, (int)mqtt.GetMaxRetransmitCount());  // Default
  mqtt.SetMaxRetransmitCount(25);
  ASSERT_EQ(25, (int)mqtt.GetMaxRetransmitCount());
  mqtt.SetMaxRetransmitCount(0);
  ASSERT_EQ(0, (int)mqtt.GetMaxRetransmitCount());  // 0 = unlimited

  return true;
}

//+------------------------------------------------------------------+
//| TEST_UnlimitedQoS1RequiresMessageExpiry                          |
//| QoS1 publishes must carry expiry when MaxRetransmitCount=0.      |
//+------------------------------------------------------------------+
bool TEST_UnlimitedQoS1RequiresMessageExpiry() {
  TEST_CASE_START();

  CMqttClient            mqtt;
  MqttPublishProperties  props;

  ResetCallbackState();
  InitPublishProperties(props);

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);
  mqtt.SetMaxRetransmitCount(0);
  mqtt.SetOnError(TestOnError);

  ASSERT_EQ((int)MQTT_PUB_EXPIRY_REQUIRED,
            (int)mqtt.Publish("signal/eurusd", "buy", QoS_1, false));
  ASSERT_EQ(0, (int)mqtt.GetQueuedMessageCount());
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_TRUE(StringFind(g_cb_error_desc, "Message Expiry Interval") >= 0);

  InitPublishProperties(props);
  props.has_message_expiry      = true;
  props.message_expiry_interval = 30;
  ASSERT_EQ((int)MQTT_PUB_QUEUED,
            (int)mqtt.Publish("signal/eurusd", "buy", QoS_1, false, props));
  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_StateEnum_Values                                            |
//| Verify ENUM_MQTT_CLIENT_STATE has the expected integer values.   |
//+------------------------------------------------------------------+
bool TEST_StateEnum_Values() {
  TEST_CASE_START();

  ASSERT_EQ(0, (int)MQTT_CLIENT_DISCONNECTED);
  ASSERT_EQ(1, (int)MQTT_CLIENT_CONNECTING);
  ASSERT_EQ(2, (int)MQTT_CLIENT_WAITING_CONNACK);
  ASSERT_EQ(3, (int)MQTT_CLIENT_CONNECTED);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishErrorEnum_Values                                     |
//| Verify ENUM_MQTT_PUBLISH_ERROR values.                           |
//+------------------------------------------------------------------+
bool TEST_PublishErrorEnum_Values() {
  TEST_CASE_START();

  ASSERT_EQ(0, (int)MQTT_PUB_OK);
  ASSERT_EQ(-1, (int)MQTT_PUB_NOT_CONNECTED);
  ASSERT_EQ(-2, (int)MQTT_PUB_NO_PACKET_ID);
  ASSERT_EQ(-3, (int)MQTT_PUB_QUEUE_FULL);
  ASSERT_EQ(-4, (int)MQTT_PUB_SEND_FAILED);
  ASSERT_EQ(-5, (int)MQTT_PUB_PACKET_TOO_BIG);
  ASSERT_EQ(-6, (int)MQTT_PUB_INVALID_TOPIC);
  ASSERT_EQ(-7, (int)MQTT_PUB_FLOW_CONTROL_FULL);
  ASSERT_EQ(-8, (int)MQTT_PUB_RECONNECTING);
  ASSERT_EQ(-9, (int)MQTT_PUB_QUEUED);
  ASSERT_EQ(-10, (int)MQTT_PUB_EXPIRY_REQUIRED);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishStringOverloads                                      |
//| Both Publish(string) overloads should queue when disconnected.   |
//+------------------------------------------------------------------+
bool TEST_PublishStringOverloads() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);

  //--- Publish(topic, payload) — uses default QoS
  ENUM_MQTT_PUBLISH_ERROR err1 = mqtt.Publish("t/1", "msg1");
  ASSERT_EQ((int)MQTT_PUB_NOT_CONNECTED, (int)err1);

  //--- Publish(topic, payload, qos, retain) — explicit QoS
  ENUM_MQTT_PUBLISH_ERROR err2 = mqtt.Publish("t/2", "msg2", QoS_2, true);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)err2);

  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_PublishBinaryOverloads                                      |
//| Binary Publish overloads should queue when disconnected.         |
//+------------------------------------------------------------------+
bool TEST_PublishBinaryOverloads() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);

  uchar                   data[] = {0xAA, 0xBB, 0xCC};

  //--- Publish(topic, payload[], len) — default QoS
  ENUM_MQTT_PUBLISH_ERROR err1   = mqtt.Publish("t/bin1", data);
  ASSERT_EQ((int)MQTT_PUB_NOT_CONNECTED, (int)err1);

  //--- Publish(topic, payload[], len, qos, retain) — explicit
  ENUM_MQTT_PUBLISH_ERROR err2 = mqtt.Publish("t/bin2", data, ArraySize(data), QoS_1, false);
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)err2);

  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_MultipleDisconnects                                         |
//| Calling Disconnect() multiple times should be safe.              |
//+------------------------------------------------------------------+
bool TEST_MultipleDisconnects() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);

  mqtt.Disconnect();
  mqtt.Disconnect();
  mqtt.Disconnect(0x04);

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_SubscribeDefaultQoS                                         |
//| Subscribe with 0xFF should use default QoS.                      |
//+------------------------------------------------------------------+
bool TEST_SubscribeDefaultQoS() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetDefaultQoS(QoS_1);

  //--- 0xFF sentinel should resolve to m_default_qos (QoS_1)
  mqtt.Subscribe("auto/qos", 0xFF);

  //--- Explicit QoS should override
  mqtt.Subscribe("explicit/qos", QoS_2);

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_UnsubscribeNonexistent                                      |
//| Unsubscribing a topic that was never registered should be safe.  |
//+------------------------------------------------------------------+
bool TEST_UnsubscribeNonexistent() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);

  //--- Should not crash or fail
  mqtt.Unsubscribe("never/registered");
  mqtt.Unsubscribe("");

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_QueuedMessageCount_AfterDisconnect                          |
//| Queued messages should persist across Disconnect() calls.        |
//+------------------------------------------------------------------+
bool TEST_QueuedMessageCount_AfterDisconnect() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetMaxQueuedMessages(10);
  mqtt.SetAutoReconnect(false);
  mqtt.SetQueueQoS0WhenDisconnected(true);

  //--- Queue some messages
  mqtt.Publish("t/1", "a");
  mqtt.Publish("t/2", "b");
  ASSERT_EQ(2, (int)mqtt.GetQueuedMessageCount());

  //--- Disconnect should NOT clear the queue (messages should be delivered on reconnect)
  mqtt.Disconnect();
  ASSERT_EQ(2, (int)mqtt.GetQueuedMessageCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_SubscribeRejectsInvalidFilter                               |
//| Malformed topic filters must be rejected locally.                |
//+------------------------------------------------------------------+
bool TEST_SubscribeRejectsInvalidFilter() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);

  mqtt.Subscribe("bad/#/filter", TestOnMessage, QoS_1);
  ASSERT_EQ(0, (int)mqtt.TestGetSubscriptionCount());

  MqttSubscribeOptions opts;
  opts.qos = QoS_1;
  mqtt.Subscribe("bad+/filter", opts);
  ASSERT_EQ(0, (int)mqtt.TestGetSubscriptionCount());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_UnsubscribeDeferredUntilAck                                 |
//| Local subscription removal must wait for successful UNSUBACK.    |
//+------------------------------------------------------------------+
bool TEST_UnsubscribeDeferredUntilAck() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.Subscribe("topic/live", TestOnMessage, QoS_1);
  ASSERT_EQ(1, (int)mqtt.TestGetSubscriptionCount());
  ASSERT_EQ(1, (int)tx.m_sent_count);

  mqtt.Unsubscribe("topic/live");
  ASSERT_EQ(1, (int)mqtt.TestGetSubscriptionCount());
  ASSERT_EQ(2, (int)tx.m_sent_count);

  uchar unsuback[] = {0xB0, 0x04, 0x00, 0x02, 0x00, 0x00};
  tx.EnqueueIncoming(unsuback);
  mqtt.Poll();

  ASSERT_EQ(0, (int)mqtt.TestGetSubscriptionCount());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_SubackKeepsMovedSubscriptionOptions              |
//| Removing a refused replay entry must keep moved slot metadata.   |
//+------------------------------------------------------------------+
bool TEST_Regression_SubackKeepsMovedSubscriptionOptions() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetSharedSubscriptionIds(true);

  MqttSubscribeOptions opts_a;
  opts_a.qos             = QoS_1;
  opts_a.no_local        = false;
  opts_a.rap             = false;
  opts_a.retain_handling = 0;
  mqtt.Subscribe("alpha/test", opts_a);

  MqttSubscribeOptions opts_b;
  opts_b.qos             = QoS_2;
  opts_b.no_local        = true;
  opts_b.rap             = true;
  opts_b.retain_handling = 2;
  mqtt.Subscribe("beta/replay/longer", opts_b);

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestReplaySubscriptions();

  ASSERT_EQ(1, (int)tx.m_sent_count);

  uchar suback[] = {0x90, 0x05, 0x00, 0x01, 0x00, 0x87, 0x02};
  mqtt.TestOnSubackReceived(suback);

  ASSERT_EQ(1, (int)mqtt.TestGetSubscriptionCount());
  ASSERT_STR_EQ("beta/replay/longer", mqtt.TestGetSubscriptionTopic(0));
  ASSERT_TRUE(mqtt.TestGetSubscriptionNoLocal(0));
  ASSERT_TRUE(mqtt.TestGetSubscriptionRap(0));
  ASSERT_EQ(2, (int)mqtt.TestGetSubscriptionRh(0));
  ASSERT_EQ(18, (int)mqtt.TestGetSubscriptionUtf8Len(0));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ControlPacketDiagnosticsCache                               |
//| SUBACK, UNSUBACK, and DISCONNECT diagnostics should be cached.   |
//+------------------------------------------------------------------+
bool TEST_ControlPacketDiagnosticsCache() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  uchar suback[] = {0x90, 0x12, 0x00, 0x01, 0x0E, 0x1F, 0x00, 0x04, 'n', 'o',
                    'p',  'e',  0x26, 0x00, 0x01, 'a',  0x00, 0x01, 'b', 0x01};
  mqtt.TestOnSubackReceived(suback);
  ASSERT_EQ(1, (int)mqtt.GetLastSubackPacketId());
  ASSERT_STR_EQ("nope", mqtt.GetLastSubackReasonString());
  ASSERT_EQ(1, (int)mqtt.GetLastSubackUserPropertyCount());
  ASSERT_STR_EQ("a", mqtt.GetLastSubackUserPropertyKey(0));
  ASSERT_STR_EQ("b", mqtt.GetLastSubackUserPropertyValue(0));

  uchar unsuback[] = {0xB0, 0x12, 0x00, 0x02, 0x0E, 0x1F, 0x00, 0x04, 'l', 'a',
                      't',  'e',  0x26, 0x00, 0x01, 'x',  0x00, 0x01, 'y', 0x00};
  tx.EnqueueIncoming(unsuback);
  mqtt.Poll();
  ASSERT_EQ(2, (int)mqtt.GetLastUnsubackPacketId());
  ASSERT_STR_EQ("late", mqtt.GetLastUnsubackReasonString());
  ASSERT_EQ(1, (int)mqtt.GetLastUnsubackUserPropertyCount());
  ASSERT_STR_EQ("x", mqtt.GetLastUnsubackUserPropertyKey(0));
  ASSERT_STR_EQ("y", mqtt.GetLastUnsubackUserPropertyValue(0));

  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  uchar disconnect[] = {0xE0, 0x13, 0x9D, 0x11, 0x1F, 0x00, 0x03, 'b',  'y',  'e', 0x1C,
                        0x00, 0x01, 'h',  0x26, 0x00, 0x01, 'k',  0x00, 0x01, 'v'};
  tx.EnqueueIncoming(disconnect);
  mqtt.Poll();
  ASSERT_EQ(0x9D, (int)mqtt.GetLastDisconnectReasonCode());
  ASSERT_STR_EQ("bye", mqtt.GetLastDisconnectReasonString());
  ASSERT_STR_EQ("h", mqtt.GetLastDisconnectServerReference());
  ASSERT_EQ(1, (int)mqtt.GetLastDisconnectUserPropertyCount());
  ASSERT_STR_EQ("k", mqtt.GetLastDisconnectUserPropertyKey(0));
  ASSERT_STR_EQ("v", mqtt.GetLastDisconnectUserPropertyValue(0));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_SubscriptionIndexRehashAndMovedLookup            |
//| Compact topic index must survive rehash and swap-with-last       |
//| updates without duplicating or losing moved subscriptions.       |
//+------------------------------------------------------------------+
bool TEST_Regression_SubscriptionIndexRehashAndMovedLookup() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);

  for (int i = 0; i < 40; i++) {
    mqtt.Subscribe(StringFormat("bulk/%d", i), QoS_1);
  }
  ASSERT_EQ(40, (int)mqtt.TestGetSubscriptionCount());

  mqtt.Unsubscribe("bulk/5");
  ASSERT_EQ(39, (int)mqtt.TestGetSubscriptionCount());

  bool removed_found = false;
  uint moved_idx     = 0;
  bool moved_found   = false;
  for (uint i = 0; i < mqtt.TestGetSubscriptionCount(); i++) {
    string topic = mqtt.TestGetSubscriptionTopic(i);
    if (topic == "bulk/5") {
      removed_found = true;
    }
    if (topic == "bulk/39") {
      moved_idx   = i;
      moved_found = true;
    }
  }
  ASSERT_FALSE(removed_found);
  ASSERT_TRUE(moved_found);

  MqttSubscribeOptions opts;
  opts.qos             = QoS_2;
  opts.no_local        = true;
  opts.rap             = true;
  opts.retain_handling = 2;
  mqtt.Subscribe("bulk/39", opts);

  ASSERT_EQ(39, (int)mqtt.TestGetSubscriptionCount());
  ASSERT_TRUE(mqtt.TestGetSubscriptionNoLocal(moved_idx));
  ASSERT_TRUE(mqtt.TestGetSubscriptionRap(moved_idx));
  ASSERT_EQ(2, (int)mqtt.TestGetSubscriptionRh(moved_idx));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_QoS2DedupeAndCallbackBoundary                    |
//| QoS2 callback should fire once at PUBREL/PUBCOMP boundary only.  |
//+------------------------------------------------------------------+
bool TEST_Regression_QoS2DedupeAndCallbackBoundary() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnMessage(TestOnMessage);

  uchar payload[];
  StringToCharArray("sig", payload, 0, WHOLE_ARRAY, CP_UTF8);
  if (ArraySize(payload) > 0 && payload[ArraySize(payload) - 1] == 0) {
    ArrayResize(payload, ArraySize(payload) - 1);
  }

  CPublish p;
  p.SetTopicName("signals/eurusd");
  p.SetPacketId(42);
  p.SetQoS_2(true);
  p.SetPayload(payload);
  uchar qos2_pub[];
  p.Build(qos2_pub);

  //--- First QoS2 publish: should store state and send PUBREC, but not callback yet.
  mqtt.TestOnPublishReceived(qos2_pub);
  ASSERT_EQ(0, g_cb_message_count);
  ASSERT_EQ(1, (int)mqtt.TestContext().flow_control.GetIncomingInFlightCount());

  //--- Duplicate QoS2 publish before PUBREL: should re-PUBREC and still no callback.
  mqtt.TestOnPublishReceived(qos2_pub);
  ASSERT_EQ(0, g_cb_message_count);
  ASSERT_EQ(1, (int)mqtt.TestContext().flow_control.GetIncomingInFlightCount());

  //--- Complete handshake via PUBREL delivered through Poll().
  CPubrel rel;
  rel.SetPacketId(42);
  uchar pubrel_pkt[];
  rel.Build(pubrel_pkt);
  tx.EnqueueIncoming(pubrel_pkt);
  mqtt.Poll();

  ASSERT_EQ(1, g_cb_message_count);
  ASSERT_EQ(42, (int)g_cb_last_packet_id);
  ASSERT_STR_EQ("signals/eurusd", g_cb_last_topic);
  ASSERT_EQ(0, (int)mqtt.TestContext().flow_control.GetIncomingInFlightCount());

  SessionMessage msg;
  ASSERT_FALSE(mqtt.TestContext().session_db.GetMessage(42, msg));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_QoS2DirectionScopedPacketIds                     |
//| Incoming QoS2 must coexist with outgoing QoS1 on the same        |
//| packet id because broker and client packet-id spaces are         |
//| independent.                                                     |
//+------------------------------------------------------------------+
bool TEST_Regression_QoS2DirectionScopedPacketIds() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnMessage(TestOnMessage);

  ASSERT_EQ((int)MQTT_PUB_OK, (int)mqtt.Publish("signals/outgoing", "local", QoS_1, false));

  SessionMessage outgoing;
  ASSERT_TRUE(mqtt.TestContext().session_db.GetMessage(1, outgoing, true));
  ASSERT_TRUE(outgoing.is_outgoing);
  ASSERT_STR_EQ("signals/outgoing", outgoing.topic);

  uchar payload[];
  StringToCharArray("remote", payload, 0, WHOLE_ARRAY, CP_UTF8);
  if (ArraySize(payload) > 0 && payload[ArraySize(payload) - 1] == 0) {
    ArrayResize(payload, ArraySize(payload) - 1);
  }

  CPublish incoming_pub;
  incoming_pub.SetTopicName("signals/incoming");
  incoming_pub.SetPacketId(1);
  incoming_pub.SetQoS_2(true);
  incoming_pub.SetPayload(payload);
  uchar qos2_pub[];
  incoming_pub.Build(qos2_pub);

  mqtt.TestOnPublishReceived(qos2_pub);

  ASSERT_EQ(0, g_cb_message_count);
  ASSERT_EQ(1, (int)mqtt.TestContext().flow_control.GetIncomingInFlightCount());
  ASSERT_EQ(2, (int)tx.m_sent_count);
  ASSERT_EQ(0x50, (int)(tx.m_sent[1].data[0] & 0xF0));

  SessionMessage incoming;
  ASSERT_TRUE(mqtt.TestContext().session_db.GetMessage(1, incoming, false));
  ASSERT_FALSE(incoming.is_outgoing);
  ASSERT_STR_EQ("signals/incoming", incoming.topic);
  ASSERT_TRUE(mqtt.TestContext().session_db.GetMessage(1, outgoing, true));
  ASSERT_STR_EQ("signals/outgoing", outgoing.topic);

  CPubrel rel;
  rel.SetPacketId(1);
  uchar pubrel_pkt[];
  rel.Build(pubrel_pkt);
  tx.EnqueueIncoming(pubrel_pkt);
  mqtt.Poll();

  ASSERT_EQ(1, g_cb_message_count);
  ASSERT_EQ(1, (int)g_cb_last_packet_id);
  ASSERT_STR_EQ("signals/incoming", g_cb_last_topic);
  ASSERT_EQ(0, (int)mqtt.TestContext().flow_control.GetIncomingInFlightCount());
  ASSERT_FALSE(mqtt.TestContext().session_db.GetMessage(1, incoming, false));
  ASSERT_TRUE(mqtt.TestContext().session_db.GetMessage(1, outgoing, true));
  ASSERT_STR_EQ("signals/outgoing", outgoing.topic);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_QoS2DedupeAcrossClientRestart                   |
//| Persisted incoming QoS2 state must suppress duplicate delivery  |
//| after a facade restart and session resume.                      |
//+------------------------------------------------------------------+
bool TEST_Regression_QoS2DedupeAcrossClientRestart() {
  TEST_CASE_START();

  const string client_id  = "test_restart_qos2_dedupe";
  const string session_id = "mqtt_" + client_id;
  ResetPersistentSessionStore(session_id);

  uchar payload[];
  StringToCharArray("sig", payload, 0, WHOLE_ARRAY, CP_UTF8);
  if (ArraySize(payload) > 0 && payload[ArraySize(payload) - 1] == 0) {
    ArrayResize(payload, ArraySize(payload) - 1);
  }

  CPublish pub;
  pub.SetTopicName("signals/eurusd");
  pub.SetPacketId(42);
  pub.SetQoS_2(true);
  pub.SetPayload(payload);
  uchar qos2_pub[];
  pub.Build(qos2_pub);

  {
    CTestTransport phase1_tx;
    CMqttClient    phase1;

    phase1.SetSessionExpiry(60);
    ASSERT_TRUE(phase1.TestContext().session_db.Init(session_id, true));
    phase1.TestInjectTransport(GetPointer(phase1_tx));
    phase1.TestSetState(MQTT_CLIENT_CONNECTED);
    phase1.TestOnPublishReceived(qos2_pub);

    ASSERT_EQ(0, g_cb_message_count);
    ASSERT_EQ(1, (int)phase1.TestContext().flow_control.GetIncomingInFlightCount());
  }

  CTestTransport phase2_tx;
  CMqttClient    phase2;

  phase2.SetHost("broker.test", 1883);
  phase2.SetClientId(client_id);
  phase2.SetCleanStart(false);
  phase2.SetSessionExpiry(60);
  phase2.SetOnMessage(TestOnMessage);
  phase2.TestInjectTransport(GetPointer(phase2_tx));

  ASSERT_EQ((int)TRANSPORT_CONNECTING, (int)phase2.Connect());

  uchar connack_pkt[] = {0x20, 0x03, 0x01, 0x00, 0x00};
  phase2_tx.EnqueueIncoming(connack_pkt);
  phase2.Poll();

  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)phase2.GetState());
  ASSERT_EQ(1, (int)phase2.TestContext().flow_control.GetIncomingInFlightCount());

  phase2_tx.ClearSentPackets();
  phase2.TestOnPublishReceived(qos2_pub);

  ASSERT_EQ(0, g_cb_message_count);
  ASSERT_EQ(1, (int)phase2.TestContext().flow_control.GetIncomingInFlightCount());
  ASSERT_EQ(1, (int)phase2_tx.m_sent_count);
  ASSERT_EQ(0x50, (int)(phase2_tx.m_sent[0].data[0] & 0xF0));

  CPubrel rel;
  rel.SetPacketId(42);
  uchar pubrel_pkt[];
  rel.Build(pubrel_pkt);
  phase2_tx.EnqueueIncoming(pubrel_pkt);
  phase2.Poll();

  ASSERT_EQ(1, g_cb_message_count);
  ASSERT_EQ(42, (int)g_cb_last_packet_id);
  ASSERT_STR_EQ("signals/eurusd", g_cb_last_topic);
  ASSERT_EQ(0, (int)phase2.TestContext().flow_control.GetIncomingInFlightCount());

  SessionMessage msg;
  ASSERT_FALSE(phase2.TestContext().session_db.GetMessage(42, msg));

  phase2.TestContext().session_db.Clear();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_PollBudgetDefersExtractedPackets                 |
//| Packets over the per-poll budget must be processed next Poll.    |
//+------------------------------------------------------------------+
bool TEST_Regression_PollBudgetDefersExtractedPackets() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnMessage(TestOnMessage);
  mqtt.SetMaxPacketsPerPoll(1);

  uchar    p1_payload[] = {0x31};
  uchar    p2_payload[] = {0x32};
  uchar    p3_payload[] = {0x33};

  CPublish p1;
  p1.SetTopicName("burst/one");
  p1.SetPayload(p1_payload);
  uchar pkt1[];
  p1.Build(pkt1);

  CPublish p2;
  p2.SetTopicName("burst/two");
  p2.SetPayload(p2_payload);
  uchar pkt2[];
  p2.Build(pkt2);

  CPublish p3;
  p3.SetTopicName("burst/three");
  p3.SetPayload(p3_payload);
  uchar pkt3[];
  p3.Build(pkt3);

  tx.EnqueueIncoming(pkt1);
  tx.EnqueueIncoming(pkt2);
  tx.EnqueueIncoming(pkt3);

  mqtt.Poll();
  ASSERT_EQ(1, g_cb_message_count);
  ASSERT_STR_EQ("burst/one", g_cb_last_topic);

  mqtt.Poll();
  ASSERT_EQ(2, g_cb_message_count);
  ASSERT_STR_EQ("burst/two", g_cb_last_topic);

  mqtt.Poll();
  ASSERT_EQ(3, g_cb_message_count);
  ASSERT_STR_EQ("burst/three", g_cb_last_topic);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_ReplaySpreadsAcrossPollCycles                    |
//| Distinct replay batches should all flush in the same replay call |
//| rather than requiring one extra Poll() per SUBSCRIBE packet.     |
//+------------------------------------------------------------------+
bool TEST_Regression_ReplaySpreadsAcrossPollCycles() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.Subscribe("replay/one", QoS_1);
  mqtt.Subscribe("replay/two", QoS_1);
  mqtt.Subscribe("replay/three", QoS_1);

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  mqtt.TestReplaySubscriptions();
  ASSERT_EQ(3, (int)tx.m_sent_count);
  ASSERT_FALSE(mqtt.TestIsReplayInProgress());
  ASSERT_EQ(3, (int)mqtt.TestGetReplayCursor());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_ReceiveMaximumEnforced                           |
//| Incoming QoS2 over client receive window should trigger error.   |
//+------------------------------------------------------------------+
bool TEST_Regression_ReceiveMaximumEnforced() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.TestContext().flow_control.SetClientReceiveMaximum(1);

  uchar    pl1[] = {0x31};
  CPublish p1;
  p1.SetTopicName("qos2/one");
  p1.SetPacketId(10);
  p1.SetQoS_2(true);
  p1.SetPayload(pl1);
  uchar pkt1[];
  p1.Build(pkt1);

  uchar    pl2[] = {0x32};
  CPublish p2;
  p2.SetTopicName("qos2/two");
  p2.SetPacketId(11);
  p2.SetQoS_2(true);
  p2.SetPayload(pl2);
  uchar pkt2[];
  p2.Build(pkt2);

  mqtt.TestOnPublishReceived(pkt1);
  ASSERT_EQ(1, (int)mqtt.TestContext().flow_control.GetIncomingInFlightCount());
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());

  mqtt.TestOnPublishReceived(pkt2);

  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED, g_cb_error_code);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_QueueTailPreservedOnDrainFailure                 |
//| Drain should preserve the unsent tail even after the current     |
//| head has already been durably handed off into in-flight state.   |
//+------------------------------------------------------------------+
bool TEST_Regression_QueueTailPreservedOnDrainFailure() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetAutoReconnect(false);
  mqtt.SetMaxQueuedMessages(10);

  //--- Build disconnected queue
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("q/1", "one", QoS_1));
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("q/2", "two", QoS_1));
  ASSERT_EQ((int)MQTT_PUB_QUEUED, (int)mqtt.Publish("q/3", "three", QoS_1));
  ASSERT_EQ(3, (int)mqtt.GetQueuedMessageCount());

  //--- Inject transport and fail from second send onward.
  tx.m_connected         = true;
  tx.m_fail_on_send_call = 2;
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  mqtt.TestDrainPublishQueue();

  ASSERT_EQ(1, (int)mqtt.GetQueuedMessageCount());
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS1Count());
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_AuthPacketHandling                               |
//| AUTH packet should be parsed and cached on the facade.           |
//+------------------------------------------------------------------+
bool TEST_Regression_AuthPacketHandling() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestSetActiveAuthMethod("SCRAM-SHA-256");
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnAuthEx(TestOnAuthEx);

  CAuth auth;
  auth.SetReasonCode(MQTT_REASON_CODE_CONTINUE_AUTHENTICATION);
  auth.SetAuthMethod("SCRAM-SHA-256");
  auth.SetReasonString("continue");
  uchar auth_data[] = {0x01, 0x02, 0x03};
  auth.SetAuthData(auth_data);
  auth.SetUserProperty("step", "challenge");
  uchar auth_pkt[];
  auth.Build(auth_pkt);

  tx.EnqueueIncoming(auth_pkt);
  mqtt.Poll();

  uchar cached_auth_data[];
  mqtt.GetLastAuthData(cached_auth_data);

  ASSERT_EQ(0, g_cb_error_count);
  ASSERT_EQ(1, g_cb_auth_ex_count);
  ASSERT_EQ((int)MQTT_REASON_CODE_CONTINUE_AUTHENTICATION, g_cb_auth_ex_reason_code);
  ASSERT_STR_EQ("continue", g_cb_auth_ex_reason_string);
  ASSERT_STR_EQ("SCRAM-SHA-256", g_cb_auth_ex_method);
  ASSERT_EQ(3, g_cb_auth_ex_data_len);
  ASSERT_EQ(1, g_cb_auth_ex_user_prop_count);
  ASSERT_STR_EQ("step", g_cb_auth_ex_user_key);
  ASSERT_STR_EQ("challenge", g_cb_auth_ex_user_val);
  ASSERT_EQ((int)MQTT_REASON_CODE_CONTINUE_AUTHENTICATION, (int)mqtt.GetLastAuthReasonCode());
  ASSERT_STR_EQ("SCRAM-SHA-256", mqtt.GetLastAuthMethod());
  ASSERT_STR_EQ("continue", mqtt.GetLastAuthReasonString());
  ASSERT_EQ(3, (int)ArraySize(cached_auth_data));
  ASSERT_EQ(0x01, (int)cached_auth_data[0]);
  ASSERT_EQ(0x02, (int)cached_auth_data[1]);
  ASSERT_EQ(0x03, (int)cached_auth_data[2]);
  ASSERT_EQ(1, (int)mqtt.GetLastAuthUserPropertyCount());
  ASSERT_STR_EQ("step", mqtt.GetLastAuthUserPropertyKey(0));
  ASSERT_STR_EQ("challenge", mqtt.GetLastAuthUserPropertyValue(0));
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_AuthMalformedDisconnect                          |
//| Malformed AUTH should trigger transport error + disconnect path. |
//+------------------------------------------------------------------+
bool TEST_Regression_AuthMalformedDisconnect() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);

  //--- Malformed AUTH: fixed header AUTH (0xF0), remaining length=1,
  //--- reason code 0x7F is invalid for AUTH (valid: 0x00, 0x18, 0x19).
  uchar bad_auth[] = {0xF0, 0x01, 0x7F};
  tx.EnqueueIncoming(bad_auth);

  mqtt.Poll();

  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_MalformedSimpleAckDisconnects                    |
//| Malformed fast-path ACK packets must trigger disconnect logic.   |
//+------------------------------------------------------------------+
bool TEST_Regression_MalformedSimpleAckDisconnects() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);

  uchar bad_puback[] = {0x40, 0x03, 0x00, 0x2A};
  tx.EnqueueIncoming(bad_puback);

  mqtt.Poll();

  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_OrphanOutgoingAcksIgnored                        |
//| Stale PUBACK/PUBREC/PUBCOMP must not trigger callbacks or I/O.   |
//+------------------------------------------------------------------+
bool TEST_Regression_OrphanOutgoingAcksIgnored() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnAck(TestOnAckEx);

  uchar puback[] = {0x40, 0x02, 0x00, 0x2A};
  tx.EnqueueIncoming(puback);
  mqtt.Poll();
  ASSERT_EQ(1, g_cb_ack_ex_count);
  ASSERT_EQ((int)PUBACK, g_cb_ack_ex_packet_type);
  ASSERT_EQ(0x2A, (int)g_cb_ack_ex_packet_id);
  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());

  uchar pubrec[] = {0x50, 0x02, 0x00, 0x2A};
  tx.EnqueueIncoming(pubrec);
  mqtt.Poll();
  ASSERT_EQ(2, g_cb_ack_ex_count);
  ASSERT_EQ((int)PUBREC, g_cb_ack_ex_packet_type);
  ASSERT_EQ(0x2A, (int)g_cb_ack_ex_packet_id);
  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());

  uchar pubcomp[] = {0x70, 0x02, 0x00, 0x2A};
  tx.EnqueueIncoming(pubcomp);
  mqtt.Poll();
  ASSERT_EQ(3, g_cb_ack_ex_count);
  ASSERT_EQ((int)PUBCOMP, g_cb_ack_ex_packet_type);
  ASSERT_EQ(0x2A, (int)g_cb_ack_ex_packet_id);
  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_PUBCOMP92CompletesQoS2Resume                    |
//| PUBCOMP 0x92 must complete QoS2 resume without error callback.  |
//+------------------------------------------------------------------+
bool TEST_Regression_PUBCOMP92CompletesQoS2Resume() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnError(TestOnError);

  ASSERT_EQ((int)MQTT_PUB_OK, (int)mqtt.Publish("resume/qos2", "sig", QoS_2, false));
  ASSERT_EQ(1, (int)mqtt.GetInFlightQoS2Count());
  ASSERT_EQ(1, (int)tx.m_sent_count);

  uchar pubrec[] = {0x50, 0x02, 0x00, 0x01};
  tx.EnqueueIncoming(pubrec);
  mqtt.Poll();
  ASSERT_EQ(2, (int)tx.m_sent_count);
  ASSERT_EQ(1, (int)mqtt.GetInFlightQoS2Count());
  ASSERT_EQ(0, g_cb_error_count);

  uchar pubcomp[] = {0x70, 0x03, 0x00, 0x01, 0x92};
  tx.EnqueueIncoming(pubcomp);
  mqtt.Poll();

  SessionMessage msg;
  ASSERT_EQ(0, g_cb_error_count);
  ASSERT_EQ(0, (int)mqtt.GetInFlightQoS2Count());
  ASSERT_FALSE(mqtt.TestContext().session_db.GetMessage(1, msg));
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_PublishParseFailureAbortsBatchAfterReentry      |
//| A fatal PUBLISH parse error must stop the current batch even if |
//| the disconnect callback mutates state immediately.              |
//+------------------------------------------------------------------+
bool TEST_Regression_PublishParseFailureAbortsBatchAfterReentry() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;
  CPublish       pub;

  ResetCallbackState();
  g_reentrant_client = &mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.Subscribe("reentry/#", TestOnMessage, QoS_1);
  mqtt.SetOnDisconnect(ReentrantOnDisconnectSetConnecting);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  uchar malformed_publish[] = {0x30, 0x0C, 0x00, 0x01, 't', 0x08, 0x26, 0x00, 0x01, 'k', 0x00, 0x02, 0xC0, 0xAF};
  uchar valid_publish[];
  pub.SetTopicName("reentry/test");
  pub.SetPayload("ok");
  pub.Build(valid_publish);

  tx.EnqueueIncoming(malformed_publish);
  tx.EnqueueIncoming(valid_publish);

  mqtt.Poll();

  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_MALFORMED_PACKET, g_cb_error_code);
  ASSERT_EQ(0, g_cb_message_count);
  ASSERT_EQ(0, (int)mqtt.GetCallbackBacklogCount());
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTING, (int)mqtt.GetState());
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(0xE0, (int)tx.m_sent[0].data[0]);
  ASSERT_EQ((int)MQTT_REASON_CODE_MALFORMED_PACKET, (int)tx.m_sent[0].data[2]);

  g_reentrant_client = NULL;
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_InboundPublishQoS3DisconnectsMalformed         |
//| Forbidden QoS 3 bits on inbound PUBLISH must close with 0x81.  |
//+------------------------------------------------------------------+
bool TEST_Regression_InboundPublishQoS3DisconnectsMalformed() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  uchar invalid_qos3_publish[] = {0x36, 0x04, 0x00, 0x01, 't', 0x00};
  tx.EnqueueIncoming(invalid_qos3_publish);

  mqtt.Poll();

  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_MALFORMED_PACKET, g_cb_disconnect_code);
  ASSERT_EQ((int)MQTT_REASON_CODE_MALFORMED_PACKET, g_cb_error_code);
  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ(0xE0, (int)tx.m_sent[0].data[0]);
  ASSERT_EQ((int)MQTT_REASON_CODE_MALFORMED_PACKET, (int)tx.m_sent[0].data[2]);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Regression_ReplaySubackCountMismatchDisconnects             |
//| Replay SUBACK cardinality mismatches must fail closed.           |
//+------------------------------------------------------------------+
bool TEST_Regression_ReplaySubackCountMismatchDisconnects() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnDisconnect(TestOnDisconnect);
  mqtt.SetOnError(TestOnError);
  mqtt.Subscribe("replay/a", QoS_1);
  mqtt.Subscribe("replay/b", QoS_1);

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestReplaySubscriptions();

  ASSERT_EQ(2, (int)tx.m_sent_count);

  uchar bad_suback[] = {0x90, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00};
  mqtt.TestOnSubackReceived(bad_suback);

  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ(2, (int)mqtt.TestGetSubscriptionCount());
  ASSERT_EQ(3, (int)tx.m_sent_count);
  ASSERT_EQ(0xE0, (int)tx.m_sent[2].data[0]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Connack_ReceiveMaximumZero_ProtocolError                    |
//| Per §3.2.2.3.3, a CONNACK with Receive Maximum = 0 is            |
//| a Protocol Error. The client MUST respond with DISCONNECT 0x82   |
//| and transition to DISCONNECTED state.                            |
//+------------------------------------------------------------------+
bool TEST_Connack_ReceiveMaximumZero_ProtocolError() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);

  //--- Craft a raw CONNACK packet with Receive Maximum = 0
  //--- Format: FixedHdr(0x20) | RemLen(6) | AckFlags(0) | ReasonCode(0=Success)
  //---         | PropsLen(3) | PropID(0x21=ReceiveMaximum) | Value(0x0000)
  uchar connack_pkt[] = {0x20, 0x06, 0x00, 0x00, 0x03, 0x21, 0x00, 0x00};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  //--- Client should have rejected with Protocol Error and disconnected
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ(0x82, g_cb_error_code);  // Protocol Error
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());

  //--- Verify a DISCONNECT packet was sent (first byte 0xE0)
  ASSERT_TRUE(tx.m_sent_count > 0);
  ASSERT_EQ(0xE0, tx.m_sent[tx.m_sent_count - 1].data[0]);
  ASSERT_EQ(0x82, tx.m_sent[tx.m_sent_count - 1].data[2]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Connack_DuplicateProperty_ProtocolError                     |
//| Duplicate singleton CONNACK properties are protocol errors.     |
//+------------------------------------------------------------------+
bool TEST_Connack_DuplicateProperty_ProtocolError() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);

  uchar connack_pkt[] = {0x20, 0x09, 0x00, 0x00, 0x06, 0x21, 0x00, 0x01, 0x21, 0x00, 0x02};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_PROTOCOL_ERROR, g_cb_error_code);
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(tx.m_sent_count > 0);
  ASSERT_EQ(0xE0, (int)tx.m_sent[tx.m_sent_count - 1].data[0]);
  ASSERT_EQ((int)MQTT_REASON_CODE_PROTOCOL_ERROR, (int)tx.m_sent[tx.m_sent_count - 1].data[2]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Connack_TruncatedProperty_MalformedPacket                   |
//| Truncated CONNACK property data should map to 0x81.             |
//+------------------------------------------------------------------+
bool TEST_Connack_TruncatedProperty_MalformedPacket() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  ResetCallbackState();
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_WAITING_CONNACK);
  mqtt.SetOnError(TestOnError);
  mqtt.SetOnDisconnect(TestOnDisconnect);

  uchar connack_pkt[] = {0x20, 0x05, 0x00, 0x00, 0x03, 0x21, 0x00};
  tx.EnqueueIncoming(connack_pkt);

  mqtt.Poll();

  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_MALFORMED_PACKET, g_cb_error_code);
  ASSERT_TRUE(g_cb_disconnect_count > 0);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_TRUE(tx.m_sent_count > 0);
  ASSERT_EQ(0xE0, (int)tx.m_sent[tx.m_sent_count - 1].data[0]);
  ASSERT_EQ((int)MQTT_REASON_CODE_MALFORMED_PACKET, (int)tx.m_sent[tx.m_sent_count - 1].data[2]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_SubscribeRejectsWildcardWhenBrokerDisablesIt                |
//| Wildcard subscriptions must be blocked after CONNACK says no.    |
//+------------------------------------------------------------------+
bool TEST_SubscribeRejectsWildcardWhenBrokerDisablesIt() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestSetServerCapabilities(false, true, true);

  mqtt.Subscribe("signals/#", QoS_1);

  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED, g_cb_error_code);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_SubscribeRejectsSharedWhenBrokerDisablesIt                  |
//| Shared subscriptions must be blocked after CONNACK says no.      |
//+------------------------------------------------------------------+
bool TEST_SubscribeRejectsSharedWhenBrokerDisablesIt() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestSetServerCapabilities(true, true, false);

  mqtt.Subscribe("$share/group/signals", QoS_1);

  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED, g_cb_error_code);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_SubscribeOmitsSubIdWhenBrokerDisablesIt                     |
//| SUBSCRIBE must omit the sub-id property when broker says no.     |
//+------------------------------------------------------------------+
bool TEST_SubscribeOmitsSubIdWhenBrokerDisablesIt() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.TestSetServerCapabilities(true, false, true);

  mqtt.Subscribe("prices/eurusd", QoS_1);

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_TRUE(ArraySize(tx.m_sent[0].data) > 5);
  ASSERT_EQ(0, tx.m_sent[0].data[4]);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_SendAuthRejectsMissingConnectMethod                         |
//| AUTH must not be sent without a CONNECT auth method.             |
//+------------------------------------------------------------------+
bool TEST_SendAuthRejectsMissingConnectMethod() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);

  uchar auth_data[] = {0x01};
  mqtt.SendAuth(MQTT_REASON_CODE_CONTINUE_AUTHENTICATION, "SCRAM-SHA-256", auth_data);

  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_PROTOCOL_ERROR, g_cb_error_code);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_SendAuthRejectsMethodMismatch                               |
//| AUTH method must match the CONNECT Authentication Method.        |
//+------------------------------------------------------------------+
bool TEST_SendAuthRejectsMethodMismatch() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.SetOnError(TestOnError);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetAuthMethod("SCRAM-SHA-256");
  mqtt.TestSetActiveAuthMethod("SCRAM-SHA-256");

  uchar auth_data[] = {0x01, 0x02};
  mqtt.SendAuth(MQTT_REASON_CODE_CONTINUE_AUTHENTICATION, "OAUTHBEARER", auth_data);

  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_TRUE(g_cb_error_count > 0);
  ASSERT_EQ((int)MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD, g_cb_error_code);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_ConnectClearsDeferredTransportPackets                       |
//| Fresh Connect() calls must drop deferred packets from prior I/O. |
//+------------------------------------------------------------------+
bool TEST_ConnectClearsDeferredTransportPackets() {
  TEST_CASE_START();

  CMqttClient mqtt;
  mqtt.SetHost("broker.test", 1883);
  mqtt.SetAutoReconnect(false);

  uchar pkt[] = {0xD0, 0x00};
  mqtt.TestQueueDeferredTransportPacket(pkt);
  ASSERT_EQ(1, (int)mqtt.TestGetDeferredTransportCount());

  mqtt.Connect();

  ASSERT_EQ(0, (int)mqtt.TestGetDeferredTransportCount());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_BlockingTransportDiagnosticsSurface                         |
//| CMqttClient should expose transport blocking-phase diagnostics.  |
//+------------------------------------------------------------------+
bool TEST_BlockingTransportDiagnosticsSurface() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.SetBlockingTransportWarnThreshold(321);
  tx.m_last_blocking_duration_us = 456789;

  ASSERT_EQ(321, (int)tx.m_blocking_warn_threshold_ms);
  ASSERT_EQ(456789, (int)mqtt.GetLastTransportBlockingDuration());
  return true;
}

//+------------------------------------------------------------------+
//| TEST_BlockingTransportHardLimitDisconnects                       |
//| Over-budget blocking phases must abort connect before CONNECT.   |
//+------------------------------------------------------------------+
bool TEST_BlockingTransportHardLimitDisconnects() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.SetOnError(TestOnError);
  mqtt.SetBlockingTransportHardLimit(321);
  mqtt.TestSetState(MQTT_CLIENT_CONNECTING);
  tx.m_last_blocking_duration_us = 456789;

  mqtt.Poll();

  ASSERT_EQ(0, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_DISCONNECTED, (int)mqtt.GetState());
  ASSERT_EQ(1, g_cb_error_count);
  ASSERT_EQ((int)TRANSPORT_ERROR_TIMEOUT, g_cb_error_code);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_BlockingTransportHardLimitAllowsWithinBudget                |
//| In-budget blocking phases must still allow CONNECT to proceed.   |
//+------------------------------------------------------------------+
bool TEST_BlockingTransportHardLimitAllowsWithinBudget() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.SetHost("broker.test", 1883);
  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.SetOnError(TestOnError);
  mqtt.SetBlockingTransportHardLimit(500);
  mqtt.TestSetState(MQTT_CLIENT_CONNECTING);
  tx.m_last_blocking_duration_us = 400000;

  mqtt.Poll();

  ASSERT_EQ(1, (int)tx.m_sent_count);
  ASSERT_EQ((int)MQTT_CLIENT_WAITING_CONNACK, (int)mqtt.GetState());
  ASSERT_EQ(0, g_cb_error_count);
  return true;
}

//+------------------------------------------------------------------+
//| TEST_PollReentrancyGuard                                         |
//| Recursive Poll() calls from callbacks must no-op safely.         |
//+------------------------------------------------------------------+
bool TEST_PollReentrancyGuard() {
  TEST_CASE_START();

  CTestTransport tx;
  CMqttClient    mqtt;

  mqtt.TestInjectTransport(GetPointer(tx));
  mqtt.TestSetState(MQTT_CLIENT_CONNECTED);
  mqtt.SetOnMessage(ReentrantOnMessage);
  g_reentrant_client = &mqtt;

  uchar    payload[] = {0x31};
  CPublish pub;
  pub.SetTopicName("guard/reentry");
  pub.SetPayload(payload);
  uchar pkt[];
  pub.Build(pkt);

  tx.EnqueueIncoming(pkt);
  mqtt.Poll();

  ASSERT_EQ(1, g_reentrant_message_count);
  ASSERT_EQ(1, tx.m_poll_calls);
  ASSERT_EQ((int)MQTT_CLIENT_CONNECTED, (int)mqtt.GetState());
  return true;
}

//+------------------------------------------------------------------+
//| OnStart — test runner                                            |
//+------------------------------------------------------------------+
void OnStart() {
  const string suite_name = "TEST_MqttClient";
  TestUtilRecordSuiteStart(suite_name);

  //--- Register test functions
  string test_names[];
  int    test_count = 0;

#define REGISTER_TEST(name)                \
  ArrayResize(test_names, test_count + 1); \
  test_names[test_count] = #name;          \
  test_count++;

  REGISTER_TEST(TEST_DefaultState)
  REGISTER_TEST(TEST_IsSafeToPublishSurface)
  REGISTER_TEST(TEST_SetHost_TCP)
  REGISTER_TEST(TEST_SetHostWS)
  REGISTER_TEST(TEST_FluentConfiguration)
  REGISTER_TEST(TEST_WillConfiguration)
  REGISTER_TEST(TEST_CallbackRegistration)
  REGISTER_TEST(TEST_SubscriptionRegistry)
  REGISTER_TEST(TEST_DefaultQoS)
  REGISTER_TEST(TEST_PublishQueuing)
  REGISTER_TEST(TEST_PublishQueueBinary)
  REGISTER_TEST(TEST_PublishQueuePayloadByteBudget)
  REGISTER_TEST(TEST_PublishQueuePropertyByteBudget)
  REGISTER_TEST(TEST_PublishQueueSingleMessageByteBudget)
  REGISTER_TEST(TEST_PublishQoS0DisconnectedDefault)
  REGISTER_TEST(TEST_PublishQoS0QueueOptIn)
  REGISTER_TEST(TEST_ConnackTimeout_Config)
  REGISTER_TEST(TEST_ConnackDeadlineStartsOnConnectSend)
  REGISTER_TEST(TEST_ConnectIncludesExtendedMqtt5Properties)
  REGISTER_TEST(TEST_ConnackResponseInformationSurface)
  REGISTER_TEST(TEST_ConnackAssignedClientIdentifierSurface)
  REGISTER_TEST(TEST_ConnackMaximumQoSSurface)
  REGISTER_TEST(TEST_ConnackReceiveMaximumSurface)
  REGISTER_TEST(TEST_ConnackServerKeepAliveSurface)
  REGISTER_TEST(TEST_ConnackServerKeepAliveDoesNotLeakIntoNextConnect)
  REGISTER_TEST(TEST_ConnackBrokerCapabilitySurface)
  REGISTER_TEST(TEST_ConnackMaximumPacketSizeOmittedClearsPreviousLimit)
  REGISTER_TEST(TEST_ConnackExtendedMetadataSurface)
  REGISTER_TEST(TEST_ConnackRejectDiagnosticsSurface)
  REGISTER_TEST(TEST_AckDiagnosticsCache)
  REGISTER_TEST(TEST_PublishFacadePropertiesSurface)
  REGISTER_TEST(TEST_InboundPublishMetadataSurface)
  REGISTER_TEST(TEST_InboundPublishInvalidUtf8StrictDisconnect)
  REGISTER_TEST(TEST_InboundPublishInvalidUtf8RelaxedDelivery)
  REGISTER_TEST(TEST_InboundPublishMetadataQoS2ResumeSurface)
  REGISTER_TEST(TEST_PublishRejectsOutgoingSubscriptionIdentifier)
  REGISTER_TEST(TEST_RetransmitStripsLegacyOutgoingSubscriptionIdentifier)
  REGISTER_TEST(TEST_ConnackSessionExpiryOverrideZeroClearsStateOnDisconnect)
  REGISTER_TEST(TEST_ConnackSessionExpiryOmittedRestoresConfiguredPolicy)
  REGISTER_TEST(TEST_QueuedPublishPropertiesSurviveDrain)
  REGISTER_TEST(TEST_ExpiredQueuedPublishesArePurgedWhileOffline)
  REGISTER_TEST(TEST_QueuedPublishExpiryRoundsUpOnDrain)
  REGISTER_TEST(TEST_DurableQueuedPublishCountSurface)
  REGISTER_TEST(TEST_FlushSessionStateNowSucceedsWhenAlreadyDurable)
  REGISTER_TEST(TEST_OfflineQueuedPublishSurvivesClientRestart)
  REGISTER_TEST(TEST_IncomingStorageErrorCountSurvivesClientRestart)
  REGISTER_TEST(TEST_EncryptedSessionRoundTrip)
  REGISTER_TEST(TEST_EncryptedSessionWrongPassphraseFailsLoad)
  REGISTER_TEST(TEST_EncryptedSessionTamperFailsLoad)
  REGISTER_TEST(TEST_FlatBufferAppendSizeRejectsOverflow)
  REGISTER_TEST(TEST_RetransmitPreservesPublishProperties)
  REGISTER_TEST(TEST_DiagnosticsCallbacksSurfaceMetadata)
  REGISTER_TEST(TEST_OutgoingAckDiagnosticsSurface)
  REGISTER_TEST(TEST_ImmediateConnectFailurePreservesReconnect)
  REGISTER_TEST(TEST_ReconnectTelemetrySurface)
  REGISTER_TEST(TEST_InFlightTelemetrySurface)
  REGISTER_TEST(TEST_QueuedAgeTelemetrySurface)
  REGISTER_TEST(TEST_CallbackBacklogTelemetrySurface)
  REGISTER_TEST(TEST_DeferredTransportBacklogTelemetrySurface)
  REGISTER_TEST(TEST_DeferredTransportBacklogOverflowDisconnects)
  REGISTER_TEST(TEST_DeferredCallbackBacklogOverflowDisconnects)
  REGISTER_TEST(TEST_LastFailureTelemetrySurface)
  REGISTER_TEST(TEST_ConnackTimeoutBrokerFailureSurface)
  REGISTER_TEST(TEST_RedirectAllowlistBlocksUnapprovedHost)
  REGISTER_TEST(TEST_RedirectAllowlistAllowsApprovedHost)
  REGISTER_TEST(TEST_RedirectAllowlistCannotBeDisabled)
  REGISTER_TEST(TEST_ConnectWithoutHost)
  REGISTER_TEST(TEST_ConnectRejectsPlaintextTransportByDefault)
  REGISTER_TEST(TEST_ConnectAllowsPlaintextTransportWhenExplicitlyPermitted)
  REGISTER_TEST(TEST_ConnectRejectsPlainWebSocketByDefault)
  REGISTER_TEST(TEST_ConnectRejectsPlaintextCredentialsByDefault)
  REGISTER_TEST(TEST_ConnectAllowsPlaintextCredentialsWhenExplicitlyPermitted)
  REGISTER_TEST(TEST_ReconnectUsesDedicatedConnectTimeout)
  REGISTER_TEST(TEST_AsyncConnectWaitsForStableTransportTurn)
  REGISTER_TEST(TEST_SendConnectDefersUntilTransportWritable)
  REGISTER_TEST(TEST_TofuRequiresProvisionedThumbprint)
  REGISTER_TEST(TEST_TofuProvisionedThumbprintPinsAndReportsPinnedTrustMode)
  REGISTER_TEST(TEST_TofuReadFailureDegradesTrustMode)
  REGISTER_TEST(TEST_TofuStrictReadFailureDisconnects)
  REGISTER_TEST(TEST_DisconnectWhileDisconnected)
  REGISTER_TEST(TEST_DisconnectWithReasonCode)
  REGISTER_TEST(TEST_DisconnectWithExtendedMetadata)
  REGISTER_TEST(TEST_PollWhileDisconnected)
  REGISTER_TEST(TEST_MetricsInitialization)
  REGISTER_TEST(TEST_MaxRetransmitConfig)
  REGISTER_TEST(TEST_UnlimitedQoS1RequiresMessageExpiry)
  REGISTER_TEST(TEST_StateEnum_Values)
  REGISTER_TEST(TEST_PublishErrorEnum_Values)
  REGISTER_TEST(TEST_PublishStringOverloads)
  REGISTER_TEST(TEST_PublishBinaryOverloads)
  REGISTER_TEST(TEST_MultipleDisconnects)
  REGISTER_TEST(TEST_SubscribeDefaultQoS)
  REGISTER_TEST(TEST_SubscribeRejectsInvalidFilter)
  REGISTER_TEST(TEST_UnsubscribeNonexistent)
  REGISTER_TEST(TEST_UnsubscribeDeferredUntilAck)
  REGISTER_TEST(TEST_Regression_SubackKeepsMovedSubscriptionOptions)
  REGISTER_TEST(TEST_ControlPacketDiagnosticsCache)
  REGISTER_TEST(TEST_Regression_SubscriptionIndexRehashAndMovedLookup)
  REGISTER_TEST(TEST_QueuedMessageCount_AfterDisconnect)
  REGISTER_TEST(TEST_Regression_QoS2DedupeAndCallbackBoundary)
  REGISTER_TEST(TEST_Regression_QoS2DirectionScopedPacketIds)
  REGISTER_TEST(TEST_Regression_QoS2DedupeAcrossClientRestart)
  REGISTER_TEST(TEST_Regression_PollBudgetDefersExtractedPackets)
  REGISTER_TEST(TEST_Regression_ReplaySpreadsAcrossPollCycles)
  REGISTER_TEST(TEST_Regression_ReceiveMaximumEnforced)
  REGISTER_TEST(TEST_Regression_QueueTailPreservedOnDrainFailure)
  REGISTER_TEST(TEST_Regression_AuthPacketHandling)
  REGISTER_TEST(TEST_Regression_AuthMalformedDisconnect)
  REGISTER_TEST(TEST_Regression_MalformedSimpleAckDisconnects)
  REGISTER_TEST(TEST_Regression_PublishParseFailureAbortsBatchAfterReentry)
  REGISTER_TEST(TEST_Regression_InboundPublishQoS3DisconnectsMalformed)
  REGISTER_TEST(TEST_Regression_OrphanOutgoingAcksIgnored)
  REGISTER_TEST(TEST_Regression_PUBCOMP92CompletesQoS2Resume)
  REGISTER_TEST(TEST_Regression_ReplaySubackCountMismatchDisconnects)
  REGISTER_TEST(TEST_Connack_ReceiveMaximumZero_ProtocolError)
  REGISTER_TEST(TEST_Connack_DuplicateProperty_ProtocolError)
  REGISTER_TEST(TEST_Connack_TruncatedProperty_MalformedPacket)
  REGISTER_TEST(TEST_SubscribeRejectsWildcardWhenBrokerDisablesIt)
  REGISTER_TEST(TEST_SubscribeRejectsSharedWhenBrokerDisablesIt)
  REGISTER_TEST(TEST_SubscribeOmitsSubIdWhenBrokerDisablesIt)
  REGISTER_TEST(TEST_SendAuthRejectsMissingConnectMethod)
  REGISTER_TEST(TEST_SendAuthRejectsMethodMismatch)
  REGISTER_TEST(TEST_ConnectClearsDeferredTransportPackets)
  REGISTER_TEST(TEST_BlockingTransportDiagnosticsSurface)
  REGISTER_TEST(TEST_BlockingTransportHardLimitDisconnects)
  REGISTER_TEST(TEST_BlockingTransportHardLimitAllowsWithinBudget)
  REGISTER_TEST(TEST_PollReentrancyGuard)

  //--- Execute tests
  int passed_tests     = 0;
  int total_assertions = 0;

  for (int i = 0; i < test_count; i++) {
    g_tests_passed = 0;
    g_tests_failed = 0;

    //--- Clear any staged comparator failure details before the next case runs.
    TestUtilClearPendingFailure();
    ResetCallbackState();

    bool result = false;

    if (test_names[i] == "TEST_DefaultState") {
      result = TEST_DefaultState();
    } else if (test_names[i] == "TEST_IsSafeToPublishSurface") {
      result = TEST_IsSafeToPublishSurface();
    } else if (test_names[i] == "TEST_SetHost_TCP") {
      result = TEST_SetHost_TCP();
    } else if (test_names[i] == "TEST_SetHostWS") {
      result = TEST_SetHostWS();
    } else if (test_names[i] == "TEST_FluentConfiguration") {
      result = TEST_FluentConfiguration();
    } else if (test_names[i] == "TEST_WillConfiguration") {
      result = TEST_WillConfiguration();
    } else if (test_names[i] == "TEST_CallbackRegistration") {
      result = TEST_CallbackRegistration();
    } else if (test_names[i] == "TEST_SubscriptionRegistry") {
      result = TEST_SubscriptionRegistry();
    } else if (test_names[i] == "TEST_DefaultQoS") {
      result = TEST_DefaultQoS();
    } else if (test_names[i] == "TEST_PublishQueuing") {
      result = TEST_PublishQueuing();
    } else if (test_names[i] == "TEST_PublishQueueBinary") {
      result = TEST_PublishQueueBinary();
    } else if (test_names[i] == "TEST_PublishQueuePayloadByteBudget") {
      result = TEST_PublishQueuePayloadByteBudget();
    } else if (test_names[i] == "TEST_PublishQueuePropertyByteBudget") {
      result = TEST_PublishQueuePropertyByteBudget();
    } else if (test_names[i] == "TEST_PublishQueueSingleMessageByteBudget") {
      result = TEST_PublishQueueSingleMessageByteBudget();
    } else if (test_names[i] == "TEST_PublishQoS0DisconnectedDefault") {
      result = TEST_PublishQoS0DisconnectedDefault();
    } else if (test_names[i] == "TEST_PublishQoS0QueueOptIn") {
      result = TEST_PublishQoS0QueueOptIn();
    } else if (test_names[i] == "TEST_ConnackTimeout_Config") {
      result = TEST_ConnackTimeout_Config();
    } else if (test_names[i] == "TEST_ConnackDeadlineStartsOnConnectSend") {
      result = TEST_ConnackDeadlineStartsOnConnectSend();
    } else if (test_names[i] == "TEST_ConnectIncludesExtendedMqtt5Properties") {
      result = TEST_ConnectIncludesExtendedMqtt5Properties();
    } else if (test_names[i] == "TEST_ConnackResponseInformationSurface") {
      result = TEST_ConnackResponseInformationSurface();
    } else if (test_names[i] == "TEST_ConnackAssignedClientIdentifierSurface") {
      result = TEST_ConnackAssignedClientIdentifierSurface();
    } else if (test_names[i] == "TEST_ConnackMaximumQoSSurface") {
      result = TEST_ConnackMaximumQoSSurface();
    } else if (test_names[i] == "TEST_ConnackReceiveMaximumSurface") {
      result = TEST_ConnackReceiveMaximumSurface();
    } else if (test_names[i] == "TEST_ConnackServerKeepAliveSurface") {
      result = TEST_ConnackServerKeepAliveSurface();
    } else if (test_names[i] == "TEST_ConnackServerKeepAliveDoesNotLeakIntoNextConnect") {
      result = TEST_ConnackServerKeepAliveDoesNotLeakIntoNextConnect();
    } else if (test_names[i] == "TEST_ConnackBrokerCapabilitySurface") {
      result = TEST_ConnackBrokerCapabilitySurface();
    } else if (test_names[i] == "TEST_ConnackMaximumPacketSizeOmittedClearsPreviousLimit") {
      result = TEST_ConnackMaximumPacketSizeOmittedClearsPreviousLimit();
    } else if (test_names[i] == "TEST_ConnackExtendedMetadataSurface") {
      result = TEST_ConnackExtendedMetadataSurface();
    } else if (test_names[i] == "TEST_ConnackRejectDiagnosticsSurface") {
      result = TEST_ConnackRejectDiagnosticsSurface();
    } else if (test_names[i] == "TEST_AckDiagnosticsCache") {
      result = TEST_AckDiagnosticsCache();
    } else if (test_names[i] == "TEST_PublishFacadePropertiesSurface") {
      result = TEST_PublishFacadePropertiesSurface();
    } else if (test_names[i] == "TEST_InboundPublishMetadataSurface") {
      result = TEST_InboundPublishMetadataSurface();
    } else if (test_names[i] == "TEST_InboundPublishInvalidUtf8StrictDisconnect") {
      result = TEST_InboundPublishInvalidUtf8StrictDisconnect();
    } else if (test_names[i] == "TEST_InboundPublishInvalidUtf8RelaxedDelivery") {
      result = TEST_InboundPublishInvalidUtf8RelaxedDelivery();
    } else if (test_names[i] == "TEST_InboundPublishMetadataQoS2ResumeSurface") {
      result = TEST_InboundPublishMetadataQoS2ResumeSurface();
    } else if (test_names[i] == "TEST_PublishRejectsOutgoingSubscriptionIdentifier") {
      result = TEST_PublishRejectsOutgoingSubscriptionIdentifier();
    } else if (test_names[i] == "TEST_RetransmitStripsLegacyOutgoingSubscriptionIdentifier") {
      result = TEST_RetransmitStripsLegacyOutgoingSubscriptionIdentifier();
    } else if (test_names[i] == "TEST_ConnackSessionExpiryOverrideZeroClearsStateOnDisconnect") {
      result = TEST_ConnackSessionExpiryOverrideZeroClearsStateOnDisconnect();
    } else if (test_names[i] == "TEST_ConnackSessionExpiryOmittedRestoresConfiguredPolicy") {
      result = TEST_ConnackSessionExpiryOmittedRestoresConfiguredPolicy();
    } else if (test_names[i] == "TEST_QueuedPublishPropertiesSurviveDrain") {
      result = TEST_QueuedPublishPropertiesSurviveDrain();
    } else if (test_names[i] == "TEST_ExpiredQueuedPublishesArePurgedWhileOffline") {
      result = TEST_ExpiredQueuedPublishesArePurgedWhileOffline();
    } else if (test_names[i] == "TEST_QueuedPublishExpiryRoundsUpOnDrain") {
      result = TEST_QueuedPublishExpiryRoundsUpOnDrain();
    } else if (test_names[i] == "TEST_DurableQueuedPublishCountSurface") {
      result = TEST_DurableQueuedPublishCountSurface();
    } else if (test_names[i] == "TEST_FlushSessionStateNowSucceedsWhenAlreadyDurable") {
      result = TEST_FlushSessionStateNowSucceedsWhenAlreadyDurable();
    } else if (test_names[i] == "TEST_OfflineQueuedPublishSurvivesClientRestart") {
      result = TEST_OfflineQueuedPublishSurvivesClientRestart();
    } else if (test_names[i] == "TEST_IncomingStorageErrorCountSurvivesClientRestart") {
      result = TEST_IncomingStorageErrorCountSurvivesClientRestart();
    } else if (test_names[i] == "TEST_EncryptedSessionRoundTrip") {
      result = TEST_EncryptedSessionRoundTrip();
    } else if (test_names[i] == "TEST_EncryptedSessionWrongPassphraseFailsLoad") {
      result = TEST_EncryptedSessionWrongPassphraseFailsLoad();
    } else if (test_names[i] == "TEST_EncryptedSessionTamperFailsLoad") {
      result = TEST_EncryptedSessionTamperFailsLoad();
    } else if (test_names[i] == "TEST_FlatBufferAppendSizeRejectsOverflow") {
      result = TEST_FlatBufferAppendSizeRejectsOverflow();
    } else if (test_names[i] == "TEST_RetransmitPreservesPublishProperties") {
      result = TEST_RetransmitPreservesPublishProperties();
    } else if (test_names[i] == "TEST_DiagnosticsCallbacksSurfaceMetadata") {
      result = TEST_DiagnosticsCallbacksSurfaceMetadata();
    } else if (test_names[i] == "TEST_OutgoingAckDiagnosticsSurface") {
      result = TEST_OutgoingAckDiagnosticsSurface();
    } else if (test_names[i] == "TEST_ImmediateConnectFailurePreservesReconnect") {
      result = TEST_ImmediateConnectFailurePreservesReconnect();
    } else if (test_names[i] == "TEST_ReconnectTelemetrySurface") {
      result = TEST_ReconnectTelemetrySurface();
    } else if (test_names[i] == "TEST_InFlightTelemetrySurface") {
      result = TEST_InFlightTelemetrySurface();
    } else if (test_names[i] == "TEST_QueuedAgeTelemetrySurface") {
      result = TEST_QueuedAgeTelemetrySurface();
    } else if (test_names[i] == "TEST_CallbackBacklogTelemetrySurface") {
      result = TEST_CallbackBacklogTelemetrySurface();
    } else if (test_names[i] == "TEST_DeferredTransportBacklogTelemetrySurface") {
      result = TEST_DeferredTransportBacklogTelemetrySurface();
    } else if (test_names[i] == "TEST_DeferredTransportBacklogOverflowDisconnects") {
      result = TEST_DeferredTransportBacklogOverflowDisconnects();
    } else if (test_names[i] == "TEST_DeferredCallbackBacklogOverflowDisconnects") {
      result = TEST_DeferredCallbackBacklogOverflowDisconnects();
    } else if (test_names[i] == "TEST_LastFailureTelemetrySurface") {
      result = TEST_LastFailureTelemetrySurface();
    } else if (test_names[i] == "TEST_ConnackTimeoutBrokerFailureSurface") {
      result = TEST_ConnackTimeoutBrokerFailureSurface();
    } else if (test_names[i] == "TEST_RedirectAllowlistBlocksUnapprovedHost") {
      result = TEST_RedirectAllowlistBlocksUnapprovedHost();
    } else if (test_names[i] == "TEST_RedirectAllowlistAllowsApprovedHost") {
      result = TEST_RedirectAllowlistAllowsApprovedHost();
    } else if (test_names[i] == "TEST_RedirectAllowlistCannotBeDisabled") {
      result = TEST_RedirectAllowlistCannotBeDisabled();
    } else if (test_names[i] == "TEST_ConnectWithoutHost") {
      result = TEST_ConnectWithoutHost();
    } else if (test_names[i] == "TEST_ConnectRejectsPlaintextTransportByDefault") {
      result = TEST_ConnectRejectsPlaintextTransportByDefault();
    } else if (test_names[i] == "TEST_ConnectAllowsPlaintextTransportWhenExplicitlyPermitted") {
      result = TEST_ConnectAllowsPlaintextTransportWhenExplicitlyPermitted();
    } else if (test_names[i] == "TEST_ConnectRejectsPlainWebSocketByDefault") {
      result = TEST_ConnectRejectsPlainWebSocketByDefault();
    } else if (test_names[i] == "TEST_ConnectRejectsPlaintextCredentialsByDefault") {
      result = TEST_ConnectRejectsPlaintextCredentialsByDefault();
    } else if (test_names[i] == "TEST_ConnectAllowsPlaintextCredentialsWhenExplicitlyPermitted") {
      result = TEST_ConnectAllowsPlaintextCredentialsWhenExplicitlyPermitted();
    } else if (test_names[i] == "TEST_ReconnectUsesDedicatedConnectTimeout") {
      result = TEST_ReconnectUsesDedicatedConnectTimeout();
    } else if (test_names[i] == "TEST_AsyncConnectWaitsForStableTransportTurn") {
      result = TEST_AsyncConnectWaitsForStableTransportTurn();
    } else if (test_names[i] == "TEST_SendConnectDefersUntilTransportWritable") {
      result = TEST_SendConnectDefersUntilTransportWritable();
    } else if (test_names[i] == "TEST_TofuRequiresProvisionedThumbprint") {
      result = TEST_TofuRequiresProvisionedThumbprint();
    } else if (test_names[i] == "TEST_TofuProvisionedThumbprintPinsAndReportsPinnedTrustMode") {
      result = TEST_TofuProvisionedThumbprintPinsAndReportsPinnedTrustMode();
    } else if (test_names[i] == "TEST_TofuReadFailureDegradesTrustMode") {
      result = TEST_TofuReadFailureDegradesTrustMode();
    } else if (test_names[i] == "TEST_TofuStrictReadFailureDisconnects") {
      result = TEST_TofuStrictReadFailureDisconnects();
    } else if (test_names[i] == "TEST_DisconnectWhileDisconnected") {
      result = TEST_DisconnectWhileDisconnected();
    } else if (test_names[i] == "TEST_DisconnectWithReasonCode") {
      result = TEST_DisconnectWithReasonCode();
    } else if (test_names[i] == "TEST_DisconnectWithExtendedMetadata") {
      result = TEST_DisconnectWithExtendedMetadata();
    } else if (test_names[i] == "TEST_PollWhileDisconnected") {
      result = TEST_PollWhileDisconnected();
    } else if (test_names[i] == "TEST_MetricsInitialization") {
      result = TEST_MetricsInitialization();
    } else if (test_names[i] == "TEST_MaxRetransmitConfig") {
      result = TEST_MaxRetransmitConfig();
    } else if (test_names[i] == "TEST_UnlimitedQoS1RequiresMessageExpiry") {
      result = TEST_UnlimitedQoS1RequiresMessageExpiry();
    } else if (test_names[i] == "TEST_StateEnum_Values") {
      result = TEST_StateEnum_Values();
    } else if (test_names[i] == "TEST_PublishErrorEnum_Values") {
      result = TEST_PublishErrorEnum_Values();
    } else if (test_names[i] == "TEST_PublishStringOverloads") {
      result = TEST_PublishStringOverloads();
    } else if (test_names[i] == "TEST_PublishBinaryOverloads") {
      result = TEST_PublishBinaryOverloads();
    } else if (test_names[i] == "TEST_MultipleDisconnects") {
      result = TEST_MultipleDisconnects();
    } else if (test_names[i] == "TEST_SubscribeDefaultQoS") {
      result = TEST_SubscribeDefaultQoS();
    } else if (test_names[i] == "TEST_SubscribeRejectsInvalidFilter") {
      result = TEST_SubscribeRejectsInvalidFilter();
    } else if (test_names[i] == "TEST_UnsubscribeNonexistent") {
      result = TEST_UnsubscribeNonexistent();
    } else if (test_names[i] == "TEST_UnsubscribeDeferredUntilAck") {
      result = TEST_UnsubscribeDeferredUntilAck();
    } else if (test_names[i] == "TEST_Regression_SubackKeepsMovedSubscriptionOptions") {
      result = TEST_Regression_SubackKeepsMovedSubscriptionOptions();
    } else if (test_names[i] == "TEST_ControlPacketDiagnosticsCache") {
      result = TEST_ControlPacketDiagnosticsCache();
    } else if (test_names[i] == "TEST_Regression_SubscriptionIndexRehashAndMovedLookup") {
      result = TEST_Regression_SubscriptionIndexRehashAndMovedLookup();
    } else if (test_names[i] == "TEST_QueuedMessageCount_AfterDisconnect") {
      result = TEST_QueuedMessageCount_AfterDisconnect();
    } else if (test_names[i] == "TEST_Regression_QoS2DedupeAndCallbackBoundary") {
      result = TEST_Regression_QoS2DedupeAndCallbackBoundary();
    } else if (test_names[i] == "TEST_Regression_QoS2DirectionScopedPacketIds") {
      result = TEST_Regression_QoS2DirectionScopedPacketIds();
    } else if (test_names[i] == "TEST_Regression_QoS2DedupeAcrossClientRestart") {
      result = TEST_Regression_QoS2DedupeAcrossClientRestart();
    } else if (test_names[i] == "TEST_Regression_PollBudgetDefersExtractedPackets") {
      result = TEST_Regression_PollBudgetDefersExtractedPackets();
    } else if (test_names[i] == "TEST_Regression_ReplaySpreadsAcrossPollCycles") {
      result = TEST_Regression_ReplaySpreadsAcrossPollCycles();
    } else if (test_names[i] == "TEST_Regression_ReceiveMaximumEnforced") {
      result = TEST_Regression_ReceiveMaximumEnforced();
    } else if (test_names[i] == "TEST_Regression_QueueTailPreservedOnDrainFailure") {
      result = TEST_Regression_QueueTailPreservedOnDrainFailure();
    } else if (test_names[i] == "TEST_Regression_AuthPacketHandling") {
      result = TEST_Regression_AuthPacketHandling();
    } else if (test_names[i] == "TEST_Regression_AuthMalformedDisconnect") {
      result = TEST_Regression_AuthMalformedDisconnect();
    } else if (test_names[i] == "TEST_Regression_MalformedSimpleAckDisconnects") {
      result = TEST_Regression_MalformedSimpleAckDisconnects();
    } else if (test_names[i] == "TEST_Regression_PublishParseFailureAbortsBatchAfterReentry") {
      result = TEST_Regression_PublishParseFailureAbortsBatchAfterReentry();
    } else if (test_names[i] == "TEST_Regression_InboundPublishQoS3DisconnectsMalformed") {
      result = TEST_Regression_InboundPublishQoS3DisconnectsMalformed();
    } else if (test_names[i] == "TEST_Regression_OrphanOutgoingAcksIgnored") {
      result = TEST_Regression_OrphanOutgoingAcksIgnored();
    } else if (test_names[i] == "TEST_Regression_PUBCOMP92CompletesQoS2Resume") {
      result = TEST_Regression_PUBCOMP92CompletesQoS2Resume();
    } else if (test_names[i] == "TEST_Regression_ReplaySubackCountMismatchDisconnects") {
      result = TEST_Regression_ReplaySubackCountMismatchDisconnects();
    } else if (test_names[i] == "TEST_Connack_ReceiveMaximumZero_ProtocolError") {
      result = TEST_Connack_ReceiveMaximumZero_ProtocolError();
    } else if (test_names[i] == "TEST_Connack_DuplicateProperty_ProtocolError") {
      result = TEST_Connack_DuplicateProperty_ProtocolError();
    } else if (test_names[i] == "TEST_Connack_TruncatedProperty_MalformedPacket") {
      result = TEST_Connack_TruncatedProperty_MalformedPacket();
    } else if (test_names[i] == "TEST_SubscribeRejectsWildcardWhenBrokerDisablesIt") {
      result = TEST_SubscribeRejectsWildcardWhenBrokerDisablesIt();
    } else if (test_names[i] == "TEST_SubscribeRejectsSharedWhenBrokerDisablesIt") {
      result = TEST_SubscribeRejectsSharedWhenBrokerDisablesIt();
    } else if (test_names[i] == "TEST_SubscribeOmitsSubIdWhenBrokerDisablesIt") {
      result = TEST_SubscribeOmitsSubIdWhenBrokerDisablesIt();
    } else if (test_names[i] == "TEST_SendAuthRejectsMissingConnectMethod") {
      result = TEST_SendAuthRejectsMissingConnectMethod();
    } else if (test_names[i] == "TEST_SendAuthRejectsMethodMismatch") {
      result = TEST_SendAuthRejectsMethodMismatch();
    } else if (test_names[i] == "TEST_ConnectClearsDeferredTransportPackets") {
      result = TEST_ConnectClearsDeferredTransportPackets();
    } else if (test_names[i] == "TEST_BlockingTransportDiagnosticsSurface") {
      result = TEST_BlockingTransportDiagnosticsSurface();
    } else if (test_names[i] == "TEST_BlockingTransportHardLimitDisconnects") {
      result = TEST_BlockingTransportHardLimitDisconnects();
    } else if (test_names[i] == "TEST_BlockingTransportHardLimitAllowsWithinBudget") {
      result = TEST_BlockingTransportHardLimitAllowsWithinBudget();
    } else if (test_names[i] == "TEST_PollReentrancyGuard") {
      result = TEST_PollReentrancyGuard();
    }

    total_assertions += g_tests_passed + g_tests_failed;

    if (result && g_tests_failed == 0) {
      passed_tests++;
      TestUtilRecordCasePass(test_names[i], g_tests_passed);
    } else {
      TestUtilRecordCaseFail(test_names[i], g_tests_failed);
    }
  }

  TestUtilFinalizeSuite(suite_name, passed_tests, test_count, total_assertions);
}
