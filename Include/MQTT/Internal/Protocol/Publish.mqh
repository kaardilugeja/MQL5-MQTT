//+------------------------------------------------------------------+
//|                                                      Publish.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 PUBLISH packet implementation per spec §3.3.            |
//| Used to transport application messages to subscribers.           |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_PUBLISH_MQH
#define MQTT_INTERNAL_PROTOCOL_PUBLISH_MQH

#include "..\\Util\\PropertyEncoder.mqh"

//+------------------------------------------------------------------+
//| PUBLISH Variable Header                                          |
//+------------------------------------------------------------------+
/*
The Variable Header of the PUBLISH Packet contains the following fields in the order: Topic Name,
Packet Identifier, and Properties.
*/

//+------------------------------------------------------------------+
//| Class CPublish                                                   |
//| Purpose: Class of MQTT Publish Control Packets                   |
//| Usage:   Used to build and parse PUBLISH packets                 |
//+------------------------------------------------------------------+
class CPublish {
 private:
  //--- Validate topic name per MQTT spec §4.7
  //--- Checks: empty, reserved $ prefix, max length (65535 bytes UTF-8), wildcards (#, +)
  bool            ValidateTopicName(const string &str);
  static bool     ValidateTopicNameUtf8Bytes(const uchar &utf8[], const int len);

  //--- Property helpers
  bool            TryGetTopicAliasProperty(ushort &alias) const;
  bool            UpsertTopicAliasProperty(const ushort alias);
  void            RemoveProperty(uchar prop_id);  // Remove first occurrence of a non-repeatable property
  static bool     ParsePublishHeader(uchar &inpkt[], uint &payload_start, string &topic_name,
                                     CTopicAliasManager *alias_mgr = NULL);

  //--- Read all properties from incoming PUBLISH
  ENUM_MQTT_ERROR ReadAllProperties(uchar &pkt[], uint props_len, uint idx);

 protected:
  //--- Publish packet configuration
  uchar  m_pubflags;   // Publish flags (retain, QoS, dup)
  uint   m_remlen;     // Remaining length
  uchar  m_topname[];  // Topic name buffer
  uchar  m_props[];    // Properties buffer
  uchar  m_payload[];  // Payload buffer
  ushort m_pktid;      // Packet identifier

  //--- Reusable VBI scratch buffers promoted from Build() locals.
  //--- Allocated once per CPublish lifetime (member of cached m_pub_builder in CMqttClient).
  //--- Eliminates 2 alloc/free cycles per Publish() call at 50 msg/s.
  uchar  m_vbi_props_buf[];                         // Properties-length VBI encoding scratch.
  uchar  m_vbi_remlen_buf[];                        // Remaining-length VBI encoding scratch.
  bool   m_allow_outgoing_subscription_identifier;  // Unit-test/replay escape hatch; client-originated PUBLISH must
                                                    // normally omit Subscription Identifier (§3.3.2.3.8).

  //--- Parsed properties (populated by Read())
  string m_parsed_topic_name;          // Topic Name
  uchar  m_parsed_payload[];           // Payload data
  uchar  m_parsed_qos;                 // QoS level (0, 1, 2)
  bool   m_parsed_retain;              // RETAIN flag
  bool   m_parsed_dup;                 // DUP flag
  uchar  m_parsed_payload_format;      // Payload Format Indicator
  bool   m_has_payload_format;         // Whether payload format was parsed
  uint   m_parsed_msg_expiry;          // Message Expiry Interval
  bool   m_has_msg_expiry;             // Whether msg expiry was parsed
  string m_parsed_content_type;        // Content Type
  string m_parsed_response_topic;      // Response Topic
  uchar  m_parsed_correlation_data[];  // Correlation Data
  ushort m_parsed_topic_alias;         // Topic Alias
  bool   m_has_topic_alias;            // Whether topic alias was parsed
  uchar  m_parsed_props_raw[];   // Exact incoming property block for facade replay and forward-compatible surfacing.
  uint   m_parsed_sub_ids[];     // All Subscription Identifier properties in wire order; one publish may match multiple
                                 // subscriptions.
  uint   m_parsed_sub_id_count;  // Number of populated entries in m_parsed_sub_ids[].
  string m_parsed_user_prop_keys[];  // User Property keys
  string m_parsed_user_prop_vals[];  // User Property values
  uint   m_parsed_user_prop_count;   // Number of user properties

 public:
  //--- Constructor declarations
  CPublish();
  ~CPublish();

  //--- Topic Alias Manager integration (MQTT 5.0 Phase 3)
  bool   SetTopicAliasWithManager(CTopicAliasManager &mgr);  // Register and set topic alias using provided manager
  bool   CanUseTopicAlias(CTopicAliasManager &mgr) const;    // Check if topic has registered alias in provided manager
  ushort GetTopicAliasFromManager(CTopicAliasManager &mgr) const;  // Get alias value from provided manager

  //--- Methods for setting Publish flags
  void
  SetRetain(const bool retain           = true,
            const bool retain_available = true);  // Enable/disable retain (retain_available from CONNACK §3.2.2.3.6)
  void SetQoS(const uchar level);                 // Set QoS level 0/1/2 — preferred; clears both bits atomically
  void SetQoS_1(const bool enable_qos1 = true);   // Set QoS bit 1 (clears QoS_2 to prevent QoS 3)
  void SetQoS_2(const bool enable_qos2 = true);   // Set QoS bit 2 (clears QoS_1 to prevent QoS 3)
  void SetDup(const bool dup = true);             // Enable/disable duplicate

  //--- Method for setting Topic Name
  void SetTopicName(const string &topic_name);
  //--- Fast-path variant that skips ValidateTopicName() — ONLY for already-validated topics.
  //--- CMqttClient caches the last successfully validated topic in m_pub_last_valid_topic and calls
  //--- this instead of SetTopicName() when the same topic is republished (common in trading signals).
  void SetTopicNameFast(const string &topic_name);
  //--- Returns true when a topic name was successfully set (m_topname is non-empty).
  bool IsTopicSet() const { return m_topname.Size() > 0; }
  //--- Clear the encoded topic name to enable alias-reuse mode in Build().
  //--- After RegisterClientAliasAuto has established the mapping, call SetTopicAlias()
  //--- and then ClearTopicName() so Build() emits a zero-length topic field with only
  //--- the alias property, saving topic_len bytes per PUBLISH per MQTT §3.3.2.3.4.
  void ClearTopicName() { ArrayResize(m_topname, 0); }

  //--- Methods for setting Properties
  void SetPayloadFormatIndicator(PAYLOAD_FORMAT_INDICATOR format);  // Payload format
  void SetMessageExpiryInterval(uint msg_expiry_interval);          // Message expiry
  void SetTopicAlias(ushort topic_alias);                           // Topic alias
  void SetResponseTopic(const string &response_topic);              // Response topic
  void SetCorrelationData(const uchar &binary_data[]);              // Correlation data
  void SetUserProperty(const string &key, const string &val);       // User property
  void SetSubscriptionIdentifier(uint subscript_id);                // Subscription ID
  void SetEncodedProperties(const uchar &props[]);                  // Copy pre-encoded properties
  void SetEncodedProperties(const uchar &props[], int start_offset,
                            int length);                            // Copy pre-encoded property slice
  void GetEncodedProperties(uchar &dest[]) const;                   // Export encoded properties
  //--- Unit-test-only escape hatch for validating legacy wire-image handling.
  //--- Production callers must not emit client-originated Subscription Identifier.
#ifdef MQTT_UNIT_TESTS
  void AllowOutgoingSubscriptionIdentifier(const bool allow = true) {
    m_allow_outgoing_subscription_identifier = allow;
  }
#endif
#ifdef MQTT_UNIT_TESTS
  static bool TestValidateTopicNameUtf8Bytes(const uchar &utf8[]) {
    return ValidateTopicNameUtf8Bytes(utf8, ArraySize(utf8));
  }
#endif
  void          SetContentType(const string &content_type);  // Content type

  //--- Methods for setting the payload
  void          SetPayload(const uchar &payload[]);     // Binary payload
  void          SetPayload(const uchar &payload[], int start_offset,
                           int length);                 // Offset+length slice (avoids intermediate copy)
  void          SetPayload(const string &payload);      // String payload
  void          SetPayloadUTF8(const string &payload);  // UTF-8 encoded payload

  //--- Reset all fields to defaults, clearing build-time arrays for reuse.
  //--- Call this before reusing a cached CPublish instance.
  void          Reset();

  //--- Method for building the final packet
  void          Build(uchar &pkt[], CTopicAliasManager *mgr = NULL, CSessionDatabase *db = NULL, uchar max_qos = 2);

  //--- Read incoming PUBLISH packet (full parsing per §3.3)
  int           Read(uchar &pkt[], CTopicAliasManager *alias_mgr = NULL);

  //--- Method for setting packet identifier
  void          SetPacketId(ushort pktid) { m_pktid = pktid; }

  //--- Static methods for reading incoming packets
  static string ReadTopicName(uchar &inpkt[], CTopicAliasManager *mgr = NULL);        // Read topic from packet
  static string ReadMessageUTF8(uchar &inpkt[], CTopicAliasManager *mgr = NULL);      // Read UTF-8 message
  static string ReadMessageRawBytes(uchar &inpkt[], CTopicAliasManager *mgr = NULL);  // Read raw bytes message

  //--- Getters for parsed state (populated by Read())
  string        GetTopicName() const { return m_parsed_topic_name; }
  ushort        GetPacketId() const { return m_pktid; }
  uchar         GetQoS() const { return m_parsed_qos; }
  bool          GetRetain() const { return m_parsed_retain; }
  bool          GetDup() const { return m_parsed_dup; }
  string        GetPayloadString() const;
  void          GetPayloadBytes(uchar &dest[]) const;
  uchar         GetPayloadFormatIndicator() const { return m_parsed_payload_format; }
  bool          HasPayloadFormat() const { return m_has_payload_format; }
  uint          GetMessageExpiryInterval() const { return m_parsed_msg_expiry; }
  bool          HasMessageExpiry() const { return m_has_msg_expiry; }
  string        GetContentType() const { return m_parsed_content_type; }
  string        GetResponseTopic() const { return m_parsed_response_topic; }
  void          GetCorrelationData(uchar &dest[]) const;
  ushort        GetTopicAlias() const { return m_parsed_topic_alias; }
  bool          HasTopicAlias() const { return m_has_topic_alias; }
  void          GetParsedPropertiesRaw(uchar &dest[]) const;
  uint          GetSubscriptionIdCount() const { return m_parsed_sub_id_count; }
  uint          GetSubscriptionId(uint index) const;
  uint          GetUserPropertyCount() const { return m_parsed_user_prop_count; }
  string        GetUserPropertyKey(uint index) const;
  string        GetUserPropertyValue(uint index) const;
};

//+------------------------------------------------------------------+
//| ReadMessageRawBytes                                              |
//| Purpose: Read message as raw bytes from PUBLISH packet           |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             mgr   - [IN] optional topic alias manager            |
//| Return: Message as string (CP_ACP encoded)                       |
//+------------------------------------------------------------------+
static string CPublish::ReadMessageRawBytes(uchar &inpkt[], CTopicAliasManager *mgr) {
  uint   payload_start = 0;
  string topic_name    = "";
  if (!ParsePublishHeader(inpkt, payload_start, topic_name, mgr)) {
    return "";
  }

  int payload_len = (int)ArraySize(inpkt) - (int)payload_start;
  if (payload_len <= 0) {
    return "";
  }

  //--- Use CP_ACP to return raw byte values without UTF-8 interpretation
  return CharArrayToString(inpkt, payload_start, payload_len, CP_ACP);
}

//+------------------------------------------------------------------+
//| ReadMessageUTF8                                                  |
//| Purpose: Read message as UTF-8 from PUBLISH packet               |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             mgr   - [IN] optional topic alias manager            |
//| Return: Message as UTF-8 string                                  |
//+------------------------------------------------------------------+
static string CPublish::ReadMessageUTF8(uchar &inpkt[], CTopicAliasManager *mgr) {
  uint   payload_start = 0;
  string topic_name    = "";
  if (!ParsePublishHeader(inpkt, payload_start, topic_name, mgr)) {
    return "";
  }

  int payload_len = (int)ArraySize(inpkt) - (int)payload_start;
  if (payload_len <= 0) {
    return "";
  }

  return CharArrayToString(inpkt, payload_start, payload_len, CP_UTF8);
}

//+------------------------------------------------------------------+
//| ReadTopicName                                                    |
//| Purpose: Read topic name from PUBLISH packet                     |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             mgr   - [IN] optional topic alias manager            |
//| Return: Topic name as string                                     |
//+------------------------------------------------------------------+
static string CPublish::ReadTopicName(uchar &inpkt[], CTopicAliasManager *mgr) {
  uint   payload_start = 0;
  string topic_name    = "";
  if (!ParsePublishHeader(inpkt, payload_start, topic_name, mgr)) {
    return "";
  }

  return topic_name;
}

//+------------------------------------------------------------------+
//| SetPayload                                                       |
//| Purpose: Set payload from byte array                             |
//| Parameters: payload - [IN] binary payload data                   |
//+------------------------------------------------------------------+
void CPublish::SetPayload(const uchar &payload[]) {
  ArrayResize(m_payload, ArraySize(payload));
  ArrayCopy(m_payload, payload, 0);
}

//+------------------------------------------------------------------+
//| SetPayload (offset+length overload)                              |
//| Purpose: Copy a slice of src[] into the payload buffer without   |
//|          requiring a caller-side intermediate array.             |
//| Parameters: payload      - [IN] source buffer                    |
//|             start_offset - [IN] starting byte in src[]           |
//|             length       - [IN] number of bytes to copy          |
//+------------------------------------------------------------------+
void CPublish::SetPayload(const uchar &payload[], int start_offset, int length) {
  ArrayResize(m_payload, length);
  ArrayCopy(m_payload, payload, 0, start_offset, length);
}

//+------------------------------------------------------------------+
//| SetPayload                                                       |
//| Purpose: Set payload from string                                 |
//| Parameters: payload - [IN] string payload                        |
//+------------------------------------------------------------------+
void CPublish::SetPayload(const string &payload) {
  uchar aux[];
  int   len = StringToCharArray(payload, aux, 0, WHOLE_ARRAY, CP_UTF8);
  //--- Exclude null terminator from payload per MQTT spec
  if (len > 0 && aux[len - 1] == 0) {
    len--;
  }
  ArrayResize(aux, len);
  ArrayResize(m_payload, len);
  ArrayCopy(m_payload, aux, 0);
}

//+------------------------------------------------------------------+
//| SetPayloadUTF8                                                   |
//| Purpose: Set UTF-8 encoded payload                               |
//| Parameters: payload - [IN] string to encode as UTF-8             |
//+------------------------------------------------------------------+
void CPublish::SetPayloadUTF8(const string &payload) {
  //--- Use StringToUTF8Bytes directly instead of EncodeUTF8String + strip prefix.
  //--- StringToUTF8Bytes produces raw UTF-8 without the 2-byte MQTT length prefix,
  //--- avoiding the wasteful encode-then-strip pattern.
  int len = StringToUTF8Bytes(payload, m_payload);
  if (len <= 0) {
    ArrayFree(m_payload);
  }
}

//+------------------------------------------------------------------+
//| SetContentType                                                   |
//| Purpose: Set content type property per §3.3.2.3.9                |
//| Parameters: content_type - [IN] MIME type string                 |
//+------------------------------------------------------------------+
void CPublish::SetContentType(const string &content_type) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_CONTENT_TYPE);
  CPropertyEncoder::EncodeStringProperty(m_props, MQTT_PROP_IDENTIFIER_CONTENT_TYPE, content_type);
}

//+------------------------------------------------------------------+
//| SetSubscriptionIdentifier                                        |
//| Purpose: Set subscription identifier property per §3.3.2.3.8     |
//| Parameters: subscript_id - [IN] subscription identifier          |
//|                            (1-268435455)                         |
//+------------------------------------------------------------------+
void CPublish::SetSubscriptionIdentifier(uint subscript_id) {
  if (subscript_id < 1 || subscript_id > 0xfffffff) {
    MQTT_LOG_ERROR("Subscription Identifier must be between 1 and 268,435,455");
    return;
  }
  CPropertyEncoder::EncodeVariableByteIntegerProperty(m_props, MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER,
                                                      subscript_id);
}

//+------------------------------------------------------------------+
//| SetEncodedProperties                                             |
//| Purpose: Replace the current property buffer with encoded bytes  |
//+------------------------------------------------------------------+
void CPublish::SetEncodedProperties(const uchar &props[]) {
  ArrayResize(m_props, ArraySize(props));
  if (ArraySize(props) > 0) {
    ArrayCopy(m_props, props);
  }
}

//+------------------------------------------------------------------+
//| SetEncodedProperties                                             |
//| Purpose: Replace the property buffer from a slice of bytes       |
//+------------------------------------------------------------------+
void CPublish::SetEncodedProperties(const uchar &props[], int start_offset, int length) {
  if (length <= 0 || start_offset < 0 || start_offset >= ArraySize(props)) {
    ArrayResize(m_props, 0);
    return;
  }

  int available = ArraySize(props) - start_offset;
  int copy_len  = (length < available) ? length : available;
  ArrayResize(m_props, copy_len);
  ArrayCopy(m_props, props, 0, start_offset, copy_len);
}

//+------------------------------------------------------------------+
//| GetEncodedProperties                                             |
//| Purpose: Export the encoded property buffer as-is                |
//+------------------------------------------------------------------+
void CPublish::GetEncodedProperties(uchar &dest[]) const {
  ArrayResize(dest, ArraySize(m_props));
  if (ArraySize(m_props) > 0) {
    ArrayCopy(dest, m_props);
  }
}

//+------------------------------------------------------------------+
//| SetUserProperty                                                  |
//| Purpose: Set user property per §3.3.2.3.7                        |
//| Parameters: key - [IN] property name                             |
//|             val - [IN] property value                            |
//+------------------------------------------------------------------+
void CPublish::SetUserProperty(const string &key, const string &val) {
  CPropertyEncoder::EncodeStringPairProperty(m_props, MQTT_PROP_IDENTIFIER_USER_PROPERTY, key, val);
}

//+------------------------------------------------------------------+
//| SetCorrelationData                                               |
//| Purpose: Set correlation data property per §3.3.2.3.6            |
//| Parameters: binary_data - [IN] binary correlation data           |
//+------------------------------------------------------------------+
void CPublish::SetCorrelationData(const uchar &binary_data[]) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_CORRELATION_DATA);
  CPropertyEncoder::EncodeBinaryProperty(m_props, MQTT_PROP_IDENTIFIER_CORRELATION_DATA, binary_data);
}

//+------------------------------------------------------------------+
//| SetResponseTopic                                                 |
//| Purpose: Set response topic property per §3.3.2.3.5              |
//| Parameters: response_topic - [IN] response topic string          |
//+------------------------------------------------------------------+
void CPublish::SetResponseTopic(const string &response_topic) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC);
  CPropertyEncoder::EncodeStringProperty(m_props, MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC, response_topic);
}

//+------------------------------------------------------------------+
//| SetTopicAlias                                                    |
//| Purpose: Set topic alias property per §3.3.2.3.4                 |
//| Parameters: topic_alias - [IN] topic alias value                 |
//+------------------------------------------------------------------+
void CPublish::SetTopicAlias(ushort topic_alias) {
  if (topic_alias == 0) {
    MQTT_LOG_ERROR("Topic Alias value 0 is not permitted per MQTT §3.3.2.3.4");
    return;
  }
  UpsertTopicAliasProperty(topic_alias);
}

//+------------------------------------------------------------------+
//| SetMessageExpiryInterval                                         |
//| Purpose: Set message expiry interval property per §3.3.2.3.3     |
//| Parameters: msg_expiry_interval - [IN] expiry interval in seconds|
//+------------------------------------------------------------------+
void CPublish::SetMessageExpiryInterval(uint msg_expiry_interval) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL);
  CPropertyEncoder::EncodeFourByteIntegerProperty(m_props, MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL,
                                                  msg_expiry_interval);
}

//+------------------------------------------------------------------+
//| SetPayloadFormatIndicator                                        |
//| Purpose: Set payload format indicator property per §3.3.2.3.2    |
//| Parameters: format - [IN] RAW_BYTES or UTF8                      |
//+------------------------------------------------------------------+
void CPublish::SetPayloadFormatIndicator(PAYLOAD_FORMAT_INDICATOR format) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR);
  CPropertyEncoder::EncodeByteProperty(m_props, MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR, (uchar)format);
}

//+------------------------------------------------------------------+
//| CPublish::SetTopicName                                           |
//| Purpose: Set topic name for PUBLISH packet                       |
//| Parameters: topic_name - topic name string                       |
//| Note: Per MQTT §4.7.1, topic names must not be empty or contain  |
//| wildcards. $-prefixed topics are allowed for clients (§4.7.2).   |
//+------------------------------------------------------------------+
void CPublish::SetTopicName(const string &topic_name) {
  //--- Validate topic name per MQTT spec §4.7
  if (!ValidateTopicName(topic_name)) {
    ArrayFree(m_topname);
    return;
  }

  //--- Encode topic name as UTF-8 string
  if (!EncodeUTF8String(topic_name, m_topname)) {
    ArrayFree(m_topname);
  }
}

//+------------------------------------------------------------------+
//| CPublish::SetTopicNameFast                                       |
//| Purpose: Set topic name WITHOUT re-validating.                   |
//|          ONLY call this when the caller has already validated    |
//|          topic_name in a prior call to SetTopicName().           |
//| Parameters: topic_name - previously-validated topic string       |
//+------------------------------------------------------------------+
void CPublish::SetTopicNameFast(const string &topic_name) {
  //--- Skip ValidateTopicName() — caller guarantees this was already validated.
  //--- Go directly to UTF-8 encoding, which is the only remaining work.
  if (!EncodeUTF8String(topic_name, m_topname)) {
    ArrayFree(m_topname);
  }
}

//+------------------------------------------------------------------+
//| Build - Assemble the final PUBLISH packet binary buffer          |
//| Purpose: Compile variable header and payload into binary form    |
//| Parameters: pkt - [OUT] the resulting PUBLISH packet bytes       |
//|             mgr - [IN] optional topic alias manager              |
//|             db  - [IN] optional session database for pktid       |
//| Note: Implements the assembly sequence defined in MQTT 5.0 §3.3  |
//+------------------------------------------------------------------+
void CPublish::Build(uchar &pkt[], CTopicAliasManager *mgr, CSessionDatabase *db, uchar max_qos) {
  if (!m_allow_outgoing_subscription_identifier) {
    int props_before = ArraySize(m_props);
    while (true) {
      int props_now = ArraySize(m_props);
      RemoveProperty(MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER);
      if (ArraySize(m_props) == props_now) {
        break;
      }
    }
    if (ArraySize(m_props) != props_before) {
      MQTT_LOG_ERROR("Client-originated PUBLISH Subscription Identifier removed per MQTT §3.3.2.3.8");
    }
  }

  //--- 0. Validate QoS against server Maximum QoS per §3.2.2.3.4
  uchar qos = (m_pubflags & 0x06) >> 1;
  if (qos > max_qos) {
    MQTT_LOG_ERROR("QoS " + (string)qos + " exceeds server Maximum QoS (" + (string)max_qos + ") per MQTT §3.2.2.3.4");
    return;
  }

  //--- 1. Resolve Topic Alias value before enforcing the Topic Name rule.
  //--- A non-zero Topic Alias property may accompany a normal Topic Name.
  //--- Only a zero-length Topic Name indicates alias reuse (§3.3.2.1, §3.3.2.3.4).
  bool   has_topic_alias   = false;
  ushort topic_alias_value = 0;

  if (TryGetTopicAliasProperty(topic_alias_value)) {
    has_topic_alias = (topic_alias_value > 0);
  }

  if (mgr != NULL && m_topname.Size() > 0) {
    ushort managed_alias = GetTopicAliasFromManager(*mgr);
    if (managed_alias > 0) {
      topic_alias_value = managed_alias;
      has_topic_alias   = true;
    }
  }

  if (has_topic_alias) {
    if (topic_alias_value == 0) {
      MQTT_LOG_ERROR("Topic Alias must be greater than 0 per MQTT §3.3.2.3.4");
      return;
    }
    if (mgr != NULL) {
      ushort alias_max = mgr.GetTopicAliasMaximum();
      if (alias_max == 0) {
        MQTT_LOG_ERROR("Topic Alias present but broker Topic Alias Maximum is 0 per MQTT §3.3.2.3.4");
        return;
      }
      if (topic_alias_value > alias_max) {
        MQTT_LOG_ERROR("Topic Alias " + (string)topic_alias_value + " exceeds broker Topic Alias Maximum ("
                       + (string)alias_max + ") per MQTT §3.3.2.3.4");
        return;
      }
    }
    UpsertTopicAliasProperty(topic_alias_value);
  }

  bool reusing_topic_alias = (m_topname.Size() == 0 && has_topic_alias);

  //--- 2. Validation: Topic Name is mandatory unless alias reuse is active.
  if (m_topname.Size() == 0 && !has_topic_alias) {
    MQTT_LOG_ERROR("Topic Name is mandatory unless Topic Alias reuse is active per MQTT §3.3.2.1");
    return;
  }

  //--- 3. Calculate Variable Header Length
  //--- Topic name (or 0-length indicator if alias used per §3.3.2.1)
  if (reusing_topic_alias) {
    m_remlen = 2;  // [0x00, 0x00]
  } else {
    m_remlen = m_topname.Size();
  }

  //--- Packet identifier (2 bytes) — Required for QoS 1 and 2 (§3.3.2.2)
  bool has_packet_id = ((m_pubflags & 0x06) != 0);
  if (has_packet_id) {
    m_remlen += 2;
  }

  //--- Properties length and Property data (§3.3.2.3)
  //--- Reuse member scratch buffers instead of local alloc/free each call
  EncodeVariableByteInteger(m_props.Size(), m_vbi_props_buf);
  m_remlen += m_vbi_props_buf.Size() + m_props.Size();

  //--- 4. Add Payload Length (§3.3.3)
  m_remlen += m_payload.Size();

  //--- 5. Final Fixed Header Encoding (§2.2.1)
  EncodeVariableByteInteger(m_remlen, m_vbi_remlen_buf);

  //--- 6. Construct Packet Array
  ArrayResize(pkt, 1 + m_vbi_remlen_buf.Size() + m_remlen);

  //--- 6a: Fixed Header Byte (Type=PUBLISH, Flags=DUP|QoS|Retain)
  pkt[0]  = (uchar)PUBLISH << 4;
  pkt[0] |= m_pubflags;

  //--- 6b: Remaining Length varint
  ArrayCopy(pkt, m_vbi_remlen_buf, 1);
  uint idx = 1 + m_vbi_remlen_buf.Size();

  //--- 7. Variable Header Construction
  //--- 7a: Topic Name
  if (reusing_topic_alias) {
    pkt[idx++] = 0;
    pkt[idx++] = 0;
  } else {
    ArrayCopy(pkt, m_topname, idx);
    idx += m_topname.Size();
  }

  //--- 7b: Packet Identifier
  if (has_packet_id) {
    if (m_pktid == 0) {
      //--- Generate new ID if not manually set
      m_pktid = SetPacketIdentifierEx(pkt, idx, db);
    } else {
      WritePacketIdentifier(pkt, idx, m_pktid);
    }
    idx += 2;
  }

  //--- 7c: Properties
  ArrayCopy(pkt, m_vbi_props_buf, idx);
  idx += m_vbi_props_buf.Size();
  ArrayCopy(pkt, m_props, idx);
  idx += m_props.Size();

  //--- 8. Payload (§3.3.3)
  ArrayCopy(pkt, m_payload, idx);
  //--- VBI buffers are reused on the next call; no ArrayFree needed.
}

//+------------------------------------------------------------------+
//| CPublish::ValidateTopicName                                      |
//| Purpose: Validate topic name per MQTT spec §4.7                  |
//| Parameters: str - topic name string                              |
//| Return: true if valid, false if rejected                         |
//| Validation per MQTT §4.7:                                        |
//|   - Must not be empty                                            |
//|   - Must not contain wildcards (# or +)                          |
//|   - Must not exceed 65535 bytes when UTF-8 encoded               |
//+------------------------------------------------------------------+
bool CPublish::ValidateTopicNameUtf8Bytes(const uchar &utf8[], const int len) {
  if (len <= 0) {
    MQTT_LOG_ERROR("Topic name cannot be empty per MQTT §4.7.1");
    return false;
  }
  if (len > 65535) {
    MQTT_LOG_ERROR("Topic name exceeds maximum length of 65535 bytes per MQTT §4.7.3");
    return false;
  }
  for (int i = 0; i < len; i++) {
    if (utf8[i] == 0) {
      MQTT_LOG_ERROR("Topic name cannot contain U+0000 per MQTT §4.7.3");
      return false;
    }
  }
  return true;
}

bool CPublish::ValidateTopicName(const string &str) {
  //--- Per MQTT §4.7.1: Topic names must not be empty
  if (StringLen(str) == 0) {
    MQTT_LOG_ERROR("Topic name cannot be empty per MQTT §4.7.1");
    return false;
  }

  //--- Per MQTT §4.7.1: Topic names must not contain wildcards
  if (StringFind(str, "#") > -1 || StringFind(str, "+") > -1) {
    MQTT_LOG_ERROR("Topic name cannot contain wildcard characters (# or +) per MQTT §4.7.1");
    return false;
  }

  //--- MQTT strings are UTF-8 on the wire and must not contain U+0000.
  uchar temp[];
  int   len = StringToCharArray(str, temp, 0, WHOLE_ARRAY, CP_UTF8);
  if (len > 0 && temp[len - 1] == 0) {
    len--;
  }
  bool valid = ValidateTopicNameUtf8Bytes(temp, len);
  ArrayFree(temp);  // Explicit cleanup of temporary buffer
  return valid;
}

//+------------------------------------------------------------------+
//| SetDup                                                           |
//| Purpose: Set duplicate delivery flag                             |
//| Parameters: dup - [IN] true to mark as duplicate                 |
//+------------------------------------------------------------------+
void CPublish::SetDup(const bool dup) { dup ? m_pubflags |= DUP_FLAG : m_pubflags &= ~DUP_FLAG; }

//+------------------------------------------------------------------+
//| CPublish::SetQoS_2                                               |
//| Purpose: Set QoS level bit 2                                     |
//| Parameters: QoS_2 - true to set QoS bit 2                        |
//| Note: Clears QoS_1 bit first — setting both simultaneously       |
//|       would encode QoS 3 which is protocol violation (§3.3.1.2)  |
//+------------------------------------------------------------------+
void CPublish::SetQoS_2(const bool enable_qos2) {
  m_pubflags &= ~(QoS_1_FLAG | QoS_2_FLAG);  // Clear both bits to prevent QoS 3
  if (enable_qos2) {
    m_pubflags |= QoS_2_FLAG;
  }
}

//+------------------------------------------------------------------+
//| SetQoS_1                                                         |
//| Purpose: Set QoS level bit 1                                     |
//| Parameters: enable_qos1 - [IN] true to set QoS bit 1             |
//| Note: Clears QoS_2 bit first — setting both simultaneously       |
//|       would encode QoS 3 which is protocol violation (§3.3.1.2)  |
//+------------------------------------------------------------------+
void CPublish::SetQoS_1(const bool enable_qos1) {
  m_pubflags &= ~(QoS_1_FLAG | QoS_2_FLAG);  // Clear both bits to prevent QoS 3
  if (enable_qos1) {
    m_pubflags |= QoS_1_FLAG;
  }
}

//+------------------------------------------------------------------+
//| CPublish::SetQoS                                                 |
//| Purpose: Set QoS level 0, 1, or 2 atomically                     |
//| Parameters: level - desired QoS (0, 1, or 2)                     |
//| Note: Preferred over SetQoS_1/SetQoS_2 — atomically clears both  |
//|       bits then sets exactly the right encoding. Silently ignores|
//|       invalid levels (3+); caller should validate beforehand.    |
//+------------------------------------------------------------------+
void CPublish::SetQoS(const uchar level) {
  m_pubflags &= ~(QoS_1_FLAG | QoS_2_FLAG);  // Clear both QoS bits
  if (level == 1) {
    m_pubflags |= QoS_1_FLAG;
  } else if (level == 2) {
    m_pubflags |= QoS_2_FLAG;
  } else if (level > 2) {
    MQTT_LOG_WARN("Invalid QoS level " + (string)(int)level + " — must be 0, 1, or 2 per §3.3.1.2");
  }
}

//+------------------------------------------------------------------+
//| CPublish::SetRetain                                              |
//| Purpose: Set retain message flag                                 |
//| Parameters: retain - true to retain message                      |
//+------------------------------------------------------------------+
void CPublish::SetRetain(const bool retain, const bool retain_available) {
  if (retain && !retain_available) {
    MQTT_LOG_WARN("Server does not support retained messages (Retain Available=0 per CONNACK §3.2.2.3.6)");
    return;
  }
  retain ? m_pubflags |= RETAIN_FLAG : m_pubflags &= ~RETAIN_FLAG;
}

//+------------------------------------------------------------------+
//| Default constructor                                              |
//+------------------------------------------------------------------+
CPublish::CPublish() {
  m_pubflags                               = 0;
  m_remlen                                 = 0;
  m_pktid                                  = 0;
  m_allow_outgoing_subscription_identifier = false;
  m_parsed_topic_name                      = "";
  m_parsed_qos                             = 0;
  m_parsed_retain                          = false;
  m_parsed_dup                             = false;
  m_parsed_payload_format                  = 0;
  m_has_payload_format                     = false;
  m_parsed_msg_expiry                      = 0;
  m_has_msg_expiry                         = false;
  m_parsed_content_type                    = "";
  m_parsed_response_topic                  = "";
  m_parsed_topic_alias                     = 0;
  m_has_topic_alias                        = false;
  ArrayResize(m_parsed_props_raw, 0);
  m_parsed_sub_id_count    = 0;
  m_parsed_user_prop_count = 0;
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CPublish::~CPublish() {}

//+------------------------------------------------------------------+
//| CPublish::Reset                                                  |
//| Purpose: Reset all scalar fields and clear build-time arrays so  |
//|          this instance can be safely reused for the next publish |
//|          call without constructing a new object.                 |
//| Note: m_props MUST be cleared — Build() appends it verbatim.     |
//|       m_payload MUST be cleared — not set when payload_len == 0. |
//+------------------------------------------------------------------+
void CPublish::Reset() {
  m_pubflags                               = 0;
  m_remlen                                 = 0;
  m_pktid                                  = 0;
  m_allow_outgoing_subscription_identifier = false;
  m_parsed_topic_name                      = "";
  m_parsed_qos                             = 0;
  m_parsed_retain                          = false;
  m_parsed_dup                             = false;
  m_parsed_payload_format                  = 0;
  m_has_payload_format                     = false;
  m_parsed_msg_expiry                      = 0;
  m_has_msg_expiry                         = false;
  m_parsed_content_type                    = "";
  m_parsed_response_topic                  = "";
  m_parsed_topic_alias                     = 0;
  m_has_topic_alias                        = false;
  ArrayResize(m_parsed_props_raw, 0);
  m_parsed_sub_id_count    = 0;
  m_parsed_user_prop_count = 0;
  //--- Clear build-time arrays.
  //--- m_topname: will be fully overwritten by SetTopicName() via EncodeUTF8String.
  //--- m_props:   MUST be zeroed — Build() appends its contents verbatim; leftover
  //---            properties from a previous call would otherwise bleed into the next packet.
  //--- m_payload: MUST be zeroed — Publish() only calls SetPayload() when payload_len > 0;
  //---            a cached non-empty buffer would produce a corrupt zero-payload packet.
  ArrayResize(m_topname, 0);
  ArrayResize(m_props, 0);
  ArrayResize(m_payload, 0);
}

//+------------------------------------------------------------------+
//| CPublish::SetTopicAliasWithManager                               |
//| Purpose: Register topic alias using global Topic Alias Manager   |
//|          and set the alias property automatically                |
//| Return: true if alias was registered and set successfully        |
//+------------------------------------------------------------------+
bool CPublish::SetTopicAliasWithManager(CTopicAliasManager &mgr) {
  //--- Get current topic name from encoded buffer
  if (m_topname.Size() < 2) {
    return false;
  }

  //--- Decode topic name from m_topname (format: [len_msb][len_lsb][utf8_bytes])
  const ushort topic_len = (ushort)((m_topname[0] << 8) | m_topname[1]);
  if (topic_len == 0 || (uint)topic_len + 2 > (uint)m_topname.Size()) {
    return false;
  }

  string topic_name = CharArrayToString(m_topname, 2, topic_len, CP_UTF8);
  if (StringLen(topic_name) == 0) {
    return false;
  }

  //--- Check if topic already has an alias
  ushort alias = GetTopicAliasFromManager(mgr);
  if (alias > 0) {
    //--- Use existing alias
    SetTopicAlias(alias);
    return true;
  }

  //--- Register new alias automatically
  if (mgr.RegisterClientAliasAuto(topic_name, alias)) {
    SetTopicAlias(alias);
    return true;
  }

  return false;
}

//+------------------------------------------------------------------+
//| CPublish::CanUseTopicAlias                                       |
//| Purpose: Check if current topic has a registered alias           |
//| Parameters: mgr - Topic Alias Manager instance                   |
//| Return: true if topic has an alias in the manager                |
//+------------------------------------------------------------------+
bool   CPublish::CanUseTopicAlias(CTopicAliasManager &mgr) const { return (GetTopicAliasFromManager(mgr) > 0); }

//+------------------------------------------------------------------+
//| CPublish::GetTopicAliasFromManager                               |
//| Purpose: Get the alias value for current topic from manager      |
//| Parameters: mgr - Topic Alias Manager instance                   |
//| Return: Alias value (0 if not found)                             |
//+------------------------------------------------------------------+
ushort CPublish::GetTopicAliasFromManager(CTopicAliasManager &mgr) const {
  //--- Get current topic name from encoded buffer
  if (m_topname.Size() < 2) {
    return 0;
  }

  //--- Decode topic name from m_topname
  const ushort topic_len = (ushort)((m_topname[0] << 8) | m_topname[1]);
  if (topic_len == 0 || (uint)topic_len + 2 > (uint)m_topname.Size()) {
    return 0;
  }

  string topic_name = CharArrayToString(m_topname, 2, topic_len, CP_UTF8);
  return mgr.GetClientAlias(topic_name);
}

//+------------------------------------------------------------------+
//| ParsePublishHeader                                               |
//| Purpose: Resolve topic name, topic alias, and payload start      |
//+------------------------------------------------------------------+
bool CPublish::ParsePublishHeader(uchar &inpkt[], uint &payload_start, string &topic_name,
                                  CTopicAliasManager *alias_mgr) {
  payload_start = 0;
  topic_name    = "";
  uint idx      = 1;
  uint pkt_size = (uint)ArraySize(inpkt);
  if (pkt_size < 2) {
    return false;
  }

  //--- Decode Remaining Length
  uint old_idx = idx;
  uint remlen  = DecodeVariableByteInteger(inpkt, idx);
  if (remlen == UINT_MAX || idx == old_idx) {
    return false;
  }

  //--- Validate total packet size against Remaining Length
  if (pkt_size < idx + remlen) {
    return false;
  }

  //--- Read Topic Name
  old_idx    = idx;
  topic_name = ReadUtf8String(inpkt, idx);
  if (idx == old_idx) {
    return false;  // Failed to read topic name (truncated or malformed)
  }

  //--- Validate Topic Name content: MUST NOT contain wildcards per §3.3.2.1
  if (StringLen(topic_name) > 0) {
    if (StringFind(topic_name, "#") >= 0 || StringFind(topic_name, "+") >= 0) {
      MQTT_LOG_ERROR("PUBLISH topic name contains wildcards");
      return false;
    }
  }

  //--- Packet Identifier is present if QoS > 0
  if ((inpkt[0] & 0x06) != 0) {
    if (idx + 2 > pkt_size) {
      return false;
    }
    idx += 2;
  }

  //--- Read Properties Length
  old_idx        = idx;
  uint props_len = DecodeVariableByteInteger(inpkt, idx);
  if (props_len == UINT_MAX || (idx == old_idx && remlen > (idx - (1 + GetVarintBytes(remlen))))) {
    //--- Note: If remlen suggests more data but props_len is missing, it's an error
    return false;
  }

  uint props_start = idx;
  uint props_end   = props_start + props_len;
  if (props_end > pkt_size) {
    return false;
  }

  ushort alias     = 0;
  bool   has_alias = false;

  //--- Scan properties for Topic Alias
  while (idx < props_end) {
    uchar prop_id   = inpkt[idx++];
    uint  value_len = 0;
    if (!CPropertyEncoder::GetPropertyValueLength(prop_id, inpkt, idx, value_len)) {
      return false;
    }

    if (prop_id == MQTT_PROP_IDENTIFIER_TOPIC_ALIAS) {
      if (idx + 2 > props_end) {
        return false;
      }
      alias = (ushort)((inpkt[idx] << 8) | inpkt[idx + 1]);
      if (alias == 0) {
        return false;
      }
      has_alias = true;
    }

    idx += value_len;
  }

  payload_start = props_end;

  //--- Register server-side alias if provided
  if (StringLen(topic_name) > 0 && has_alias) {
    if (alias_mgr != NULL) {
      alias_mgr.RegisterServerAlias(topic_name, alias);
    }
  }

  //--- If topic name is zero-length, it MUST have a valid alias
  if (StringLen(topic_name) == 0) {
    if (!has_alias) {
      MQTT_LOG_ERROR("PUBLISH has zero-length topic but no Topic Alias property");
      return false;
    }
    if (alias_mgr != NULL) {
      topic_name = alias_mgr.ResolveServerAlias(alias);
    } else {
      MQTT_LOG_ERROR("PUBLISH topic alias 0x" + (string)alias + " could not be resolved (No Manager provided)");
      return false;
    }
    if (StringLen(topic_name) == 0) {
      MQTT_LOG_ERROR("PUBLISH topic alias 0x" + (string)alias + " could not be resolved");
      return false;
    }
  }

  return true;
}

//+------------------------------------------------------------------+
//| RemoveProperty                                                   |
//| Purpose: Remove the first (and only) occurrence of a             |
//|          non-repeatable property from m_props.                   |
//|          Must be called before re-encoding to prevent duplicate  |
//|          properties per MQTT 5.0 §2.2.2.2.                       |
//| Parameters: prop_id - MQTT property identifier byte              |
//+------------------------------------------------------------------+
void CPublish::RemoveProperty(uchar prop_id) {
  uint  idx           = 0;
  uint  buf_size      = (uint)ArraySize(m_props);
  uint  filtered_size = 0;
  bool  removed       = false;
  uchar filtered_props[];

  while (idx < buf_size) {
    uint  entry_start = idx;
    uchar cur_id      = m_props[idx++];
    uint  value_len   = 0;
    if (!CPropertyEncoder::GetPropertyValueLength(cur_id, m_props, idx, value_len)) {
      return;  // Malformed buffer — bail out safely
    }
    idx += value_len;

    if (idx > buf_size) {
      return;
    }

    uint entry_len = idx - entry_start;
    if (cur_id == prop_id) {
      removed = true;
      continue;
    }

    uint new_size = filtered_size + entry_len;
    uint reserve  = (new_size > 64) ? (new_size / 2) : 64;
    ArrayResize(filtered_props, (int)new_size, (int)reserve);
    ArrayCopy(filtered_props, m_props, (int)filtered_size, (int)entry_start, (int)entry_len);
    filtered_size = new_size;
  }

  if (!removed) {
    return;
  }

  ArrayResize(m_props, (int)filtered_size);
  if (filtered_size > 0) {
    ArrayCopy(m_props, filtered_props, 0, 0, (int)filtered_size);
  }
}

//+------------------------------------------------------------------+
//| UpsertTopicAliasProperty                                         |
//| Purpose: Update or insert topic alias property in m_props        |
//| Parameters: alias - [IN] topic alias value                       |
//| Return: true if updated/inserted successfully                    |
//+------------------------------------------------------------------+
bool CPublish::UpsertTopicAliasProperty(const ushort alias) {
  uint idx      = 0;
  uint buf_size = (uint)ArraySize(m_props);

  while (idx < buf_size) {
    uchar prop_id   = m_props[idx++];
    uint  value_len = 0;
    if (!CPropertyEncoder::GetPropertyValueLength(prop_id, m_props, idx, value_len)) {
      return false;
    }

    if (prop_id == MQTT_PROP_IDENTIFIER_TOPIC_ALIAS) {
      if (idx + 1 >= buf_size) {
        return false;
      }
      m_props[idx]     = (uchar)((alias >> 8) & 0xFF);
      m_props[idx + 1] = (uchar)(alias & 0xFF);
      return true;
    }
    idx += value_len;
  }

  CPropertyEncoder::EncodeTwoByteIntegerProperty(m_props, MQTT_PROP_IDENTIFIER_TOPIC_ALIAS, alias);
  return true;
}

//+------------------------------------------------------------------+
//| TryGetTopicAliasProperty                                         |
//| Purpose: Read Topic Alias property from the local property store |
//| Parameters: alias - [OUT] parsed Topic Alias value               |
//| Return: true if a Topic Alias property is present                |
//+------------------------------------------------------------------+
bool CPublish::TryGetTopicAliasProperty(ushort &alias) const {
  alias         = 0;

  uint idx      = 0;
  uint buf_size = (uint)ArraySize(m_props);
  while (idx < buf_size) {
    uchar prop_id = m_props[idx++];
    if (prop_id == MQTT_PROP_IDENTIFIER_TOPIC_ALIAS) {
      if (idx + 1 >= buf_size) {
        return false;
      }
      alias = (ushort)((m_props[idx] << 8) | m_props[idx + 1]);
      return true;
    }

    uint value_len = 0;
    if (!CPropertyEncoder::GetPropertyValueLength(prop_id, m_props, idx, value_len)) {
      return false;
    }
    idx += value_len;
  }

  return false;
}

//+------------------------------------------------------------------+
//| Read and process incoming PUBLISH packet                         |
//| Parameters: pkt - input packet buffer                            |
//| Return: MQTT_OK on success, or appropriate error code            |
//| Layout: [type+flags:1][remlen:1-4][topname:2+N][pktid:0-2]       |
//|         [propslen:1-4][props…][payload…]                         |
//| Note: Parses all properties, resolves topic aliases, validates   |
//|       with flow control, and tracks QoS 2 in session database.   |
//+------------------------------------------------------------------+
int CPublish::Read(uchar &pkt[], CTopicAliasManager *alias_mgr) {
  uint pkt_size           = (uint)ArraySize(pkt);

  //--- Reset all accumulated parsed-property state before parsing.
  //--- Without this, reused CPublish instances accumulate subscription IDs,
  //--- user properties, and stale flag values across multiple Read() calls.
  m_parsed_topic_name     = "";
  m_parsed_payload_format = 0;
  m_has_payload_format    = false;
  m_parsed_msg_expiry     = 0;
  m_has_msg_expiry        = false;
  m_parsed_content_type   = "";
  m_parsed_response_topic = "";
  ArrayFree(m_parsed_correlation_data);
  m_parsed_topic_alias = 0;
  m_has_topic_alias    = false;
  ArrayFree(m_parsed_props_raw);
  ArrayFree(m_parsed_sub_ids);
  m_parsed_sub_id_count = 0;
  ArrayFree(m_parsed_user_prop_keys);
  ArrayFree(m_parsed_user_prop_vals);
  m_parsed_user_prop_count = 0;
  ArrayFree(m_parsed_payload);

  //--- Bounds check: need at least type + 1 remlen byte = 2 bytes
  if (pkt_size < 2) {
    MQTT_LOG_ERROR("PUBLISH packet too short");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  //--- Extract flags from fixed header byte per §3.3.1
  uchar header_byte = pkt[0];
  m_parsed_dup      = (header_byte & 0x08) != 0;  // Bit 3: DUP
  m_parsed_qos      = (header_byte >> 1) & 0x03;  // Bits 2-1: QoS
  m_parsed_retain   = (header_byte & 0x01) != 0;  // Bit 0: RETAIN

  //--- Validate QoS per §3.3.1.2
  if (m_parsed_qos > 2) {
    MQTT_LOG_ERROR("Invalid PUBLISH QoS value: " + (string)(int)m_parsed_qos);
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  //--- DUP must be 0 for QoS 0 per §3.3.1.1 - This is a protocol violation
  if (m_parsed_qos == 0 && m_parsed_dup) {
    MQTT_LOG_ERROR("DUP flag must be 0 for QoS 0 per MQTT-3.3.1-1");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  //--- Decode remaining length
  uint idx    = 1;
  uint remlen = DecodeVariableByteInteger(pkt, idx);
  if (remlen == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Remaining Length in PUBLISH");
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (remlen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid PUBLISH Remaining Length: %d", remlen));
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  uint remlen_bytes     = GetVarintBytes(remlen);
  uint var_header_start = 1 + remlen_bytes;

  //--- Bounds check: ensure buffer has all data declared by remlen
  if (pkt_size < var_header_start + remlen) {
    MQTT_LOG_ERROR("PUBLISH packet truncated");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  //--- Read Topic Name (UTF-8 Encoded String) per §3.3.2.1
  idx                       = var_header_start;
  ENUM_MQTT_ERROR topic_err = TryReadUtf8String(pkt, idx, m_parsed_topic_name);
  if (topic_err != MQTT_OK) {
    MQTT_LOG_ERROR("PUBLISH topic name malformed or truncated");
    return topic_err;
  }

  //--- Read Packet Identifier (2 bytes) if QoS > 0 per §3.3.2.2
  m_pktid = 0;
  if (m_parsed_qos > 0) {
    if (idx + 1 >= pkt_size) {
      MQTT_LOG_ERROR("PUBLISH packet truncated at Packet Identifier");
      return MQTT_ERROR_BUFFER_OVERFLOW;
    }
    m_pktid  = (ushort)((pkt[idx] << 8) | pkt[idx + 1]);
    idx     += 2;

    if (m_pktid == 0) {
      MQTT_LOG_ERROR("Packet Identifier 0 is not valid for QoS " + (string)(int)m_parsed_qos + " PUBLISH per §2.2.1");
      return MQTT_ERROR_PROTOCOL_VIOLATION;
    }
  }

  //--- Read Properties Length per §3.3.2.3
  uint propslen = DecodeVariableByteInteger(pkt, idx);
  if (propslen == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Properties Length in PUBLISH");
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (propslen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid PUBLISH Properties Length: %d", propslen));
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  uint props_start = idx;
  uint props_end   = props_start + propslen;

  if (props_end > pkt_size) {
    MQTT_LOG_ERROR("PUBLISH properties extend past end of packet");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  ArrayResize(m_parsed_props_raw, (int)propslen);
  if (propslen > 0) {
    ArrayCopy(m_parsed_props_raw, pkt, 0, (int)props_start, (int)propslen);
  }

  //--- Parse all properties
  if (propslen > 0) {
    ENUM_MQTT_ERROR props_err = ReadAllProperties(pkt, propslen, props_start);
    if (props_err != MQTT_OK) {
      return (int)props_err;
    }
  }

  //--- Topic Alias resolution per §3.3.2.3.4
  if (alias_mgr != NULL) {
    if (StringLen(m_parsed_topic_name) > 0 && m_has_topic_alias) {
      alias_mgr.RegisterServerAlias(m_parsed_topic_name, m_parsed_topic_alias);
    }

    if (StringLen(m_parsed_topic_name) == 0) {
      if (!m_has_topic_alias) {
        MQTT_LOG_ERROR("PUBLISH with empty topic name requires Topic Alias");
        return MQTT_ERROR_PROTOCOL_VIOLATION;
      }
      m_parsed_topic_name = alias_mgr.ResolveServerAlias(m_parsed_topic_alias);
      if (StringLen(m_parsed_topic_name) == 0) {
        MQTT_LOG_ERROR("Could not resolve Topic Alias " + (string)(int)m_parsed_topic_alias);
        return MQTT_ERROR_PROTOCOL_VIOLATION;
      }
    }
  } else if (StringLen(m_parsed_topic_name) == 0 && m_has_topic_alias) {
    MQTT_LOG_WARN("Cannot resolve Topic Alias " + (string)(int)m_parsed_topic_alias + " (No Manager provided)");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  //--- Extract payload (everything after properties)
  uint payload_start = props_end;
  int  payload_len   = (int)(var_header_start + remlen) - (int)payload_start;
  if (payload_len > 0) {
    ArrayResize(m_parsed_payload, payload_len);
    ArrayCopy(m_parsed_payload, pkt, 0, payload_start, payload_len);
  } else {
    ArrayResize(m_parsed_payload, 0);
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| Read all properties from incoming PUBLISH packet                 |
//| Parameters: pkt - packet buffer                                  |
//|             props_len - properties length in bytes               |
//|             idx - starting index of properties data              |
//| Return: Number of properties read                                |
//| Note: Per §3.3.2.3 the following properties are valid:           |
//|       - Payload Format Indicator (0x01)                          |
//|       - Message Expiry Interval (0x02)                           |
//|       - Content Type (0x03)                                      |
//|       - Response Topic (0x08)                                    |
//|       - Correlation Data (0x09)                                  |
//|       - Subscription Identifier (0x0B) - may repeat              |
//|       - Topic Alias (0x23)                                       |
//|       - User Property (0x26) - may repeat                        |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR CPublish::ReadAllProperties(uchar &pkt[], uint props_len, uint idx) {
  uint props_count = 0;
  uint pkt_size    = ArraySize(pkt);
  uint props_start = idx;
  uint props_end   = props_start + props_len;
  bool seen_props[256];  // Track seen non-repeatable property IDs
  ArrayInitialize(seen_props, false);

  if (props_end < props_start || props_end > pkt_size) {
    MQTT_LOG_ERROR("PUBLISH properties exceed packet bounds");
    return MQTT_ERROR_MALFORMED_PACKET;
  }

  while (idx < props_end) {
    if (idx >= pkt_size || idx >= props_end) {
      MQTT_LOG_ERROR("PUBLISH properties read past end of packet");
      return MQTT_ERROR_MALFORMED_PACKET;
    }

    uchar prop_id = pkt[idx];
    idx++;
    uint prop_val_start = idx;

    //--- Detect duplicate non-repeatable properties per §2.2.2.2
    //--- Repeatable: User Property (0x26) and Subscription Identifier (0x0B)
    if (prop_id != MQTT_PROP_IDENTIFIER_USER_PROPERTY && prop_id != MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER) {
      if (seen_props[prop_id]) {
        MQTT_LOG_ERROR("Duplicate non-repeatable PUBLISH property 0x" + StringFormat("%02X", prop_id)
                       + " is a Protocol Error per §2.2.2.2");
        return MQTT_ERROR_PROTOCOL_VIOLATION;
      }
      seen_props[prop_id] = true;
    }

    switch (prop_id) {
      //--- Payload Format Indicator (§3.3.2.3.2) - Byte
      case MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR: {
        if (idx >= props_end) {
          MQTT_LOG_ERROR("Payload Format Indicator property truncated per §3.3.2.3.2");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        m_parsed_payload_format = pkt[idx];
        if (m_parsed_payload_format != RAW_BYTES && m_parsed_payload_format != UTF8) {
          MQTT_LOG_ERROR("Payload Format Indicator must be 0 or 1 per §3.3.2.3.2");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        m_has_payload_format = true;
        idx++;
        props_count++;
      } break;

      //--- Message Expiry Interval (§3.3.2.3.3) - Four Byte Integer
      case MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL: {
        if (idx + 4 > props_end) {
          MQTT_LOG_ERROR("Message Expiry Interval property truncated per §3.3.2.3.3");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        bool ok             = false;
        m_parsed_msg_expiry = ReadFourByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR("Message Expiry Interval property malformed per §3.3.2.3.3");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        m_has_msg_expiry = true;
        props_count++;
      } break;

      //--- Content Type (§3.3.2.3.9) - UTF-8 Encoded String
      case MQTT_PROP_IDENTIFIER_CONTENT_TYPE: {
        ENUM_MQTT_ERROR err = TryReadUtf8String(pkt, idx, m_parsed_content_type);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("Content Type property truncated or malformed per §3.3.2.3.9");
          return err;
        }
        if (idx > props_end) {
          MQTT_LOG_ERROR("Content Type property exceeded declared property length");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        props_count++;
      } break;

      //--- Response Topic (§3.3.2.3.5) - UTF-8 Encoded String
      case MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC: {
        ENUM_MQTT_ERROR err = TryReadUtf8String(pkt, idx, m_parsed_response_topic);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("Response Topic property truncated or malformed per §3.3.2.3.5");
          return err;
        }
        if (idx > props_end) {
          MQTT_LOG_ERROR("Response Topic property exceeded declared property length");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        props_count++;
      } break;

      //--- Correlation Data (§3.3.2.3.6) - Binary Data
      case MQTT_PROP_IDENTIFIER_CORRELATION_DATA: {
        ENUM_MQTT_ERROR err = TryReadBinaryData(pkt, idx, m_parsed_correlation_data);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("Correlation Data property truncated or malformed per §3.3.2.3.6");
          return err;
        }
        if (idx > props_end) {
          MQTT_LOG_ERROR("Correlation Data property exceeded declared property length");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        props_count++;
      } break;

      //--- Subscription Identifier (§3.3.2.3.8) - Variable Byte Integer
      case MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER: {
        uint  temp_idx     = idx;
        int   byte_count   = 0;
        uchar encoded_byte = 0;
        bool  complete     = false;
        while (temp_idx < props_end && byte_count < 4) {
          encoded_byte = pkt[temp_idx++];
          byte_count++;
          if ((encoded_byte & 0x80) == 0) {
            complete = true;
            break;
          }
        }
        if (!complete || (byte_count >= 4 && (encoded_byte & 0x80) != 0)) {
          MQTT_LOG_ERROR("Subscription Identifier property truncated or malformed per §3.3.2.3.8");
          return MQTT_ERROR_MALFORMED_VARINT;
        }
        uint sub_id = DecodeVariableByteInteger(pkt, idx);
        //--- Subscription Identifier value 0 is a Protocol Error per §3.3.2.3.8
        if (sub_id == 0) {
          MQTT_LOG_ERROR("Subscription Identifier value 0 is a Protocol Error per §3.3.2.3.8");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        uint new_count = m_parsed_sub_id_count + 1;
        //--- Use reserve for exponential growth (growth by 8 elements at a time to minimize reallocations)
        ArrayResize(m_parsed_sub_ids, new_count, 8);
        m_parsed_sub_ids[m_parsed_sub_id_count] = sub_id;
        m_parsed_sub_id_count                   = new_count;
        props_count++;
      } break;

      //--- Topic Alias (§3.3.2.3.4) - Two Byte Integer
      case MQTT_PROP_IDENTIFIER_TOPIC_ALIAS: {
        if (idx + 2 > props_end) {
          MQTT_LOG_ERROR("Topic Alias property truncated per §3.3.2.3.4");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        bool ok              = false;
        m_parsed_topic_alias = ReadTwoByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR("Topic Alias property malformed per §3.3.2.3.4");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        m_has_topic_alias = true;
        if (m_parsed_topic_alias == 0) {
          MQTT_LOG_ERROR("Topic Alias 0 is not valid per §3.3.2.3.4");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        props_count++;
      } break;

      //--- User Property (§3.3.2.3.7) - UTF-8 String Pair
      case MQTT_PROP_IDENTIFIER_USER_PROPERTY: {
        string          userprop_pair[2];
        ENUM_MQTT_ERROR err = TryReadUserProperty(pkt, idx, userprop_pair);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("User Property truncated or malformed per §3.3.2.3.7");
          return err;
        }
        if (idx > props_end) {
          MQTT_LOG_ERROR("User Property exceeded declared property length");
          return MQTT_ERROR_MALFORMED_PACKET;
        }
        uint new_count = m_parsed_user_prop_count + 1;
        //--- Use reserve for exponential growth
        ArrayResize(m_parsed_user_prop_keys, new_count, 8);
        ArrayResize(m_parsed_user_prop_vals, new_count, 8);
        m_parsed_user_prop_keys[m_parsed_user_prop_count] = userprop_pair[0];
        m_parsed_user_prop_vals[m_parsed_user_prop_count] = userprop_pair[1];
        m_parsed_user_prop_count                          = new_count;
        props_count++;
      } break;

      default:
        //--- Unknown/unexpected property in PUBLISH per §3.3.2.3
        MQTT_LOG_ERROR("Unknown property 0x" + StringFormat("%02X", prop_id) + " in PUBLISH at index "
                       + (string)(idx - 1));
        return MQTT_ERROR_PROTOCOL_VIOLATION;
    }

    if (idx < prop_val_start || idx > props_end) {
      MQTT_LOG_ERROR("PUBLISH property parser consumed an invalid number of bytes");
      return MQTT_ERROR_MALFORMED_PACKET;
    }
  }

  if (idx != props_end) {
    MQTT_LOG_ERROR("PUBLISH properties did not consume the declared property length exactly");
    return MQTT_ERROR_MALFORMED_PACKET;
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| GetPayloadString                                                 |
//| Purpose: Get payload as string                                   |
//| Return: Payload as UTF-8 string                                  |
//+------------------------------------------------------------------+
string CPublish::GetPayloadString() const {
  int len = ArraySize(m_parsed_payload);
  if (len <= 0) {
    return "";
  }
  return CharArrayToString(m_parsed_payload, 0, len, CP_UTF8);
}

//+------------------------------------------------------------------+
//| GetPayloadBytes                                                  |
//| Purpose: Get payload as byte array                               |
//| Parameters: dest - [OUT] output buffer for payload               |
//+------------------------------------------------------------------+
void CPublish::GetPayloadBytes(uchar &dest[]) const {
  ArrayResize(dest, ArraySize(m_parsed_payload));
  if (ArraySize(m_parsed_payload) > 0) {
    ArrayCopy(dest, m_parsed_payload);
  }
}

//+------------------------------------------------------------------+
//| GetCorrelationData                                               |
//| Purpose: Get correlation data                                    |
//| Parameters: dest - [OUT] output buffer for correlation data      |
//+------------------------------------------------------------------+
void CPublish::GetCorrelationData(uchar &dest[]) const {
  ArrayResize(dest, ArraySize(m_parsed_correlation_data));
  if (ArraySize(m_parsed_correlation_data) > 0) {
    ArrayCopy(dest, m_parsed_correlation_data);
  }
}

//+------------------------------------------------------------------+
//| GetParsedPropertiesRaw                                           |
//+------------------------------------------------------------------+
void CPublish::GetParsedPropertiesRaw(uchar &dest[]) const {
  ArrayResize(dest, ArraySize(m_parsed_props_raw));
  if (ArraySize(m_parsed_props_raw) > 0) {
    ArrayCopy(dest, m_parsed_props_raw);
  }
}

//+------------------------------------------------------------------+
//| GetSubscriptionId                                                |
//| Purpose: Get subscription identifier by index                    |
//| Parameters: index - [IN] zero-based index                        |
//| Return: Subscription ID, or 0 if index out of bounds             |
//+------------------------------------------------------------------+
uint CPublish::GetSubscriptionId(uint index) const {
  if (index >= m_parsed_sub_id_count) {
    return 0;
  }
  return m_parsed_sub_ids[index];
}

//+------------------------------------------------------------------+
//| GetUserPropertyKey                                               |
//| Purpose: Get user property key by index                          |
//| Parameters: index - [IN] zero-based index                        |
//| Return: Key string, or "" if index out of bounds                 |
//+------------------------------------------------------------------+
string CPublish::GetUserPropertyKey(uint index) const {
  if (index >= m_parsed_user_prop_count) {
    return "";
  }
  return m_parsed_user_prop_keys[index];
}

//+------------------------------------------------------------------+
//| GetUserPropertyValue                                             |
//| Purpose: Get user property value by index                        |
//| Parameters: index - [IN] zero-based index                        |
//| Return: Value string, or "" if index out of bounds               |
//+------------------------------------------------------------------+
string CPublish::GetUserPropertyValue(uint index) const {
  if (index >= m_parsed_user_prop_count) {
    return "";
  }
  return m_parsed_user_prop_vals[index];
}

#endif  // MQTT_INTERNAL_PROTOCOL_PUBLISH_MQH
