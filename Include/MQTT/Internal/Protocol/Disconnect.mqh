//+------------------------------------------------------------------+
//|                                                   Disconnect.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 DISCONNECT packet implementation per spec §3.14.        |
//| Used to gracefully disconnect from the MQTT broker.              |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_DISCONNECT_MQH
#define MQTT_INTERNAL_PROTOCOL_DISCONNECT_MQH

#include "..\\..\\MQTT.mqh"
#include "..\\Util\\PropertyReader.mqh"

//+------------------------------------------------------------------+
//| Class CDisconnect                                                |
//| Purpose: Class for building MQTT DISCONNECT packets              |
//| Usage:   Used to gracefully disconnect from broker               |
//+------------------------------------------------------------------+
class CDisconnect {
 private:
  //--- Packet length tracking
  uint            m_remlen;          // Remaining length
  uint            m_remlen_bytes;    // Bytes needed for remaining length
  uint            m_propslen;        // Properties length
  uint            m_propslen_bytes;  // Bytes needed for properties length

  //--- Reason code (default: 0x00 - Normal disconnection)
  uchar           m_reason_code;

  //--- Properties buffer
  uchar           m_properties[];

  //--- Track if we should include reason code and properties
  bool            m_include_reason_and_props;

  //--- Parsed properties (populated by Read())
  string          m_parsed_reason_string;     // Reason String
  string          m_parsed_server_reference;  // Server Reference
  uint            m_parsed_session_expiry;    // Session Expiry Interval
  bool            m_has_session_expiry;       // Whether session expiry was parsed
  string          m_parsed_user_prop_keys[];  // User Property keys
  string          m_parsed_user_prop_vals[];  // User Property values
  uint            m_parsed_user_prop_count;   // Number of user properties

  //--- Read properties from incoming DISCONNECT packet
  ENUM_MQTT_ERROR ReadProperties(uchar &pkt[], uint props_len, uint idx);
  void            RemoveProperty(uchar prop_id);

 public:
  //--- Constructor declarations
  CDisconnect();
  ~CDisconnect();

  //--- Packet validation
  static bool IsDisconnect(uchar &inpkt[]);  // Check if packet is DISCONNECT

  //--- Build the final DISCONNECT packet
  void        Build(uchar &pkt[]);

  //--- Read incoming DISCONNECT packet (full parsing per §3.14)
  int         Read(uchar &pkt[]);

  //--- Set reason code (0x00 = Normal disconnection)
  void        SetReasonCode(uchar reason_code);

  //--- Read disconnect reason code
  uchar       ReadDisconnReasonCode(uchar &inpkt[]);

  //--- Set reason string property
  void        SetReasonString(const string reason);

  //--- Set user property (appends to existing)
  void        SetUserProperty(const string key, const string val);

  //--- Set server reference property
  void        SetServerReference(const string server_ref);

  //--- Set Session Expiry Interval property per §3.14.2.2.2
  //--- Allows modifying the session lifetime established in CONNECT.
  //--- Set to 0 to convert a persistent session to clean on disconnect.
  void        SetSessionExpiryInterval(uint seconds);

  //--- Read server reference
  string      ReadServerReference(uchar &inpkt[], uint &idx);

  //--- Getters for parsed state (populated by Read())
  uchar       GetReasonCode() const { return m_reason_code; }
  string      GetReasonString() const { return m_parsed_reason_string; }
  string      GetServerReference() const { return m_parsed_server_reference; }
  bool        HasSessionExpiry() const { return m_has_session_expiry; }
  uint        GetSessionExpiryInterval() const { return m_parsed_session_expiry; }
  uint        GetUserPropertyCount() const { return m_parsed_user_prop_count; }
  string      GetUserPropertyKey(uint index) const;
  string      GetUserPropertyValue(uint index) const;
};

//+------------------------------------------------------------------+
//| Read server reference from DISCONNECT                            |
//| Parameters: inpkt - input packet buffer                          |
//|             idx - starting index (updated to reflect bytes read) |
//| Return: Server reference as string                               |
//+------------------------------------------------------------------+
string CDisconnect::ReadServerReference(uchar &inpkt[], uint &idx) { return ReadUtf8String(inpkt, idx); }

//+------------------------------------------------------------------+
//| Set reason string property                                       |
//| Parameters: reason - human-readable reason string                |
//+------------------------------------------------------------------+
void   CDisconnect::SetReasonString(const string reason) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_REASON_STRING);

  //--- Delegate to shared helper
  AppendReasonString(m_properties, reason);

  //--- Update properties length and flag
  m_propslen                 = ArraySize(m_properties);
  m_include_reason_and_props = true;
}

//+------------------------------------------------------------------+
//| Set user property                                                |
//| Parameters: key - property name                                  |
//|             val - property value                                 |
//+------------------------------------------------------------------+
void CDisconnect::SetUserProperty(const string key, const string val) {
  //--- Delegate to shared helper
  AppendUserProperty(m_properties, key, val);

  //--- Update properties length and flag
  m_propslen                 = ArraySize(m_properties);
  m_include_reason_and_props = true;
}

//+------------------------------------------------------------------+
//| Set Session Expiry Interval property per §3.14.2.2.2             |
//| Parameters: seconds - session expiry interval in seconds         |
//| Note: Per §3.14.2.2.2, if the Session Expiry Interval in the     |
//|       DISCONNECT packet is absent, the Session Expiry Interval   |
//|       from CONNECT is used. Set to 0 to request immediate        |
//|       session deletion on disconnect.                            |
//+------------------------------------------------------------------+
void CDisconnect::SetSessionExpiryInterval(uint seconds) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_SESSION_EXPIRY_INTERVAL);

  //--- Encode Session Expiry Interval property (ID 0x11, Four Byte Integer)
  CPropertyEncoder::EncodeFourByteIntegerProperty(m_properties, MQTT_PROP_IDENTIFIER_SESSION_EXPIRY_INTERVAL, seconds);

  //--- Update properties length and flag
  m_propslen                 = ArraySize(m_properties);
  m_include_reason_and_props = true;
}

//+------------------------------------------------------------------+
//| Set server reference property                                    |
//| Parameters: server_ref - server reference string                 |
//+------------------------------------------------------------------+
void CDisconnect::SetServerReference(const string server_ref) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_SERVER_REFERENCE);

  //--- Delegate to shared helper
  AppendServerReference(m_properties, server_ref);

  //--- Update properties length and flag
  m_propslen                 = ArraySize(m_properties);
  m_include_reason_and_props = true;
}

//+------------------------------------------------------------------+
//| RemoveProperty                                                   |
//| Purpose: Remove a non-repeatable property before re-encoding     |
//+------------------------------------------------------------------+
void CDisconnect::RemoveProperty(uchar prop_id) {
  uint idx      = 0;
  uint buf_size = (uint)ArraySize(m_properties);

  while (idx < buf_size) {
    uint  start     = idx;
    uchar cur_id    = m_properties[idx++];
    uint  value_len = 0;
    if (!CPropertyEncoder::GetPropertyValueLength(cur_id, m_properties, idx, value_len)) {
      return;
    }
    uint total_len = 1 + value_len;

    if (cur_id == prop_id) {
      uint remaining = buf_size - (start + total_len);
      if (remaining > 0) {
        ArrayCopy(m_properties, m_properties, start, start + total_len, remaining);
      }
      ArrayResize(m_properties, buf_size - total_len);
      return;
    }

    idx += value_len;
  }
}

//+------------------------------------------------------------------+
//| ReadDisconnReasonCode                                            |
//| Purpose: Read disconnect reason code from incoming DISCONNECT    |
//| Parameters: inpkt - input packet buffer                          |
//| Return: Disconnect reason code (0xFF on error)                   |
//+------------------------------------------------------------------+
uchar CDisconnect::ReadDisconnReasonCode(uchar &inpkt[]) {
  //--- Need at least 2 bytes (packet type + remaining length)
  if (ArraySize(inpkt) < 2) {
    MQTT_LOG_ERROR("DISCONNECT packet too short to read reason code per §3.14");
    return 0xFF;
  }

  //--- Decode remaining length (variable byte integer starting at index 1)
  uint idx     = 1;
  uint rem_len = DecodeVariableByteInteger(inpkt, idx);

  //--- If remaining length is 0, reason code is omitted → 0x00 per §3.14.2.1
  if (rem_len == 0) {
    return MQTT_REASON_CODE_NORMAL_DISCONNECTION;
  }

  //--- Bounds check: ensure reason code byte is within buffer
  if (idx >= (uint)ArraySize(inpkt)) {
    MQTT_LOG_ERROR("DISCONNECT packet truncated, cannot read reason code");
    return 0xFF;
  }

  return inpkt[idx];
}

//+------------------------------------------------------------------+
//| Set reason code                                                  |
//| Parameters: reason_code - disconnect reason code                 |
//| Valid codes per MQTT v5.0 §3.14.2.1:                             |
//|   0x00 Normal disconnection                                      |
//|   0x04 Disconnect with Will Message                              |
//|   0x80 Unspecified error                                         |
//|   0x81 Malformed Packet                                          |
//|   0x82 Protocol Error                                            |
//|   0x83 Implementation specific error                             |
//|   0x87 Not authorized                                            |
//|   0x89 Server busy                                               |
//|   0x8B Server shutting down                                      |
//|   0x8D Keep Alive timeout                                        |
//|   0x8E Session taken over                                        |
//|   0x8F Topic Filter invalid                                      |
//|   0x90 Topic Name invalid                                        |
//|   0x93 Receive Maximum exceeded                                  |
//|   0x94 Topic Alias invalid                                       |
//|   0x95 Packet too large                                          |
//|   0x96 Message rate too high                                     |
//|   0x97 Quota exceeded                                            |
//|   0x98 Administrative action                                     |
//|   0x99 Payload format invalid                                    |
//|   0x9A Retain not supported                                      |
//|   0x9B QoS not supported                                         |
//|   0x9C Use another server                                        |
//|   0x9D Server moved                                              |
//|   0x9E Shared Subscriptions not supported                        |
//|   0x9F Connection rate exceeded                                  |
//|   0xA0 Maximum connect time                                      |
//|   0xA1 Subscription Identifiers not supported                    |
//|   0xA2 Wildcard Subscriptions not supported                      |
//+------------------------------------------------------------------+
void CDisconnect::SetReasonCode(uchar reason_code) {
  //--- Validate reason code per §3.14.2.1
  if (reason_code != 0x00 && reason_code != 0x04 && reason_code != 0x80 && reason_code != 0x81 && reason_code != 0x82
      && reason_code != 0x83 && reason_code != 0x87 && reason_code != 0x89 && reason_code != 0x8B && reason_code != 0x8D
      && reason_code != 0x8E && reason_code != 0x8F && reason_code != 0x90 && reason_code != 0x93 && reason_code != 0x94
      && reason_code != 0x95 && reason_code != 0x96 && reason_code != 0x97 && reason_code != 0x98 && reason_code != 0x99
      && reason_code != 0x9A && reason_code != 0x9B && reason_code != 0x9C && reason_code != 0x9D && reason_code != 0x9E
      && reason_code != 0x9F && reason_code != 0xA0 && reason_code != 0xA1 && reason_code != 0xA2) {
    MQTT_LOG_ERROR("Invalid DISCONNECT reason code 0x" + StringFormat("%02X", reason_code) + " per MQTT §3.14.2.1");
    return;
  }

  m_reason_code = reason_code;
  //--- If reason code is non-zero, we must include it and properties
  if (reason_code != MQTT_REASON_CODE_NORMAL_DISCONNECTION) {
    m_include_reason_and_props = true;
  }
}

//+------------------------------------------------------------------+
//| Build the final DISCONNECT packet                                |
//| Parameters: pkt - output packet buffer                           |
//+------------------------------------------------------------------+
void CDisconnect::Build(uchar &pkt[]) {
  /*
  Per MQTT 5.0 spec §3.14.2:
  The Reason Code and Property Length can be omitted if the Reason
  Code is 0x00 (Normal disconnection) and there are no Properties.
  In this case the DISCONNECT has a Remaining Length of 0. (§3.14.2.1)
  */

  if (!m_include_reason_and_props && m_reason_code == MQTT_REASON_CODE_NORMAL_DISCONNECTION) {
    //--- Simple case: no reason code, no properties
    m_remlen       = 0;
    m_remlen_bytes = 1;

    ArrayResize(pkt, 2);  // Packet type + remaining length (0)
    pkt[0] = DISCONNECT << 4;
    pkt[1] = 0;           // Remaining length = 0
  } else {
    //--- Full case: include reason code and properties
    m_propslen_bytes = GetVarintBytes(m_propslen);

    //--- Remaining length = reason code (1) + property length bytes + properties
    m_remlen         = 1 + m_propslen_bytes + m_propslen;
    m_remlen_bytes   = GetVarintBytes(m_remlen);

    //--- Resize packet
    ArrayResize(pkt, 1 + m_remlen_bytes + m_remlen);

    //--- Set packet type
    pkt[0] = DISCONNECT << 4;

    //--- Set remaining length
    uchar remlen_buf[];
    EncodeVariableByteInteger(m_remlen, remlen_buf);
    ArrayCopy(pkt, remlen_buf, 1);

    //--- Set reason code
    pkt[1 + m_remlen_bytes] = m_reason_code;

    //--- Set property length
    uchar propslen_buf[];
    EncodeVariableByteInteger(m_propslen, propslen_buf);
    ArrayCopy(pkt, propslen_buf, 1 + m_remlen_bytes + 1);

    //--- Copy properties
    ArrayCopy(pkt, m_properties, 1 + m_remlen_bytes + 1 + m_propslen_bytes);
  }
}

//+------------------------------------------------------------------+
//| IsDisconnect                                                     |
//| Purpose: Check if packet is a DISCONNECT                         |
//| Parameters: inpkt - input packet buffer                          |
//| Return: true if packet is DISCONNECT, false otherwise            |
//+------------------------------------------------------------------+
static bool CDisconnect::IsDisconnect(uchar &inpkt[]) {
  if (ArraySize(inpkt) < 1) {
    return false;
  }
  return inpkt[0] == (DISCONNECT << 4);
}

//+------------------------------------------------------------------+
//| CDisconnect                                                      |
//| Purpose: Constructor - initializes remaining length to 0         |
//+------------------------------------------------------------------+
CDisconnect::CDisconnect() {
  m_remlen                   = 0;
  m_remlen_bytes             = 1;
  m_propslen                 = 0;
  m_propslen_bytes           = 1;
  m_reason_code              = MQTT_REASON_CODE_NORMAL_DISCONNECTION;
  m_include_reason_and_props = false;
  m_parsed_reason_string     = "";
  m_parsed_server_reference  = "";
  m_parsed_session_expiry    = 0;
  m_has_session_expiry       = false;
  m_parsed_user_prop_count   = 0;
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CDisconnect::~CDisconnect() {}

//+------------------------------------------------------------------+
//| Read - Parse an incoming DISCONNECT packet                       |
//| Purpose: Extract redirection or error state and props per §3.14  |
//| Parameters: pkt - [IN] the raw packet bytes                      |
//| Return: MQTT_OK (0) on success, or an error code                 |
//| Layout: [type:1][remlen:1-4][reason:0-1][propslen:0-4][props…]   |
//| Note: Per §3.14.2, if remaining length is 0, reason code is      |
//|       implicitly 0x00 (Normal disconnection) with no properties. |
//|       If remaining length is 1, reason code is present but no    |
//|       properties. Otherwise, reason code + properties follow.    |
//+------------------------------------------------------------------+
int CDisconnect::Read(uchar &pkt[]) {
  m_reason_code             = MQTT_REASON_CODE_NORMAL_DISCONNECTION;
  m_parsed_reason_string    = "";
  m_parsed_server_reference = "";
  m_parsed_session_expiry   = 0;
  m_has_session_expiry      = false;
  m_parsed_user_prop_count  = 0;
  ArrayFree(m_parsed_user_prop_keys);
  ArrayFree(m_parsed_user_prop_vals);

  //--- Bounds check: need at least type + 1 remlen byte = 2 bytes
  if (ArraySize(pkt) < 2) {
    MQTT_LOG_ERROR("DISCONNECT packet too short");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  //--- Validate packet type: must be DISCONNECT (0xE0) per §3.14.1
  if (pkt[0] != (DISCONNECT << 4)) {
    MQTT_LOG_ERROR("Expected DISCONNECT packet (0xE0), got 0x" + StringFormat("%02X", pkt[0]));
    return MQTT_ERROR_INVALID_PACKET_TYPE;
  }

  uint idx    = 1;
  uint remlen = DecodeVariableByteInteger(pkt, idx);

  //--- Validate Remaining Length
  if (remlen == UINT_MAX) {
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (remlen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid DISCONNECT Remaining Length: %d", remlen));
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  uint remlen_bytes     = GetVarintBytes(remlen);
  uint var_header_start = 1 + remlen_bytes;

  //--- Bounds check: ensure buffer has all data declared by remlen
  if (ArraySize(pkt) < (int)(var_header_start + remlen)) {
    MQTT_LOG_ERROR("DISCONNECT packet truncated");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  //--- Per §3.14.2: If Remaining Length is 0, Reason Code is implicitly 0x00
  if (remlen == 0) {
    m_reason_code = MQTT_REASON_CODE_NORMAL_DISCONNECTION;
    return MQTT_OK;
  }

  //--- Read Reason Code (always at var_header_start)
  m_reason_code = pkt[var_header_start];

  //--- Validate reason code per §3.14.2.1
  if (m_reason_code != 0x00 && m_reason_code != 0x04 && m_reason_code != 0x80 && m_reason_code != 0x81
      && m_reason_code != 0x82 && m_reason_code != 0x83 && m_reason_code != 0x87 && m_reason_code != 0x89
      && m_reason_code != 0x8B && m_reason_code != 0x8D && m_reason_code != 0x8E && m_reason_code != 0x8F
      && m_reason_code != 0x90 && m_reason_code != 0x93 && m_reason_code != 0x94 && m_reason_code != 0x95
      && m_reason_code != 0x96 && m_reason_code != 0x97 && m_reason_code != 0x98 && m_reason_code != 0x99
      && m_reason_code != 0x9A && m_reason_code != 0x9B && m_reason_code != 0x9C && m_reason_code != 0x9D
      && m_reason_code != 0x9E && m_reason_code != 0x9F && m_reason_code != 0xA0 && m_reason_code != 0xA1
      && m_reason_code != 0xA2) {
    MQTT_LOG_ERROR("Invalid DISCONNECT reason code 0x" + StringFormat("%02X", m_reason_code)
                   + " received per MQTT §3.14.2.1");
    return MQTT_ERROR_INVALID_REASON_CODE;
  }

  //--- Per §3.14.2: If Remaining Length is 1, no Properties present
  if (remlen == 1) {
    return MQTT_OK;
  }

  //--- Remaining Length >= 2: Properties present
  uint prop_idx   = var_header_start + 1;
  uint propslen   = ReadPropertyLength(pkt, prop_idx);
  uint packet_end = var_header_start + remlen;

  if (propslen == UINT_MAX) {
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (propslen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid DISCONNECT Properties Length: %d", propslen));
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  uint propslen_bytes = GetVarintBytes(propslen);
  uint props_start    = var_header_start + 1 + propslen_bytes;
  uint props_end      = props_start + propslen;

  if (props_end > packet_end) {
    MQTT_LOG_ERROR("DISCONNECT properties exceed declared Remaining Length");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }
  if (props_end != packet_end) {
    MQTT_LOG_ERROR("DISCONNECT properties do not consume the declared Remaining Length exactly");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  if (propslen > 0) {
    ENUM_MQTT_ERROR props_err = ReadProperties(pkt, propslen, props_start);
    if (props_err != MQTT_OK) {
      return props_err;
    }
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| Read properties from DISCONNECT packet                           |
//| Delegates to shared CPropertyReader for §3.14.2.2 properties.    |
//| Parameters: pkt - packet buffer                                  |
//|             props_len - properties length in bytes               |
//|             idx - starting index of properties data              |
//| Return: Number of properties read                                |
//| Note: Per §3.14.2.2 the following properties are valid:          |
//|       - Session Expiry Interval (0x11)                           |
//|       - Reason String (0x1F)                                     |
//|       - User Property (0x26)                                     |
//|       - Server Reference (0x1C)                                  |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR CDisconnect::ReadProperties(uchar &pkt[], uint props_len, uint idx) {
  CPropertyReader reader;
  uint            allowed =
    PROP_ALLOW_SESSION_EXPIRY | PROP_ALLOW_REASON_STRING | PROP_ALLOW_USER_PROPERTY | PROP_ALLOW_SERVER_REFERENCE;
  reader.ReadProperties(pkt, props_len, idx, allowed, "DISCONNECT");
  if (reader.HasError()) {
    return reader.GetErrorCode();
  }

  //--- Copy parsed results into member variables
  if (reader.HasReasonString()) {
    m_parsed_reason_string = reader.GetReasonString();
  }
  if (reader.HasServerReference()) {
    m_parsed_server_reference = reader.GetServerReference();
  }
  if (reader.HasSessionExpiry()) {
    m_parsed_session_expiry = reader.GetSessionExpiry();
    m_has_session_expiry    = true;
  }
  //--- Copy user properties
  uint up_count = reader.GetUserPropertyCount();
  if (up_count > 0) {
    ArrayResize(m_parsed_user_prop_keys, m_parsed_user_prop_count + up_count);
    ArrayResize(m_parsed_user_prop_vals, m_parsed_user_prop_count + up_count);
    for (uint i = 0; i < up_count; i++) {
      m_parsed_user_prop_keys[m_parsed_user_prop_count + i] = reader.GetUserPropertyKey(i);
      m_parsed_user_prop_vals[m_parsed_user_prop_count + i] = reader.GetUserPropertyValue(i);
    }
    m_parsed_user_prop_count += up_count;
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| Get user property key by index                                   |
//| Parameters: index - zero-based index                             |
//| Return: Key string, or "" if index out of bounds                 |
//+------------------------------------------------------------------+
string CDisconnect::GetUserPropertyKey(uint index) const {
  if (index >= m_parsed_user_prop_count) {
    return "";
  }
  return m_parsed_user_prop_keys[index];
}

//+------------------------------------------------------------------+
//| Get user property value by index                                 |
//| Parameters: index - zero-based index                             |
//| Return: Value string, or "" if index out of bounds               |
//+------------------------------------------------------------------+
string CDisconnect::GetUserPropertyValue(uint index) const {
  if (index >= m_parsed_user_prop_count) {
    return "";
  }
  return m_parsed_user_prop_vals[index];
}

#endif  // MQTT_DISCONNECT_MQH
