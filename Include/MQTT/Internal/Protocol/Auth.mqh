//+------------------------------------------------------------------+
//|                                                         Auth.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 AUTH packet implementation per spec §3.15.              |
//| Used for extended authentication exchange with the broker.       |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_AUTH_MQH
#define MQTT_INTERNAL_PROTOCOL_AUTH_MQH

#include "..\\..\\MQTT.mqh"
#include "..\\Util\\PropertyReader.mqh"

//+------------------------------------------------------------------+
//| Class CAuth                                                      |
//| Purpose: Class for building MQTT AUTH packets                    |
//| Usage:   Used for extended authentication (MQTT v5.0)            |
//+------------------------------------------------------------------+
class CAuth {
 private:
  //--- Packet length tracking
  uint            m_remlen;          // Remaining length
  uint            m_remlen_bytes;    // Bytes for remaining length
  uint            m_propslen;        // Properties length
  uint            m_propslen_bytes;  // Bytes for properties length

  //--- Reason code (default: 0x00 - Success)
  uchar           m_reason_code;

  //--- Authentication method (required for AUTH packets)
  bool            m_has_auth_method;

  //--- Properties buffer
  uchar           m_properties[];

  //--- Internal buffer for authentication method
  uchar           m_auth_method_buf[];

  //--- Internal buffer for authentication data
  uchar           m_auth_data_buf[];

  //--- Internal buffer for AUTH Reason String
  uchar           m_reason_string_buf[];

  //--- Scratch buffers reused across Build() calls for hot-path varint encoding.
  uchar           m_propslen_buf[];
  uchar           m_remlen_buf[];

  //--- Parsed properties (populated by Read())
  string          m_parsed_reason_string;     // Reason String
  string          m_parsed_auth_method;       // Authentication Method
  uchar           m_parsed_auth_data[];       // Authentication Data
  string          m_parsed_user_prop_keys[];  // User Property keys
  string          m_parsed_user_prop_vals[];  // User Property values
  uint            m_parsed_user_prop_count;   // Number of user properties

  //--- Read properties from incoming AUTH packet
  ENUM_MQTT_ERROR ReadProperties(uchar &pkt[], uint props_len, uint idx);

 public:
  //--- Constructor declarations
  CAuth();
  ~CAuth();

  //--- Packet validation
  static bool IsAuth(uchar &inpkt[]);  // Check if packet is AUTH

  //--- Build the final AUTH packet
  void        Build(uchar &pkt[]);

  //--- Read incoming AUTH packet (full parsing per §3.15)
  int         Read(uchar &pkt[]);

  //--- Set reason code (0x00=Success, 0x18=Continue, 0x19=Re-authenticate)
  void        SetReasonCode(uchar reason_code);

  //--- Set authentication method (REQUIRED for AUTH packets)
  void        SetAuthMethod(const string auth_method);

  //--- Set authentication data (optional)
  void        SetAuthData(const uchar &auth_data[]);

  //--- Set AUTH Reason String (optional)
  void        SetReasonString(const string reason_string);

  //--- Set user property (appends to existing)
  void        SetUserProperty(const string key, const string val);

  //--- Getters for parsed state (populated by Read())
  uchar       GetReasonCode() const { return m_reason_code; }
  string      GetReasonString() const { return m_parsed_reason_string; }
  string      GetAuthMethod() const { return m_parsed_auth_method; }
  void        GetAuthData(uchar &dest[]) const;
  uint        GetUserPropertyCount() const { return m_parsed_user_prop_count; }
  string      GetUserPropertyKey(uint index) const;
  string      GetUserPropertyValue(uint index) const;
};

//+------------------------------------------------------------------+
//| Set reason code with validation per §3.15.2.1                    |
//| Parameters: reason_code - AUTH reason code                       |
//| Valid: 0x00 (Success), 0x18 (Continue Auth), 0x19 (Re-auth)      |
//+------------------------------------------------------------------+
void CAuth::SetReasonCode(uchar reason_code) {
  if (reason_code != MQTT_REASON_CODE_SUCCESS && reason_code != MQTT_REASON_CODE_CONTINUE_AUTHENTICATION
      && reason_code != MQTT_REASON_CODE_RE_AUTHENTICATE) {
    MQTT_LOG_ERROR("Invalid AUTH reason code 0x" + StringFormat("%02X", reason_code)
                   + " per MQTT §3.15.2.1. Valid codes: 0x00, 0x18, 0x19");
    return;
  }
  m_reason_code = reason_code;
}

//+------------------------------------------------------------------+
//| Set authentication method (REQUIRED)                             |
//| Parameters: auth_method - authentication method name             |
//+------------------------------------------------------------------+
void CAuth::SetAuthMethod(const string auth_method) {
  //--- Encode authentication method as UTF-8 string
  if (!EncodeUTF8String(auth_method, m_auth_method_buf)) {
    m_has_auth_method = false;
    return;
  }
  m_has_auth_method = true;
}

//+------------------------------------------------------------------+
//| Set authentication data (optional)                               |
//| Parameters: auth_data - binary authentication data               |
//+------------------------------------------------------------------+
void CAuth::SetAuthData(const uchar &auth_data[]) {
  uint datalen = ArraySize(auth_data);

  //--- Binary Data length field is two bytes, so the maximum value is 65535 per §1.5.6
  if (datalen > 65535) {
    MQTT_LOG_ERROR("Authentication Data exceeds maximum binary data length of 65535 bytes per §1.5.6");
    return;
  }

  //--- Buffer: [len_MSB][len_LSB][raw_data] per §1.5.6 Binary Data
  //--- Note: Property identifier is added separately in Build()
  ArrayResize(m_auth_data_buf, datalen + 2);
  //--- Two Byte Integer length prefix (MSB, LSB)
  m_auth_data_buf[0] = (uchar)((datalen >> 8) & 0xFF);
  m_auth_data_buf[1] = (uchar)(datalen & 0xFF);
  //--- Copy raw binary data after length prefix
  ArrayCopy(m_auth_data_buf, auth_data, 2);
}

//+------------------------------------------------------------------+
//| Set AUTH Reason String (optional)                                |
//| Parameters: reason_string - human-readable diagnostic text       |
//+------------------------------------------------------------------+
void CAuth::SetReasonString(const string reason_string) {
  if (!EncodeUTF8String(reason_string, m_reason_string_buf)) {
    ArrayResize(m_reason_string_buf, 0);
  }
}

//+------------------------------------------------------------------+
//| Set user property                                                |
//| Parameters: key - property name                                  |
//|             val - property value                                 |
//+------------------------------------------------------------------+
void CAuth::SetUserProperty(const string key, const string val) {
  //--- Delegate to shared helper
  AppendUserProperty(m_properties, key, val);

  //--- Update properties length
  m_propslen = ArraySize(m_properties);
}

//+------------------------------------------------------------------+
//| Build the final AUTH packet                                      |
//| Parameters: pkt - output packet buffer                           |
//+------------------------------------------------------------------+
void CAuth::Build(uchar &pkt[]) {
  /*
  Per MQTT 5.0 spec §3.15.2:
  The AUTH packet contains:
  - Fixed Header: Packet type (15) << 4 | Reserved (0) (§3.15.1)
  - Variable Header:
    - Reason Code (1 byte) (§3.15.2.1)
    - Properties Length (Variable Byte Integer) (§3.15.2.2)
    - Properties:
      - Authentication Method (required, identifier 0x15) (§3.15.2.2.2)
      - Authentication Data (optional, identifier 0x16) (§3.15.2.2.3)
      - User Property (optional, multiple allowed, identifier 0x26) (§3.15.2.2.4)
  - Payload: None (§3.15.3)
  */

  //--- Must have authentication method
  if (!m_has_auth_method) {
    MQTT_LOG_ERROR("AUTH packet requires Authentication Method property");
    ArrayResize(pkt, 0);
    return;
  }

  //--- Calculate properties length
  m_propslen = 0;

  if (ArraySize(m_reason_string_buf) > 0) {
    m_propslen += 1 + ArraySize(m_reason_string_buf);
  }

  //--- Authentication Method property: identifier (1) + UTF-8 string (2 + len)
  m_propslen += 1 + ArraySize(m_auth_method_buf);

  //--- Authentication Data property if present (1 byte identifier + data)
  if (ArraySize(m_auth_data_buf) > 0) {
    m_propslen += 1 + ArraySize(m_auth_data_buf);
  }

  //--- Add user properties
  m_propslen       += ArraySize(m_properties);

  //--- Calculate properties length bytes
  m_propslen_bytes  = GetVarintBytes(m_propslen);

  //--- Remaining length = reason code (1) + property length bytes + properties
  m_remlen          = 1 + m_propslen_bytes + m_propslen;
  m_remlen_bytes    = GetVarintBytes(m_remlen);

  //--- Resize packet: packet type (1) + remaining length bytes + remaining length
  ArrayResize(pkt, 1 + m_remlen_bytes + m_remlen);

  //--- Set packet type (AUTH=15) << 4 | reserved=0
  pkt[0] = AUTH << 4;

  //--- Set remaining length
  EncodeVariableByteInteger(m_remlen, m_remlen_buf);
  ArrayCopy(pkt, m_remlen_buf, 1);

  //--- Set reason code
  pkt[1 + m_remlen_bytes] = m_reason_code;

  //--- Set property length
  EncodeVariableByteInteger(m_propslen, m_propslen_buf);
  ArrayCopy(pkt, m_propslen_buf, 1 + m_remlen_bytes + 1);

  //--- Current position for copying properties
  uint idx = 1 + m_remlen_bytes + 1 + m_propslen_bytes;

  if (ArraySize(m_reason_string_buf) > 0) {
    pkt[idx++] = MQTT_PROP_IDENTIFIER_REASON_STRING;
    ArrayCopy(pkt, m_reason_string_buf, idx);
    idx += ArraySize(m_reason_string_buf);
  }

  //--- Copy Authentication Method property
  pkt[idx++] = MQTT_PROP_IDENTIFIER_AUTHENTICATION_METHOD;
  ArrayCopy(pkt, m_auth_method_buf, idx);
  idx += ArraySize(m_auth_method_buf);

  //--- Copy Authentication Data property if present
  if (ArraySize(m_auth_data_buf) > 0) {
    pkt[idx++] = MQTT_PROP_IDENTIFIER_AUTHENTICATION_DATA;
    ArrayCopy(pkt, m_auth_data_buf, idx);
    idx += ArraySize(m_auth_data_buf);
  }

  //--- Copy user properties
  if (ArraySize(m_properties) > 0) {
    ArrayCopy(pkt, m_properties, idx);
  }
}

//+------------------------------------------------------------------+
//| Check if packet is an AUTH                                       |
//| Parameters: inpkt - input packet buffer                          |
//| Return: true if packet is AUTH, false otherwise                  |
//+------------------------------------------------------------------+
static bool CAuth::IsAuth(uchar &inpkt[]) {
  if (ArraySize(inpkt) < 1) {
    return false;
  }
  return inpkt[0] == (AUTH << 4);
}

//+------------------------------------------------------------------+
//| Constructor - initializes remaining length to 0                  |
//+------------------------------------------------------------------+
CAuth::CAuth() {
  m_remlen                 = 0;
  m_remlen_bytes           = 1;
  m_propslen               = 0;
  m_propslen_bytes         = 1;
  m_reason_code            = MQTT_REASON_CODE_SUCCESS;
  m_has_auth_method        = false;
  m_parsed_reason_string   = "";
  m_parsed_auth_method     = "";
  m_parsed_user_prop_count = 0;
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CAuth::~CAuth() {
  //--- Securely zero credential buffers before deallocation
  SecureZeroArray(m_auth_data_buf);
  SecureZeroArray(m_auth_method_buf);
  SecureZeroArray(m_reason_string_buf);
  SecureZeroArray(m_parsed_auth_data);
}

//+------------------------------------------------------------------+
//| Read - Parse an incoming AUTH packet                             |
//| Purpose: Extract authentication state and properties per §3.15   |
//| Parameters: pkt - [IN] the raw packet bytes                      |
//| Return: MQTT_OK (0) on success, or an error code                 |
//| Layout: [type:1][remlen:1-4][reason:0-1][propslen:0-4][props…]   |
//| Note: Per §3.15.2, if remaining length is 0, reason code is      |
//|       implicitly 0x00 (Success) with no properties.              |
//|       If remaining length is 1, reason code is present but no    |
//|       properties. Otherwise, reason code + properties follow.    |
//+------------------------------------------------------------------+
int CAuth::Read(uchar &pkt[]) {
  m_reason_code            = MQTT_REASON_CODE_SUCCESS;
  m_parsed_reason_string   = "";
  m_parsed_auth_method     = "";
  m_parsed_user_prop_count = 0;
  ArrayFree(m_parsed_auth_data);
  ArrayFree(m_parsed_user_prop_keys);
  ArrayFree(m_parsed_user_prop_vals);

  //--- Bounds check: need at least type + 1 remlen byte = 2 bytes
  if (ArraySize(pkt) < 2) {
    MQTT_LOG_ERROR("AUTH packet too short");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  //--- Validate packet type: must be AUTH (0xF0) per §3.15.1
  if (pkt[0] != (AUTH << 4)) {
    MQTT_LOG_ERROR("Expected AUTH packet (0xF0), got 0x" + StringFormat("%02X", pkt[0]));
    return MQTT_ERROR_INVALID_PACKET_TYPE;
  }

  uint idx    = 1;
  uint remlen = DecodeVariableByteInteger(pkt, idx);

  //--- Validate Remaining Length
  if (remlen == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Remaining Length in AUTH");
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (remlen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid AUTH Remaining Length: %d", remlen));
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  uint remlen_bytes     = GetVarintBytes(remlen);
  uint var_header_start = 1 + remlen_bytes;

  //--- Bounds check: ensure buffer has all data declared by remlen
  if (ArraySize(pkt) < (int)(var_header_start + remlen)) {
    MQTT_LOG_ERROR("AUTH packet truncated");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  //--- Per §3.15.2: If Remaining Length is 0, Reason Code is implicitly 0x00
  if (remlen == 0) {
    m_reason_code = MQTT_REASON_CODE_SUCCESS;
    return MQTT_OK;
  }

  //--- Read Reason Code (always at var_header_start)
  m_reason_code = pkt[var_header_start];

  //--- Validate reason code
  if (m_reason_code != MQTT_REASON_CODE_SUCCESS && m_reason_code != MQTT_REASON_CODE_CONTINUE_AUTHENTICATION
      && m_reason_code != MQTT_REASON_CODE_RE_AUTHENTICATE) {
    MQTT_LOG_ERROR("Invalid AUTH reason code 0x" + StringFormat("%02X", m_reason_code)
                   + " received from broker per MQTT §3.15.2.1");
    return MQTT_ERROR_INVALID_REASON_CODE;
  }

  //--- Per §3.15.2: If Remaining Length is 1, no Properties present
  if (remlen == 1) {
    return MQTT_OK;
  }

  //--- Remaining Length >= 2: Properties present
  uint prop_idx = var_header_start + 1;
  uint propslen = ReadPropertyLength(pkt, prop_idx);

  if (propslen == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Properties Length in AUTH");
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (propslen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid AUTH Properties Length: %d", propslen));
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  uint propslen_bytes = GetVarintBytes(propslen);
  uint props_start    = var_header_start + 1 + propslen_bytes;
  uint packet_end     = var_header_start + remlen;

  if (props_start > packet_end) {
    MQTT_LOG_ERROR("AUTH properties length exceeds remaining length boundary");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  uint props_end = props_start + propslen;
  if (props_end > packet_end) {
    MQTT_LOG_ERROR("AUTH properties length exceeds remaining length boundary");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }
  if (props_end != packet_end) {
    MQTT_LOG_ERROR("AUTH properties length does not exactly consume the remaining length");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  if (propslen > 0) {
    ENUM_MQTT_ERROR props_err = ReadProperties(pkt, propslen, props_start);
    if (props_err != MQTT_OK) {
      return props_err;
    }
  }

  if (ArraySize(m_parsed_auth_data) > 0 && StringLen(m_parsed_auth_method) == 0) {
    MQTT_LOG_ERROR("AUTH Authentication Data requires Authentication Method per MQTT §3.15.2.2");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| Read properties from AUTH packet                                 |
//| Delegates to shared CPropertyReader for §3.15.2.2 properties.    |
//| Parameters: pkt - packet buffer                                  |
//|             props_len - properties length in bytes               |
//|             idx - starting index of properties data              |
//| Return: Number of properties read                                |
//| Note: Per §3.15.2.2 the following properties are valid:          |
//|       - Authentication Method (0x15) - REQUIRED                  |
//|       - Authentication Data (0x16)                               |
//|       - User Property (0x26)                                     |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR CAuth::ReadProperties(uchar &pkt[], uint props_len, uint idx) {
  CPropertyReader reader;
  uint allowed = PROP_ALLOW_REASON_STRING | PROP_ALLOW_AUTH_METHOD | PROP_ALLOW_AUTH_DATA | PROP_ALLOW_USER_PROPERTY;
  reader.ReadProperties(pkt, props_len, idx, allowed, "AUTH");
  if (reader.HasError()) {
    return reader.GetErrorCode();
  }

  //--- Copy parsed results into member variables
  if (reader.HasReasonString()) {
    m_parsed_reason_string = reader.GetReasonString();
  }
  if (reader.HasAuthMethod()) {
    m_parsed_auth_method = reader.GetAuthMethod();
  }
  if (reader.HasAuthData()) {
    reader.GetAuthData(m_parsed_auth_data);
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
//| Get parsed authentication data                                   |
//| Parameters: dest - output buffer for auth data                   |
//+------------------------------------------------------------------+
void CAuth::GetAuthData(uchar &dest[]) const {
  ArrayResize(dest, ArraySize(m_parsed_auth_data));
  if (ArraySize(m_parsed_auth_data) > 0) {
    ArrayCopy(dest, m_parsed_auth_data);
  }
}

//+------------------------------------------------------------------+
//| Get user property key by index                                   |
//| Parameters: index - zero-based index                             |
//| Return: Key string, or "" if index out of bounds                 |
//+------------------------------------------------------------------+
string CAuth::GetUserPropertyKey(uint index) const {
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
string CAuth::GetUserPropertyValue(uint index) const {
  if (index >= m_parsed_user_prop_count) {
    return "";
  }
  return m_parsed_user_prop_vals[index];
}

#endif  // MQTT_AUTH_MQH
