//+------------------------------------------------------------------+
//|                                                      ConnAck.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 CONNACK packet implementation per spec §3.2.            |
//| Used to parse connection acknowledgment from broker.             |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_CONNACK_MQH
#define MQTT_INTERNAL_PROTOCOL_CONNACK_MQH

#include "..\\..\\MQTT.mqh"

//+------------------------------------------------------------------+
//| Class CConnack                                                   |
//| Purpose: Class for parsing MQTT CONNACK packets (MQTT v5.0)      |
//| Usage:   Used to read and interpret server connection responses  |
//+------------------------------------------------------------------+
class CConnack {
 private:
  //--- Packet state (populated by Read())
  bool            m_session_present;
  uchar           m_reason_code;
  uint            m_props_len;

  //--- Parsed properties
  string          m_reason_string;
  uint            m_session_expiry;
  bool            m_has_session_expiry;
  ushort          m_receive_max;
  uchar           m_max_qos;
  bool            m_retain_available;
  uint            m_max_pkt_size;
  string          m_assigned_client_id;
  ushort          m_topic_alias_max;
  bool            m_wildcard_sub_available;
  bool            m_sub_id_available;
  bool            m_shared_sub_available;
  ushort          m_server_keep_alive;
  string          m_response_info;
  string          m_server_reference;
  string          m_auth_method;
  uchar           m_auth_data[];

  //--- User Properties (MQTT 5.0 §3.2.2.3.8) — parallel key/value arrays
  string          m_user_prop_keys[];
  string          m_user_prop_values[];

  //--- Property helpers
  static bool     IsValidReasonCode(const uchar reason_code);
  ENUM_MQTT_ERROR ReadAllProperties(uchar &pkt[], uint props_len, uint idx);

 public:
  //--- Constructor and Destructor
  CConnack();
  CConnack(uchar &inpkt[]);
  ~CConnack();

  //--- Packet validation
  static bool IsConnack(uchar &inpkt[]);  // Check if packet is CONNACK

  //--- Main parsing method
  int         Read(uchar &pkt[]);

  //--- Getters for parsed state
  bool        IsSessionPresent() const { return m_session_present; }
  uchar       GetReasonCode() const { return m_reason_code; }
  string      GetReasonString() const { return m_reason_string; }
  bool        HasSessionExpiry() const { return m_has_session_expiry; }
  uint        GetSessionExpiryInterval() const { return m_session_expiry; }
  ushort      GetReceiveMaximum() const { return m_receive_max; }
  uchar       GetMaximumQoS() const { return m_max_qos; }
  bool        IsRetainAvailable() const { return m_retain_available; }
  uint        GetMaximumPacketSize() const { return m_max_pkt_size; }
  string      GetAssignedClientIdentifier() const { return m_assigned_client_id; }
  ushort      GetTopicAliasMaximum() const { return m_topic_alias_max; }
  bool        IsWildcardSubscriptionAvailable() const { return m_wildcard_sub_available; }
  bool        IsSubscriptionIdentifierAvailable() const { return m_sub_id_available; }
  bool        IsSharedSubscriptionAvailable() const { return m_shared_sub_available; }
  ushort      GetServerKeepAlive() const { return m_server_keep_alive; }
  string      GetResponseInformation() const { return m_response_info; }
  string      GetServerReference() const { return m_server_reference; }
  string      GetAuthenticationMethod() const { return m_auth_method; }
  void        GetAuthenticationData(uchar &dest[]) const;
  string      GetAuthenticationDataString() const;

  //--- User properties (may contain broker config data like rate limits, feature flags)
  uint        GetUserPropertyCount() const { return ArraySize(m_user_prop_keys); }
  string      GetUserPropertyKey(const uint idx) const {
    return (idx < (uint)ArraySize(m_user_prop_keys)) ? m_user_prop_keys[idx] : "";
  }
  string GetUserPropertyValue(const uint idx) const {
    return (idx < (uint)ArraySize(m_user_prop_values)) ? m_user_prop_values[idx] : "";
  }

  //--- Property Length (for testing)
  uint GetPropertiesLength() const { return m_props_len; }
};

//+------------------------------------------------------------------+
//| Read - Parse an incoming CONNACK packet                          |
//| Purpose: Extract session state, reason code, and all properties  |
//| Parameters: pkt - [IN] the raw packet bytes                      |
//| Return: MQTT_OK (0) on success, or an error code                 |
//| Note: Implements the parsing logic defined in MQTT 5.0 §3.2      |
//+------------------------------------------------------------------+
int CConnack::Read(uchar &pkt[]) {
  m_session_present        = false;
  m_reason_code            = 0x00;
  m_props_len              = 0;
  m_reason_string          = "";
  m_session_expiry         = 0;
  m_has_session_expiry     = false;
  m_receive_max            = 65535;
  m_max_qos                = 2;
  m_retain_available       = true;
  m_max_pkt_size           = 0;
  m_assigned_client_id     = "";
  m_topic_alias_max        = 0;
  m_wildcard_sub_available = true;
  m_sub_id_available       = true;
  m_shared_sub_available   = true;
  m_server_keep_alive      = 0;
  m_response_info          = "";
  m_server_reference       = "";
  m_auth_method            = "";
  ArrayFree(m_auth_data);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_values);

  uint pkt_size = ArraySize(pkt);
  //--- Bounds check: minimum size (Type + RemLen + Flags + Reason + PropsLen)
  if (pkt_size < 4) {
    MQTT_LOG_ERROR("CONNACK packet too short (got " + (string)pkt_size + " bytes, need >= 4)");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  //--- Validation: Header byte check
  if (!IsConnack(pkt)) {
    MQTT_LOG_ERROR("Expected CONNACK packet (0x20), got 0x" + StringFormat("%02X", pkt[0]));
    return MQTT_ERROR_INVALID_PACKET_TYPE;
  }

  //--- Header reserved flags (lower nibble) MUST be 0 per §2.2.2.2
  if ((pkt[0] & 0x0F) != 0) {
    MQTT_LOG_ERROR("CONNACK fixed header reserved flags must be 0 per §2.2.2.2, got 0x"
                   + StringFormat("%02X", pkt[0] & 0x0F));
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  uint idx    = 1;
  //--- Step 1: Decode Remaining Length (§2.1.3)
  uint remlen = DecodeVariableByteInteger(pkt, idx);
  if (remlen == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Remaining Length in CONNACK");
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  uint packet_end = idx + remlen;

  //--- Step 2: Ensure buffer contains the full packet
  if (pkt_size < packet_end) {
    MQTT_LOG_ERROR("CONNACK packet truncated (declared " + (string)remlen + " bytes, have " + (string)(pkt_size - idx)
                   + ")");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  //--- Step 3: Parse Connect Acknowledge Flags (§3.2.2.1)
  //--- Bit 0 is 'Session Present'. Bits 7-1 are reserved and MUST be 0.
  uchar ack_flags = pkt[idx++];
  if ((ack_flags & 0xFE) != 0) {
    MQTT_LOG_ERROR("CONNACK reserved flags bits 7-1 must be 0 per §3.2.2.1, got 0x" + StringFormat("%02X", ack_flags));
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }
  m_session_present = (ack_flags & 0x01) != 0;

  //--- Step 4: Parse Connect Reason Codes (§3.2.2.2)
  //--- 0x00 = Success. >= 0x80 = Error.
  m_reason_code     = pkt[idx++];
  if (!IsValidReasonCode(m_reason_code)) {
    MQTT_LOG_ERROR("Invalid CONNACK reason code 0x" + StringFormat("%02X", m_reason_code) + " per §3.2.2.2");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  //--- §3.2.2.2: If reason code is non-zero, Session Present MUST be 0.
  //--- This is a server-side constraint; detecting the violation protects
  //--- against buggy brokers corrupting session state.
  if (m_reason_code != 0x00 && m_session_present) {
    MQTT_LOG_ERROR("Protocol violation: Session Present=1 with non-zero CONNACK reason code 0x"
                   + StringFormat("%02X", m_reason_code) + " per §3.2.2.2");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  //--- Step 5: Parse Properties Length and Properties (§3.2.2.3)
  m_props_len = DecodeVariableByteInteger(pkt, idx);
  if (m_props_len == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Properties Length in CONNACK");
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  uint props_start = idx;
  uint props_end   = props_start + m_props_len;
  if (props_end > packet_end) {
    MQTT_LOG_ERROR("CONNACK properties length (" + (string)m_props_len + ") exceeds remaining length boundary");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }
  if (props_end != packet_end) {
    MQTT_LOG_ERROR("CONNACK properties length does not exactly consume the remaining length");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  if (m_props_len > 0) {
    ENUM_MQTT_ERROR props_err = ReadAllProperties(pkt, m_props_len, props_start);
    if (props_err != MQTT_OK) {
      return props_err;
    }
  }

  if (ArraySize(m_auth_data) > 0 && StringLen(m_auth_method) == 0) {
    MQTT_LOG_ERROR("CONNACK Authentication Data requires Authentication Method per MQTT §3.2.2.3.13");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| IsValidReasonCode                                                |
//| Purpose: Validate CONNACK reason codes per MQTT 5.0 §3.2.2.2     |
//+------------------------------------------------------------------+
bool CConnack::IsValidReasonCode(const uchar reason_code) {
  switch (reason_code) {
    case MQTT_REASON_CODE_SUCCESS:
    case MQTT_REASON_CODE_UNSPECIFIED_ERROR:
    case MQTT_REASON_CODE_MALFORMED_PACKET:
    case MQTT_REASON_CODE_PROTOCOL_ERROR:
    case MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR:
    case MQTT_REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION:
    case MQTT_REASON_CODE_CLIENT_IDENTIFIER_NOT_VALID:
    case MQTT_REASON_CODE_BAD_USER_NAME_OR_PASSWORD:
    case MQTT_REASON_CODE_NOT_AUTHORIZED:
    case MQTT_REASON_CODE_SERVER_UNAVAILABLE:
    case MQTT_REASON_CODE_SERVER_BUSY:
    case MQTT_REASON_CODE_BANNED:
    case MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD:
    case MQTT_REASON_CODE_TOPIC_NAME_INVALID:
    case MQTT_REASON_CODE_PACKET_TOO_LARGE:
    case MQTT_REASON_CODE_QUOTA_EXCEEDED:
    case MQTT_REASON_CODE_PAYLOAD_FORMAT_INVALID:
    case MQTT_REASON_CODE_RETAIN_NOT_SUPPORTED:
    case MQTT_REASON_CODE_QOS_NOT_SUPPORTED:
    case MQTT_REASON_CODE_USE_ANOTHER_SERVER:
    case MQTT_REASON_CODE_SERVER_MOVED:
    case MQTT_REASON_CODE_CONNECTION_RATE_EXCEEDED:
      return true;
  }
  return false;
}

//+------------------------------------------------------------------+
//| ReadAllProperties                                                |
//| Purpose: Read all properties from incoming CONNACK packet        |
//| Parameters: pkt - raw packet buffer                              |
//|             props_len - length of properties section             |
//|             idx - current index in buffer                        |
//| Return: ENUM_MQTT_ERROR (MQTT_OK on success)                     |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR CConnack::ReadAllProperties(uchar &pkt[], uint props_len, uint idx) {
  uint pkt_size  = ArraySize(pkt);
  uint props_end = idx + props_len;
  bool seen_props[256];  // Track seen non-repeatable property IDs
  ArrayInitialize(seen_props, false);

  while (idx < props_end) {
    if (idx >= pkt_size) {
      MQTT_LOG_ERROR("CONNACK properties read past end of packet");
      return MQTT_ERROR_BUFFER_OVERFLOW;
    }

    uchar prop_id = pkt[idx++];

    //--- Detect duplicate non-repeatable properties per §2.2.2.2
    //--- User Property (0x26) is the only repeatable CONNACK property
    if (prop_id != MQTT_PROP_IDENTIFIER_USER_PROPERTY) {
      if (seen_props[prop_id]) {
        MQTT_LOG_ERROR("Duplicate non-repeatable CONNACK property 0x" + StringFormat("%02X", prop_id)
                       + " is a Protocol Error per §2.2.2.2");
        return MQTT_ERROR_PROTOCOL_VIOLATION;
      }
      seen_props[prop_id] = true;
    }

    switch (prop_id) {
      case MQTT_PROP_IDENTIFIER_SESSION_EXPIRY_INTERVAL: {
        bool ok          = true;
        m_session_expiry = ReadFourByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR("CONNACK Session Expiry Interval truncated");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        m_has_session_expiry = true;
      } break;
      case MQTT_PROP_IDENTIFIER_RECEIVE_MAXIMUM: {
        bool ok       = true;
        m_receive_max = ReadTwoByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR("CONNACK Receive Maximum truncated");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        if (m_receive_max == 0) {
          MQTT_LOG_ERROR("CONNACK Receive Maximum = 0 is a Protocol Error per §3.2.2.3.1");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_MAXIMUM_QOS:
        if (idx >= props_end) {
          MQTT_LOG_ERROR("CONNACK Maximum QoS property value missing");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        m_max_qos = pkt[idx++];
        if (m_max_qos > 1) {
          MQTT_LOG_ERROR("CONNACK Maximum QoS must be 0 or 1 per §3.2.2.3.2");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        break;
      case MQTT_PROP_IDENTIFIER_RETAIN_AVAILABLE: {
        if (idx >= props_end) {
          MQTT_LOG_ERROR("CONNACK Retain Available property value missing");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        uchar retain_available = pkt[idx++];
        if (retain_available > 1) {
          MQTT_LOG_ERROR("CONNACK Retain Available must be 0 or 1 per §3.2.2.3.3");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        m_retain_available = (retain_available != 0);
      } break;
      case MQTT_PROP_IDENTIFIER_MAXIMUM_PACKET_SIZE: {
        bool ok        = true;
        m_max_pkt_size = ReadFourByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR("CONNACK Maximum Packet Size truncated");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        if (m_max_pkt_size == 0) {
          MQTT_LOG_ERROR("CONNACK Maximum Packet Size must not be 0 per §3.2.2.3.5");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_ASSIGNED_CLIENT_IDENTIFIER: {
        ENUM_MQTT_ERROR err = TryReadUtf8String(pkt, idx, m_assigned_client_id);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("CONNACK Assigned Client Identifier malformed or truncated");
          return err;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_TOPIC_ALIAS_MAXIMUM: {
        bool ok           = true;
        m_topic_alias_max = ReadTwoByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR("CONNACK Topic Alias Maximum truncated");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_REASON_STRING: {
        ENUM_MQTT_ERROR err = TryReadUtf8String(pkt, idx, m_reason_string);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("CONNACK Reason String malformed or truncated");
          return err;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_WILDCARD_SUBSCRIPTION_AVAILABLE: {
        if (idx >= props_end) {
          MQTT_LOG_ERROR("CONNACK Wildcard Subscription Available property value missing");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        uchar wildcard_available = pkt[idx++];
        if (wildcard_available > 1) {
          MQTT_LOG_ERROR("CONNACK Wildcard Subscription Available must be 0 or 1 per §3.2.2.3.8");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        m_wildcard_sub_available = (wildcard_available != 0);
      } break;
      case MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER_AVAILABLE: {
        if (idx >= props_end) {
          MQTT_LOG_ERROR("CONNACK Subscription Identifier Available property value missing");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        uchar sub_id_available = pkt[idx++];
        if (sub_id_available > 1) {
          MQTT_LOG_ERROR("CONNACK Subscription Identifier Available must be 0 or 1 per §3.2.2.3.8");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        m_sub_id_available = (sub_id_available != 0);
      } break;
      case MQTT_PROP_IDENTIFIER_SHARED_SUBSCRIPTION_AVAILABLE: {
        if (idx >= props_end) {
          MQTT_LOG_ERROR("CONNACK Shared Subscription Available property value missing");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
        uchar shared_available = pkt[idx++];
        if (shared_available > 1) {
          MQTT_LOG_ERROR("CONNACK Shared Subscription Available must be 0 or 1 per §3.2.2.3.8");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        m_shared_sub_available = (shared_available != 0);
      } break;
      case MQTT_PROP_IDENTIFIER_SERVER_KEEP_ALIVE: {
        bool ok             = true;
        m_server_keep_alive = ReadTwoByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR("CONNACK Server Keep Alive truncated");
          return MQTT_ERROR_BUFFER_OVERFLOW;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_RESPONSE_INFORMATION: {
        ENUM_MQTT_ERROR err = TryReadUtf8String(pkt, idx, m_response_info);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("CONNACK Response Information malformed or truncated");
          return err;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_SERVER_REFERENCE: {
        ENUM_MQTT_ERROR err = TryReadUtf8String(pkt, idx, m_server_reference);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("CONNACK Server Reference malformed or truncated");
          return err;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_AUTHENTICATION_METHOD: {
        ENUM_MQTT_ERROR err = TryReadUtf8String(pkt, idx, m_auth_method);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("CONNACK Authentication Method malformed or truncated");
          return err;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_AUTHENTICATION_DATA: {
        ENUM_MQTT_ERROR err = TryReadBinaryData(pkt, idx, m_auth_data);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("CONNACK Authentication Data malformed or truncated");
          return err;
        }
      } break;
      case MQTT_PROP_IDENTIFIER_USER_PROPERTY: {
        //--- Per §3.2.2.3.8 multiple User Properties are allowed; capture all of them.
        string          user_prop[2];
        ENUM_MQTT_ERROR err = TryReadUserProperty(pkt, idx, user_prop);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("CONNACK User Property malformed or truncated");
          return err;
        }
        uint n = ArraySize(m_user_prop_keys);
        ArrayResize(m_user_prop_keys, n + 1);
        ArrayResize(m_user_prop_values, n + 1);
        m_user_prop_keys[n]   = user_prop[0];
        m_user_prop_values[n] = user_prop[1];
      } break;
      default: {
        MQTT_LOG_ERROR("Unknown CONNACK property identifier 0x" + StringFormat("%02X", prop_id)
                       + " is a Protocol Error per §2.2.2.2");
        return MQTT_ERROR_PROTOCOL_VIOLATION;
      } break;
    }

    if (idx > props_end) {
      MQTT_LOG_ERROR("CONNACK property 0x" + StringFormat("%02X", prop_id) + " overruns declared properties length");
      return MQTT_ERROR_INVALID_PROPS_LEN;
    }
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| GetAuthenticationData                                            |
//| Purpose: Copy parsed authentication data to output buffer        |
//| Parameters: dest - output buffer for binary data                 |
//+------------------------------------------------------------------+
void CConnack::GetAuthenticationData(uchar &dest[]) const {
  ArrayResize(dest, ArraySize(m_auth_data));
  if (ArraySize(m_auth_data) > 0) {
    ArrayCopy(dest, m_auth_data);
  }
}

//+------------------------------------------------------------------+
//| GetAuthenticationDataString                                      |
//| Purpose: Get authentication data as a string                     |
//| Return: UTF-8 string containing authentication data              |
//+------------------------------------------------------------------+
string CConnack::GetAuthenticationDataString() const {
  if (ArraySize(m_auth_data) == 0) {
    return "";
  }
  return CharArrayToString(m_auth_data, 0, WHOLE_ARRAY, CP_UTF8);
}

//+------------------------------------------------------------------+
//| IsConnack                                                        |
//+------------------------------------------------------------------+
bool CConnack::IsConnack(uchar &inpkt[]) {
  if (ArraySize(inpkt) < 1) {
    return false;
  }
  return (inpkt[0] >> 4) == CONNACK;
}

//+------------------------------------------------------------------+
//| Constructors and Destructor                                      |
//+------------------------------------------------------------------+
CConnack::CConnack() {
  m_session_present        = false;
  m_reason_code            = 0x00;
  m_props_len              = 0;
  m_session_expiry         = 0;
  m_receive_max            = 65535;
  m_max_qos                = 2;
  m_retain_available       = true;
  m_max_pkt_size           = 0;
  m_topic_alias_max        = 0;
  m_wildcard_sub_available = true;
  m_sub_id_available       = true;
  m_shared_sub_available   = true;
  m_server_keep_alive      = 0;
}

CConnack::CConnack(uchar &inpkt[]) {
  m_session_present        = false;
  m_reason_code            = 0x00;
  m_props_len              = 0;
  m_session_expiry         = 0;
  m_receive_max            = 65535;
  m_max_qos                = 2;
  m_retain_available       = true;
  m_max_pkt_size           = 0;
  m_topic_alias_max        = 0;
  m_wildcard_sub_available = true;
  m_sub_id_available       = true;
  m_shared_sub_available   = true;
  m_server_keep_alive      = 0;
  Read(inpkt);
}

CConnack::~CConnack() { ArrayFree(m_auth_data); }

#endif  // MQTT_CONNACK_MQH
