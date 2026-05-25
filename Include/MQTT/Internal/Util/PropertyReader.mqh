//+------------------------------------------------------------------+
//|                                               PropertyReader.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Reusable MQTT 5.0 property reader for ack/control packets.       |
//| Eliminates duplicated ReadProperties() switch/case blocks across |
//| Puback, Pubrec, Pubrel, Pubcomp, Auth, and Disconnect packets.   |
//| Each packet class delegates to CPropertyReader::ReadProperties() |
//| with a bitmask of allowed property identifiers.                  |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_UTIL_PROPERTY_READER_MQH
#define MQTT_INTERNAL_UTIL_PROPERTY_READER_MQH

#include "Defines.mqh"
#include "Logger.mqh"

//+------------------------------------------------------------------+
//| Property capability flags                                        |
//| OR-combine to declare which properties a packet type allows.     |
//+------------------------------------------------------------------+
#define PROP_ALLOW_REASON_STRING    0x01  // §3.x.2.2 Reason String (0x1F)
#define PROP_ALLOW_USER_PROPERTY    0x02  // §3.x.2.2 User Property (0x26)
#define PROP_ALLOW_AUTH_METHOD      0x04  // §3.15.2.2.2 Authentication Method (0x15)
#define PROP_ALLOW_AUTH_DATA        0x08  // §3.15.2.2.3 Authentication Data (0x16)
#define PROP_ALLOW_SERVER_REFERENCE 0x10  // §3.14.2.2.5 Server Reference (0x1C)
#define PROP_ALLOW_SESSION_EXPIRY   0x20  // §3.14.2.2.2 Session Expiry Interval (0x11)

//+------------------------------------------------------------------+
//| Class CPropertyReader                                            |
//| Purpose: Shared property parser for MQTT 5.0 ack/control packets |
//|          Collects parsed values for the caller to retrieve.      |
//+------------------------------------------------------------------+
class CPropertyReader {
 private:
  //--- Parsed property storage
  string          m_reason_string;         // Reason String (0x1F)
  bool            m_has_reason_string;     // Whether a reason string was parsed
  string          m_server_reference;      // Server Reference (0x1C)
  bool            m_has_server_reference;  // Whether a server reference was parsed
  string          m_auth_method;           // Authentication Method (0x15)
  bool            m_has_auth_method;       // Whether auth method was parsed
  uchar           m_auth_data[];           // Authentication Data (0x16)
  bool            m_has_auth_data;         // Whether auth data was parsed
  string          m_user_prop_keys[];      // User Property keys
  string          m_user_prop_vals[];      // User Property values
  uint            m_user_prop_count;       // Number of user properties parsed
  uint            m_session_expiry;        // Session Expiry Interval (0x11)
  bool            m_has_session_expiry;    // Whether Session Expiry Interval was parsed
  bool            m_has_error;             // Set true on unknown/disallowed property (Malformed Packet per §2.2.2)
  ENUM_MQTT_ERROR m_error_code;            // Parse failure classification
  int             m_last_error_prop_id;    // Property ID that caused the error (-1 if none)

 public:
  CPropertyReader();
  ~CPropertyReader();

  //--- Reset all parsed state (call before re-parsing)
  void            Reset();

  //+------------------------------------------------------------------+
  //| ReadProperties — Main property loop                              |
  //| Parameters:                                                      |
  //|   pkt        - raw packet buffer                                 |
  //|   props_len  - total property bytes to consume                   |
  //|   idx        - starting index of property data (updated)         |
  //|   allowed    - bitmask of PROP_ALLOW_* flags                     |
  //|   pkt_name   - packet type name for log messages                 |
  //| Return: Number of properties successfully read                   |
  //+------------------------------------------------------------------+
  uint            ReadProperties(uchar &pkt[], uint props_len, uint &idx, uint allowed, const string pkt_name);

  //--- Getters for parsed results
  bool            HasReasonString() const { return m_has_reason_string; }
  string          GetReasonString() const { return m_reason_string; }
  bool            HasServerReference() const { return m_has_server_reference; }
  string          GetServerReference() const { return m_server_reference; }
  bool            HasAuthMethod() const { return m_has_auth_method; }
  string          GetAuthMethod() const { return m_auth_method; }
  bool            HasAuthData() const { return m_has_auth_data; }
  void            GetAuthData(uchar &dest[]) const;
  uint            GetUserPropertyCount() const { return m_user_prop_count; }
  string          GetUserPropertyKey(uint index) const;
  string          GetUserPropertyValue(uint index) const;

  bool            HasSessionExpiry() const { return m_has_session_expiry; }
  uint            GetSessionExpiry() const { return m_session_expiry; }

  //--- Error status (per §2.2.2: unknown properties are Malformed Packet)
  bool            HasError() const { return m_has_error; }
  ENUM_MQTT_ERROR GetErrorCode() const { return m_error_code; }
  int             GetLastErrorPropertyId() const { return m_last_error_prop_id; }
};

//+------------------------------------------------------------------+
//| Constructor                                                      |
//+------------------------------------------------------------------+
CPropertyReader::CPropertyReader() { Reset(); }

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CPropertyReader::~CPropertyReader() {
  ArrayFree(m_auth_data);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_vals);
}

//+------------------------------------------------------------------+
//| Reset — clear all parsed state for reuse                         |
//+------------------------------------------------------------------+
void CPropertyReader::Reset() {
  m_reason_string        = "";
  m_has_reason_string    = false;
  m_server_reference     = "";
  m_has_server_reference = false;
  m_auth_method          = "";
  m_has_auth_method      = false;
  m_has_auth_data        = false;
  m_user_prop_count      = 0;
  m_session_expiry       = 0;
  m_has_session_expiry   = false;
  m_has_error            = false;
  m_error_code           = MQTT_OK;
  m_last_error_prop_id   = -1;
  ArrayFree(m_auth_data);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_vals);
}

//+------------------------------------------------------------------+
//| ReadProperties — Shared property reading loop                    |
//| Iterates over the property bytes, dispatching each property ID   |
//| to the appropriate handler based on the allowed bitmask.         |
//| Unknown or disallowed properties cause an immediate return.      |
//+------------------------------------------------------------------+
uint CPropertyReader::ReadProperties(uchar &pkt[], uint props_len, uint &idx, uint allowed, const string pkt_name) {
  uint props_count = 0;
  uint bytes       = 0;
  uint pkt_size    = ArraySize(pkt);
  uint props_end   = idx + props_len;
  bool seen_props[256];
  ArrayInitialize(seen_props, false);

  while (bytes < props_len) {
    //--- Bounds check before reading property identifier
    if (idx >= pkt_size) {
      MQTT_LOG_ERROR(pkt_name + " properties read past end of packet");
      m_has_error          = true;
      m_error_code         = MQTT_ERROR_BUFFER_OVERFLOW;
      m_last_error_prop_id = -1;
      return props_count;
    }

    uchar prop_id = pkt[idx];
    idx++;
    bytes++;
    uint prop_val_start = idx;

    //--- Only User Property is repeatable across these control packets.
    if (prop_id != MQTT_PROP_IDENTIFIER_USER_PROPERTY) {
      if (seen_props[prop_id]) {
        MQTT_LOG_ERROR("Duplicate non-repeatable " + pkt_name + " property 0x" + StringFormat("%02X", prop_id)
                       + " is a Protocol Error per §2.2.2.2");
        m_has_error          = true;
        m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
        m_last_error_prop_id = (int)prop_id;
        return props_count;
      }
      seen_props[prop_id] = true;
    }

    switch (prop_id) {
      //--- Reason String (0x1F) — UTF-8 Encoded String
      case MQTT_PROP_IDENTIFIER_REASON_STRING: {
        if ((allowed & PROP_ALLOW_REASON_STRING) == 0) {
          MQTT_LOG_ERROR("Property Reason String (0x1F) not allowed in " + pkt_name);
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        ENUM_MQTT_ERROR err = TryReadUtf8StringWithinBounds(pkt, idx, props_end, m_reason_string);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR(pkt_name + " Reason String malformed or truncated");
          m_has_error          = true;
          m_error_code         = err;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        m_has_reason_string = true;
        MQTT_LOG_DEBUG(pkt_name + " Reason String: " + m_reason_string);
        props_count++;
      } break;

      //--- User Property (0x26) — UTF-8 String Pair
      case MQTT_PROP_IDENTIFIER_USER_PROPERTY: {
        if ((allowed & PROP_ALLOW_USER_PROPERTY) == 0) {
          MQTT_LOG_ERROR("Property User Property (0x26) not allowed in " + pkt_name);
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        string          userprop_pair[2];
        ENUM_MQTT_ERROR err = TryReadUserPropertyWithinBounds(pkt, idx, props_end, userprop_pair);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR(pkt_name + " User Property malformed or truncated");
          m_has_error          = true;
          m_error_code         = err;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        uint new_count = m_user_prop_count + 1;
        ArrayResize(m_user_prop_keys, new_count);
        ArrayResize(m_user_prop_vals, new_count);
        m_user_prop_keys[m_user_prop_count] = userprop_pair[0];
        m_user_prop_vals[m_user_prop_count] = userprop_pair[1];
        m_user_prop_count                   = new_count;
        MQTT_LOG_DEBUG(pkt_name + " User Property: " + userprop_pair[0] + ": " + userprop_pair[1]);
        props_count++;
      } break;

      //--- Authentication Method (0x15) — UTF-8 Encoded String
      case MQTT_PROP_IDENTIFIER_AUTHENTICATION_METHOD: {
        if ((allowed & PROP_ALLOW_AUTH_METHOD) == 0) {
          MQTT_LOG_ERROR("Property Authentication Method (0x15) not allowed in " + pkt_name);
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        ENUM_MQTT_ERROR err = TryReadUtf8StringWithinBounds(pkt, idx, props_end, m_auth_method);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR(pkt_name + " Authentication Method malformed or truncated");
          m_has_error          = true;
          m_error_code         = err;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        m_has_auth_method = true;
        MQTT_LOG_DEBUG(pkt_name + " Authentication Method: " + m_auth_method);
        props_count++;
      } break;

      //--- Authentication Data (0x16) — Binary Data
      case MQTT_PROP_IDENTIFIER_AUTHENTICATION_DATA: {
        if ((allowed & PROP_ALLOW_AUTH_DATA) == 0) {
          MQTT_LOG_ERROR("Property Authentication Data (0x16) not allowed in " + pkt_name);
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        if (idx + 2 > props_end) {
          MQTT_LOG_ERROR(pkt_name + " Authentication Data length field truncated");
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_BUFFER_OVERFLOW;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        ENUM_MQTT_ERROR err = TryReadBinaryData(pkt, idx, m_auth_data);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR(pkt_name + " Authentication Data malformed or truncated");
          m_has_error          = true;
          m_error_code         = err;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        m_has_auth_data = true;
        MQTT_LOG_DEBUG(pkt_name + " Authentication Data: " + (string)ArraySize(m_auth_data) + " bytes");
        props_count++;
      } break;

      //--- Server Reference (0x1C) — UTF-8 Encoded String
      case MQTT_PROP_IDENTIFIER_SERVER_REFERENCE: {
        if ((allowed & PROP_ALLOW_SERVER_REFERENCE) == 0) {
          MQTT_LOG_ERROR("Property Server Reference (0x1C) not allowed in " + pkt_name);
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        ENUM_MQTT_ERROR err = TryReadUtf8StringWithinBounds(pkt, idx, props_end, m_server_reference);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR(pkt_name + " Server Reference malformed or truncated");
          m_has_error          = true;
          m_error_code         = err;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        m_has_server_reference = true;
        MQTT_LOG_DEBUG(pkt_name + " Server Reference: " + m_server_reference);
        props_count++;
      } break;

      //--- Session Expiry Interval (0x11) — Four Byte Integer (§3.14.2.2.2)
      case MQTT_PROP_IDENTIFIER_SESSION_EXPIRY_INTERVAL: {
        if ((allowed & PROP_ALLOW_SESSION_EXPIRY) == 0) {
          MQTT_LOG_ERROR("Property Session Expiry Interval (0x11) not allowed in " + pkt_name);
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        bool ok          = true;
        m_session_expiry = ReadFourByteInt(pkt, idx, ok);
        if (!ok) {
          MQTT_LOG_ERROR(pkt_name + " Session Expiry Interval truncated");
          m_has_error          = true;
          m_error_code         = MQTT_ERROR_BUFFER_OVERFLOW;
          m_last_error_prop_id = (int)prop_id;
          return props_count;
        }
        m_has_session_expiry = true;
        MQTT_LOG_DEBUG(pkt_name + " Session Expiry Interval: " + (string)m_session_expiry + "s");
        props_count++;
      } break;

      default:
        //--- Unknown/unexpected property — per §2.2.2, this is a Malformed Packet.
        //--- Set error status so callers can trigger Protocol Error disconnect.
        MQTT_LOG_ERROR("Unknown property 0x" + StringFormat("%02X", prop_id) + " in " + pkt_name + " at index "
                       + (string)(idx - 1));
        m_has_error          = true;
        m_error_code         = MQTT_ERROR_PROTOCOL_VIOLATION;
        m_last_error_prop_id = (int)prop_id;
        return props_count;
    }
    bytes += (idx - prop_val_start);
  }
  return props_count;
}

//+------------------------------------------------------------------+
//| GetAuthData — copy parsed auth data to caller buffer             |
//+------------------------------------------------------------------+
void CPropertyReader::GetAuthData(uchar &dest[]) const {
  ArrayResize(dest, ArraySize(m_auth_data));
  if (ArraySize(m_auth_data) > 0) {
    ArrayCopy(dest, m_auth_data);
  }
}

//+------------------------------------------------------------------+
//| GetUserPropertyKey — get key by index                            |
//+------------------------------------------------------------------+
string CPropertyReader::GetUserPropertyKey(uint index) const {
  if (index >= m_user_prop_count) {
    return "";
  }
  return m_user_prop_keys[index];
}

//+------------------------------------------------------------------+
//| GetUserPropertyValue — get value by index                        |
//+------------------------------------------------------------------+
string CPropertyReader::GetUserPropertyValue(uint index) const {
  if (index >= m_user_prop_count) {
    return "";
  }
  return m_user_prop_vals[index];
}

#endif  // MQTT_INTERNAL_UTIL_PROPERTY_READER_MQH
