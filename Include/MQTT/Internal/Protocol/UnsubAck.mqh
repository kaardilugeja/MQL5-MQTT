//+------------------------------------------------------------------+
//|                                                     UnsubAck.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 UNSUBACK packet implementation per spec §3.11.          |
//| Used to parse unsubscription acknowledgment from broker.         |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_UNSUBACK_MQH
#define MQTT_INTERNAL_PROTOCOL_UNSUBACK_MQH

bool IsValidUnsubackReasonCode(uchar reason_code) {
  return reason_code == 0x00 || reason_code == 0x11 || reason_code == 0x80 || reason_code == 0x83 || reason_code == 0x87
      || reason_code == 0x8F || reason_code == 0x91;
}

//+------------------------------------------------------------------+
//| Class CUnsuback                                                  |
//| Purpose: Class for parsing MQTT UNSUBACK packets                 |
//| Usage:   Used to read unsubscription acknowledgment from server  |
//+------------------------------------------------------------------+
class CUnsuback {
 private:
  ushort m_pktid;           // Packet Identifier (Variable Header)
  uchar  m_reason_codes[];  // List of Reason Codes (Payload)
  string m_reason_string;   // Reason String (Properties)
  string m_user_prop_keys[];
  string m_user_prop_vals[];
  uint   m_user_prop_count;

 public:
  //--- Constructor declarations
  CUnsuback();
  ~CUnsuback();

  //--- Packet validation
  static bool IsUnsuback(uchar &inpkt[]);  // Check if packet is UNSUBACK

  //--- Main parsing methods
  int         Read(uchar &inpkt[]);                            // Parse full packet
  string      ReadReasonString(uchar &inpkt[], uint &idx);     // Read string helper
  void        ReadPayload(uchar &inpkt[], uchar &dest_buf[]);  // Extract reason codes

  //--- Getters for parsed data
  ushort      GetPacketIdentifier() const { return m_pktid; }
  string      GetReasonString() const { return m_reason_string; }
  uint        GetUserPropertyCount() const { return m_user_prop_count; }
  string GetUserPropertyKey(uint index) const { return (index < m_user_prop_count) ? m_user_prop_keys[index] : ""; }
  string GetUserPropertyValue(uint index) const { return (index < m_user_prop_count) ? m_user_prop_vals[index] : ""; }
  void   GetReasonCodes(uchar &dest[]) const { ArrayCopy(dest, m_reason_codes); }
};

//+------------------------------------------------------------------+
//| ReadPayload                                                      |
//| Purpose: Read payload (unsubscription reason codes) from UNSUBACK|
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             dest_buf - [OUT] output buffer for reason codes      |
//+------------------------------------------------------------------+
void CUnsuback::ReadPayload(uchar &inpkt[], uchar &dest_buf[]) {
  ArrayFree(dest_buf);
  uint pkt_size = (uint)ArraySize(inpkt);
  if (pkt_size < 4) {
    MQTT_LOG_ERROR("Packet too small for UNSUBACK");
    return;
  }
  //--- Reject packets that are not UNSUBACK
  if (!IsUnsuback(inpkt)) {
    MQTT_LOG_ERROR("Packet is not UNSUBACK in ReadPayload");
    return;
  }

  uint idx     = 1;
  //--- Decode Remaining Length using standard varint decoder
  uint rem_len = DecodeVariableByteInteger(inpkt, idx);
  if (rem_len == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Remaining Length in UNSUBACK");
    return;
  }

  //--- Use rem_len boundary (not ArraySize) to prevent trailing buffer
  //--- bytes from being mis-parsed as reason codes. Mirrors CSuback::ReadPayload().
  const uint packet_end = idx + rem_len;

  //--- Check if there is enough space for Packet ID (2) and Property Length (min 1)
  if (idx + 3 > packet_end) {
    MQTT_LOG_ERROR("Packet truncated before Variable Header in UNSUBACK");
    return;
  }

  idx            += 2;  // Skip Packet Identifier

  //--- Read Property Length
  uint props_len  = DecodeVariableByteInteger(inpkt, idx);
  if (props_len == UINT_MAX) {
    return;  // Error already printed by decoder
  }

  if (idx + props_len > packet_end) {
    MQTT_LOG_ERROR("Property length exceeds packet boundary in UNSUBACK");
    return;
  }
  idx             += props_len;  // Skip Properties

  //--- The rest is the payload (Reason Codes) — bounded by rem_len, not ArraySize
  int payload_len  = (int)packet_end - (int)idx;
  if (payload_len > 0) {
    ArrayResize(dest_buf, payload_len);
    ArrayCopy(dest_buf, inpkt, 0, idx, payload_len);
  }
}

//+------------------------------------------------------------------+
//| ReadReasonString                                                 |
//| Purpose: Read reason string from UNSUBACK                        |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             idx - [IN/OUT] starting index                        |
//| Return: Reason string                                            |
//+------------------------------------------------------------------+
string CUnsuback::ReadReasonString(uchar &inpkt[], uint &idx) {
  //--- Read reason string from UNSUBACK
  return ReadUtf8String(inpkt, idx);
}

//+------------------------------------------------------------------+
//| Default constructor                                              |
//+------------------------------------------------------------------+
CUnsuback::CUnsuback()
    : m_pktid(0)
    , m_reason_string("")
    , m_user_prop_count(0) {
  ArrayFree(m_reason_codes);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_vals);
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CUnsuback::~CUnsuback() {
  ArrayFree(m_reason_codes);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_vals);
}

//+------------------------------------------------------------------+
//| Read and parse UNSUBACK packet                                   |
//| Parameters: inpkt - input packet buffer                          |
//| Return: MQTT_OK on success, or appropriate error code            |
//+------------------------------------------------------------------+
int CUnsuback::Read(uchar &inpkt[]) {
  //--- Clear all state so a reused instance never carries over properties or reason
  //--- codes from a previous UNSUBACK packet.
  m_reason_string   = "";
  m_user_prop_count = 0;
  ArrayFree(m_reason_codes);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_vals);

  if (!IsUnsuback(inpkt)) {
    return MQTT_ERROR_INVALID_PACKET_TYPE;
  }
  if ((inpkt[0] & 0x0F) != 0x00) {
    MQTT_LOG_ERROR("UNSUBACK fixed-header flags must be 0 per §3.11.1");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  uint idx     = 1;
  //--- Decode Remaining Length (Fixed Header)
  uint rem_len = DecodeVariableByteInteger(inpkt, idx);
  if (rem_len == UINT_MAX) {
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  if (idx + rem_len > (uint)ArraySize(inpkt)) {
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }
  //--- Record the exclusive end of THIS packet per §3.11.3
  //--- Using rem_len boundary (not ArraySize) prevents trailing buffer bytes from
  //--- being mis-parsed as reason codes when the framer passes oversized buffers.
  const uint packet_end = idx + rem_len;

  //--- 1. Read Packet Identifier (2 bytes, Variable Header start)
  if (rem_len < 2) {
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }
  bool pktid_ok = true;
  m_pktid       = ReadTwoByteInt(inpkt, idx, pktid_ok);
  if (!pktid_ok) {
    MQTT_LOG_ERROR("UNSUBACK Packet Identifier read failed — truncated packet");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  //--- 2. Read Properties (Variable Header)
  uint props_len = DecodeVariableByteInteger(inpkt, idx);
  if (props_len == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed UNSUBACK properties length varint");
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (props_len > 0) {
    uint props_end = idx + props_len;
    if (props_end > packet_end) {
      MQTT_LOG_ERROR("UNSUBACK properties length (" + (string)props_len + ") exceeds Remaining Length boundary");
      return MQTT_ERROR_INVALID_PROPS_LEN;
    }

    bool seen_reason_string = false;  // Reason String is non-repeatable per §2.2.2.2

    while (idx < props_end) {
      uchar prop_id = inpkt[idx++];
      //--- Handle MQTT v5.0 specific properties for UNSUBACK
      if (prop_id == MQTT_PROP_IDENTIFIER_REASON_STRING) {
        //--- Detect duplicate Reason String per §2.2.2.2
        if (seen_reason_string) {
          MQTT_LOG_ERROR("Duplicate UNSUBACK Reason String property is a Protocol Error per §2.2.2.2");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        seen_reason_string  = true;
        ENUM_MQTT_ERROR err = TryReadUtf8StringWithinBounds(inpkt, idx, props_end, m_reason_string);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("UNSUBACK Reason String malformed or truncated");
          return err;
        }
      } else if (prop_id == MQTT_PROP_IDENTIFIER_USER_PROPERTY) {
        string          user_prop[2];
        ENUM_MQTT_ERROR err = TryReadUserPropertyWithinBounds(inpkt, idx, props_end, user_prop);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("UNSUBACK User Property malformed or truncated");
          return err;
        }
        uint new_count = m_user_prop_count + 1;
        ArrayResize(m_user_prop_keys, new_count);
        ArrayResize(m_user_prop_vals, new_count);
        m_user_prop_keys[m_user_prop_count] = user_prop[0];
        m_user_prop_vals[m_user_prop_count] = user_prop[1];
        m_user_prop_count                   = new_count;
      } else {
        MQTT_LOG_ERROR("Unknown UNSUBACK property identifier 0x" + StringFormat("%02X", prop_id)
                       + " is a Protocol Error per §2.2.2.2");
        return MQTT_ERROR_PROTOCOL_VIOLATION;
      }

      if (idx > props_end) {
        MQTT_LOG_ERROR("UNSUBACK property payload overruns declared properties length");
        return MQTT_ERROR_INVALID_PROPS_LEN;
      }
    }
    idx = props_end;  // Guaranteed position after properties
  }

  //--- 3. Read Payload (Reason Codes)
  //--- Each byte in payload is a Reason Code for a specific unsubscription
  int payload_len = (int)packet_end - (int)idx;
  if (payload_len > 0) {
    ArrayResize(m_reason_codes, payload_len);
    ArrayCopy(m_reason_codes, inpkt, 0, idx, payload_len);
    for (int i = 0; i < payload_len; i++) {
      uchar reason_code = m_reason_codes[i];
      if (!IsValidUnsubackReasonCode(reason_code)) {
        MQTT_LOG_ERROR("Invalid UNSUBACK reason code 0x" + StringFormat("%02X", reason_code));
        return MQTT_ERROR_INVALID_REASON_CODE;
      }
    }
  } else {
    ArrayFree(m_reason_codes);
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| IsUnsuback                                                       |
//| Purpose: Check if packet is an UNSUBACK                          |
//| Parameters: inpkt - [IN] input packet buffer                     |
//| Return: true if packet is UNSUBACK, false otherwise              |
//+------------------------------------------------------------------+
static bool CUnsuback::IsUnsuback(uchar &inpkt[]) {
  //--- Check if packet is an UNSUBACK (Type 11)
  if (ArraySize(inpkt) < 1) {
    return false;
  }
  return (inpkt[0] >> 4) == UNSUBACK;
}

#endif  // MQTT_UNSUBACK_MQH
