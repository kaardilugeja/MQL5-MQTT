//+------------------------------------------------------------------+
//|                                                       SubAck.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 SUBACK packet implementation per spec §3.9.             |
//| Used to parse subscription acknowledgment from broker.           |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_SUBACK_MQH
#define MQTT_INTERNAL_PROTOCOL_SUBACK_MQH

#include "..\\..\\MQTT.mqh"

bool IsValidSubackReasonCode(uchar reason_code) {
  return reason_code == 0x00 || reason_code == 0x01 || reason_code == 0x02 || reason_code == 0x80 || reason_code == 0x83
      || reason_code == 0x87 || reason_code == 0x8F || reason_code == 0x91 || reason_code == 0x97 || reason_code == 0x9E
      || reason_code == 0xA1 || reason_code == 0xA2;
}

//+------------------------------------------------------------------+
//| Class CSuback                                                    |
//| Purpose: Class for parsing MQTT SUBACK packets (MQTT v5.0)       |
//| Usage:   Used to read subscription acknowledgment from server    |
//+------------------------------------------------------------------+
class CSuback {
 private:
  ushort m_pktid;           // Packet Identifier (Variable Header)
  uchar  m_reason_codes[];  // List of Reason Codes (Payload)
  string m_reason_string;   // Reason String (Properties)
  string m_user_prop_keys[];
  string m_user_prop_vals[];
  uint   m_user_prop_count;

 public:
  //--- Constructor and Destructor
  CSuback();
  ~CSuback();

  //--- Packet identification
  static bool IsSuback(uchar &inpkt[]);  // Check if packet is SUBACK type

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
//| Default constructor                                              |
//+------------------------------------------------------------------+
CSuback::CSuback()
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
CSuback::~CSuback() {
  ArrayFree(m_reason_codes);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_vals);
}

//+------------------------------------------------------------------+
//| IsSuback                                                         |
//| Purpose: Check if packet is a SUBACK                             |
//| Parameters: inpkt - [IN] input packet buffer                     |
//| Return: true if packet is SUBACK, false otherwise                |
//+------------------------------------------------------------------+
bool CSuback::IsSuback(uchar &inpkt[]) {
  //--- Fixed header byte 1: Packet Type (4 bits) + Flags (4 bits)
  //--- SUBACK (0x90) type is 9
  if (ArraySize(inpkt) < 2) {
    return false;
  }
  return (inpkt[0] >> 4) == SUBACK;
}

//+------------------------------------------------------------------+
//| Read - Parse an incoming SUBACK packet                           |
//| Purpose: Extract packet identifier and reason codes per §3.9     |
//| Parameters: inpkt - [IN] the raw packet bytes                    |
//| Return: MQTT_OK (0) on success, or an error code                 |
//| Note: Implements the parsing logic defined in MQTT 5.0 §3.9      |
//+------------------------------------------------------------------+
int CSuback::Read(uchar &inpkt[]) {
  //--- Clear all state so a reused instance never carries over properties or reason
  //--- codes from a previous SUBACK packet.
  m_reason_string   = "";
  m_user_prop_count = 0;
  ArrayFree(m_reason_codes);
  ArrayFree(m_user_prop_keys);
  ArrayFree(m_user_prop_vals);

  //--- Validation: Fixed Header byte check
  if (!IsSuback(inpkt)) {
    MQTT_LOG_ERROR("Expected SUBACK packet (0x90), got 0x"
                   + StringFormat("%02X", (ArraySize(inpkt) > 0 ? inpkt[0] : 0)));
    return MQTT_ERROR_INVALID_PACKET_TYPE;
  }
  if ((inpkt[0] & 0x0F) != 0x00) {
    MQTT_LOG_ERROR("SUBACK fixed-header flags must be 0 per §3.9.1");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  uint idx     = 1;
  //--- Step 1: Decode Remaining Length (§2.1.3)
  uint rem_len = DecodeVariableByteInteger(inpkt, idx);
  if (rem_len == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed Remaining Length in SUBACK");
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  //--- Step 2: Ensure buffer contains the full packet
  if (idx + rem_len > (uint)ArraySize(inpkt)) {
    MQTT_LOG_ERROR("SUBACK packet truncated (declared " + (string)rem_len + " bytes but buffer is smaller)");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }
  //--- Record the exclusive end of THIS packet per §3.9.3
  //--- Using rem_len boundary (not ArraySize) prevents trailing buffer bytes from
  //--- being mis-parsed as reason codes when the framer passes oversized buffers.
  const uint packet_end = idx + rem_len;

  //--- Step 3: Read Packet Identifier (2 bytes, Variable Header start)
  if (rem_len < 2) {
    MQTT_LOG_ERROR("SUBACK Remaining Length too short for Packet Identifier (got " + (string)rem_len + ", need >= 2)");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }
  bool pktid_ok = true;
  m_pktid       = ReadTwoByteInt(inpkt, idx, pktid_ok);
  if (!pktid_ok) {
    MQTT_LOG_ERROR("SUBACK Packet Identifier read failed — truncated packet");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  //--- Step 4: Parse Properties Length and Properties (§3.9.2.1)
  uint props_len = DecodeVariableByteInteger(inpkt, idx);
  if (props_len == UINT_MAX) {
    MQTT_LOG_ERROR("Malformed SUBACK properties length varint");
    return MQTT_ERROR_MALFORMED_VARINT;
  }
  if (props_len > 0) {
    uint props_end = idx + props_len;
    if (props_end > packet_end) {
      MQTT_LOG_ERROR("SUBACK properties length (" + (string)props_len + ") exceeds Remaining Length boundary");
      return MQTT_ERROR_INVALID_PROPS_LEN;
    }

    bool seen_reason_string = false;  // Reason String is non-repeatable per §2.2.2.2

    while (idx < props_end) {
      uchar prop_id = inpkt[idx++];
      //--- Handle MQTT v5.0 specific properties for SUBACK
      if (prop_id == MQTT_PROP_IDENTIFIER_REASON_STRING) {
        //--- Detect duplicate Reason String per §2.2.2.2
        if (seen_reason_string) {
          MQTT_LOG_ERROR("Duplicate SUBACK Reason String property is a Protocol Error per §2.2.2.2");
          return MQTT_ERROR_PROTOCOL_VIOLATION;
        }
        seen_reason_string  = true;
        ENUM_MQTT_ERROR err = TryReadUtf8StringWithinBounds(inpkt, idx, props_end, m_reason_string);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("SUBACK Reason String malformed or truncated");
          return err;
        }
      } else if (prop_id == MQTT_PROP_IDENTIFIER_USER_PROPERTY) {
        string          user_prop[2];
        ENUM_MQTT_ERROR err = TryReadUserPropertyWithinBounds(inpkt, idx, props_end, user_prop);
        if (err != MQTT_OK) {
          MQTT_LOG_ERROR("SUBACK User Property malformed or truncated");
          return err;
        }
        uint new_count = m_user_prop_count + 1;
        ArrayResize(m_user_prop_keys, new_count);
        ArrayResize(m_user_prop_vals, new_count);
        m_user_prop_keys[m_user_prop_count] = user_prop[0];
        m_user_prop_vals[m_user_prop_count] = user_prop[1];
        m_user_prop_count                   = new_count;
      } else {
        MQTT_LOG_ERROR("Unknown SUBACK property identifier 0x" + StringFormat("%02X", prop_id)
                       + " is a Protocol Error per §2.2.2.2");
        return MQTT_ERROR_PROTOCOL_VIOLATION;
      }

      if (idx > props_end) {
        MQTT_LOG_ERROR("SUBACK property payload overruns declared properties length");
        return MQTT_ERROR_INVALID_PROPS_LEN;
      }
    }
    idx = props_end;
  }

  //--- Step 5: Read Payload (Reason Codes) per §3.9.3
  //--- Each byte in payload is a Reason Code for a specific subscription filter
  int payload_len = (int)packet_end - (int)idx;
  if (payload_len > 0) {
    ArrayResize(m_reason_codes, payload_len);
    ArrayCopy(m_reason_codes, inpkt, 0, idx, payload_len);
    for (int i = 0; i < payload_len; i++) {
      uchar reason_code = m_reason_codes[i];
      if (!IsValidSubackReasonCode(reason_code)) {
        MQTT_LOG_ERROR("Invalid SUBACK reason code 0x" + StringFormat("%02X", reason_code));
        return MQTT_ERROR_INVALID_REASON_CODE;
      }
    }
  } else {
    ArrayFree(m_reason_codes);
  }

  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| ReadReasonString                                                 |
//| Purpose: Read reason string from SUBACK                          |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             idx   - [IN/OUT] starting index                      |
//| Return: Reason string                                            |
//+------------------------------------------------------------------+
string CSuback::ReadReasonString(uchar &inpkt[], uint &idx) { return ReadUtf8String(inpkt, idx); }

//+------------------------------------------------------------------+
//| ReadPayload                                                      |
//| Purpose: Read payload (subscription reason codes) from SUBACK    |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             dest_buf - [OUT] output buffer for reason codes      |
//| Note: Extracts reason codes without full class instantiation.    |
//+------------------------------------------------------------------+
void   CSuback::ReadPayload(uchar &inpkt[], uchar &dest_buf[]) {
  ArrayFree(dest_buf);
  uint pkt_size = (uint)ArraySize(inpkt);
  if (pkt_size < 4) {
    return;
  }

  uint idx     = 1;
  //--- Skip Remaining Length using the canonical varint decoder
  uint rem_len = DecodeVariableByteInteger(inpkt, idx);
  if (rem_len == UINT_MAX) {
    return;
  }

  //--- Use rem_len boundary (not ArraySize) to prevent trailing buffer bytes
  //--- from being mis-parsed as reason codes when the framer passes oversized buffers.
  const uint packet_end = idx + rem_len;
  if (packet_end > pkt_size) {
    return;  // Truncated packet
  }

  //--- Check space for PktID(2) + PropLen(1)
  if (idx + 3 > packet_end) {
    return;
  }
  idx            += 2;  // Skip Packet Identifier

  //--- Read Properties length
  uint props_len  = DecodeVariableByteInteger(inpkt, idx);
  if (props_len == UINT_MAX) {
    return;
  }

  if (idx + props_len > packet_end) {
    return;
  }
  idx             += props_len;  // Skip Properties

  //--- Everything remaining (within rem_len boundary) is the Reason Codes payload
  int payload_len  = (int)packet_end - (int)idx;
  if (payload_len > 0) {
    ArrayResize(dest_buf, payload_len);
    ArrayCopy(dest_buf, inpkt, 0, idx, payload_len);
  }
}

#endif  // MQTT_SUBACK_MQH
