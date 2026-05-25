//+------------------------------------------------------------------+
//|                                                       PubAck.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 PUBACK packet implementation per spec §3.4.             |
//| Used for QoS 1 publish acknowledgment.                           |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_PUBACK_MQH
#define MQTT_INTERNAL_PROTOCOL_PUBACK_MQH

#include "..\\..\\MQTT.mqh"
#include "..\\Util\\PropertyReader.mqh"

//+------------------------------------------------------------------+
//| PUBACK Variable Header                                           |
//+------------------------------------------------------------------+
/*
The Variable Header of the PUBACK Packet contains the following fields in the order: Packet Identifier
from the PUBLISH packet that is being acknowledged, PUBACK Reason Code, Property Length, and the
Properties.
*/

//+------------------------------------------------------------------+
//| Class CPuback                                                    |
//| Purpose: Class for MQTT Puback Control Packets                   |
//| Usage:   Used for QoS 1 publish acknowledgments                  |
//+------------------------------------------------------------------+
class CPuback {
 private:
  void RemoveProperty(uchar prop_id);

  //--- Validate reason code per MQTT spec §3.4.2.1
  //--- Only the following reason codes are valid for PUBACK:
  //--- 0x00 = Success
  //--- 0x10 = No Matching Subscribers
  //--- 0x80 = Unspecified Error
  //--- 0x83 = Implementation Specific Error
  //--- 0x87 = Not Authorized
  //--- 0x90 = Topic Name Invalid
  //--- 0x91 = Packet Identifier In Use
  //--- 0x97 = Quota Exceeded
  //--- 0x99 = Payload Format Invalid
  bool IsValidReasonCode(uchar code) {
    switch (code) {
      case 0x00:  // Success
      case 0x10:  // No Matching Subscribers
      case 0x80:  // Unspecified Error
      case 0x83:  // Implementation Specific Error
      case 0x87:  // Not Authorized
      case 0x90:  // Topic Name Invalid
      case 0x91:  // Packet Identifier In Use
      case 0x97:  // Quota Exceeded
      case 0x99:  // Payload Format Invalid
        return true;
      default:
        return false;
    }
  }

  //--- Packet state tracking
  uint  m_remlen;          // Remaining length
  uint  m_remlen_bytes;    // Bytes for remaining length
  uchar m_reasoncode;      // Reason code
  uint  m_propslen;        // Properties length
  uint  m_propslen_bytes;  // Bytes for properties length
  uint  m_pktid;           // Packet identifier
  uchar m_properties[];

 public:
  //--- Method for reading incoming packets
  int  Read(uchar &pkt[]);

  //--- Method for building the final packet
  void Build(uchar &pkt[]);

  //--- Constructor declarations
  CPuback(void) {
    m_pktid          = 0;
    m_reasoncode     = 0x00;
    m_remlen         = 0;
    m_remlen_bytes   = 0;
    m_propslen       = 0;
    m_propslen_bytes = 0;
  };
  CPuback(uchar &inpkt[]);

  //--- Destructor
  ~CPuback(void) {};

  //--- Setter methods for building outgoing packets
  void            SetPacketId(ushort pktid) { m_pktid = pktid; };
  void            SetReasonCode(uchar reasoncode);
  void            SetReasonString(const string reason);
  void            SetUserProperty(const string key, const string val);

  //--- Getter methods
  ushort          GetPacketId() const { return (ushort)m_pktid; };
  uchar           GetReasonCode() const { return m_reasoncode; };

  //--- Static packet validation
  static bool     IsPuback(uchar &inpkt[]);

  //--- Packet reading methods
  ushort          ReadPacketIdentifier(uchar &pkt[], uint idx);
  uchar           ReadReasonCode(uchar &pkt[], uint idx);
  ENUM_MQTT_ERROR ReadProperties(uchar &pkt[], uint props_len, uint idx);
  string          ReadReasonString(uchar &inpkt[], uint &idx);
};

//+------------------------------------------------------------------+
//| ReadReasonString                                                 |
//| Purpose: Read reason string from PUBACK                          |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             idx - [IN/OUT] starting index                        |
//| Return: Reason string                                            |
//+------------------------------------------------------------------+
string CPuback::ReadReasonString(uchar &inpkt[], uint &idx) { return ReadUtf8String(inpkt, idx); }

//+------------------------------------------------------------------+
//| RemoveProperty                                                   |
//+------------------------------------------------------------------+
void   CPuback::RemoveProperty(uchar prop_id) {
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
//| IsPuback                                                         |
//| Purpose: Check if packet is a PUBACK                             |
//| Parameters: inpkt - [IN] input packet buffer                     |
//| Return: true if packet is PUBACK, false otherwise                |
//+------------------------------------------------------------------+
static bool CPuback::IsPuback(uchar &inpkt[]) {
  if (ArraySize(inpkt) < 1) {
    return false;
  }
  return inpkt[0] == (PUBACK << 4);
};

//+------------------------------------------------------------------+
//| Read and process incoming PUBACK packet                          |
//| Parameters: pkt - input packet buffer                            |
//| Return: MQTT_OK on success, or appropriate error code            |
//| Layout: [type:1][remlen:1-4][pktid:2][reason:1][propslen:1-4][…] |
//+------------------------------------------------------------------+
int CPuback::Read(uchar &pkt[]) {
  //--- Bounds check: need at least type + 1 remlen byte + 2 pktid = 4 bytes
  if (ArraySize(pkt) < 4) {
    MQTT_LOG_ERROR("PUBACK packet too short");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  uint idx = 1;
  m_remlen = DecodeVariableByteInteger(pkt, idx);

  //--- Validate Remaining Length per §3.4.2
  if (m_remlen < 2 || m_remlen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid Remaining Length: %d", m_remlen));
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  //--- Get the remaining length encoding size
  m_remlen_bytes        = GetVarintBytes(m_remlen);

  //--- The variable header starts after: byte 0 (pkt type) + remlen bytes
  uint var_header_start = 1 + m_remlen_bytes;
  uint packet_end       = var_header_start + m_remlen;

  //--- Bounds check: ensure buffer has all the data declared by remlen
  if (ArraySize(pkt) < (int)packet_end) {
    MQTT_LOG_ERROR("PUBACK packet truncated");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  //--- Read packet identifier (always present, 2 bytes)
  m_pktid = ReadPacketIdentifier(pkt, var_header_start);
  if (m_pktid == 0) {
    MQTT_LOG_ERROR("Packet Identifier 0 is not valid for PUBACK per §2.2.1");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  //--- Per §3.4.2: If Remaining Length is 2, there is no Reason Code
  //--- and no Properties. The Reason Code is implicitly 0x00 (Success).
  if (m_remlen == 2) {
    m_reasoncode     = 0x00;
    m_propslen       = 0;
    m_propslen_bytes = 0;
    return MQTT_OK;
  }

  //--- Per §3.4.2: If Remaining Length is 3, there is a Reason Code
  //--- with no Properties. Read the Reason Code at offset +2 from var header.
  m_reasoncode = ReadReasonCode(pkt, var_header_start + 2);

  if (!IsValidReasonCode(m_reasoncode)) {
    MQTT_LOG_ERROR("Invalid PUBACK reason code 0x" + StringFormat("%02X", m_reasoncode) + " per MQTT §3.4.2.1");
    return MQTT_ERROR_INVALID_REASON_CODE;
  }

  if (m_remlen == 3) {
    m_propslen       = 0;
    m_propslen_bytes = 0;

    if (m_reasoncode >= 0x80) {
      HandlePublishError(m_reasoncode);
    }
    return MQTT_OK;
  }

  //--- Remaining Length >= 4: Reason Code + Properties present
  //--- Read properties length (starts at var_header_start + 3)
  uint prop_idx = var_header_start + 3;
  m_propslen    = ReadPropertyLength(pkt, prop_idx);

  //--- Validate Property Length
  if (m_propslen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid Properties Length: %d", m_propslen));
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  //--- Get the properties length encoding size
  m_propslen_bytes = GetVarintBytes(m_propslen);

  //--- Properties data starts after: var_header_start + 3 (pktid 2 + reason 1) + propslen_bytes
  uint props_start = var_header_start + 3 + m_propslen_bytes;
  uint props_end   = props_start + m_propslen;

  if (props_start > packet_end || props_end > packet_end) {
    MQTT_LOG_ERROR("PUBACK properties length (" + (string)m_propslen + ") exceeds Remaining Length boundary");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }
  if (props_end != packet_end) {
    MQTT_LOG_ERROR("PUBACK properties length does not exactly consume the remaining length");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  //--- Read properties if present
  if (m_propslen > 0) {
    ENUM_MQTT_ERROR props_err = ReadProperties(pkt, m_propslen, props_start);
    if (props_err != MQTT_OK) {
      return props_err;
    }
  }

  if (m_reasoncode >= 0x80) {
    HandlePublishError(m_reasoncode);
  }
  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| ReadProperties                                                   |
//| Purpose: Read properties from PUBACK packet                      |
//| Delegates to shared CPropertyReader for §3.4.2.2 properties.     |
//| Parameters: pkt - [IN] packet buffer                             |
//|             props_len - [IN] properties length in bytes          |
//|             idx - [IN] starting index of properties data         |
//| Return: Number of properties read                                |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR CPuback::ReadProperties(uchar &pkt[], uint props_len, uint idx) {
  CPropertyReader reader;
  uint            allowed = PROP_ALLOW_REASON_STRING | PROP_ALLOW_USER_PROPERTY;
  reader.ReadProperties(pkt, props_len, idx, allowed, "PUBACK");
  return reader.HasError() ? reader.GetErrorCode() : MQTT_OK;
}

//+------------------------------------------------------------------+
//| ReadReasonCode                                                   |
//| Purpose: Read reason code from packet with bounds checking       |
//| Parameters: pkt - [IN] packet buffer                             |
//|             idx - [IN] index of reason code                      |
//| Return: Reason code value, or 0xFF on bounds error               |
//+------------------------------------------------------------------+
uchar CPuback::ReadReasonCode(uchar &pkt[], uint idx) {
  if (idx >= (uint)ArraySize(pkt)) {
    MQTT_LOG_ERROR("ReadReasonCode index " + (string)idx + " out of bounds");
    return 0xFF;
  }
  return (uchar)pkt[idx];
}

//+------------------------------------------------------------------+
//| Read packet identifier from packet with bounds checking          |
//| Parameters: pkt - packet buffer                                  |
//|             idx - index of packet ID MSB                         |
//| Return: Packet identifier, or 0 on bounds error                  |
//+------------------------------------------------------------------+
ushort CPuback::ReadPacketIdentifier(uchar &pkt[], uint idx) {
  if (idx + 2 > (uint)ArraySize(pkt)) {
    MQTT_LOG_ERROR("ReadPacketIdentifier index " + (string)idx + " out of bounds");
    return 0;
  }
  return (ushort)((pkt[idx] * 256) + pkt[idx + 1]);
}

//+------------------------------------------------------------------+
//| Build                                                            |
//| Purpose: Build the final PUBACK packet                           |
//| Parameters: pkt - [OUT] output packet buffer                     |
//| Note: Per §3.4.2 if Reason Code is 0x00 and no properties,       |
//|       Remaining Length can be 2 (Packet ID only)                 |
//+------------------------------------------------------------------+
void CPuback::Build(uchar &pkt[]) {
  if (m_reasoncode == 0x00 && ArraySize(m_properties) == 0) {
    //--- Optimized: Reason Code 0x00 with no properties
    //--- Remaining Length = 2 (Packet Identifier only)
    ArrayResize(pkt, 4);
    pkt[0] = (uchar)PUBACK << 4;              // Fixed header byte 1
    pkt[1] = 2;                               // Remaining Length = 2
    pkt[2] = (uchar)((m_pktid >> 8) & 0xFF);  // Packet ID MSB
    pkt[3] = (uchar)(m_pktid & 0xFF);         // Packet ID LSB
    return;
  }

  uchar props_len_buf[];
  EncodeVariableByteInteger((uint)ArraySize(m_properties), props_len_buf);

  uint  remlen = 2 + 1 + (uint)ArraySize(props_len_buf) + (uint)ArraySize(m_properties);
  uchar remlen_buf[];
  EncodeVariableByteInteger(remlen, remlen_buf);

  ArrayResize(pkt, 1 + ArraySize(remlen_buf) + (int)remlen);
  uint idx   = 0;
  pkt[idx++] = (uchar)PUBACK << 4;
  ArrayCopy(pkt, remlen_buf, idx, 0, ArraySize(remlen_buf));
  idx        += (uint)ArraySize(remlen_buf);
  pkt[idx++]  = (uchar)((m_pktid >> 8) & 0xFF);
  pkt[idx++]  = (uchar)(m_pktid & 0xFF);
  pkt[idx++]  = m_reasoncode;
  ArrayCopy(pkt, props_len_buf, idx, 0, ArraySize(props_len_buf));
  idx += (uint)ArraySize(props_len_buf);
  if (ArraySize(m_properties) > 0) {
    ArrayCopy(pkt, m_properties, idx, 0, ArraySize(m_properties));
  }
};

//+------------------------------------------------------------------+
//| Constructor with packet buffer                                   |
//| Parameters: inpkt - input packet buffer                          |
//+------------------------------------------------------------------+
CPuback::CPuback(uchar &inpkt[]) {
  m_pktid          = 0;
  m_reasoncode     = 0x00;
  m_remlen         = 0;
  m_remlen_bytes   = 0;
  m_propslen       = 0;
  m_propslen_bytes = 0;
  ArrayResize(m_properties, 0);
  Read(inpkt);
}

//+------------------------------------------------------------------+
//| Set reason code with validation                                  |
//| Parameters: reasoncode - reason code to set                      |
//+------------------------------------------------------------------+
void CPuback::SetReasonCode(uchar reasoncode) {
  if (!IsValidReasonCode(reasoncode)) {
    MQTT_LOG_ERROR("Invalid PUBACK reason code 0x" + StringFormat("%02X", reasoncode)
                   + " per MQTT §3.4.2.1. Valid codes: 0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99");
    return;
  }
  m_reasoncode = reasoncode;
}

//+------------------------------------------------------------------+
//| Set reason string                                                |
//+------------------------------------------------------------------+
void CPuback::SetReasonString(const string reason) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_REASON_STRING);
  AppendReasonString(m_properties, reason);
}

//+------------------------------------------------------------------+
//| Set user property                                                |
//+------------------------------------------------------------------+
void CPuback::SetUserProperty(const string key, const string val) { AppendUserProperty(m_properties, key, val); }

#endif  // MQTT_PUBACK_MQH
