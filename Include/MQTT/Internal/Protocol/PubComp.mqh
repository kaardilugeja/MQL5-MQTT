//+------------------------------------------------------------------+
//|                                                      PubComp.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 PUBCOMP packet implementation per spec §3.7.            |
//| Used for QoS 2 delivery part 3 - publish complete.               |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_PUBCOMP_MQH
#define MQTT_INTERNAL_PROTOCOL_PUBCOMP_MQH

#include "..\\..\\MQTT.mqh"
#include "..\\Util\\PropertyReader.mqh"

//+------------------------------------------------------------------+
//| Class CPubcomp                                                   |
//| Purpose: Class for MQTT PUBCOMP packets (QoS 2 step 3)           |
//| Usage:   Client sends PUBCOMP after receiving PUBREL             |
//+------------------------------------------------------------------+
class CPubcomp {
 private:
  void RemoveProperty(uchar prop_id);

  //--- Validate reason code per MQTT spec §3.7.2.1
  //--- PUBCOMP MUST have reason code 0x00 (Success) or 0x92 (Packet Identifier Not Found)
  bool IsValidReasonCode(uchar code) {
    if (code == 0x00 || code == 0x92) {
      return true;
    }
    return false;
  }

  //--- Packet state
  uint  m_pktid;       // Packet identifier
  uchar m_reasoncode;  // Reason code
  uchar m_properties[];

 public:
  //--- Constructor declarations
  CPubcomp() {
    m_pktid      = 0;
    m_reasoncode = 0x00;
  };
  CPubcomp(uchar &inpkt[]);

  //--- Destructor
  ~CPubcomp();

  //--- Setter methods for building outgoing packets
  void        SetPacketId(ushort pktid) { m_pktid = pktid; };
  void        SetReasonCode(uchar reasoncode);
  void        SetReasonString(const string reason);
  void        SetUserProperty(const string key, const string val);

  //--- Build the final PUBCOMP packet
  void        Build(uchar &pkt[]);

  //--- Read incoming PUBCOMP packet
  int         Read(uchar &pkt[]);

  //--- Packet validation
  static bool IsPubcomp(uchar &inpkt[]);  // Check if packet is PUBCOMP

  //--- Read reason code from PUBCOMP
  uchar       ReadReasonCode(uchar &inpkt[], uint idx);

  //--- Read reason string from PUBCOMP
  string      ReadReasonString(uchar &inpkt[], uint &idx);

  //--- Getters for parsed state
  ushort      GetPacketId() const { return (ushort)m_pktid; }
  uchar       GetReasonCode() const { return m_reasoncode; }

 private:
  //--- Read properties from PUBCOMP packet
  ENUM_MQTT_ERROR ReadProperties(uchar &pkt[], uint props_len, uint idx);
};

//+------------------------------------------------------------------+
//| ReadReasonString                                                 |
//| Purpose: Read reason string from PUBCOMP                         |
//| Parameters: inpkt - [IN] input packet buffer                     |
//|             idx - [IN/OUT] starting index                        |
//| Return: Reason string                                            |
//+------------------------------------------------------------------+
string CPubcomp::ReadReasonString(uchar &inpkt[], uint &idx) { return ReadUtf8String(inpkt, idx); }

//+------------------------------------------------------------------+
//| RemoveProperty                                                   |
//+------------------------------------------------------------------+
void   CPubcomp::RemoveProperty(uchar prop_id) {
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
//| Read reason code from PUBCOMP with bounds checking               |
//| Parameters: inpkt - input packet buffer                          |
//|             idx - index of reason code                           |
//| Return: Reason code value, or 0xFF on bounds error               |
//+------------------------------------------------------------------+
uchar CPubcomp::ReadReasonCode(uchar &inpkt[], uint idx) {
  if (idx >= (uint)ArraySize(inpkt)) {
    MQTT_LOG_ERROR("PUBCOMP ReadReasonCode index " + (string)idx + " out of bounds");
    return 0xFF;
  }
  return (uchar)inpkt[idx];
};

//+------------------------------------------------------------------+
//| IsPubcomp                                                        |
//| Purpose: Check if packet is a PUBCOMP                            |
//| Parameters: inpkt - [IN] input packet buffer                     |
//| Return: true if packet is PUBCOMP, false otherwise               |
//+------------------------------------------------------------------+
static bool CPubcomp::IsPubcomp(uchar &inpkt[]) {
  if (ArraySize(inpkt) < 1) {
    return false;
  }
  return inpkt[0] == (PUBCOMP << 4);
}

//+------------------------------------------------------------------+
//| Read and process incoming PUBCOMP packet                         |
//| Parameters: pkt - input packet buffer                            |
//| Return: MQTT_OK on success, or appropriate error code            |
//| Layout: [type:1][remlen:1-4][pktid:2][reason:1][propslen:1-4][…] |
//+------------------------------------------------------------------+
int CPubcomp::Read(uchar &pkt[]) {
  //--- Bounds check: need at least type + 1 remlen byte + 2 pktid = 4 bytes
  if (ArraySize(pkt) < 4) {
    MQTT_LOG_ERROR("PUBCOMP packet too short");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }

  uint idx    = 1;
  uint remlen = DecodeVariableByteInteger(pkt, idx);

  //--- Validate Remaining Length per §3.7.2
  if (remlen < 2 || remlen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid PUBCOMP Remaining Length: %d", remlen));
    return MQTT_ERROR_MALFORMED_VARINT;
  }

  uint remlen_bytes     = GetVarintBytes(remlen);
  uint var_header_start = 1 + remlen_bytes;
  uint packet_end       = var_header_start + remlen;

  //--- Bounds check: ensure buffer has all data declared by remlen
  if (ArraySize(pkt) < (int)packet_end) {
    MQTT_LOG_ERROR("PUBCOMP packet truncated");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  //--- Read packet identifier (always present, 2 bytes)
  if (var_header_start + 1 >= (uint)ArraySize(pkt)) {
    MQTT_LOG_ERROR("PUBCOMP packet too short for packet ID");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }
  m_pktid = (ushort)((pkt[var_header_start] * 256) + pkt[var_header_start + 1]);
  if (m_pktid == 0) {
    MQTT_LOG_ERROR("Packet Identifier 0 is not valid for PUBCOMP per §2.2.1");
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }

  //--- Per §3.7.2: If Remaining Length is 2, Reason Code is implicitly 0x00
  if (remlen == 2) {
    m_reasoncode = 0x00;
    return MQTT_OK;
  }

  //--- Read Reason Code
  m_reasoncode = ReadReasonCode(pkt, var_header_start + 2);

  if (!IsValidReasonCode(m_reasoncode)) {
    MQTT_LOG_ERROR("Invalid PUBCOMP reason code 0x" + StringFormat("%02X", m_reasoncode) + " per MQTT §3.7.2.1");
    return MQTT_ERROR_INVALID_REASON_CODE;
  }

  //--- Per §3.7.2: If Remaining Length is 3, no Properties present
  if (remlen == 3) {
    if (m_reasoncode >= 0x80) {
      HandlePublishError(m_reasoncode);
    }
    return MQTT_OK;
  }

  //--- Remaining Length >= 4: Properties present
  uint prop_idx = var_header_start + 3;
  uint propslen = ReadPropertyLength(pkt, prop_idx);

  if (propslen > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR(StringFormat("Invalid PUBCOMP Properties Length: %d", propslen));
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  uint propslen_bytes = GetVarintBytes(propslen);
  uint props_start    = var_header_start + 3 + propslen_bytes;
  uint props_end      = props_start + propslen;

  if (props_start > packet_end || props_end > packet_end) {
    MQTT_LOG_ERROR("PUBCOMP properties length (" + (string)propslen + ") exceeds Remaining Length boundary");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }
  if (props_end != packet_end) {
    MQTT_LOG_ERROR("PUBCOMP properties length does not exactly consume the remaining length");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  if (propslen > 0) {
    ENUM_MQTT_ERROR props_err = ReadProperties(pkt, propslen, props_start);
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
//| Read properties from PUBCOMP packet                              |
//| Delegates to shared CPropertyReader for §3.7.2.2 properties.     |
//| Parameters: pkt - packet buffer                                  |
//|             props_len - properties length in bytes               |
//|             idx - starting index of properties data              |
//| Return: Number of properties read                                |
//| Note: Per §3.7.2.2 only Reason String and User Property allowed  |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR CPubcomp::ReadProperties(uchar &pkt[], uint props_len, uint idx) {
  CPropertyReader reader;
  uint            allowed = PROP_ALLOW_REASON_STRING | PROP_ALLOW_USER_PROPERTY;
  reader.ReadProperties(pkt, props_len, idx, allowed, "PUBCOMP");
  return reader.HasError() ? reader.GetErrorCode() : MQTT_OK;
};

//+------------------------------------------------------------------+
//| Build the final PUBCOMP packet                                   |
//| Parameters: pkt - output packet buffer                           |
//| Note: Per §3.7.2 if Reason Code is 0x00 and no properties,       |
//|       Remaining Length can be 2 (Packet ID only)                 |
//+------------------------------------------------------------------+
void CPubcomp::Build(uchar &pkt[]) {
  if (m_reasoncode == 0x00 && ArraySize(m_properties) == 0) {
    //--- Optimized: Reason Code 0x00 with no properties
    ArrayResize(pkt, 4);
    pkt[0] = (uchar)PUBCOMP << 4;             // Fixed header (reserved flags = 0)
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
  pkt[idx++] = (uchar)PUBCOMP << 4;
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
//| Parameters: inpkt - input PUBCOMP packet buffer                  |
//+------------------------------------------------------------------+
CPubcomp::CPubcomp(uchar &inpkt[]) {
  m_pktid      = 0;
  m_reasoncode = 0x00;
  ArrayResize(m_properties, 0);
  Read(inpkt);
};

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CPubcomp::~CPubcomp() {}

//+------------------------------------------------------------------+
//| Set reason code with validation                                  |
//| Parameters: reasoncode - reason code to set                      |
//+------------------------------------------------------------------+
void CPubcomp::SetReasonCode(uchar reasoncode) {
  if (!IsValidReasonCode(reasoncode)) {
    MQTT_LOG_ERROR("Invalid PUBCOMP reason code 0x" + StringFormat("%02X", reasoncode)
                   + " per MQTT §3.7.2.1. Valid codes: 0x00 (Success), 0x92 (Packet Identifier Not Found)");
    return;
  }
  m_reasoncode = reasoncode;
}

//+------------------------------------------------------------------+
//| Set reason string                                                |
//+------------------------------------------------------------------+
void CPubcomp::SetReasonString(const string reason) {
  RemoveProperty(MQTT_PROP_IDENTIFIER_REASON_STRING);
  AppendReasonString(m_properties, reason);
}

//+------------------------------------------------------------------+
//| Set user property                                                |
//+------------------------------------------------------------------+
void CPubcomp::SetUserProperty(const string key, const string val) { AppendUserProperty(m_properties, key, val); }

#endif  // MQTT_PUBCOMP_MQH
