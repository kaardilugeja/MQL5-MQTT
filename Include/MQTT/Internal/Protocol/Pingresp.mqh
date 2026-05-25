//+------------------------------------------------------------------+
//|                                                     Pingresp.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 PINGRESP packet implementation per spec §3.13.          |
//| Used as heartbeat response from broker.                          |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_PINGRESP_MQH
#define MQTT_INTERNAL_PROTOCOL_PINGRESP_MQH

#include "..\\..\\MQTT.mqh"

//+------------------------------------------------------------------+
//| Class CPingresp                                                  |
//| Purpose: Class for parsing MQTT PINGRESP packets                 |
//| Usage:   Response to PINGREQ for connection keep-alive           |
//+------------------------------------------------------------------+
class CPingresp {
 private:
 public:
  //--- Constructor and Destructor
  CPingresp();
  ~CPingresp();

  //--- Main parsing method
  int         Read(uchar &pkt[]);

  //--- Packet validation
  static bool IsPingresp(uchar &pkt[]);  // Check if packet is PINGRESP
};

//+------------------------------------------------------------------+
//| Read                                                             |
//| Purpose: Read and validate incoming PINGRESP packet              |
//| Parameters: pkt - [IN] input packet buffer                       |
//| Return: MQTT_OK on success, or appropriate error code            |
//+------------------------------------------------------------------+
int CPingresp::Read(uchar &pkt[]) {
  if (!IsPingresp(pkt)) {
    MQTT_LOG_ERROR("Expected PINGRESP packet (0xD0), got 0x" + StringFormat("%02X", (ArraySize(pkt) > 0 ? pkt[0] : 0)));
    return MQTT_ERROR_INVALID_PACKET_TYPE;
  }
  //--- Per §3.13.1 Fixed Header: PINGRESP is always exactly 2 bytes
  int pkt_size = ArraySize(pkt);
  if (pkt_size < 2) {
    MQTT_LOG_ERROR("PINGRESP packet too short (got " + (string)pkt_size + " bytes, expected 2)");
    return MQTT_ERROR_PACKET_TOO_SHORT;
  }
  if (pkt_size > 2) {
    MQTT_LOG_ERROR("PINGRESP packet too large (got " + (string)pkt_size + " bytes, expected exactly 2 per §3.13.1)");
    return MQTT_ERROR_PACKET_TOO_LARGE;
  }
  //--- Remaining length must be 0
  if (pkt[1] != 0) {
    MQTT_LOG_ERROR("PINGRESP Remaining Length must be 0 per §3.13.1, got " + (string)pkt[1]);
    return MQTT_ERROR_PROTOCOL_VIOLATION;
  }
  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| IsPingresp                                                       |
//| Purpose: Check if packet is a PINGRESP                           |
//| Parameters: pkt - [IN] input packet buffer                       |
//| Return: true if packet is PINGRESP, false otherwise              |
//+------------------------------------------------------------------+
bool CPingresp::IsPingresp(uchar &pkt[]) {
  if (ArraySize(pkt) < 1) {
    return false;
  }
  //--- PINGRESP type (13) is upper 4 bits, flags (0) are lower 4 bits
  //--- Fixed Header MUST be 208 (0xD0) per spec §3.13.1
  return (pkt[0] == 208);
}

//+------------------------------------------------------------------+
//| Default constructor                                              |
//+------------------------------------------------------------------+
CPingresp::CPingresp() {}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CPingresp::~CPingresp() {}

#endif  // MQTT_INTERNAL_PROTOCOL_PINGRESP_MQH
