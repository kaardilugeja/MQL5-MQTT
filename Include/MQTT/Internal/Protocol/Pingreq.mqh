//+------------------------------------------------------------------+
//|                                                      Pingreq.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 PINGREQ packet implementation per spec §3.13.           |
//| Used to keep connection alive (heartbeat request).               |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_PINGREQ_MQH
#define MQTT_INTERNAL_PROTOCOL_PINGREQ_MQH

#include "..\\..\\MQTT.mqh"

//+------------------------------------------------------------------+
//| Class CPingreq                                                   |
//| Purpose: Class for building MQTT PINGREQ packets                 |
//| Usage:   Used to keep connection alive                           |
//+------------------------------------------------------------------+
class CPingreq {
 private:
 public:
  //--- Constructor declarations
  CPingreq();
  ~CPingreq();

  //--- Build the final PINGREQ packet
  void Build(uchar &pkt[]);
};

//+------------------------------------------------------------------+
//| Build                                                            |
//| Purpose: Build the final PINGREQ packet                          |
//| Parameters: pkt - [OUT] output packet buffer                     |
//| Note: PINGREQ has a fixed size of 2 bytes with no payload per    |
//|       MQTT v5.0 spec §3.12.                                      |
//+------------------------------------------------------------------+
void CPingreq::Build(uchar &pkt[]) {
  ArrayResize(pkt, 2);
  pkt[0] = PINGREQ << 4;  // Packet type
  pkt[1] = 0;             // Remaining length (0)
};

//+------------------------------------------------------------------+
//| Default constructor                                              |
//+------------------------------------------------------------------+
CPingreq::CPingreq() {}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CPingreq::~CPingreq() {}

#endif  // MQTT_INTERNAL_PROTOCOL_PINGREQ_MQH
