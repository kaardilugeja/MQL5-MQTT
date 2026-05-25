//+------------------------------------------------------------------+
//|                                                   ITransport.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Transport interface for MQTT 5.0 client library.                 |
//|                                                                  |
//| Defines the polymorphic contract that both CMqttTransport (raw   |
//| TCP/TLS) and CWebSocketTransport (WS/WSS) implement, enabling    |
//| the CMqttClient facade to operate transport-agnostically.        |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_TRANSPORT_ITRANSPORT_MQH
#define MQTT_INTERNAL_TRANSPORT_ITRANSPORT_MQH

#include "..\\Util\\Defines.mqh"

//+------------------------------------------------------------------+
//| IMqttTransport                                                   |
//| Purpose: Abstract interface for MQTT transport layers.           |
//|          Both raw TCP/TLS and WebSocket transports implement     |
//|          this interface so CMqttClient can swap transports       |
//|          without modifying client internals.                     |
//+------------------------------------------------------------------+
class IMqttTransport {
 public:
  virtual ~IMqttTransport() {}

  //--- Connection lifecycle
  virtual void                         Disconnect()                                       = 0;
  virtual bool                         IsConnected() const                                = 0;
  virtual bool                         IsConnecting() const                               = 0;
  virtual ENUM_TRANSPORT_CONNECT_PHASE GetConnectPhase() const                            = 0;

  //--- I/O
  virtual ENUM_TRANSPORT_ERROR         Send(const uchar &pkt[], int len = -1)             = 0;
  virtual ENUM_TRANSPORT_ERROR         Poll(PacketBuffer &out_packets[], uint &out_count) = 0;

  //--- Configuration
  virtual void                         SetMaxPacketSize(uint max_size)                    = 0;
  virtual void                         SetMaxBufferSize(uint max_size)                    = 0;
  virtual void                         SetKeepAlive(uint seconds)                         = 0;
  virtual void                         SetPingRespTimeout(uint sec)                       = 0;
  virtual void                         SetReadTimeout(uint ms)                            = 0;
  virtual void                         SetBlockingOperationWarnThreshold(uint ms)         = 0;

  //--- Socket handle access (for TOFU certificate inspection etc.)
  virtual int                          GetSocket() const                                  = 0;

  //--- Last PINGREQ→PINGRESP round-trip in microseconds (0 if none measured)
  virtual ulong                        GetLastPingRTT_us() const                          = 0;
  virtual ulong                        GetLastBlockingOperationDuration_us() const        = 0;
};

#endif  // MQTT_INTERNAL_TRANSPORT_ITRANSPORT_MQH
