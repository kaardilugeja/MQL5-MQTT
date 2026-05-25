//+------------------------------------------------------------------+
//|                                                MockTransport.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Mock transport implementation for unit testing.                  |
//|                                                                  |
//| Implements IMqttTransport so CMqttClient can be tested without   |
//| a real broker connection.  Provides:                             |
//|   - Pre-loaded inbound packet queue (simulates broker replies)   |
//|   - Capture of outbound packets (assert what client sends)       |
//|   - Configurable error injection for fault-path testing          |
//|   - Keep-alive and connection state simulation                   |
//|                                                                  |
//| Usage:                                                           |
//|   CMockTransport mock;                                           |
//|   mock.SetConnected(true);                                       |
//|   mock.EnqueueInboundPacket(connack_bytes);                      |
//|   client.TestInjectTransport(GetPointer(mock));                  |
//|   client.Poll();                                                 |
//|   // Assert: mock.GetSentPacketCount() == 1                      |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_TRANSPORT_MOCK_TRANSPORT_MQH
#define MQTT_INTERNAL_TRANSPORT_MOCK_TRANSPORT_MQH

#include "ITransport.mqh"

//+------------------------------------------------------------------+
//| CMockTransport                                                   |
//| Purpose: Test double for IMqttTransport                          |
//+------------------------------------------------------------------+
class CMockTransport : public IMqttTransport {
 private:
  //--- Connection simulation
  bool                         m_connected;
  bool                         m_connecting;
  ENUM_TRANSPORT_CONNECT_PHASE m_connect_phase;

  //--- Inbound packet queue (packets "received from broker")
  PacketBuffer                 m_inbound[];
  uint                         m_inbound_count;

  //--- Outbound packet capture (packets "sent to broker")
  PacketBuffer                 m_sent[];
  uint                         m_sent_count;

  //--- Error injection
  ENUM_TRANSPORT_ERROR         m_next_send_error;  // If != TRANSPORT_OK, the next Send() returns this
  ENUM_TRANSPORT_ERROR         m_next_poll_error;  // If != TRANSPORT_OK, the next Poll() returns this
  bool                         m_send_error_once;  // If true, error clears after one injection
  bool                         m_poll_error_once;  // If true, error clears after one injection

  //--- Keep-alive tracking (for test assertions)
  uint                         m_keepalive_sec;
  uint                         m_pingresp_timeout;
  uint                         m_max_pkt_size;
  uint                         m_max_buf_size;
  uint                         m_read_timeout;
  uint                         m_blocking_warn_threshold_ms;

 public:
  CMockTransport();
  ~CMockTransport();

  //--- IMqttTransport interface implementation
  virtual void                         Disconnect() override;
  virtual bool                         IsConnected() const override;
  virtual bool                         IsConnecting() const override;
  virtual ENUM_TRANSPORT_CONNECT_PHASE GetConnectPhase() const override;
  virtual ENUM_TRANSPORT_ERROR         Send(const uchar &pkt[], int len = -1) override;
  virtual ENUM_TRANSPORT_ERROR         Poll(PacketBuffer &out_packets[], uint &out_count) override;
  virtual void                         SetMaxPacketSize(uint max_size) override;
  virtual void                         SetMaxBufferSize(uint max_size) override;
  virtual void                         SetKeepAlive(uint seconds) override;
  virtual void                         SetPingRespTimeout(uint sec) override;
  virtual void                         SetReadTimeout(uint ms) override;
  virtual void                         SetBlockingOperationWarnThreshold(uint ms) override;
  virtual int                          GetSocket() const override { return INVALID_HANDLE; }
  virtual ulong                        GetLastPingRTT_us() const override { return 0; }
  virtual ulong                        GetLastBlockingOperationDuration_us() const override { return 0; }

  //--- Test setup methods
  void                                 SetConnected(bool connected);
  void                                 SetConnecting(bool connecting);

  //--- Enqueue a pre-built packet that Poll() will deliver
  void                                 EnqueueInboundPacket(const uchar &pkt[]);

  //--- Enqueue multiple packets at once
  void                                 EnqueueInboundPackets(const PacketBuffer &packets[], uint count);

  //--- Error injection: make the next Send() / Poll() return an error
  void                                 InjectSendError(ENUM_TRANSPORT_ERROR err, bool once = true);
  void                                 InjectPollError(ENUM_TRANSPORT_ERROR err, bool once = true);
  void                                 ClearErrors();

  //--- Outbound packet inspection
  uint                                 GetSentPacketCount() const;
  bool                                 GetSentPacket(uint index, uchar &dest[]) const;
  void                                 ClearSentPackets();

  //--- Inbound queue inspection
  uint                                 GetPendingInboundCount() const;

  //--- Configuration inspection (verify client configured transport correctly)
  uint                                 GetKeepAlive() const { return m_keepalive_sec; }
  uint                                 GetPingRespTimeout() const { return m_pingresp_timeout; }
  uint                                 GetMaxPacketSize() const { return m_max_pkt_size; }
  uint                                 GetMaxBufferSize() const { return m_max_buf_size; }
  uint                                 GetReadTimeout() const { return m_read_timeout; }

  //--- Reset all state
  void                                 Reset();
};

//+------------------------------------------------------------------+
//| Constructor                                                       |
//+------------------------------------------------------------------+
CMockTransport::CMockTransport() {
  m_connected                  = false;
  m_connecting                 = false;
  m_connect_phase              = TRANSPORT_PHASE_IDLE;
  m_inbound_count              = 0;
  m_sent_count                 = 0;
  m_next_send_error            = TRANSPORT_OK;
  m_next_poll_error            = TRANSPORT_OK;
  m_send_error_once            = true;
  m_poll_error_once            = true;
  m_keepalive_sec              = 60;
  m_pingresp_timeout           = 0;
  m_max_pkt_size               = 268435455;
  m_max_buf_size               = 0;
  m_read_timeout               = 100;
  m_blocking_warn_threshold_ms = 0;
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CMockTransport::~CMockTransport() { Reset(); }

//+------------------------------------------------------------------+
//| Reset - Clear all state                                          |
//+------------------------------------------------------------------+
void CMockTransport::Reset() {
  m_connected       = false;
  m_connecting      = false;
  m_connect_phase   = TRANSPORT_PHASE_IDLE;
  m_inbound_count   = 0;
  m_sent_count      = 0;
  m_next_send_error = TRANSPORT_OK;
  m_next_poll_error = TRANSPORT_OK;
  ArrayResize(m_inbound, 0);
  ArrayResize(m_sent, 0);
}

//+------------------------------------------------------------------+
//| Disconnect                                                       |
//+------------------------------------------------------------------+
void CMockTransport::Disconnect() {
  m_connected     = false;
  m_connecting    = false;
  m_connect_phase = TRANSPORT_PHASE_IDLE;
}

//+------------------------------------------------------------------+
//| IsConnected                                                      |
//+------------------------------------------------------------------+
bool                         CMockTransport::IsConnected() const { return m_connected; }

//+------------------------------------------------------------------+
//| IsConnecting                                                     |
//+------------------------------------------------------------------+
bool                         CMockTransport::IsConnecting() const { return m_connecting; }

//+------------------------------------------------------------------+
//| GetConnectPhase                                                  |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_CONNECT_PHASE CMockTransport::GetConnectPhase() const { return m_connect_phase; }

//+------------------------------------------------------------------+
//| SetConnected - Directly set connection state for testing         |
//+------------------------------------------------------------------+
void                         CMockTransport::SetConnected(bool connected) {
  m_connected = connected;
  if (connected) {
    m_connecting    = false;
    m_connect_phase = TRANSPORT_PHASE_CONNECTED;
  } else if (!m_connecting) {
    m_connect_phase = TRANSPORT_PHASE_IDLE;
  }
}

//+------------------------------------------------------------------+
//| SetConnecting - Simulate async connect in-progress               |
//+------------------------------------------------------------------+
void CMockTransport::SetConnecting(bool connecting) {
  m_connecting = connecting;
  if (connecting) {
    m_connected     = false;
    m_connect_phase = TRANSPORT_PHASE_TCP_CONNECTING;
  } else if (!m_connected) {
    m_connect_phase = TRANSPORT_PHASE_IDLE;
  }
}

//+------------------------------------------------------------------+
//| Send - Capture outbound packet                                   |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMockTransport::Send(const uchar &pkt[], int len) {
  //--- Error injection
  if (m_next_send_error != TRANSPORT_OK) {
    ENUM_TRANSPORT_ERROR err = m_next_send_error;
    if (m_send_error_once) {
      m_next_send_error = TRANSPORT_OK;
    }
    return err;
  }

  if (!m_connected) {
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- Capture the packet
  int send_len = (len < 0) ? ArraySize(pkt) : len;
  ArrayResize(m_sent, m_sent_count + 1);
  ArrayResize(m_sent[m_sent_count].data, send_len);
  ArrayCopy(m_sent[m_sent_count].data, pkt, 0, 0, send_len);
  m_sent_count++;

  return TRANSPORT_OK;
}

//+------------------------------------------------------------------+
//| Poll - Return pre-loaded inbound packets                         |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMockTransport::Poll(PacketBuffer &out_packets[], uint &out_count) {
  out_count = 0;
  ArrayResize(out_packets, 0);

  //--- Error injection
  if (m_next_poll_error != TRANSPORT_OK) {
    ENUM_TRANSPORT_ERROR err = m_next_poll_error;
    if (m_poll_error_once) {
      m_next_poll_error = TRANSPORT_OK;
    }
    return err;
  }

  if (!m_connected) {
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- Deliver all queued inbound packets
  if (m_inbound_count > 0) {
    ArrayResize(out_packets, m_inbound_count);
    for (uint i = 0; i < m_inbound_count; i++) {
      int pkt_size = ArraySize(m_inbound[i].data);
      ArrayResize(out_packets[i].data, pkt_size);
      ArrayCopy(out_packets[i].data, m_inbound[i].data, 0, 0, pkt_size);
    }
    out_count       = m_inbound_count;
    m_inbound_count = 0;
    ArrayResize(m_inbound, 0);
  }

  return TRANSPORT_OK;
}

//+------------------------------------------------------------------+
//| EnqueueInboundPacket - Add a packet to the inbound queue         |
//+------------------------------------------------------------------+
void CMockTransport::EnqueueInboundPacket(const uchar &pkt[]) {
  ArrayResize(m_inbound, m_inbound_count + 1);
  int pkt_size = ArraySize(pkt);
  ArrayResize(m_inbound[m_inbound_count].data, pkt_size);
  ArrayCopy(m_inbound[m_inbound_count].data, pkt, 0, 0, pkt_size);
  m_inbound_count++;
}

//+------------------------------------------------------------------+
//| EnqueueInboundPackets - Add multiple packets at once             |
//+------------------------------------------------------------------+
void CMockTransport::EnqueueInboundPackets(const PacketBuffer &packets[], uint count) {
  for (uint i = 0; i < count; i++) {
    EnqueueInboundPacket(packets[i].data);
  }
}

//+------------------------------------------------------------------+
//| Error injection                                                  |
//+------------------------------------------------------------------+
void CMockTransport::InjectSendError(ENUM_TRANSPORT_ERROR err, bool once) {
  m_next_send_error = err;
  m_send_error_once = once;
}

void CMockTransport::InjectPollError(ENUM_TRANSPORT_ERROR err, bool once) {
  m_next_poll_error = err;
  m_poll_error_once = once;
}

void CMockTransport::ClearErrors() {
  m_next_send_error = TRANSPORT_OK;
  m_next_poll_error = TRANSPORT_OK;
}

//+------------------------------------------------------------------+
//| Outbound packet inspection                                       |
//+------------------------------------------------------------------+
uint CMockTransport::GetSentPacketCount() const { return m_sent_count; }

bool CMockTransport::GetSentPacket(uint index, uchar &dest[]) const {
  if (index >= m_sent_count) {
    return false;
  }
  int pkt_size = ArraySize(m_sent[index].data);
  ArrayResize(dest, pkt_size);
  ArrayCopy(dest, m_sent[index].data, 0, 0, pkt_size);
  return true;
}

void CMockTransport::ClearSentPackets() {
  ArrayResize(m_sent, 0);
  m_sent_count = 0;
}

//+------------------------------------------------------------------+
//| Inbound queue inspection                                         |
//+------------------------------------------------------------------+
uint CMockTransport::GetPendingInboundCount() const { return m_inbound_count; }

//+------------------------------------------------------------------+
//| Configuration setters (record values for test assertions)        |
//+------------------------------------------------------------------+
void CMockTransport::SetMaxPacketSize(uint max_size) { m_max_pkt_size = max_size; }
void CMockTransport::SetMaxBufferSize(uint max_size) { m_max_buf_size = max_size; }
void CMockTransport::SetKeepAlive(uint seconds) { m_keepalive_sec = seconds; }
void CMockTransport::SetPingRespTimeout(uint sec) { m_pingresp_timeout = sec; }
void CMockTransport::SetReadTimeout(uint ms) { m_read_timeout = ms; }
void CMockTransport::SetBlockingOperationWarnThreshold(uint ms) { m_blocking_warn_threshold_ms = ms; }

#endif  // MQTT_INTERNAL_TRANSPORT_MOCK_TRANSPORT_MQH
