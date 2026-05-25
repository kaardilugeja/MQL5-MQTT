//+------------------------------------------------------------------+
//|                                                  FlowControl.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Flow control implementation for MQTT 5.0 per spec §4.9.          |
//| Tracks in-flight QoS 1/2 messages and enforces Receive Maximum.  |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_CONNECTION_FLOWCONTROL_MQH
#define MQTT_INTERNAL_CONNECTION_FLOWCONTROL_MQH

#include "..\\Util\\Logger.mqh"
#include <Generic\HashMap.mqh>

//+------------------------------------------------------------------+
//| Forward Declarations                                             |
//+------------------------------------------------------------------+
//--- QoS levels defined in MQTT.mqh, using uchar definition here
//--- QoS_0 = 0, QoS_1 = 1, QoS_2 = 2

//+------------------------------------------------------------------+
//| ENUM_REG_QOS_RESULT                                              |
//| Purpose: Return value for RegisterIncomingQoS() so callers can   |
//|       distinguish duplicate delivery from flow-control overflow. |
//+------------------------------------------------------------------+
enum ENUM_REG_QOS_RESULT {
  REG_OK          = 0,  // Slot allocated successfully
  REG_DUPLICATE   = 1,  // Packet ID already in-flight — DUP retransmission
  REG_WINDOW_FULL = 2   // Client Receive Maximum exceeded — Protocol Error per §4.9.1
};

//+------------------------------------------------------------------+
//| Struct InFlightMessage                                           |
//| Purpose: Tracks in-flight QoS 1/2 message state                  |
//+------------------------------------------------------------------+
struct InFlightMessage {
  ushort packet_id;
  uchar  qos_level;
  ulong  mono_timestamp_us;  // GetMicrosecondCount() at registration time
  uint   packet_size;
};

//+------------------------------------------------------------------+
//| Class CFlowControl                                               |
//| Purpose: Implements MQTT 5.0 Flow Control per spec §4.9          |
//| Usage:   Tracks in-flight QoS 1/2 messages and enforces          |
//|          Receive Maximum and Maximum Packet Size                 |
//+------------------------------------------------------------------+
class CFlowControl {
 private:
  //--- Server limits (from CONNACK)
  ushort          m_receive_maximum;      // Maximum in-flight QoS 1/2 messages
  uint            m_maximum_packet_size;  // Maximum packet size accepted by server

  //--- Client limits (from CONNECT)
  ushort          m_client_receive_maximum;      // Maximum in-flight QoS 1/2 client accepts
  uint            m_client_maximum_packet_size;  // Maximum packet size client accepts

  //--- In-flight message tracking
  InFlightMessage m_in_flight[];              // Outgoing QoS 1/2 publishes awaiting PUBACK/PUBCOMP.
  uint            m_in_flight_count;          // Number of occupied outgoing slots in m_in_flight[].
  ushort          m_incoming_inflight_count;  // Broker-originated QoS 1/2 publishes still awaiting completion.
  //--- MQTT 5 §4.9 applies Receive Maximum separately in each direction, so outgoing
  //--- and incoming QoS handshakes keep distinct packet-id occupancy bitfields.
  uint            m_in_flight_bitfield[];   // Fast outgoing packet-id occupancy test without scanning m_in_flight[].
  CHashMap<ushort, int> m_in_flight_index;  // packet_id → array index, O(1) amortised.
  uint                  m_incoming_bitfield[];  // Fast incoming packet-id occupancy test for duplicate/overflow checks.

  //--- Bitfield helpers
  bool _InFlightBitTest(ushort id) const { return (m_in_flight_bitfield[id >> 5] & ((uint)1 << (id & 0x1F))) != 0; }
  void _InFlightBitSet(ushort id) { m_in_flight_bitfield[id >> 5] |= ((uint)1 << (id & 0x1F)); }
  void _InFlightBitClear(ushort id) { m_in_flight_bitfield[id >> 5] &= ~((uint)1 << (id & 0x1F)); }
  bool _IncomingBitTest(ushort id) const { return (m_incoming_bitfield[id >> 5] & ((uint)1 << (id & 0x1F))) != 0; }
  void _IncomingBitSet(ushort id) { m_incoming_bitfield[id >> 5] |= ((uint)1 << (id & 0x1F)); }
  void _IncomingBitClear(ushort id) { m_incoming_bitfield[id >> 5] &= ~((uint)1 << (id & 0x1F)); }

  //--- Cached QoS in-flight counts for O(1) queries
  uint m_cached_qos1_inflight;  // Diagnostic/metric count; does not create a separate Receive Maximum window.
  uint m_cached_qos2_inflight;  // Diagnostic/metric count; outgoing slots are still governed by §4.9 as one window.

  //--- Statistics
  uint m_total_sent_qos1;  // Lifetime outgoing QoS 1 registrations.
  uint m_total_sent_qos2;  // Lifetime outgoing QoS 2 registrations.
  uint m_total_acked;      // Lifetime outgoing completions via PUBACK/PUBCOMP.
  uint m_total_released;   // Lifetime incoming completions released from broker-driven state.

  //--- Private helper methods
  int  FindInFlightMessage(const ushort packet_id);
  bool IsQoS1or2(const uchar qos_level) const;

 public:
  //--- Constructor/Destructor
  CFlowControl();
  ~CFlowControl();

  //--- Configuration - Server limits (set from CONNACK)
  void                SetReceiveMaximum(const ushort max);
  void                SetMaximumPacketSize(const uint max_size);

  //--- Configuration - Client limits (set from CONNECT)
  void                SetClientReceiveMaximum(const ushort max);
  void                SetClientMaximumPacketSize(const uint max_size);

  //--- Getters
  ushort              GetReceiveMaximum() const;
  uint                GetMaximumPacketSize() const;
  ushort              GetClientReceiveMaximum() const;
  uint                GetClientMaximumPacketSize() const;

  //--- Flow control checks
  bool                CanSendQoSMessage(const uchar qos_level) const;
  bool                CanSendQoS1Message() const;
  bool                CanSendQoS2Message() const;
  uint                GetAvailableSlots() const;

  //--- Packet size validation
  bool                ValidateOutgoingPacketSize(const uint packet_size) const;
  bool                ValidateClientPacketSize(const uint packet_size) const;
  bool                ValidateIncomingPacketSize(const uint packet_size) const;

  //--- In-flight tracking
  bool                RegisterOutgoingQoS(const ushort packet_id, const uchar qos_level, const uint packet_size);
  ENUM_REG_QOS_RESULT RegisterIncomingQoS(const ushort packet_id, const uchar qos_level, const uint packet_size);
  bool                ReleaseIncomingQoS(const ushort packet_id);
  bool                ReleaseQoS(const ushort packet_id);
  bool                IsInFlight(const ushort packet_id) const;

  //--- Acknowledgment tracking
  void                OnPubackReceived(const ushort packet_id);
  void                OnPubrecReceived(const ushort packet_id);
  void                OnPubrelReceived(const ushort packet_id);
  void                OnPubcompReceived(const ushort packet_id);

  //--- Management
  uint                GetInFlightCount() const;
  ushort              GetIncomingInFlightCount() const;
  uint                GetInFlightQoS1Count() const;
  uint                GetInFlightQoS2Count() const;
  void                ResetTransientState();
  void                ResetServerLimits();
  void                ResetAll();
  void                Reset();
  void                ClearInFlight();

  //--- Timeout/Stalled detection
  uint                GetStalledMessageCount(const uint timeout_seconds) const;
  bool                HasStalledMessages(const uint timeout_seconds) const;
};

//+------------------------------------------------------------------+
//| Constructor                                                      |
//+------------------------------------------------------------------+
CFlowControl::CFlowControl()
    : m_receive_maximum(65535)
    , m_maximum_packet_size(0)
    , m_client_receive_maximum(65535)
    , m_client_maximum_packet_size(0)
    , m_in_flight_count(0)
    , m_incoming_inflight_count(0)
    , m_total_sent_qos1(0)
    , m_total_sent_qos2(0)
    , m_total_acked(0)
    , m_total_released(0)
    , m_cached_qos1_inflight(0)
    , m_cached_qos2_inflight(0) {
  ArrayResize(m_in_flight, 0);
  ArrayResize(m_in_flight_bitfield, 2048);
  ArrayInitialize(m_in_flight_bitfield, 0);
  //--- Initialize incoming flow control bitfield
  ArrayResize(m_incoming_bitfield, 2048);
  ArrayInitialize(m_incoming_bitfield, 0);
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CFlowControl::~CFlowControl() { ResetAll(); }

//+------------------------------------------------------------------+
//| SetReceiveMaximum                                                |
//| Purpose: Set server's Receive Maximum (from CONNACK)             |
//| Parameters: max - maximum in-flight QoS 1/2 messages             |
//| Note: Per spec §3.2.2.3.3, if the value is 0, the client MUST    |
//|       treat it as a Protocol Error. Value is used to limit the   |
//|       number of QoS 1 and QoS 2 PUBLISH packets in-flight.       |
//+------------------------------------------------------------------+
void CFlowControl::SetReceiveMaximum(const ushort max) {
  if (max == 0) {
    MQTT_LOG_WARN("Receive Maximum of 0 is a Protocol Error per MQTT spec §3.2.2.3.3");
    return;
  }
  m_receive_maximum = max;
}

//+------------------------------------------------------------------+
//| SetMaximumPacketSize                                             |
//| Purpose: Set server's Maximum Packet Size (from CONNACK)         |
//| Parameters: max_size - maximum packet size in bytes              |
//| Note: Per spec §3.2.2.3.5, this is the total packet size (fixed  |
//|       header + variable header + payload) the server accepts.    |
//+------------------------------------------------------------------+
void CFlowControl::SetMaximumPacketSize(const uint max_size) {
  if (max_size == 0) {
    MQTT_LOG_WARN("Maximum Packet Size of 0 is a Protocol Error per MQTT spec §3.2.2.3.5");
    return;
  }
  //--- Per spec, Maximum Packet Size must be at least 5 bytes
  if (max_size < 5) {
    MQTT_LOG_WARN("Maximum Packet Size " + (string)max_size + " is less than minimum 5 bytes");
    return;
  }
  m_maximum_packet_size = max_size;
}

//+------------------------------------------------------------------+
//| SetClientReceiveMaximum                                          |
//| Purpose: Set client's Receive Maximum (sent in CONNECT)          |
//| Parameters: max - max in-flight QoS 1/2 messages client accepts  |
//| Note: Per MQTT 5.0 §3.1.2.11.4, a value of 0 is a Protocol Error |
//+------------------------------------------------------------------+
void CFlowControl::SetClientReceiveMaximum(const ushort max) {
  if (max == 0) {
    MQTT_LOG_ERROR("Client Receive Maximum of 0 is a Protocol Error per MQTT spec §3.1.2.11.4 — value rejected");
    return;
  }
  m_client_receive_maximum = max;
}

//+------------------------------------------------------------------+
//| SetClientMaximumPacketSize                                       |
//| Purpose: Set client's Maximum Packet Size (sent in CONNECT)      |
//| Parameters: max_size - maximum packet size client accepts        |
//| Note: Per spec §3.1.2.11.5, value 0 is a Protocol Error; minimum |
//|       meaningful size is 5 bytes (1 fixed-header + 4 rem-length).|
//+------------------------------------------------------------------+
void CFlowControl::SetClientMaximumPacketSize(const uint max_size) {
  if (max_size == 0) {
    MQTT_LOG_WARN("Client Maximum Packet Size of 0 is a Protocol Error per MQTT spec §3.1.2.11.5");
    return;
  }
  if (max_size < 5) {
    MQTT_LOG_WARN("Client Maximum Packet Size " + (string)max_size + " is less than minimum 5 bytes");
    return;
  }
  m_client_maximum_packet_size = max_size;
}

//+------------------------------------------------------------------+
//| GetReceiveMaximum                                                |
//| Return: Server's Receive Maximum                                 |
//+------------------------------------------------------------------+
ushort CFlowControl::GetReceiveMaximum() const { return m_receive_maximum; }

//+------------------------------------------------------------------+
//| GetMaximumPacketSize                                             |
//| Return: Server's Maximum Packet Size (0 = no limit)              |
//+------------------------------------------------------------------+
uint   CFlowControl::GetMaximumPacketSize() const { return m_maximum_packet_size; }

//+------------------------------------------------------------------+
//| GetClientReceiveMaximum                                          |
//| Return: Client's Receive Maximum                                 |
//+------------------------------------------------------------------+
ushort CFlowControl::GetClientReceiveMaximum() const { return m_client_receive_maximum; }

//+------------------------------------------------------------------+
//| GetClientMaximumPacketSize                                       |
//| Return: Client's Maximum Packet Size (0 = no limit)              |
//+------------------------------------------------------------------+
uint   CFlowControl::GetClientMaximumPacketSize() const { return m_client_maximum_packet_size; }

//+------------------------------------------------------------------+
//| IsQoS1or2                                                        |
//| Purpose: Check if QoS level requires flow control tracking       |
//| Parameters: qos_level - QoS level (0, 1, or 2)                   |
//| Return: true if QoS 1 or 2                                       |
//| Note: Flow control is NOT applied to QoS 0 per MQTT 5.0 §4.9.    |
//+------------------------------------------------------------------+
bool   CFlowControl::IsQoS1or2(const uchar qos_level) const { return (qos_level == 1 || qos_level == 2); }

//+------------------------------------------------------------------+
//| FindInFlightMessage                                              |
//| Purpose: Find message in in-flight array by packet ID            |
//| Parameters: packet_id - packet identifier to search for          |
//| Return: Array index or -1 if not found                           |
//+------------------------------------------------------------------+
int    CFlowControl::FindInFlightMessage(const ushort packet_id) {
  if (!_InFlightBitTest(packet_id)) {
    return -1;                                    // O(1) bitfield check
  }
  int idx = -1;
  m_in_flight_index.TryGetValue(packet_id, idx);  // O(1) amortised HashMap lookup
  return idx;
}

//+------------------------------------------------------------------+
//| CanSendQoSMessage                                                |
//| Purpose: Check if a QoS message can be sent (flow control)       |
//| Parameters: qos_level - QoS level of message to send             |
//| Return: true if within Receive Maximum limit                     |
//+------------------------------------------------------------------+
bool CFlowControl::CanSendQoSMessage(const uchar qos_level) const {
  //--- QoS 0 messages are not subject to flow control
  if (qos_level == 0) {
    return true;
  }

  //--- Check if QoS level is valid for flow control
  if (!IsQoS1or2(qos_level)) {
    return false;
  }

  //--- Check if we have available slots
  return (GetAvailableSlots() > 0);
}

//+------------------------------------------------------------------+
//| CanSendQoS1Message                                               |
//| Purpose: Check if a QoS 1 message can be sent                    |
//| Return: true if within Receive Maximum limit                     |
//+------------------------------------------------------------------+
bool CFlowControl::CanSendQoS1Message() const { return CanSendQoSMessage(1); }

//+------------------------------------------------------------------+
//| CanSendQoS2Message                                               |
//| Purpose: Check if a QoS 2 message can be sent                    |
//| Return: true if within Receive Maximum limit                     |
//+------------------------------------------------------------------+
bool CFlowControl::CanSendQoS2Message() const { return CanSendQoSMessage(2); }

//+------------------------------------------------------------------+
//| GetAvailableSlots                                                |
//| Purpose: Get number of available in-flight slots                 |
//| Return: Available slots (0 if at limit)                          |
//+------------------------------------------------------------------+
uint CFlowControl::GetAvailableSlots() const {
  if (m_in_flight_count >= m_receive_maximum) {
    return 0;
  }
  //--- Use uint subtraction to avoid silent truncation if m_in_flight_count
  //--- ever exceeds ushort range due to an upstream bug.
  return (m_receive_maximum - (uint)m_in_flight_count);
}

//+------------------------------------------------------------------+
//| ValidateOutgoingPacketSize                                       |
//| Purpose: Validate outgoing packet vs servers Maximum Packet Size |
//| Parameters: packet_size - size of packet to send                 |
//| Return: true if packet size is acceptable or no limit set        |
//+------------------------------------------------------------------+
bool CFlowControl::ValidateOutgoingPacketSize(const uint packet_size) const {
  //--- If no Maximum Packet Size set, any size is valid
  if (m_maximum_packet_size == 0) {
    return true;
  }
  return (packet_size <= m_maximum_packet_size);
}

//+------------------------------------------------------------------+
//| ValidateClientPacketSize                                         |
//| Purpose: Validate packet vs client Maximum Packet Size           |
//| Parameters: packet_size - size of packet to validate             |
//| Return: true if packet size is acceptable or no limit set        |
//+------------------------------------------------------------------+
bool CFlowControl::ValidateClientPacketSize(const uint packet_size) const {
  if (m_client_maximum_packet_size == 0) {
    return true;
  }
  return (packet_size <= m_client_maximum_packet_size);
}

//+------------------------------------------------------------------+
//| ValidateIncomingPacketSize                                       |
//| Purpose: Validate incoming packet vs clients Maximum Packet Size |
//| Parameters: packet_size - size of received packet                |
//| Return: true if packet size is acceptable or no limit set        |
//+------------------------------------------------------------------+
bool CFlowControl::ValidateIncomingPacketSize(const uint packet_size) const {
  //--- If no client Maximum Packet Size set, any size is valid
  if (m_client_maximum_packet_size == 0) {
    return true;
  }
  return (packet_size <= m_client_maximum_packet_size);
}

//+------------------------------------------------------------------+
//| RegisterOutgoingQoS                                              |
//| Purpose: Register outgoing QoS 1/2 msg for flow control tracking |
//| Parameters: packet_id - packet identifier                        |
//|             qos_level - QoS level (1 or 2)                       |
//|             packet_size - size of the packet                     |
//| Return: true if registered successfully                          |
//+------------------------------------------------------------------+
bool CFlowControl::RegisterOutgoingQoS(const ushort packet_id, const uchar qos_level, const uint packet_size) {
  //--- Validate packet ID (0 is not valid per spec)
  if (packet_id == 0) {
    MQTT_LOG_ERROR("Packet ID 0 is not valid for QoS 1/2 messages");
    return false;
  }

  //--- Only track QoS 1 and 2
  if (!IsQoS1or2(qos_level)) {
    return true;  // QoS 0 doesn't need tracking
  }

  //--- Check if we can send (flow control)
  if (!CanSendQoSMessage(qos_level)) {
    MQTT_LOG_ERROR("Receive Maximum exceeded, cannot send QoS " + (string)(int)qos_level + " message");
    return false;
  }

  //--- Check if packet ID is already in use
  if (IsInFlight(packet_id)) {
    MQTT_LOG_ERROR("Packet ID " + (string)packet_id + " is already in-flight");
    return false;
  }

  //--- Add to in-flight tracking
  const uint new_idx = m_in_flight_count;
  //--- Use reserve for exponential growth (Receive Maximum can be up to 65535)
  ArrayResize(m_in_flight, new_idx + 1, 16);
  m_in_flight[new_idx].packet_id         = packet_id;
  m_in_flight[new_idx].qos_level         = qos_level;
  m_in_flight[new_idx].mono_timestamp_us = GetMicrosecondCount();
  m_in_flight[new_idx].packet_size       = packet_size;
  //--- Maintain O(1) lookup structures
  _InFlightBitSet(packet_id);
  m_in_flight_index.Add(packet_id, (int)new_idx);
  m_in_flight_count++;

  //--- Maintain cached QoS counts
  if (qos_level == 1) {
    m_cached_qos1_inflight++;
  } else if (qos_level == 2) {
    m_cached_qos2_inflight++;
  }

  //--- Update statistics
  if (qos_level == 1) {
    m_total_sent_qos1++;
  } else {
    m_total_sent_qos2++;
  }

  return true;
}

//+------------------------------------------------------------------+
//| RegisterIncomingQoS                                              |
//| Purpose: Track received QoS 1/2 msgs (for client-side limits)    |
//| Parameters: packet_id - packet identifier                        |
//|             qos_level - QoS level (1 or 2)                       |
//|             packet_size - size of the packet                     |
//| Return: true if registered successfully                          |
//+------------------------------------------------------------------+
ENUM_REG_QOS_RESULT CFlowControl::RegisterIncomingQoS(const ushort packet_id, const uchar qos_level,
                                                      const uint packet_size) {
  //--- QoS 0 is never subject to flow control
  if (!IsQoS1or2(qos_level)) {
    return REG_OK;
  }

  //--- Idempotent: if this packet ID is already registered as incoming in-flight
  //--- (e.g. broker retransmitted with DUP=1 before PUBREL), do not consume a
  //--- second slot. Return REG_DUPLICATE so the caller can re-send the ack
  //--- without re-delivering to the application. Returning a distinct value
  //--- (not REG_WINDOW_FULL) prevents: a new DUP=1 packet while the window
  //--- is full would otherwise be misidentified as a duplicate.
  if (_IncomingBitTest(packet_id)) {
    return REG_DUPLICATE;
  }

  //--- Enforce client Receive Maximum per MQTT 5.0 §4.9.1.
  //--- The broker MUST NOT send more unacknowledged QoS 1/2 PUBLISHes than the
  //--- value advertised by the client in the CONNECT Receive Maximum property.
  //--- If this limit is exceeded the caller MUST send DISCONNECT with reason
  //--- code 0x93 (Receive Maximum Exceeded) and close the connection.
  if (m_incoming_inflight_count >= m_client_receive_maximum) {
    MQTT_LOG_ERROR("Broker violated client Receive Maximum (" + (string)m_client_receive_maximum
                   + ") — send DISCONNECT 0x93 per MQTT §4.9.1");
    return REG_WINDOW_FULL;
  }

  //--- Increment the shared incoming slot counter (covers both QoS 1 and 2).
  //--- Released by ReleaseIncomingQoS() when PUBACK (QoS 1) or PUBCOMP (QoS 2)
  //--- is sent from the client to the broker.
  m_incoming_inflight_count++;

  //--- Track which packet IDs have active incoming flow control slots
  _IncomingBitSet(packet_id);

  //--- Incoming QoS flow-control uses a dedicated counter and does not share
  //--- the outgoing in-flight bitmap/index tables. Packet identifiers are
  //--- direction-scoped in MQTT and may overlap between incoming/outgoing flows.

  return REG_OK;
}

//+------------------------------------------------------------------+
//| ReleaseQoS                                                       |
//| Purpose: Remove message from in-flight tracking                  |
//| Parameters: packet_id - packet identifier to release             |
//| Return: true if message was found and released                   |
//+------------------------------------------------------------------+
bool CFlowControl::ReleaseQoS(const ushort packet_id) {
  if (!_InFlightBitTest(packet_id)) {
    return false;  // O(1) check
  }
  int idx = -1;
  m_in_flight_index.TryGetValue(packet_id, idx);
  if (idx < 0) {
    return false;
  }
  //--- Decrement cached QoS count before swap
  uchar released_qos = m_in_flight[idx].qos_level;
  if (released_qos == 1 && m_cached_qos1_inflight > 0) {
    m_cached_qos1_inflight--;
  } else if (released_qos == 2 && m_cached_qos2_inflight > 0) {
    m_cached_qos2_inflight--;
  }

  //--- Clear lookup structures for the released packet
  _InFlightBitClear(packet_id);
  m_in_flight_index.Remove(packet_id);

  //--- O(1) removal: swap with last element instead of shifting
  const uint last_idx = m_in_flight_count - 1;
  if ((uint)idx < last_idx) {
    m_in_flight[idx] = m_in_flight[last_idx];
    //--- Update moved element's index in the HashMap
    m_in_flight_index.Remove(m_in_flight[idx].packet_id);
    m_in_flight_index.Add(m_in_flight[idx].packet_id, idx);  // Update moved element's index
  }

  m_in_flight_count--;

  m_total_released++;
  return true;
}

//+------------------------------------------------------------------+
//| IsInFlight                                                       |
//| Purpose: Check if a packet ID is currently in-flight             |
//| Parameters: packet_id - packet identifier to check               |
//| Return: true if message is in-flight                             |
//+------------------------------------------------------------------+
bool CFlowControl::IsInFlight(const ushort packet_id) const {
  return _InFlightBitTest(packet_id);  // O(1) bitfield lookup
}

//+------------------------------------------------------------------+
//| OnPubackReceived                                                 |
//| Purpose: Handle PUBACK received for QoS 1 message                |
//| Parameters: packet_id - packet identifier from PUBACK            |
//+------------------------------------------------------------------+
void CFlowControl::OnPubackReceived(const ushort packet_id) {
  if (ReleaseQoS(packet_id)) {
    m_total_acked++;
  }
}

//+------------------------------------------------------------------+
//| OnPubrecReceived                                                 |
//| Purpose: Handle PUBREC received for QoS 2 message                |
//| Parameters: packet_id - packet identifier from PUBREC            |
//| NOTE: Intentionally empty — no flow-control state change is      |
//|       required here. Per MQTT 5.0 §4.3.3 the in-flight slot is   |
//|       held across the full PUBLISH → PUBREC → PUBREL → PUBCOMP   |
//|       exchange and is not released until PUBCOMP (see            |
//|       OnPubcompReceived). Caller is responsible for sending the  |
//|       outbound PUBREL packet in response to the received PUBREC. |
//+------------------------------------------------------------------+
void CFlowControl::OnPubrecReceived(const ushort packet_id) {
  //--- No state change needed: slot remains reserved until PUBCOMP.
}

//+------------------------------------------------------------------+
//| OnPubrelReceived                                                 |
//| Purpose: Handle PUBREL received for incoming QoS 2               |
//| Parameters: packet_id - packet identifier from PUBREL            |
//| NOTE: Intentionally empty at the outgoing-slot level — the       |
//|       in-flight slot for incoming QoS 2 is occupied from the     |
//|       initial PUBLISH receipt until the client sends PUBCOMP     |
//|       (not until PUBREL arrives). The Receive Maximum slot       |
//|       therefore remains held here and will be released by the    |
//|       caller via ReleaseIncomingQoS() after the PUBCOMP is sent. |
//|       Caller is responsible for sending the outbound PUBCOMP.    |
//+------------------------------------------------------------------+
void CFlowControl::OnPubrelReceived(const ushort packet_id) {
  //--- Incoming slot remains held until the caller sends PUBCOMP and
  //--- calls ReleaseIncomingQoS(packet_id). No state change here.
}

//+------------------------------------------------------------------+
//| OnPubcompReceived                                                |
//| Purpose: Handle PUBCOMP received for QoS 2 message               |
//| Parameters: packet_id - packet identifier from PUBCOMP           |
//+------------------------------------------------------------------+
void CFlowControl::OnPubcompReceived(const ushort packet_id) {
  if (ReleaseQoS(packet_id)) {
    m_total_acked++;
  }
}

//+------------------------------------------------------------------+
//| ReleaseIncomingQoS                                               |
//| Purpose: Release one incoming flow-control slot.                 |
//|          Call this when the client sends PUBACK (QoS 1) or       |
//|          PUBCOMP (QoS 2) for a message received from the broker. |
//| Parameters: packet_id - packet identifier (reserved for future   |
//|             per-ID validation; currently only the counter is     |
//|             decremented)                                         |
//| Return: true if a slot was released, false if counter was 0      |
//+------------------------------------------------------------------+
bool CFlowControl::ReleaseIncomingQoS(const ushort packet_id) {
  //--- Per-ID bitmap check makes release idempotent — prevents double-decrement
  //--- on PUBREL retransmission (defense-in-depth).
  if (!_IncomingBitTest(packet_id)) {
    return false;  // Not registered or already released — idempotent
  }
  _IncomingBitClear(packet_id);
  if (m_incoming_inflight_count > 0) {
    m_incoming_inflight_count--;
  }
  return true;
}

//+------------------------------------------------------------------+
//| GetInFlightCount                                                 |
//| Purpose: Get total number of in-flight messages                  |
//| Return: Count of in-flight messages                              |
//+------------------------------------------------------------------+
uint   CFlowControl::GetInFlightCount() const { return m_in_flight_count; }

//+------------------------------------------------------------------+
//| GetIncomingInFlightCount                                         |
//| Purpose: Get number of unacknowledged incoming QoS 1/2 messages  |
//|          from the broker (against the client Receive Maximum)    |
//| Return: Count of incoming in-flight messages                     |
//+------------------------------------------------------------------+
ushort CFlowControl::GetIncomingInFlightCount() const { return m_incoming_inflight_count; }

//+------------------------------------------------------------------+
//| GetInFlightQoS1Count                                             |
//| Purpose: Get count of in-flight QoS 1 messages                   |
//| Return: Count of QoS 1 messages in-flight                        |
//+------------------------------------------------------------------+
uint   CFlowControl::GetInFlightQoS1Count() const {
  return m_cached_qos1_inflight;  // O(1) cached count
}

//+------------------------------------------------------------------+
//| GetInFlightQoS2Count                                             |
//| Purpose: Get count of in-flight QoS 2 messages                   |
//| Return: Count of QoS 2 messages in-flight                        |
//+------------------------------------------------------------------+
uint CFlowControl::GetInFlightQoS2Count() const {
  return m_cached_qos2_inflight;  // O(1) cached count
}

//+------------------------------------------------------------------+
//| ClearInFlight                                                    |
//| Purpose: Remove all in-flight messages                           |
//+------------------------------------------------------------------+
void CFlowControl::ClearInFlight() {
  //--- Clear lookup structures for all active entries
  for (uint i = 0; i < m_in_flight_count; i++) {
    _InFlightBitClear(m_in_flight[i].packet_id);
  }
  m_in_flight_index.Clear();
  ArrayResize(m_in_flight, 0);
  m_in_flight_count      = 0;
  m_cached_qos1_inflight = 0;
  m_cached_qos2_inflight = 0;
}

//+------------------------------------------------------------------+
//| Reset                                                            |
//| Purpose: Reset all state (for new session)                       |
//+------------------------------------------------------------------+
void CFlowControl::ResetTransientState() {
  ClearInFlight();
  m_incoming_inflight_count = 0;
  //--- Reset incoming flow control bitfield
  ArrayInitialize(m_incoming_bitfield, 0);
  m_total_sent_qos1 = 0;
  m_total_sent_qos2 = 0;
  m_total_acked     = 0;
  m_total_released  = 0;
}

//+------------------------------------------------------------------+
//| ResetServerLimits                                                |
//| Purpose: Clear CONNACK-derived server limits for a fresh         |
//|          network connection while preserving client config       |
//+------------------------------------------------------------------+
void CFlowControl::ResetServerLimits() {
  m_receive_maximum     = 65535;
  m_maximum_packet_size = 0;
}

//+------------------------------------------------------------------+
//| ResetAll                                                         |
//| Purpose: Reset both transient state and configured limits        |
//+------------------------------------------------------------------+
void CFlowControl::ResetAll() {
  ResetTransientState();
  ResetServerLimits();
  m_client_receive_maximum     = 65535;
  m_client_maximum_packet_size = 0;
}

//+------------------------------------------------------------------+
//| Reset                                                            |
//| Purpose: Reset connection-scoped state while preserving client   |
//|          CONNECT-advertised limits                               |
//+------------------------------------------------------------------+
void CFlowControl::Reset() {
  ResetTransientState();
  ResetServerLimits();
}

//+------------------------------------------------------------------+
//| GetStalledMessageCount                                           |
//| Purpose: Get count of messages that have timed out               |
//| Parameters: timeout_seconds - timeout threshold                  |
//| Return: Count of stalled messages                                |
//+------------------------------------------------------------------+
uint CFlowControl::GetStalledMessageCount(const uint timeout_seconds) const {
  //--- Use monotonic GetMicrosecondCount() instead of TimeLocal() to avoid
  //--- clock-skew issues (NTP sync, DST transitions, VM time drift).
  const ulong now_us     = GetMicrosecondCount();
  const ulong timeout_us = (ulong)timeout_seconds * 1000000;
  uint        count      = 0;
  for (uint i = 0; i < m_in_flight_count; i++) {
    if ((now_us - m_in_flight[i].mono_timestamp_us) > timeout_us) {
      count++;
    }
  }
  return count;
}

//+------------------------------------------------------------------+
//| HasStalledMessages                                               |
//| Purpose: Check if any messages have timed out                    |
//| Parameters: timeout_seconds - timeout threshold                  |
//| Return: true if stalled messages exist                           |
//+------------------------------------------------------------------+
bool CFlowControl::HasStalledMessages(const uint timeout_seconds) const {
  return (GetStalledMessageCount(timeout_seconds) > 0);
}

#endif  // MQTT_FLOWCONTROL_MQH
