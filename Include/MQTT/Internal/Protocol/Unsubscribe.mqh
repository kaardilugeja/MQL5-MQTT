//+------------------------------------------------------------------+
//|                                                  Unsubscribe.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 UNSUBSCRIBE packet implementation per spec §3.10.       |
//| Used to unsubscribe from topics on the broker.                   |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_UNSUBSCRIBE_MQH
#define MQTT_INTERNAL_PROTOCOL_UNSUBSCRIBE_MQH

#include "..\\..\\MQTT.mqh"

//+------------------------------------------------------------------+
//| Class CUnsubscribe                                               |
//| Purpose: Class for building MQTT UNSUBSCRIBE packets             |
//| Usage:   Used to unsubscribe from MQTT topics                    |
//|          Supports multiple topic filters per MQTT 5.0 spec       |
//+------------------------------------------------------------------+
class CUnsubscribe {
 private:
  //--- Packet length tracking
  uint   m_remlen;          // Remaining length
  uint   m_remlen_bytes;    // Bytes needed for remaining length
  uint   m_propslen;        // Properties length
  uint   m_propslen_bytes;  // Bytes needed for properties length
  ushort m_pktid;           // Packet identifier

  //--- Store the encoded topic filters
  struct TopicFilterEntry {
    uchar filter[];  // UTF-8 encoded topic filter
  };
  TopicFilterEntry m_topic_filters[];
  int              m_topic_filter_count;

  //--- Properties buffer
  uchar            m_properties[];

  //--- Internal methods for packet construction
  void             AddRemainingLength(uchar &pkt[], uint idx = 1);
  void             AddPropertyLength(uchar &pkt[]);
  static bool      IsValidTopicFilter(const string filter);

 public:
  //--- Constructor declarations
  CUnsubscribe();
  ~CUnsubscribe();

  //--- Build the final UNSUBSCRIBE packet
  void   Build(uchar &pkt[], CSessionDatabase *db = NULL);

  //--- Add topic filter to the unsubscribe list
  void   AddTopicFilter(const string topic);

  //--- Set user property (appends to existing user properties)
  void   SetUserProperty(const string key, const string val);

  //--- Set packet identifier
  void   SetPacketId(ushort pktid) { m_pktid = pktid; }
  ushort GetPacketId() const { return m_pktid; }
};

//+------------------------------------------------------------------+
//| AddTopicFilter                                                   |
//| Purpose: Add topic filter to unsubscribe list                    |
//| Parameters: topic - [IN] topic filter to unsubscribe from        |
//| Note: Can be called multiple times for multiple topics.          |
//+------------------------------------------------------------------+
bool CUnsubscribe::IsValidTopicFilter(const string filter) {
  if (StringLen(filter) == 0) {
    return false;
  }

  string segments[];
  int    count = StringSplit(filter, '/', segments);
  if (count <= 0) {
    return false;
  }

  for (int i = 0; i < count; i++) {
    string segment = segments[i];
    if (StringFind(segment, "#") >= 0) {
      if (segment != "#" || i != count - 1) {
        return false;
      }
    }
    if (StringFind(segment, "+") >= 0 && segment != "+") {
      return false;
    }
  }

  return true;
}

//+------------------------------------------------------------------+
//| AddTopicFilter                                                   |
//| Purpose: Add topic filter to unsubscribe list                    |
//| Parameters: topic - [IN] topic filter to unsubscribe from        |
//| Note: Can be called multiple times for multiple topics.          |
//+------------------------------------------------------------------+
void CUnsubscribe::AddTopicFilter(const string topic) {
  if (!IsValidTopicFilter(topic)) {
    MQTT_LOG_ERROR("Topic Filter is invalid per MQTT §4.7.1.2: " + topic);
    return;
  }

  //--- Encode topic filter first so invalid UTF-8 does not allocate a dead entry.
  uchar aux[];
  if (!EncodeUTF8String(topic, aux)) {
    return;
  }

  //--- Resize topic filters array to accommodate new entry
  int new_idx = m_topic_filter_count;
  //--- Use reserve for exponential growth
  ArrayResize(m_topic_filters, m_topic_filter_count + 1, 8);
  m_topic_filter_count++;

  //--- Copy encoded result to the new topic filter entry
  ArrayCopy(m_topic_filters[new_idx].filter, aux);
}

//+------------------------------------------------------------------+
//| SetUserProperty                                                  |
//| Purpose: Set user property per §3.10.2.1.2                       |
//| Parameters: key - [IN] property name                             |
//|             val - [IN] property value                            |
//| Note: Can be called multiple times to add multiple user          |
//|       properties. They are appended to the properties buffer.    |
//+------------------------------------------------------------------+
void CUnsubscribe::SetUserProperty(const string key, const string val) {
  //--- Delegate to shared helper
  AppendUserProperty(m_properties, key, val);

  //--- Update properties length
  m_propslen = ArraySize(m_properties);
}

//+------------------------------------------------------------------+
//| AddPropertyLength                                                |
//| Purpose: Add property length to packet                           |
//| Parameters: pkt - [IN/OUT] packet buffer                         |
//+------------------------------------------------------------------+
void CUnsubscribe::AddPropertyLength(uchar &pkt[]) {
  //--- Temporary buffer for variable byte integer
  uchar aux[] = {};

  //--- Encode properties length as variable byte integer
  EncodeVariableByteInteger(m_propslen, aux);

  //--- Insert at position after packet type, remlen, and packet ID
  ArrayCopy(pkt, aux, m_remlen_bytes + 3);
  //--- Note: m_remlen already includes m_propslen_bytes from Build() calculation
  //--- Do NOT add m_propslen_bytes here to avoid double-counting
}

//+------------------------------------------------------------------+
//| AddRemainingLength                                               |
//| Purpose: Add remaining length to packet                          |
//| Parameters: pkt - [IN/OUT] packet buffer                         |
//|             idx - [IN] index where to insert                     |
//+------------------------------------------------------------------+
void CUnsubscribe::AddRemainingLength(uchar &pkt[], uint idx = 1) {
  //--- Temporary buffer for variable byte integer
  uchar aux[] = {};

  //--- Encode remaining length as variable byte integer
  EncodeVariableByteInteger(m_remlen, aux);

  //--- Insert at specified index (after packet type byte)
  ArrayCopy(pkt, aux, idx, 0, aux.Size());
}

//+------------------------------------------------------------------+
//| Build - Assemble the final UNSUBSCRIBE packet binary buffer      |
//| Purpose: Compile variable header and topic filters into binary   |
//| Parameters: pkt - [OUT] the resulting UNSUBSCRIBE packet bytes   |
//|             db  - [IN] optional session database for pktid       |
//| Note: Implements the assembly sequence defined in MQTT 5.0 §3.10 |
//+------------------------------------------------------------------+
void CUnsubscribe::Build(uchar &pkt[], CSessionDatabase *db) {
  //--- Per MQTT §3.10.3: Payload MUST contain at least one Topic Filter
  if (m_topic_filter_count == 0) {
    MQTT_LOG_ERROR("UNSUBSCRIBE MUST contain at least one Topic Filter per §3.10.3");
    ArrayResize(pkt, 0);
    return;
  }

  //--- 1. Calculate Required Lengths (§3.10.2 and §3.10.3)
  //--- Step 1a: Sum the size of all Topic Filters to be removed
  uint topic_filters_size = 0;
  for (int i = 0; i < m_topic_filter_count; i++) {
    topic_filters_size += ArraySize(m_topic_filters[i].filter);
  }

  //--- Step 1b: Calculate Property Section length
  m_propslen_bytes = GetVarintBytes(m_propslen);

  //--- Step 1c: Calculate Remaining Length (§2.1.3)
  //--- Fixed Header (2 bytes for PktID) + Property Length + Properties + Topic Filters
  m_remlen         = 2 + m_propslen_bytes + m_propslen + topic_filters_size;

  //--- Step 1d: Calculate bytes needed for the Remaining Length varint
  m_remlen_bytes   = GetVarintBytes(m_remlen);

  //--- 2. Resource Allocation
  //--- Total size = Fixed Hdr Byte (1) + RemLen Varint + Payload
  ArrayResize(pkt, 1 + m_remlen_bytes + m_remlen);

  //--- 3. Construction: Fixed Header (§2.1)
  //--- Type = UNSUBSCRIBE (10). Bit 1 must be 1 per spec (§3.10.1).
  pkt[0] = (UNSUBSCRIBE << 4) | 2;

  //--- 3b: Set the Remaining Length field
  AddRemainingLength(pkt);

  //--- 4. Construction: Variable Header (§3.10.2)
  //--- 4a: Packet Identifier (§3.10.2.1)
  if (m_pktid == 0) {
    m_pktid = SetPacketIdentifierEx(pkt, m_remlen_bytes + 1, db);
  } else {
    WritePacketIdentifier(pkt, m_remlen_bytes + 1, m_pktid);
  }

  //--- 4b: Properties (§3.10.2.1.2) - Includes Property Length indicator
  AddPropertyLength(pkt);

  //--- Copy property bytes into the buffer
  uint props_start = 1 + m_remlen_bytes + 2 + m_propslen_bytes;
  ArrayCopy(pkt, m_properties, props_start);

  //--- 5. Construction: Payload (§3.10.3)
  //--- Append each Topic Filter to be unsubscribed from
  uint topic_start = props_start + m_propslen;
  for (int i = 0; i < m_topic_filter_count; i++) {
    ArrayCopy(pkt, m_topic_filters[i].filter, topic_start);
    topic_start += ArraySize(m_topic_filters[i].filter);
  }
}

//+------------------------------------------------------------------+
//| Constructor - initializes remaining length                       |
//+------------------------------------------------------------------+
CUnsubscribe::CUnsubscribe() {
  //--- Initialize remaining length & bytes needed for remaining length
  m_remlen             = 0;
  m_remlen_bytes       = 1;
  m_propslen           = 0;
  m_propslen_bytes     = 1;
  m_topic_filter_count = 0;
  m_pktid              = 0;
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CUnsubscribe::~CUnsubscribe() {}

#endif  // MQTT_UNSUBSCRIBE_MQH
