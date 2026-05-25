//+------------------------------------------------------------------+
//|                                                    Subscribe.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 SUBSCRIBE packet implementation per spec §3.8.          |
//| Used to subscribe to topic filters from the broker.              |
//| Shared Subscription support per spec §4.8.                       |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_SUBSCRIBE_MQH
#define MQTT_INTERNAL_PROTOCOL_SUBSCRIBE_MQH

#include "..\\..\\MQTT.mqh"
#include "..\\Util\\PropertyEncoder.mqh"

//+------------------------------------------------------------------+
//| Class CSubscribe                                                 |
//| Purpose: Class for building MQTT SUBSCRIBE packets               |
//| Usage:   Used to subscribe to MQTT topics                        |
//|          Supports multiple topic filters per MQTT 5.0 spec       |
//+------------------------------------------------------------------+
class CSubscribe {
 private:
  //--- Packet length tracking
  uint   m_remlen;          // Remaining length
  uint   m_remlen_bytes;    // Bytes needed for remaining length
  uint   m_propslen;        // Properties length
  uint   m_propslen_bytes;  // Bytes needed for properties length
  ushort m_pktid;           // Packet identifier

  //--- Internal methods for packet construction
  void   AddRemainingLength(uchar &pkt[], uint idx = 1);  // Add remaining length
  void   AddPropertyLength(uchar &pkt[]);                 // Add property length

 protected:
  //--- Store the encoded topic filters
  struct TopicFilterEntry {
    uchar filter[];                       // UTF-8 encoded topic filter + subscription options
  };

  TopicFilterEntry m_topic_filters[];     // Array of topic filters
  int              m_topic_filter_count;  // Count of topic filters

  //--- Properties buffer
  uchar            m_properties[];

 public:
  //--- Constructor declarations
  CSubscribe();
  ~CSubscribe();

  //--- Build the final SUBSCRIBE packet
  void          Build(uchar &pkt[], CSessionDatabase *db = NULL);

  //--- Set subscription identifier property per §3.8.2.1.2 (Property 0x0B)
  void          SetSubscriptionIdentifier(uint sub_id);

  //--- Set user property per §3.8.2.1.2 (Property 0x26)
  void          SetUserProperty(const string key, const string val);

  //--- Set topic filter with subscription options
  void          SetTopicFilter(const string topic_filter, uchar subopts_flags = 0);

  //--- Set a Shared Subscription topic filter ( §4.8)
  //--- Constructs and validates a "$share/<group_id>/<topic_filter>" filter.
  //--- Note: Shared subscriptions do not support No Local (subopts bit 2) per §4.8.6.
  bool          SetSharedTopicFilter(const string group_id, const string topic_filter, uchar subopts_flags = 0);

  //--- Set / get packet identifier
  void          SetPacketId(ushort pktid) { m_pktid = pktid; }
  ushort        GetPacketId() const { return m_pktid; }

  //--- Static helpers for Shared Subscription topic filter validation and construction
  static bool   IsSharedSubscriptionFilter(const string filter);
  static bool   IsValidTopicFilter(const string filter);
  static string BuildSharedTopicFilter(const string group_id, const string topic_filter);
};

//+------------------------------------------------------------------+
//| CSubscribe::IsSharedSubscriptionFilter                           |
//| Purpose: Check if a topic filter is a valid Shared Subscription  |
//|          format ($share/<GroupId>/<TopicFilter>) per §4.8.       |
//| Parameters: filter - topic filter string to check                |
//| Return: true if the filter starts with '$share/' and has a valid |
//|         GroupId and actual topic filter segment.                 |
//+------------------------------------------------------------------+
bool CSubscribe::IsSharedSubscriptionFilter(const string filter) {
  //--- Shared subscriptions must start with "$share/"
  if (StringLen(filter) < 9) {
    return false;  // "$share/a/" is the minimum
  }
  if (StringSubstr(filter, 0, 7) != "$share/") {
    return false;
  }

  //--- Find the second '/' separator (after GroupId)
  int second_slash = StringFind(filter, "/", 7);
  if (second_slash < 8) {
    return false;  // GroupId must be at least 1 char
  }
  if (second_slash >= StringLen(filter) - 1) {
    return false;  // TopicFilter must be non-empty
  }

  //--- GroupId must not contain '+', '#', or '/'
  string group_id = StringSubstr(filter, 7, second_slash - 7);
  if (StringFind(group_id, "+") >= 0 || StringFind(group_id, "#") >= 0 || StringFind(group_id, "/") >= 0) {
    return false;
  }

  return true;
}

//+------------------------------------------------------------------+
//| CSubscribe::IsValidTopicFilter                                   |
//| Purpose: Validate topic filter wildcard placement per §4.7.1.2   |
//| Parameters: filter - topic filter string to check                |
//| Return: true if the filter is syntactically valid                |
//+------------------------------------------------------------------+
bool CSubscribe::IsValidTopicFilter(const string filter) {
  if (StringLen(filter) == 0) {
    return false;
  }

  if (StringSubstr(filter, 0, 7) == "$share/") {
    if (!IsSharedSubscriptionFilter(filter)) {
      return false;
    }
    int second_slash = StringFind(filter, "/", 7);
    if (second_slash < 0 || second_slash >= StringLen(filter) - 1) {
      return false;
    }
    return IsValidTopicFilter(StringSubstr(filter, second_slash + 1));
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
//| CSubscribe::BuildSharedTopicFilter                               |
//| Purpose: Build a canonical Shared Subscription topic filter      |
//|          string from its parts per §4.8.                         |
//| Parameters: group_id     - ShareName (must not contain +,#,/)    |
//|             topic_filter - the actual topic filter               |
//| Return: "$share/<group_id>/<topic_filter>" or "" on invalid input|
//+------------------------------------------------------------------+
string CSubscribe::BuildSharedTopicFilter(const string group_id, const string topic_filter) {
  //--- Validate group_id
  if (StringLen(group_id) == 0) {
    MQTT_LOG_ERROR("Shared Subscription GroupId must not be empty");
    return "";
  }
  if (StringFind(group_id, "+") >= 0 || StringFind(group_id, "#") >= 0 || StringFind(group_id, "/") >= 0) {
    MQTT_LOG_ERROR("Shared Subscription GroupId must not contain '+', '#', or '/'");
    return "";
  }
  //--- Validate topic_filter is non-empty
  if (StringLen(topic_filter) == 0) {
    MQTT_LOG_ERROR("Shared Subscription TopicFilter must not be empty");
    return "";
  }
  if (!IsValidTopicFilter(topic_filter)) {
    MQTT_LOG_ERROR("Shared Subscription TopicFilter is invalid per MQTT §4.7.1.2");
    return "";
  }
  return "$share/" + group_id + "/" + topic_filter;
}

//+------------------------------------------------------------------+
//| SetTopicFilter                                                   |
//| Purpose: Set topic filter for subscription                       |
//| Parameters: topic_filter - [IN] topic filter string              |
//|            subopts_flags - [IN] subscription options (QoS, etc.) |
//| Note: Can be called multiple times to subscribe to multiple      |
//|       topic filters in a single SUBSCRIBE packet.                |
//+------------------------------------------------------------------+
void CSubscribe::SetTopicFilter(const string topic_filter, uchar subopts_flags) {
  if (!IsValidTopicFilter(topic_filter)) {
    MQTT_LOG_ERROR("Topic Filter is invalid per MQTT §4.7.1.2: " + topic_filter);
    return;
  }
  //--- Encode topic filter to UTF-8 format before growing the array.
  //--- Invalid UTF-8 must not leave behind a partially initialised entry.
  uchar aux[];
  if (!EncodeUTF8String(topic_filter, aux)) {
    return;
  }

  //--- Resize topic filters array to accommodate new entry
  int new_idx = m_topic_filter_count;
  //--- Use reserve for exponential growth
  ArrayResize(m_topic_filters, m_topic_filter_count + 1, 8);
  m_topic_filter_count++;

  //--- Resize to fit UTF-8 encoded bytes + 1 byte for subscription options
  //--- EncodeUTF8String already adds 2 bytes for length prefix
  ArrayResize(aux, aux.Size() + 1);

  //--- Set the last byte to subscription options flags
  aux[aux.Size() - 1] = subopts_flags;

  //--- Copy encoded result to the new topic filter entry
  ArrayCopy(m_topic_filters[new_idx].filter, aux);
}

//+------------------------------------------------------------------+
//| SetSharedTopicFilter                                             |
//| Purpose: Subscribe to a Shared Subscription per §4.8.            |
//| Parameters: group_id - [IN] ShareName (no '+','#','/')           |
//|             topic_filter - [IN] actual topic filter              |
//|             subopts_flags - [IN] subscription options            |
//| Return: true on success, false if arguments are invalid          |
//| Reference: MQTT 5.0 §4.8                                         |
//+------------------------------------------------------------------+
bool CSubscribe::SetSharedTopicFilter(const string group_id, const string topic_filter, uchar subopts_flags) {
  //--- The No Local option (bit 2) MUST NOT be set for shared subscriptions §4.8.6
  if ((subopts_flags & MQTT_SUB_OPTS_NON_LOCAL) != 0) {
    MQTT_LOG_ERROR("'No Local' flag (0x04) is not permitted for Shared Subscriptions per MQTT 5.0 §4.8.6");
    return false;
  }

  //--- Build and validate the composite filter
  string shared_filter = BuildSharedTopicFilter(group_id, topic_filter);
  if (shared_filter == "") {
    return false;
  }

  SetTopicFilter(shared_filter, subopts_flags);
  return true;
}

//+------------------------------------------------------------------+
//| SetUserProperty                                                  |
//| Purpose: Set user property per §3.8.2.1.2                        |
//| Parameters: key - [IN] property name                             |
//|             val - [IN] property value                            |
//| Note: Can be called multiple times to add multiple user          |
//|       properties. They are appended to the properties buffer.    |
//+------------------------------------------------------------------+
void CSubscribe::SetUserProperty(const string key, const string val) {
  CPropertyEncoder::EncodeStringPairProperty(m_properties, MQTT_PROP_IDENTIFIER_USER_PROPERTY, key, val);
  m_propslen = ArraySize(m_properties);
}

//+------------------------------------------------------------------+
//| SetSubscriptionIdentifier                                        |
//| Purpose: Set subscription identifier per §3.8.2.1.2              |
//| Parameters: sub_id - [IN] subscription identifier (1-268435455)  |
//+------------------------------------------------------------------+
void CSubscribe::SetSubscriptionIdentifier(uint sub_id) {
  if (sub_id < 1 || sub_id > 0xfffffff) {
    MQTT_LOG_ERROR("Subscription Identifier must be between 1 and 268,435,455");
    return;
  }

  CPropertyEncoder::EncodeVariableByteIntegerProperty(m_properties, MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER,
                                                      sub_id);
  m_propslen = ArraySize(m_properties);
}

//+------------------------------------------------------------------+
//| AddPropertyLength                                                |
//| Purpose: Add property length to packet                           |
//| Parameters: pkt - [IN/OUT] packet buffer                         |
//+------------------------------------------------------------------+
void CSubscribe::AddPropertyLength(uchar &pkt[]) {
  //--- Temporary buffer for variable byte integer
  uchar aux[];

  //--- Encode properties length as variable byte integer
  EncodeVariableByteInteger(m_propslen, aux);

  //--- Insert at position after packet type, remlen, and packet ID
  ArrayCopy(pkt, aux, m_remlen_bytes + 3);

  //--- Calculate bytes needed for properties length
  m_propslen_bytes = GetVarintBytes(m_propslen);

  //--- Note: m_remlen already includes m_propslen_bytes from Build() calculation
  //--- Do NOT add m_propslen_bytes here to avoid double-counting
}

//+------------------------------------------------------------------+
//| AddRemainingLength                                               |
//| Purpose: Add remaining length to packet                          |
//| Parameters: pkt - [IN/OUT] packet buffer                         |
//|             idx - [IN] index where to insert                     |
//+------------------------------------------------------------------+
void CSubscribe::AddRemainingLength(uchar &pkt[], uint idx = 1) {
  //--- Temporary buffer for variable byte integer
  uchar aux[];

  //--- Encode remaining length as variable byte integer
  EncodeVariableByteInteger(m_remlen, aux);

  //--- Insert at specified index (after packet type byte)
  ArrayCopy(pkt, aux, idx, 0, aux.Size());
}

//+------------------------------------------------------------------+
//| Build - Assemble the final SUBSCRIBE packet binary buffer        |
//| Purpose: Compile variable header and topic filters into binary   |
//| Parameters: pkt - [OUT] the resulting SUBSCRIBE packet bytes     |
//|             db  - [IN] optional session database for pktid       |
//| Note: Implements the assembly sequence defined in MQTT 5.0 §3.8  |
//+------------------------------------------------------------------+
void CSubscribe::Build(uchar &pkt[], CSessionDatabase *db) {
  //--- Per MQTT §3.8.3: Payload MUST contain at least one Topic Filter
  if (m_topic_filter_count == 0) {
    MQTT_LOG_ERROR("SUBSCRIBE MUST contain at least one Topic Filter per §3.8.3");
    ArrayResize(pkt, 0);
    return;
  }

  //--- 1. Calculate Required Lengths (§3.8.2 and §3.8.3)
  //--- Step 1a: Sum the size of all Topic Filters + Subscribe Options
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
  //--- Type = SUBSCRIBE (8). Bit 1 must be 1 per spec (§3.8.1).
  pkt[0] = (SUBSCRIBE << 4) | 2;

  //--- 3b: Set the Remaining Length field
  AddRemainingLength(pkt);

  //--- 4. Construction: Variable Header (§3.8.2)
  //--- 4a: Packet Identifier (§3.8.2.1)
  if (m_pktid == 0) {
    m_pktid = SetPacketIdentifierEx(pkt, m_remlen_bytes + 1, db);
  } else {
    WritePacketIdentifier(pkt, m_remlen_bytes + 1, m_pktid);
  }

  //--- 4b: Properties (§3.8.2.1.2) - Includes Property Length indicator
  AddPropertyLength(pkt);

  //--- Copy property bytes into the buffer
  uint props_start = 1 + m_remlen_bytes + 2 + m_propslen_bytes;
  ArrayCopy(pkt, m_properties, props_start);

  //--- 5. Construction: Payload (§3.8.3)
  //--- Append each Topic Filter + Subscription Options pair
  uint topic_start = props_start + m_propslen;
  for (int i = 0; i < m_topic_filter_count; i++) {
    ArrayCopy(pkt, m_topic_filters[i].filter, topic_start);
    topic_start += ArraySize(m_topic_filters[i].filter);
  }
}

//+------------------------------------------------------------------+
//| Default constructor                                              |
//+------------------------------------------------------------------+
CSubscribe::CSubscribe() {
  //--- Initialize remaining length & related fields
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
CSubscribe::~CSubscribe() {}

#endif  // MQTT_SUBSCRIBE_MQH
