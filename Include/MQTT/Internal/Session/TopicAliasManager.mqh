//+------------------------------------------------------------------+
//|                                            TopicAliasManager.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Topic Alias Manager for MQTT 5.0 topic aliasing per §3.3.2.3.4.  |
//| Reduces packet size by replacing topic names with alias values.  |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_SESSION_TOPICALIASMANAGER_MQH
#define MQTT_INTERNAL_SESSION_TOPICALIASMANAGER_MQH

#include <Generic\HashMap.mqh>

//+------------------------------------------------------------------+
//| Class CTopicAliasManager                                         |
//| Purpose: Manage Topic Aliases per MQTT 5.0 §3.3.2.3.4.           |
//|                                                                  |
//| Background:                                                      |
//|   A Topic Alias is an integer value that replaces the topic name |
//|   in a PUBLISH packet. This reduces the size of the packet.      |
//|   Mappings are scoped to the current Network Connection and last |
//|   until the connection is closed (§3.3.2.3.4).                   |
//+------------------------------------------------------------------+
class CTopicAliasManager {
 private:
  //--- Internal storage for client-to-server mappings (client sends PUBLISH with alias)
  CHashMap<string, ushort> m_client_topic_to_alias;
  CHashMap<ushort, string> m_client_alias_to_topic;

  //--- LRU tracking for client alias recycling.
  //--- A binary min-heap keeps the oldest alias at slot 0 for O(1) lookup and
  //--- O(log n) touch/eviction updates when the alias table is full.
  ushort                   m_client_lru_heap_alias[];
  ulong                    m_client_lru_heap_ts[];
  uint                     m_client_lru_heap_count;
  uint                     m_client_alias_heap_pos[];  // alias -> heap index + 1 (0 = absent)

  //--- Internal storage for server-to-client mappings (server sends PUBLISH with alias)
  CHashMap<ushort, string> m_server_alias_to_topic;

  //--- Maximum topic alias value allowed (from CONNACK Topic Alias Maximum property)
  ushort                   m_topic_alias_maximum;

  //--- Client-side Topic Alias Maximum (advertised in CONNECT per §3.1.2.11.8)
  //--- Limits the number of aliases the server may use in incoming PUBLISH packets.
  ushort                   m_client_topic_alias_maximum;

  //--- Current highest client alias in use (for auto-assignment)
  ushort                   m_highest_client_alias;

  void                     _EnsureClientHeapCapacity(const ushort alias);
  void                     _SwapClientHeapEntries(uint left, uint right);
  void                     _SiftClientAliasUp(uint index);
  void                     _SiftClientAliasDown(uint index);
  void                     _TrackClientAliasUsage(const ushort alias, const ulong used_ms);
  void                     _RemoveClientAliasUsage(const ushort alias);

  //--- Find the least-recently-used client alias for eviction
  ushort                   _FindLRUClientAlias();

 public:
  //--- Constructor/Destructor
  CTopicAliasManager();
  ~CTopicAliasManager();

  //--- Configuration
  void   SetTopicAliasMaximum(const ushort max);
  ushort GetTopicAliasMaximum() const;
  void   SetClientTopicAliasMaximum(const ushort max);
  ushort GetClientTopicAliasMaximum() const;

  //--- Validation
  bool   IsValidAlias(const ushort alias) const;
  bool   IsAliasAvailable(const ushort alias) const;

  //--- Client-to-server mappings (outgoing PUBLISH)
  bool   RegisterClientAlias(const string &topic_name, const ushort alias);
  bool   RegisterClientAliasAuto(const string &topic_name, ushort &assigned_alias);
  ushort GetClientAlias(const string &topic_name);
  bool   HasClientTopic(const string &topic_name);
  bool   HasClientAlias(const ushort alias);

  //--- Explicit alias deregistration (frees the alias slot for reuse)
  bool   DeregisterClientAlias(const string &topic_name);

  //--- Update usage timestamp for an alias (call on every PUBLISH using it)
  void   TouchClientAlias(const ushort alias);

  //--- Server-to-client mappings (incoming PUBLISH)
  bool   RegisterServerAlias(const string &topic_name, const ushort alias);
  string ResolveServerAlias(const ushort alias);
  bool   HasServerAlias(const ushort alias);

  //--- Management
  void   ClearClientMappings();
  void   ClearServerMappings();
  void   ClearAll();
  uint   GetClientMappingCount();
  uint   GetServerMappingCount();
};

//+------------------------------------------------------------------+
//| Constructor                                                      |
//+------------------------------------------------------------------+
CTopicAliasManager::CTopicAliasManager()
    : m_topic_alias_maximum(0)
    , m_client_topic_alias_maximum(0)
    , m_highest_client_alias(0)
    , m_client_lru_heap_count(0) {}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CTopicAliasManager::~CTopicAliasManager() { ClearAll(); }

//+------------------------------------------------------------------+
//| _EnsureClientHeapCapacity                                        |
//| Purpose: Ensure alias->heap-position storage covers this alias   |
//+------------------------------------------------------------------+
void CTopicAliasManager::_EnsureClientHeapCapacity(const ushort alias) {
  int current_size = ArraySize(m_client_alias_heap_pos);
  if (current_size > (int)alias) {
    return;
  }

  int new_size = (int)alias + 1;
  int old_size = current_size;
  ArrayResize(m_client_alias_heap_pos, new_size, 16);
  for (int i = old_size; i < new_size; i++) {
    m_client_alias_heap_pos[i] = 0;
  }
}

//+------------------------------------------------------------------+
//| _SwapClientHeapEntries                                           |
//| Purpose: Swap two heap nodes and refresh alias positions         |
//+------------------------------------------------------------------+
void CTopicAliasManager::_SwapClientHeapEntries(uint left, uint right) {
  if (left == right) {
    return;
  }

  ushort left_alias              = m_client_lru_heap_alias[left];
  ushort right_alias             = m_client_lru_heap_alias[right];
  ulong  left_ts                 = m_client_lru_heap_ts[left];

  m_client_lru_heap_alias[left]  = right_alias;
  m_client_lru_heap_alias[right] = left_alias;
  m_client_lru_heap_ts[left]     = m_client_lru_heap_ts[right];
  m_client_lru_heap_ts[right]    = left_ts;

  _EnsureClientHeapCapacity(left_alias);
  _EnsureClientHeapCapacity(right_alias);
  m_client_alias_heap_pos[left_alias]  = right + 1;
  m_client_alias_heap_pos[right_alias] = left + 1;
}

//+------------------------------------------------------------------+
//| _SiftClientAliasUp                                               |
//| Purpose: Restore heap order after a timestamp decrease/insert    |
//+------------------------------------------------------------------+
void CTopicAliasManager::_SiftClientAliasUp(uint index) {
  while (index > 0) {
    uint parent = (index - 1) / 2;
    if (m_client_lru_heap_ts[parent] <= m_client_lru_heap_ts[index]) {
      return;
    }
    _SwapClientHeapEntries(parent, index);
    index = parent;
  }
}

//+------------------------------------------------------------------+
//| _SiftClientAliasDown                                             |
//| Purpose: Restore heap order after a timestamp increase/remove    |
//+------------------------------------------------------------------+
void CTopicAliasManager::_SiftClientAliasDown(uint index) {
  while (true) {
    uint left = index * 2 + 1;
    if (left >= m_client_lru_heap_count) {
      return;
    }

    uint right    = left + 1;
    uint smallest = left;
    if (right < m_client_lru_heap_count && m_client_lru_heap_ts[right] < m_client_lru_heap_ts[left]) {
      smallest = right;
    }
    if (m_client_lru_heap_ts[index] <= m_client_lru_heap_ts[smallest]) {
      return;
    }

    _SwapClientHeapEntries(index, smallest);
    index = smallest;
  }
}

//+------------------------------------------------------------------+
//| _TrackClientAliasUsage                                           |
//| Purpose: Insert/update an alias timestamp in the LRU heap        |
//+------------------------------------------------------------------+
void CTopicAliasManager::_TrackClientAliasUsage(const ushort alias, const ulong used_ms) {
  if (!m_client_alias_to_topic.ContainsKey(alias)) {
    return;
  }

  _EnsureClientHeapCapacity(alias);
  uint heap_pos = m_client_alias_heap_pos[alias];
  if (heap_pos > 0) {
    uint  index                 = heap_pos - 1;
    ulong old_ts                = m_client_lru_heap_ts[index];
    m_client_lru_heap_ts[index] = used_ms;
    if (used_ms < old_ts) {
      _SiftClientAliasUp(index);
    } else if (used_ms > old_ts) {
      _SiftClientAliasDown(index);
    }
    return;
  }

  uint index = m_client_lru_heap_count;
  ArrayResize(m_client_lru_heap_alias, (int)(index + 1), 8);
  ArrayResize(m_client_lru_heap_ts, (int)(index + 1), 8);
  m_client_lru_heap_alias[index] = alias;
  m_client_lru_heap_ts[index]    = used_ms;
  m_client_alias_heap_pos[alias] = index + 1;
  m_client_lru_heap_count++;
  _SiftClientAliasUp(index);
}

//+------------------------------------------------------------------+
//| _RemoveClientAliasUsage                                          |
//| Purpose: Remove an alias from the LRU heap                       |
//+------------------------------------------------------------------+
void CTopicAliasManager::_RemoveClientAliasUsage(const ushort alias) {
  if (ArraySize(m_client_alias_heap_pos) <= (int)alias) {
    return;
  }

  uint heap_pos = m_client_alias_heap_pos[alias];
  if (heap_pos == 0 || m_client_lru_heap_count == 0) {
    return;
  }

  uint index                     = heap_pos - 1;
  uint last                      = m_client_lru_heap_count - 1;
  m_client_alias_heap_pos[alias] = 0;
  if (index != last) {
    ushort moved_alias             = m_client_lru_heap_alias[last];
    ulong  moved_ts                = m_client_lru_heap_ts[last];
    m_client_lru_heap_alias[index] = moved_alias;
    m_client_lru_heap_ts[index]    = moved_ts;
    _EnsureClientHeapCapacity(moved_alias);
    m_client_alias_heap_pos[moved_alias] = index + 1;
  }

  m_client_lru_heap_count--;
  ArrayResize(m_client_lru_heap_alias, (int)m_client_lru_heap_count);
  ArrayResize(m_client_lru_heap_ts, (int)m_client_lru_heap_count);
  if (index < m_client_lru_heap_count) {
    if (index > 0 && m_client_lru_heap_ts[index] < m_client_lru_heap_ts[(index - 1) / 2]) {
      _SiftClientAliasUp(index);
    } else {
      _SiftClientAliasDown(index);
    }
  }
}

//+------------------------------------------------------------------+
//| SetTopicAliasMaximum                                             |
//| Purpose: Set the maximum allowed topic alias value               |
//| Parameters: max - maximum alias value (0 = disabled/unsupported) |
//+------------------------------------------------------------------+
void   CTopicAliasManager::SetTopicAliasMaximum(const ushort max) { m_topic_alias_maximum = max; }

//+------------------------------------------------------------------+
//| GetTopicAliasMaximum                                             |
//| Purpose: Get the current topic alias maximum                     |
//| Return: Maximum allowed alias value (0 = not set)                |
//+------------------------------------------------------------------+
ushort CTopicAliasManager::GetTopicAliasMaximum() const { return m_topic_alias_maximum; }

//+------------------------------------------------------------------+
//| SetClientTopicAliasMaximum                                       |
//| Purpose: Set the client-side Topic Alias Maximum (advertised in  |
//|        CONNECT). Limits server-to-client aliases per §3.1.2.11.8 |
//| Parameters: max - maximum alias value (0 = disabled)             |
//+------------------------------------------------------------------+
void   CTopicAliasManager::SetClientTopicAliasMaximum(const ushort max) { m_client_topic_alias_maximum = max; }

//+------------------------------------------------------------------+
//| GetClientTopicAliasMaximum                                       |
//| Purpose: Get client-side Topic Alias Maximum                     |
//| Return: Maximum alias value (0 = not set)                        |
//+------------------------------------------------------------------+
ushort CTopicAliasManager::GetClientTopicAliasMaximum() const { return m_client_topic_alias_maximum; }

//+------------------------------------------------------------------+
//| IsValidAlias                                                     |
//| Purpose: Check if an alias value is valid per MQTT spec          |
//|          Valid aliases: 1-65535 (0 not permitted per §3.3.2.3.4) |
//| Parameters: alias - alias value to validate                      |
//| Return: true if alias is in valid range                          |
//+------------------------------------------------------------------+
bool   CTopicAliasManager::IsValidAlias(const ushort alias) const {
  //--- Alias 0 is not permitted per MQTT 5.0 spec §3.3.2.3.4
  return (alias >= 1);
}

//+------------------------------------------------------------------+
//| IsAliasAvailable                                                 |
//| Purpose: Check if alias can be used (valid & not exceeding max)  |
//| Parameters: alias - alias value to check                         |
//| Return: true if alias can be used                                |
//+------------------------------------------------------------------+
bool CTopicAliasManager::IsAliasAvailable(const ushort alias) const {
  if (!IsValidAlias(alias)) {
    return false;
  }
  //--- If Topic Alias Maximum is set, enforce it
  if (m_topic_alias_maximum > 0 && alias > m_topic_alias_maximum) {
    return false;
  }
  return true;
}

//+------------------------------------------------------------------+
//| RegisterClientAlias                                              |
//| Purpose: Register a client-to-server topic alias mapping         |
//|          Used when client assigns an alias to a topic            |
//| Parameters: topic_name - the topic name                          |
//|             alias - the alias value (1-65535)                    |
//| Return: true if registration successful                          |
//+------------------------------------------------------------------+
bool CTopicAliasManager::RegisterClientAlias(const string &topic_name, const ushort alias) {
  //--- Validate inputs
  if (StringLen(topic_name) == 0) {
    MQTT_LOG_ERROR("Cannot register empty topic name");
    return false;
  }

  if (!IsAliasAvailable(alias)) {
    MQTT_LOG_ERROR("Alias " + (string)alias + " is not available (max=" + (string)m_topic_alias_maximum + ")");
    return false;
  }

  //--- Remove old alias for this topic if it exists
  ushort old_alias = 0;
  if (m_client_topic_to_alias.TryGetValue(topic_name, old_alias)) {
    m_client_topic_to_alias.Remove(topic_name);
    m_client_alias_to_topic.Remove(old_alias);
    _RemoveClientAliasUsage(old_alias);
  }

  //--- Remove old topic for this alias if it exists
  string old_topic = "";
  if (m_client_alias_to_topic.TryGetValue(alias, old_topic)) {
    m_client_topic_to_alias.Remove(old_topic);
    //--- Also remove the alias→topic and heap entry so the Add calls below succeed
    m_client_alias_to_topic.Remove(alias);
    _RemoveClientAliasUsage(alias);
  }

  //--- Add new mapping
  m_client_topic_to_alias.Add(topic_name, alias);
  m_client_alias_to_topic.Add(alias, topic_name);

  //--- Update highest alias if needed
  if (alias > m_highest_client_alias) {
    m_highest_client_alias = alias;
  }

  _TrackClientAliasUsage(alias, GetMicrosecondCount() / 1000);

  return true;
}

//+------------------------------------------------------------------+
//| RegisterClientAliasAuto                                          |
//| Purpose: Automatically assign next available alias to a topic    |
//| Parameters: topic_name - the topic name                          |
//|             assigned_alias - output parameter for assigned value |
//| Return: true if registration successful                          |
//+------------------------------------------------------------------+
bool CTopicAliasManager::RegisterClientAliasAuto(const string &topic_name, ushort &assigned_alias) {
  //--- Validate inputs
  if (StringLen(topic_name) == 0) {
    MQTT_LOG_ERROR("Cannot register empty topic name");
    return false;
  }

  //--- Check if topic already has a mapping
  const ushort existing_alias = GetClientAlias(topic_name);
  if (existing_alias > 0) {
    assigned_alias = existing_alias;
    return true;
  }

  //--- Find next available alias
  ushort next_alias = m_highest_client_alias + 1;
  if (next_alias == 0) {
    next_alias = 1;  // ushort wrap-around guard
  }

  //--- Enter LRU eviction when the alias counter has reached or exceeded the maximum.
  //--- Without the >= check, a counter that wraps from 65535 to 0→1 permanently pins
  //--- every subsequent allocation to alias 1 (overwriting it on every call).
  if (m_topic_alias_maximum > 0
      && (next_alias > m_topic_alias_maximum || m_highest_client_alias >= m_topic_alias_maximum)) {
    //--- All alias slots consumed — recycle the least-recently-used alias (LRU eviction)
    ushort lru_alias = _FindLRUClientAlias();
    if (lru_alias == 0) {
      MQTT_LOG_ERROR("Cannot auto-assign alias - Topic Alias Maximum reached and no alias to evict");
      return false;
    }

    //--- Evict the LRU entry
    string evicted_topic = "";
    if (m_client_alias_to_topic.TryGetValue(lru_alias, evicted_topic)) {
      m_client_topic_to_alias.Remove(evicted_topic);
      m_client_alias_to_topic.Remove(lru_alias);
      _RemoveClientAliasUsage(lru_alias);
      MQTT_LOG_DEBUG("Evicted LRU topic alias " + (string)lru_alias + " (topic=\"" + evicted_topic + "\")");
    }

    //--- Reassign the evicted alias to the new topic
    if (RegisterClientAlias(topic_name, lru_alias)) {
      assigned_alias = lru_alias;
      TouchClientAlias(lru_alias);
      return true;
    }
    return false;
  }

  //--- Register the new alias
  if (RegisterClientAlias(topic_name, next_alias)) {
    assigned_alias = next_alias;
    TouchClientAlias(next_alias);
    return true;
  }

  return false;
}

//+------------------------------------------------------------------+
//| GetClientAlias                                                   |
//| Purpose: Get the alias assigned to a topic (client→server)       |
//| Parameters: topic_name - topic to look up                        |
//| Return: Alias value (0 if not found)                             |
//+------------------------------------------------------------------+
ushort CTopicAliasManager::GetClientAlias(const string &topic_name) {
  ushort alias = 0;
  if (m_client_topic_to_alias.TryGetValue(topic_name, alias)) {
    return alias;
  }
  return 0;  // Not found
}

//+------------------------------------------------------------------+
//| HasClientTopic                                                   |
//| Purpose: Check if a topic has a client alias mapping             |
//| Parameters: topic_name - topic to check                          |
//| Return: true if topic has an alias mapping                       |
//+------------------------------------------------------------------+
bool CTopicAliasManager::HasClientTopic(const string &topic_name) {
  return m_client_topic_to_alias.ContainsKey(topic_name);
}

//+------------------------------------------------------------------+
//| HasClientAlias                                                   |
//| Purpose: Check if an alias value is in use by client mappings    |
//| Parameters: alias - [IN] alias value to check                    |
//| Return: true if alias is in use                                  |
//+------------------------------------------------------------------+
bool CTopicAliasManager::HasClientAlias(const ushort alias) { return m_client_alias_to_topic.ContainsKey(alias); }

//+------------------------------------------------------------------+
//| RegisterServerAlias                                              |
//| Purpose: Register a topic alias mapping from the server          |
//|          Used when receiving PUBLISH from server with Topic Alias|
//| Parameters: topic_name - the topic name                          |
//|             alias - the alias value (1-65535)                    |
//| Return: true if registration successful                          |
//+------------------------------------------------------------------+
bool CTopicAliasManager::RegisterServerAlias(const string &topic_name, const ushort alias) {
  //--- Validate inputs
  if (StringLen(topic_name) == 0) {
    MQTT_LOG_ERROR("Cannot register empty topic name for server alias");
    return false;
  }

  if (!IsValidAlias(alias)) {
    MQTT_LOG_ERROR("Invalid alias value " + (string)alias);
    return false;
  }

  //--- Validate against client-advertised Topic Alias Maximum per §3.1.2.11.8.
  //--- If the client advertised a maximum, the server MUST NOT send aliases
  //--- exceeding that value. Violation is a Protocol Error.
  if (m_client_topic_alias_maximum > 0 && alias > m_client_topic_alias_maximum) {
    MQTT_LOG_ERROR("Server sent Topic Alias " + (string)alias + " exceeding client-advertised maximum "
                   + (string)m_client_topic_alias_maximum + " — Protocol Error per §3.3.2.3.4");
    return false;
  }

  //--- Remove existing mapping if present (server may re-use aliases per §3.3.2.3.4)
  string old_topic = "";
  if (m_server_alias_to_topic.TryGetValue(alias, old_topic)) {
    m_server_alias_to_topic.Remove(alias);
  }

  return m_server_alias_to_topic.Add(alias, topic_name);
}

//+------------------------------------------------------------------+
//| ResolveServerAlias                                               |
//| Purpose: Get topic name from server alias (for incoming PUBLISH) |
//|          Per spec §3.3.2.3.4: When Topic Alias is > 0 and        |
//|          Topic Name is empty, resolve the full topic name        |
//| Parameters: alias - alias value to resolve                       |
//| Return: Topic name string (empty if not found)                   |
//+------------------------------------------------------------------+
string CTopicAliasManager::ResolveServerAlias(const ushort alias) {
  string topic = "";
  if (m_server_alias_to_topic.TryGetValue(alias, topic)) {
    return topic;
  }
  return "";  // Not found
}

//+------------------------------------------------------------------+
//| HasServerAlias                                                   |
//| Purpose: Check if a server alias exists                          |
//| Parameters: alias - [IN] alias value to check                    |
//| Return: true if alias is in server mappings                      |
//+------------------------------------------------------------------+
bool CTopicAliasManager::HasServerAlias(const ushort alias) { return m_server_alias_to_topic.ContainsKey(alias); }

//+------------------------------------------------------------------+
//| ClearClientMappings                                              |
//| Purpose: Remove all client-to-server mappings                    |
//+------------------------------------------------------------------+
void CTopicAliasManager::ClearClientMappings() {
  m_client_alias_to_topic.Clear();
  m_client_topic_to_alias.Clear();
  m_highest_client_alias  = 0;
  m_client_lru_heap_count = 0;
  ArrayResize(m_client_lru_heap_alias, 0);
  ArrayResize(m_client_lru_heap_ts, 0);
  ArrayResize(m_client_alias_heap_pos, 0);
}

//+------------------------------------------------------------------+
//| ClearServerMappings                                              |
//| Purpose: Remove all server-to-client mappings                    |
//+------------------------------------------------------------------+
void CTopicAliasManager::ClearServerMappings() { m_server_alias_to_topic.Clear(); }

//+------------------------------------------------------------------+
//| ClearAll                                                         |
//| Purpose: Remove all mappings (client and server)                 |
//+------------------------------------------------------------------+
void CTopicAliasManager::ClearAll() {
  ClearClientMappings();
  ClearServerMappings();
}

//+------------------------------------------------------------------+
//| GetClientMappingCount                                            |
//| Purpose: Get number of client mappings                           |
//| Return: Count of client-to-server mappings                       |
//+------------------------------------------------------------------+
uint CTopicAliasManager::GetClientMappingCount() { return (uint)m_client_topic_to_alias.Count(); }

//+------------------------------------------------------------------+
//| GetServerMappingCount                                            |
//| Purpose: Get number of server mappings                           |
//| Return: Count of server-to-client mappings                       |
//+------------------------------------------------------------------+
uint CTopicAliasManager::GetServerMappingCount() { return (uint)m_server_alias_to_topic.Count(); }

//+------------------------------------------------------------------+
//| DeregisterClientAlias                                            |
//| Purpose: Explicitly free a client alias slot for reuse           |
//| Parameters: topic_name - topic whose alias should be freed       |
//| Return: true if alias was found and removed                      |
//+------------------------------------------------------------------+
bool CTopicAliasManager::DeregisterClientAlias(const string &topic_name) {
  ushort alias = 0;
  if (!m_client_topic_to_alias.TryGetValue(topic_name, alias)) {
    return false;  // Topic has no alias
  }
  m_client_topic_to_alias.Remove(topic_name);
  m_client_alias_to_topic.Remove(alias);
  _RemoveClientAliasUsage(alias);
  MQTT_LOG_DEBUG("Deregistered client alias " + (string)alias + " for topic \"" + topic_name + "\"");
  return true;
}

//+------------------------------------------------------------------+
//| TouchClientAlias                                                 |
//| Purpose: Update the LRU timestamp for a client alias             |
//| Parameters: alias - alias value to update                        |
//+------------------------------------------------------------------+
void CTopicAliasManager::TouchClientAlias(const ushort alias) {
  _TrackClientAliasUsage(alias, GetMicrosecondCount() / 1000);
}

//+------------------------------------------------------------------+
//| _FindLRUClientAlias                                              |
//| Purpose: Find the least-recently-used client alias for eviction  |
//| Return: Alias value of the LRU entry (0 if none exist)           |
//+------------------------------------------------------------------+
ushort CTopicAliasManager::_FindLRUClientAlias() {
  return (m_client_lru_heap_count > 0) ? m_client_lru_heap_alias[0] : 0;
}

#endif  // MQTT_TOPICALIASMGR_MQH
