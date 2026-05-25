//+------------------------------------------------------------------+
//|                                                 TopicMatcher.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Trie-based MQTT topic filter matcher (§4.7).                     |
//|                                                                  |
//| Replaces the O(n×m) linear-scan dispatch in _OnPublishReceived   |
//| with O(d) per incoming PUBLISH, where d = topic segment depth.   |
//|                                                                  |
//| Supports MQTT wildcards:                                         |
//|   '+'  (MQTT_PLUS_WILDCARD)  — single-segment wildcard           |
//|   '#'  (MQTT_HASH_WILDCARD)  — multi-level wildcard              |
//| Shared subscription prefixes ($share/group/…) are normalised     |
//| before insertion so matching works on the real topic filter.     |
//|                                                                  |
//| Usage                                                            |
//|   CTopicMatcher m;                                               |
//|   m.AddFilter("sensors/+/temp", sub_index);                      |
//|   m.AddFilter("events/#", other_sub_index);                      |
//|   uint results[], count;                                         |
//|   m.Match("sensors/room1/temp", results, count);                 |
//|   // → results = { sub_index }                                   |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_UTIL_TOPIC_MATCHER_MQH
#define MQTT_INTERNAL_UTIL_TOPIC_MATCHER_MQH

//+------------------------------------------------------------------+
//| TrieNode — internal node in the topic filter trie                |
//| Stored in a flat pool array (CTopicMatcher::m_nodes[]).          |
//+------------------------------------------------------------------+
struct MqttTrieNode {
  //--- Exact-segment children: parallel arrays (key → pool index)
  string children_keys[];
  int    children_idx[];
  uint   children_count;

  int    plus_child;  // Pool index of the '+' child node (-1 if none)
  int    hash_child;  // Pool index of the '#' child node (-1 if none)

  //--- Subscription indices registered at this node
  //--- (entries from CMqttClient::m_subs[] whose filter ends here)
  uint   sub_ids[];
  uint   sub_count;
};

//+------------------------------------------------------------------+
//| Class CTopicMatcher                                              |
//| Purpose: O(topic-depth) MQTT topic dispatch via trie.            |
//| Interface mirrors the linear-scan in _OnPublishReceived so the   |
//| switch from O(n) to O(d) is a drop-in code change.               |
//+------------------------------------------------------------------+
class CTopicMatcher {
 private:
  MqttTrieNode m_nodes[];     // Node pool (m_nodes[0] = root)
  uint         m_node_count;
  uint         m_max_sub_id;  // Highest sub_index ever registered; used to size dedup bitfield

  //--- Allocate a new node and return its pool index
  int          _NewNode();

  //--- Find child index for an exact segment; -1 if not found
  int          _FindChild(int node_idx, const string segment) const;

  //--- Locate the child slot for an exact segment using binary search.
  //--- Returns the found slot, or the insertion slot when not found.
  int          _FindChildSlot(int node_idx, const string segment, bool &found) const;

  //--- Get or create child node for a segment ('+' / '#' use dedicated fields)
  int          _GetOrCreateChild(int node_idx, const string segment);

  bool         _NodeHasChildren(int node_idx) const;
  bool         _NodeIsEmpty(int node_idx) const;
  void         _DetachChild(int parent_idx, const string segment);
  void         _DeleteNode(int node_idx);

  //--- Recursive match helper
  //--- node_idx: current trie node
  //--- segments: topic segments split by '/'
  //--- seg_pos:  current segment index being matched
  //--- out:      accumulator for matching subscription indices
  //--- count:    current count in out[]
  //--- seen:     boolean bitfield indexed by sub_id for O(1) duplicate detection
  void         _Match(int node_idx, const string &segments[], int seg_count, int seg_pos, uint &out[], uint &count,
                      bool &seen[]) const;

  //--- Split a topic/filter string by '/' into an array of segments.
  //--- Returns the number of segments produced.
  static int   _Split(const string src, string &out_segments[]);

 public:
  CTopicMatcher();

  //--- Add a subscription filter to the trie.
  //--- sub_index: index into CMqttClient::m_subs[].
  void AddFilter(const string filter, uint sub_index);

  //--- Remove all entries for a filter (all registered sub indices).
  void RemoveFilter(const string filter);

  //--- Remove a specific sub_index from all nodes (used on unsubscribe).
  void RemoveSubIndex(uint sub_index);

  //--- Match an incoming topic and return all matching subscription indices.
  //--- Caller is responsible for sizing the out[] buffer; typical max = m_sub_count.
  void Match(const string topic, uint &out[], uint &count) const;

  //--- Reset trie (remove all filters).
  void Clear();

  //--- Return true when the trie is empty (no filters registered).
  bool IsEmpty() const { return m_node_count == 0; }

#ifdef MQTT_UNIT_TESTS
  uint TestGetNodeCount() const { return m_node_count; }
#endif
};

//+------------------------------------------------------------------+
//| Constructor                                                      |
//+------------------------------------------------------------------+
CTopicMatcher::CTopicMatcher() {
  m_node_count = 0;
  m_max_sub_id = 0;
  ArrayFree(m_nodes);
}

//+------------------------------------------------------------------+
//| _NewNode                                                         |
//| Purpose: Allocate a fresh node in the node pool                  |
//| Return: Pool index of the new node                               |
//+------------------------------------------------------------------+
int CTopicMatcher::_NewNode() {
  uint idx = m_node_count;
  ArrayResize(m_nodes, idx + 1, 16);
  m_nodes[idx].children_count = 0;
  m_nodes[idx].plus_child     = -1;
  m_nodes[idx].hash_child     = -1;
  m_nodes[idx].sub_count      = 0;
  ArrayFree(m_nodes[idx].children_keys);
  ArrayFree(m_nodes[idx].children_idx);
  ArrayFree(m_nodes[idx].sub_ids);
  m_node_count++;
  return (int)idx;
}

//+------------------------------------------------------------------+
//| _FindChildSlot                                                   |
//| Purpose: Binary-search exact-segment children                    |
//| Parameters: node_idx - [IN] parent node index                    |
//|             segment - [IN] exact segment string                  |
//|             found - [OUT] true when the segment exists           |
//| Return: Existing slot or insertion position                      |
//+------------------------------------------------------------------+
int CTopicMatcher::_FindChildSlot(int node_idx, const string segment, bool &found) const {
  found = false;
  if (node_idx < 0 || (uint)node_idx >= m_node_count) {
    return -1;
  }

  int left  = 0;
  int right = (int)m_nodes[node_idx].children_count - 1;
  while (left <= right) {
    int mid = left + ((right - left) >> 1);
    int cmp = StringCompare(m_nodes[node_idx].children_keys[mid], segment);
    if (cmp == 0) {
      found = true;
      return mid;
    }
    if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return left;
}

//+------------------------------------------------------------------+
//| _FindChild                                                       |
//| Purpose: O(log k) lookup of an exact-segment child               |
//| Parameters: node_idx - [IN] parent node index                    |
//|             segment - [IN] exact segment string                  |
//| Return: Pool index of the child, or -1 if not found              |
//+------------------------------------------------------------------+
int CTopicMatcher::_FindChild(int node_idx, const string segment) const {
  bool found = false;
  int  slot  = _FindChildSlot(node_idx, segment, found);
  if (!found || slot < 0) {
    return -1;
  }
  return m_nodes[node_idx].children_idx[slot];
}

//+------------------------------------------------------------------+
//| _GetOrCreateChild                                                |
//| Purpose: Return existing child or allocate a new one             |
//| Parameters: node_idx - [IN] parent node index                    |
//|             segment - [IN] segment string (+, #, or literal)     |
//| Return: Pool index of the child node                             |
//+------------------------------------------------------------------+
int CTopicMatcher::_GetOrCreateChild(int node_idx, const string segment) {
  if (segment == "+") {
    if (m_nodes[node_idx].plus_child < 0) {
      m_nodes[node_idx].plus_child = _NewNode();
    }
    return m_nodes[node_idx].plus_child;
  }
  if (segment == "#") {
    if (m_nodes[node_idx].hash_child < 0) {
      m_nodes[node_idx].hash_child = _NewNode();
    }
    return m_nodes[node_idx].hash_child;
  }
  //--- Exact segment
  bool found = false;
  int  slot  = _FindChildSlot(node_idx, segment, found);
  if (found && slot >= 0) {
    return m_nodes[node_idx].children_idx[slot];
  }

  int  new_idx = _NewNode();
  uint count   = m_nodes[node_idx].children_count;
  ArrayResize(m_nodes[node_idx].children_keys, count + 1, 4);
  ArrayResize(m_nodes[node_idx].children_idx, count + 1, 4);
  for (int i = (int)count; i > slot; i--) {
    m_nodes[node_idx].children_keys[i] = m_nodes[node_idx].children_keys[i - 1];
    m_nodes[node_idx].children_idx[i]  = m_nodes[node_idx].children_idx[i - 1];
  }
  m_nodes[node_idx].children_keys[slot] = segment;
  m_nodes[node_idx].children_idx[slot]  = new_idx;
  m_nodes[node_idx].children_count      = count + 1;
  return new_idx;
}

//+------------------------------------------------------------------+
//| _NodeHasChildren                                                 |
//+------------------------------------------------------------------+
bool CTopicMatcher::_NodeHasChildren(int node_idx) const {
  if (node_idx < 0 || (uint)node_idx >= m_node_count) {
    return false;
  }

  return m_nodes[node_idx].plus_child >= 0 || m_nodes[node_idx].hash_child >= 0 || m_nodes[node_idx].children_count > 0;
}

//+------------------------------------------------------------------+
//| _NodeIsEmpty                                                     |
//+------------------------------------------------------------------+
bool CTopicMatcher::_NodeIsEmpty(int node_idx) const {
  if (node_idx < 0 || (uint)node_idx >= m_node_count) {
    return true;
  }

  return m_nodes[node_idx].sub_count == 0 && !_NodeHasChildren(node_idx);
}

//+------------------------------------------------------------------+
//| _DetachChild                                                     |
//+------------------------------------------------------------------+
void CTopicMatcher::_DetachChild(int parent_idx, const string segment) {
  if (parent_idx < 0 || (uint)parent_idx >= m_node_count) {
    return;
  }

  if (segment == "+") {
    m_nodes[parent_idx].plus_child = -1;
    return;
  }
  if (segment == "#") {
    m_nodes[parent_idx].hash_child = -1;
    return;
  }

  bool found = false;
  int  slot  = _FindChildSlot(parent_idx, segment, found);
  if (!found || slot < 0) {
    return;
  }

  uint count = m_nodes[parent_idx].children_count;
  for (uint i = (uint)slot; i + 1 < count; i++) {
    m_nodes[parent_idx].children_keys[i] = m_nodes[parent_idx].children_keys[i + 1];
    m_nodes[parent_idx].children_idx[i]  = m_nodes[parent_idx].children_idx[i + 1];
  }
  m_nodes[parent_idx].children_count = count - 1;
  ArrayResize(m_nodes[parent_idx].children_keys, m_nodes[parent_idx].children_count);
  ArrayResize(m_nodes[parent_idx].children_idx, m_nodes[parent_idx].children_count);
}

//+------------------------------------------------------------------+
//| _DeleteNode                                                      |
//+------------------------------------------------------------------+
void CTopicMatcher::_DeleteNode(int node_idx) {
  if (node_idx <= 0 || (uint)node_idx >= m_node_count) {
    return;
  }

  int last_idx = (int)m_node_count - 1;
  if (node_idx != last_idx) {
    m_nodes[node_idx] = m_nodes[last_idx];

    for (uint n = 0; n < (uint)last_idx; n++) {
      if ((int)n == node_idx) {
        continue;
      }
      if (m_nodes[n].plus_child == last_idx) {
        m_nodes[n].plus_child = node_idx;
      }
      if (m_nodes[n].hash_child == last_idx) {
        m_nodes[n].hash_child = node_idx;
      }
      for (uint c = 0; c < m_nodes[n].children_count; c++) {
        if (m_nodes[n].children_idx[c] == last_idx) {
          m_nodes[n].children_idx[c] = node_idx;
        }
      }
    }
  }

  ArrayFree(m_nodes[last_idx].children_keys);
  ArrayFree(m_nodes[last_idx].children_idx);
  ArrayFree(m_nodes[last_idx].sub_ids);
  m_node_count--;
  ArrayResize(m_nodes, m_node_count);
}

//+------------------------------------------------------------------+
//| _Split                                                           |
//| Purpose: Split a string by '/' into segments array               |
//| Parameters: src - [IN] source string                             |
//|             out_segments - [OUT] output segments array           |
//| Return: Number of segments produced                              |
//+------------------------------------------------------------------+
int CTopicMatcher::_Split(const string src, string &out_segments[]) {
  ArrayFree(out_segments);
  int count = 0;
  int len   = StringLen(src);
  int start = 0;

  //--- Handle empty leading segment ("/" → ["", ""])
  for (int i = 0; i <= len; i++) {
    if (i == len || StringGetCharacter(src, i) == '/') {
      string seg = StringSubstr(src, start, i - start);
      ArrayResize(out_segments, count + 1, 8);
      out_segments[count++] = seg;
      start                 = i + 1;
    }
  }
  return count;
}

//+------------------------------------------------------------------+
//| AddFilter                                                        |
//+------------------------------------------------------------------+
void CTopicMatcher::AddFilter(const string filter, uint sub_index) {
  //--- Ensure root node exists
  if (m_node_count == 0) {
    _NewNode();  // root = index 0
  }

  //--- Handle shared subscription prefix $share/GroupId/ — strip it
  string actual_filter = filter;
  if (StringSubstr(filter, 0, 7) == "$share/") {
    int second_slash = StringFind(filter, "/", 7);
    if (second_slash >= 0) {
      actual_filter = StringSubstr(filter, second_slash + 1);
    }
  }

  string segments[];
  int    seg_count = _Split(actual_filter, segments);
  int    node_idx  = 0;  // Start from root

  for (int s = 0; s < seg_count; s++) {
    node_idx = _GetOrCreateChild(node_idx, segments[s]);
    //--- '#' is always a leaf per MQTT spec §4.7.1.2
    if (segments[s] == "#") {
      break;
    }
  }

  //--- Register sub_index at this leaf node (avoid duplicates)
  uint n = m_nodes[node_idx].sub_count;
  for (uint i = 0; i < n; i++) {
    if (m_nodes[node_idx].sub_ids[i] == sub_index) {
      return;  // Already registered
    }
  }
  ArrayResize(m_nodes[node_idx].sub_ids, n + 1, 4);
  m_nodes[node_idx].sub_ids[n] = sub_index;
  m_nodes[node_idx].sub_count  = n + 1;
  //--- Track highest sub_index for dedup bitfield sizing in Match()
  if (sub_index > m_max_sub_id) {
    m_max_sub_id = sub_index;
  }
}

//+------------------------------------------------------------------+
//| RemoveFilter                                                     |
//| Purpose: Clear all sub indices at the leaf node for filter       |
//| Parameters: filter - [IN] topic filter string to remove          |
//+------------------------------------------------------------------+
void CTopicMatcher::RemoveFilter(const string filter) {
  if (m_node_count == 0) {
    return;
  }

  string actual_filter = filter;
  if (StringSubstr(filter, 0, 7) == "$share/") {
    int second_slash = StringFind(filter, "/", 7);
    if (second_slash >= 0) {
      actual_filter = StringSubstr(filter, second_slash + 1);
    }
  }

  string segments[];
  int    seg_count = _Split(actual_filter, segments);
  int    node_idx  = 0;
  int    path[];
  ArrayResize(path, seg_count + 1);
  path[0] = 0;

  for (int s = 0; s < seg_count; s++) {
    string seg = segments[s];
    int    next;
    if (seg == "+") {
      next = m_nodes[node_idx].plus_child;
    } else if (seg == "#") {
      next = m_nodes[node_idx].hash_child;
    } else {
      next = _FindChild(node_idx, seg);
    }
    if (next < 0) {
      return;  // Filter not in trie
    }
    node_idx    = next;
    path[s + 1] = node_idx;
    if (seg == "#") {
      break;
    }
  }

  //--- Clear sub_ids at this node
  ArrayFree(m_nodes[node_idx].sub_ids);
  m_nodes[node_idx].sub_count = 0;

  for (int depth = seg_count - 1; depth >= 0; depth--) {
    int current_idx = path[depth + 1];
    if (current_idx <= 0 || !_NodeIsEmpty(current_idx)) {
      break;
    }
    _DetachChild(path[depth], segments[depth]);
    int last_idx = (int)m_node_count - 1;
    _DeleteNode(current_idx);
    //--- After swap-with-last, update any remaining path entries that
    //--- pointed to the moved node so subsequent iterations use the
    //--- correct pool index.
    if (current_idx != last_idx) {
      for (int j = 0; j <= depth; j++) {
        if (path[j] == last_idx) {
          path[j] = current_idx;
        }
      }
    }
  }

  if (m_node_count == 1 && _NodeIsEmpty(0)) {
    ArrayFree(m_nodes[0].children_keys);
    ArrayFree(m_nodes[0].children_idx);
    ArrayFree(m_nodes[0].sub_ids);
    ArrayResize(m_nodes, 0);
    m_node_count = 0;
  }
}

//+------------------------------------------------------------------+
//| RemoveSubIndex                                                   |
//| Purpose: Remove a specific sub index from all nodes              |
//| Parameters: sub_index - [IN] index to remove                     |
//| Note: Used when the subscription array is compacted.             |
//+------------------------------------------------------------------+
void CTopicMatcher::RemoveSubIndex(uint sub_index) {
  for (uint n = 0; n < m_node_count; n++) {
    uint cnt = m_nodes[n].sub_count;
    for (uint i = 0; i < cnt; i++) {
      if (m_nodes[n].sub_ids[i] == sub_index) {
        //--- Swap with last and shrink
        m_nodes[n].sub_ids[i] = m_nodes[n].sub_ids[cnt - 1];
        m_nodes[n].sub_count--;
        ArrayResize(m_nodes[n].sub_ids, m_nodes[n].sub_count);
        cnt--;
        i--;  // Re-check this slot (it now holds the swapped value)
      }
    }
  }
  //--- Update m_max_sub_id when the removed index equals the current maximum.
  //--- Prevents the Match() dedup bitfield from growing permanently after churn.
  //--- Only rescans when needed (amortized O(1) for random removal order).
  if (sub_index >= m_max_sub_id) {
    m_max_sub_id = 0;
    for (uint n = 0; n < m_node_count; n++) {
      uint cnt = m_nodes[n].sub_count;
      for (uint i = 0; i < cnt; i++) {
        if (m_nodes[n].sub_ids[i] > m_max_sub_id) {
          m_max_sub_id = m_nodes[n].sub_ids[i];
        }
      }
    }
  }

  //--- Prune trie nodes that became empty after sub_id removal to prevent
  //--- monotonic memory growth from subscription churn
  bool _pruned = true;
  while (_pruned) {
    _pruned = false;
    for (int ni = (int)m_node_count - 1; ni > 0; ni--) {
      if (!_NodeIsEmpty(ni)) {
        continue;
      }
      //--- Find parent of this empty node
      int    _parent_idx = -1;
      string _parent_seg = "";
      for (uint pi = 0; pi < m_node_count && _parent_idx < 0; pi++) {
        if (m_nodes[pi].plus_child == ni) {
          _parent_idx = (int)pi;
          _parent_seg = "+";
          break;
        }
        if (m_nodes[pi].hash_child == ni) {
          _parent_idx = (int)pi;
          _parent_seg = "#";
          break;
        }
        for (uint ci = 0; ci < m_nodes[pi].children_count; ci++) {
          if (m_nodes[pi].children_idx[ci] == ni) {
            _parent_idx = (int)pi;
            _parent_seg = m_nodes[pi].children_keys[ci];
            break;
          }
        }
      }
      if (_parent_idx >= 0) {
        _DetachChild(_parent_idx, _parent_seg);
        _DeleteNode(ni);
        _pruned = true;
        break;  // Restart scan — node indices shifted after swap-with-last deletion
      }
    }
  }
  //--- Clean up root if it became empty
  if (m_node_count == 1 && _NodeIsEmpty(0)) {
    ArrayFree(m_nodes[0].children_keys);
    ArrayFree(m_nodes[0].children_idx);
    ArrayFree(m_nodes[0].sub_ids);
    ArrayResize(m_nodes, 0);
    m_node_count = 0;
  }
}

//+------------------------------------------------------------------+
//| _Match                                                           |
//| Purpose: Recursive trie descent for topic matching               |
//| Parameters: node_idx - [IN] current trie node index              |
//|             segments - [IN] topic segments array                 |
//|             seg_count - [IN] total number of segments            |
//|             seg_pos - [IN] current segment position              |
//|             out - [OUT] matching subscription indices            |
//|             count - [OUT] count of items in out array            |
//|             seen - [IN/OUT] dedup bitfield                       |
//+------------------------------------------------------------------+
void CTopicMatcher::_Match(int node_idx, const string &segments[], int seg_count, int seg_pos, uint &out[], uint &count,
                           bool &seen[]) const {
  if (node_idx < 0 || (uint)node_idx >= m_node_count) {
    return;
  }

  //--- '#' child always matches the remainder of the topic
  int hash_child = m_nodes[node_idx].hash_child;
  if (hash_child >= 0) {
    //--- Per §4.7.2: '#' does not match topics beginning with '$'
    bool is_dollar_hash = (seg_pos == 0 && seg_count > 0 && StringGetCharacter(segments[seg_pos], 0) == '$');
    if (!is_dollar_hash) {
      //--- Collect all sub_ids at the '#' node
      uint n = m_nodes[hash_child].sub_count;
      for (uint i = 0; i < n; i++) {
        uint sub = m_nodes[hash_child].sub_ids[i];
        //--- O(1) dedup via boolean bitfield
        if (sub < (uint)ArraySize(seen) && !seen[sub]) {
          seen[sub] = true;
          ArrayResize(out, count + 1, 8);
          out[count++] = sub;
        }
      }
    }
  }

  if (seg_pos >= seg_count) {
    //--- Topic fully consumed — collect sub_ids registered at this node
    uint n = m_nodes[node_idx].sub_count;
    for (uint i = 0; i < n; i++) {
      uint sub = m_nodes[node_idx].sub_ids[i];
      //--- O(1) dedup via boolean bitfield
      if (sub < (uint)ArraySize(seen) && !seen[sub]) {
        seen[sub] = true;
        ArrayResize(out, count + 1, 8);
        out[count++] = sub;
      }
    }
    return;
  }

  //--- '+' child matches any single segment (but NOT leading '$' per §4.7.2)
  int plus_child = m_nodes[node_idx].plus_child;
  if (plus_child >= 0) {
    //--- Per §4.7.2: '+' and '#' do not match topics beginning with '$'
    bool is_dollar = (seg_pos == 0 && StringGetCharacter(segments[seg_pos], 0) == '$');
    if (!is_dollar) {
      _Match(plus_child, segments, seg_count, seg_pos + 1, out, count, seen);
    }
  }

  //--- Exact-segment child
  string seg         = segments[seg_pos];
  int    exact_child = _FindChild(node_idx, seg);
  if (exact_child >= 0) {
    _Match(exact_child, segments, seg_count, seg_pos + 1, out, count, seen);
  }
}

//+------------------------------------------------------------------+
//| Match                                                            |
//| Purpose: Find all subscription indices matching the given topic  |
//| Parameters: topic - [IN] incoming PUBLISH topic name             |
//|             out - [OUT] matching subscription indices            |
//|             count - [OUT] number of matches found                |
//+------------------------------------------------------------------+
void CTopicMatcher::Match(const string topic, uint &out[], uint &count) const {
  count = 0;
  //--- Do NOT call ArrayFree(out) here. Callers may pre-allocate a reusable
  //--- member buffer (CMqttClient::m_match_scratch[]) to avoid GC allocation at
  //--- 200 msg/s. The buffer grows as needed during _Match() and is reused next call.
  if (m_node_count == 0) {
    return;
  }

  //--- Allocate dedup bitfield sized to the highest registered sub_id + 1.
  //--- O(1) seen-check replaces the previous O(k) linear scan per insertion.
  bool seen[];
  uint seen_size = m_max_sub_id + 1;
  ArrayResize(seen, (int)seen_size);
  ArrayInitialize(seen, false);

  string segments[];
  int    seg_count = _Split(topic, segments);
  _Match(0, segments, seg_count, 0, out, count, seen);
}

//+------------------------------------------------------------------+
//| Clear                                                            |
//| Purpose: Reset trie to empty state                               |
//+------------------------------------------------------------------+
void CTopicMatcher::Clear() {
  ArrayFree(m_nodes);
  m_node_count = 0;
  m_max_sub_id = 0;
}

#endif  // MQTT_TOPIC_MATCHER_MQH
