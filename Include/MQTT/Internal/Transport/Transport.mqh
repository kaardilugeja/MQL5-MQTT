//+------------------------------------------------------------------+
//|                                                    Transport.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT Transport & Framing Layer                                   |
//|                                                                  |
//| Solves the "packet-level only" gap in the library. Provides:     |
//|   1. CPacketFramer  — TCP stream → complete MQTT packet splitter |
//|   2. CKeepAlive     — PINGREQ/PINGRESP timer management          |
//|   3. CMqttTransport — Unified TLS/plain socket send+recv helper  |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_TRANSPORT_TRANSPORT_MQH
#define MQTT_INTERNAL_TRANSPORT_TRANSPORT_MQH
#include "..\\Util\\Defines.mqh"
#include "ITransport.mqh"
#include "..\\Protocol\\Pingreq.mqh"

//+------------------------------------------------------------------+
//| CPacketFramer                                                    |
//|                                                                  |
//| Purpose: Reassemble raw TCP stream bytes into complete MQTT      |
//|          Control Packets per MQTT 5.0 §2.1.                      |
//|                                                                  |
//| Architecture:                                                    |
//|   A TCP stream is continuous and does not preserve packet        |
//|   boundaries. A single read can return a partial packet, several |
//|   packets, or a mix. CPacketFramer implements a stateful buffer  |
//|   that parses the Fixed Header (§2.1.2) to determine the         |
//|   Remaining Length (§2.1.3) of each packet.                      |
//+------------------------------------------------------------------+
class CPacketFramer {
 private:
  uchar m_buf[];                    // Internal accumulation buffer
  uint  m_head;                     // Read cursor (start of unprocessed data)
  uint  m_tail;                     // Write cursor (end of accumulated data)
  uint  m_max_pkt_size;             // Maximum accepted incoming packet size
  uint  m_configured_max_buf_size;  // User-configured backing-buffer ceiling
  uint  m_max_buf_size;             // Maximum backing-buffer size (0 = unlimited)
  bool  m_overflow;                 // Set when a buffer-limit violation drops data

  //--- Grow internal buffer by appending chunk
  void  _Append(const uchar &chunk[], int len);

  //--- Peek at remaining length starting at internal offset.
  //--- Returns UINT_MAX on malformed/incomplete varint, else the decoded value.
  //--- Sets *bytes_used to length of varint encoding (1-4 bytes).
  uint  _PeekRemLen(uint offset, uint &bytes_used) const;

 public:
  CPacketFramer(uint max_pkt_size = 268435455);
  ~CPacketFramer();

  //--- Feed raw bytes into the internal buffer
  void Feed(const uchar &chunk[], int len);

  //--- Attempt to extract the next complete packet from the buffer.
  //--- Returns true if a complete packet was extracted into 'pkt'.
  //--- Returns false if more bytes are needed or the stream is corrupt
  //--- (check error param when false for detail).
  bool NextPacket(uchar &pkt[], ENUM_TRANSPORT_ERROR &error);

  //--- Discard all buffered data (call on reconnect)
  void Reset();

  //--- Advance the read cursor by one byte without discarding remaining data.
  //--- Use after a BAD_FRAME error to attempt resync with the stream rather
  //--- than losing all buffered packets.
  bool SkipOneByte();

  //--- STAB-001: Scan forward and skip bytes until the current head byte's upper
  //--- nibble encodes a valid MQTT packet type (1-15).  This is a heuristic —
  //--- it can produce false positives, but it limits the blast radius of a
  //--- single corrupt byte to at most a few skipped bytes rather than a
  //--- cascade of BAD_FRAME errors.
  //--- Returns the number of bytes skipped (0 if the current byte is already valid).
  uint SkipToNextValidHeader();

  //--- Bytes available in the internal buffer (unprocessed)
  uint Available() const;

  //--- Set maximum acceptable incoming packet size (bytes)
  void SetMaxPacketSize(uint max_size) { m_max_pkt_size = (max_size > 0) ? max_size : 268435455; }

  //--- Set maximum backing-buffer size in bytes (0 = unlimited).
  //--- When the limit is hit, the framer retains as many already-buffered
  //--- bytes as possible and the stream becomes connection-fatal once
  //--- parsing reaches the truncated boundary.
  void SetMaxBufferSize(uint max_size) {
    m_configured_max_buf_size = max_size;
    m_max_buf_size            = max_size;
  }
};

//+------------------------------------------------------------------+
//| CPacketFramer constructor                                        |
//+------------------------------------------------------------------+
CPacketFramer::CPacketFramer(uint max_pkt_size) {
  m_head                    = 0;
  m_tail                    = 0;
  m_max_pkt_size            = (max_pkt_size > 0) ? max_pkt_size : 268435455;
  m_configured_max_buf_size = 0;
  m_max_buf_size            = 0;  // Unlimited by default
  m_overflow                = false;
  ArrayResize(m_buf, 4096, 4096);
}

//+------------------------------------------------------------------+
//| CPacketFramer destructor                                         |
//+------------------------------------------------------------------+
CPacketFramer::~CPacketFramer() { ArrayFree(m_buf); }

//+------------------------------------------------------------------+
//| Reset - Discard all buffered data                                |
//+------------------------------------------------------------------+
void CPacketFramer::Reset() {
  m_head     = 0;
  m_tail     = 0;
  m_overflow = false;
}

//+------------------------------------------------------------------+
//| Available - Return unprocessed byte count                        |
//+------------------------------------------------------------------+
uint CPacketFramer::Available() const { return (m_tail >= m_head) ? (m_tail - m_head) : 0; }

//+------------------------------------------------------------------+
//| _Append - Append raw bytes into the internal buffer              |
//+------------------------------------------------------------------+
void CPacketFramer::_Append(const uchar &chunk[], int len) {
  if (len <= 0) {
    return;
  }

  //--- Compact the buffer if the head has advanced significantly —
  //--- avoids unbounded memory growth on a long-lived connection.
  //--- Use multiplication to avoid integer-division truncation triggering compaction
  //--- at 40% utilisation on odd m_tail values (e.g. m_tail=5).
  //--- m_head * 2 >= m_tail is equivalent to m_head >= m_tail/2 but strictly 50% correct.
  if (m_head > 0 && m_head * 2 >= m_tail) {
    uint avail = Available();
    if (avail > 0) {
      //--- In-place compaction (MQL-001 pattern): MQL5's ArrayCopy handles
      //--- overlapping regions correctly when dest offset < src offset.
      ArrayCopy(m_buf, m_buf, 0, m_head, avail);
    }
    m_tail = avail;
    m_head = 0;
  }

  //--- Guard against unbounded buffer growth.
  //--- Preserve as many already-buffered bytes as possible so packets fully
  //--- assembled before the overflow boundary can still be extracted.
  if (m_max_buf_size > 0 && m_tail + (uint)len > m_max_buf_size) {
    uint free_space = (m_tail < m_max_buf_size) ? (m_max_buf_size - m_tail) : 0;
    if (free_space > 0) {
      if (m_tail + free_space > (uint)ArraySize(m_buf)) {
        ArrayResize(m_buf, m_tail + free_space + 4096, 4096);
      }
      ArrayCopy(m_buf, chunk, m_tail, 0, (int)free_space);
      m_tail += free_space;
    }
    MQTT_LOG_ERROR("Buffer limit (" + (string)m_max_buf_size
                   + " bytes) exceeded — preserving buffered prefix and marking stream as truncated");
    m_overflow = true;
    return;
  }

  //--- Grow the backing store if needed
  if (m_tail + (uint)len > (uint)ArraySize(m_buf)) {
    ArrayResize(m_buf, m_tail + (uint)len + 4096, 4096);
  }

  ArrayCopy(m_buf, chunk, m_tail, 0, len);
  m_tail += (uint)len;
}

//+------------------------------------------------------------------+
//| _PeekRemLen - Decode varint remaining-length at internal offset  |
//| Returns: decoded varint value, or UINT_MAX on error/incomplete   |
//| bytes_used: set to number of varint bytes consumed (1-4)         |
//+------------------------------------------------------------------+
uint CPacketFramer::_PeekRemLen(uint offset, uint &bytes_used) const {
  bytes_used       = 0;
  uint  multiplier = 1;
  uint  value      = 0;
  uint  idx        = offset;

  uchar last_byte  = 0;
  do {
    if (idx >= m_tail) {
      //--- Incomplete — need more data from the stream
      return UINT_MAX;
    }

    last_byte   = m_buf[idx];
    value      += (last_byte & 0x7F) * multiplier;
    multiplier *= 128;
    idx++;
    bytes_used++;

    if ((last_byte & 0x80) == 0) {
      break;  // Continuation bit clear — varint complete and valid
    }

    //--- After 4 bytes, if the continuation bit is still set the varint is
    //--- malformed (5th byte would be required, which is illegal).
    if (bytes_used == 4) {
      return UINT_MAX;
    }

  } while (true);

  return value;
}

//+------------------------------------------------------------------+
//| SkipOneByte - Advance read cursor by one byte                    |
//| Purpose: Attempt stream resync after a BAD_FRAME error without   |
//|          discarding packets that may follow the corrupt byte.    |
//| Return: true if a byte was skipped, false if buffer is empty     |
//+------------------------------------------------------------------+
bool CPacketFramer::SkipOneByte() {
  if (Available() == 0) {
    return false;
  }
  m_head++;
  return true;
}

//+------------------------------------------------------------------+
//| SkipToNextValidHeader - Scan for next valid MQTT packet type     |
//| Purpose: STAB-001 heuristic resync. Skips past bytes until we    |
//|          find one whose upper nibble (bits 7-4) is a valid MQTT  |
//|          packet type (1-15, i.e. CONNECT through AUTH).          |
//|          This limits the blast radius of a corrupt byte.         |
//| Return: Number of bytes skipped (0 if current head is valid)     |
//+------------------------------------------------------------------+
uint CPacketFramer::SkipToNextValidHeader() {
  uint skipped = 0;
  while (Available() > 0) {
    uchar type_nibble = (m_buf[m_head] >> 4) & 0x0F;
    //--- Valid MQTT packet types are 1 (CONNECT) through 15 (AUTH)
    if (type_nibble >= 1 && type_nibble <= 15) {
      break;  // Found a plausible header byte
    }
    m_head++;
    skipped++;
  }
  return skipped;
}

//+------------------------------------------------------------------+
//| Feed - Accumulate incoming raw bytes                             |
//+------------------------------------------------------------------+
void CPacketFramer::Feed(const uchar &chunk[], int len) { _Append(chunk, len); }

//+------------------------------------------------------------------+
//| NextPacket - Extract next complete MQTT packet from buffer       |
//| Purpose: Parse the fixed header and extract a complete packet    |
//| Parameters: pkt - [OUT] the extracted MQTT packet data           |
//|             error - [OUT] error code if framing fails            |
//| Return: true if packet extracted, false if need more data        |
//| Note: Implements the parsing logic defined in MQTT 5.0 §2.1      |
//+------------------------------------------------------------------+
bool CPacketFramer::NextPacket(uchar &pkt[], ENUM_TRANSPORT_ERROR &error) {
  error = TRANSPORT_OK;
  ArrayFree(pkt);

  uint avail = Available();

  //--- Case 1: Need at least 2 bytes (Type + at least 1-byte Remaining Length)
  if (avail < 2) {
    if (m_overflow && avail > 0) {
      MQTT_LOG_ERROR("Buffer overflow truncated an incoming packet header — connection should be closed.");
      m_head     = m_tail;
      m_overflow = false;
      error      = TRANSPORT_ERROR_PKT_TOO_BIG;
    } else if (m_overflow && avail == 0) {
      MQTT_LOG_ERROR("Buffered packets before the overflow boundary were drained, but later stream bytes were lost — "
                     "connection should be closed.");
      m_overflow = false;
      error      = TRANSPORT_ERROR_PKT_TOO_BIG;
    }
    return false;
  }

  //--- Case 2: Decode the Remaining Length varint starting at offset 1
  //--- Per MQTT 5.0 §2.1.3, this can be 1-4 bytes.
  uint remlen_bytes = 0;
  uint remlen       = _PeekRemLen(m_head + 1, remlen_bytes);

  if (remlen == UINT_MAX) {
    if (remlen_bytes >= 4) {
      //--- Definitive error: 4 bytes read but continuation bit still set
      error = TRANSPORT_ERROR_BAD_FRAME;
    } else if (m_overflow) {
      MQTT_LOG_ERROR("Buffer overflow truncated the Remaining Length field — connection should be closed.");
      m_head     = m_tail;
      m_overflow = false;
      error      = TRANSPORT_ERROR_PKT_TOO_BIG;
    }
    //--- Otherwise, we just need more data from the socket
    return false;
  }

  //--- Case 3: Calculate total packet size (Fixed Hdr + Varint length + Payload)
  uint total_len = 1 + remlen_bytes + remlen;

  //--- Case 4: Security Check — Reject packets exceeding configured maximum size
  if (total_len > m_max_pkt_size) {
    error      = TRANSPORT_ERROR_PKT_TOO_BIG;
    //--- Advance head to skip the problematic frame for potential recovery
    uint skip  = (total_len < avail) ? total_len : avail;
    m_head    += skip;
    return false;
  }

  //--- Case 5: Partial packet — Not all bytes have arrived yet
  if (avail < total_len) {
    if (m_overflow) {
      MQTT_LOG_ERROR(
        "Buffer overflow truncated a packet after preserving earlier buffered traffic — connection should be closed.");
      m_head     = m_tail;
      m_overflow = false;
      error      = TRANSPORT_ERROR_PKT_TOO_BIG;
    }
    return false;
  }

  //--- Success: Extract the complete contiguous packet
  ArrayResize(pkt, total_len);
  ArrayCopy(pkt, m_buf, 0, m_head, total_len);
  m_head += total_len;

  return true;
}

//+------------------------------------------------------------------+
//| CKeepAlive                                                       |
//|                                                                  |
//| Purpose: Manage MQTT keep-alive timers.                          |
//| The client MUST send PINGREQ if no packet is sent within the     |
//| keep-alive interval. If no PINGRESP arrives within the           |
//| m_pingresp_timeout window, the connection is declared dead.      |
//|                                                                  |
//| Reference: MQTT 5.0 §3.1.2.10, §3.12, §3.13                      |
//+------------------------------------------------------------------+
class CKeepAlive {
 private:
  uint  m_keep_alive_sec;    // Negotiated keep-alive in seconds (0 = disabled)
  ulong m_last_send_ms;      // Last outbound activity timestamp that drives the next idle-time PINGREQ check.
  ulong m_pingreq_sent_ms;   // Millisecond watchdog start for the currently outstanding PINGREQ.
  uint  m_pingresp_timeout;  // Seconds to wait for PINGRESP before declaring the connection half-open/dead.
  ulong m_pingreq_sent_us;   // Microsecond copy of the same PINGREQ send instant so RTT can be measured precisely.
  ulong m_last_rtt_us;       // Last PINGREQ→PINGRESP round-trip in microseconds.

 public:
  CKeepAlive();

  //--- Configure keep-alive in seconds (0 to disable)
  void  SetKeepAlive(uint seconds) { m_keep_alive_sec = seconds; }
  uint  GetKeepAlive() const { return m_keep_alive_sec; }

  //--- Configure PINGRESP deadline in seconds (0 = same as keep-alive interval)
  void  SetPingRespTimeout(uint seconds);

  //--- Call this whenever ANY packet is sent to the broker
  void  OnPacketSent();

  //--- Call this when a PINGREQ is specifically sent (separate from OnPacketSent)
  void  OnPingreqSent();

  //--- Call this whenever a PINGRESP is received
  void  OnPingRespReceived();

  //--- Returns true if it is time to send a PINGREQ
  bool  NeedsPing() const;

  //--- Returns true if PINGRESP wait has timed out (connection dead)
  bool  IsPingTimedOut() const;

  //--- Returns true if keep-alive is disabled
  bool  IsDisabled() const { return m_keep_alive_sec == 0; }

  //--- Last measured PINGREQ→PINGRESP round-trip in microseconds (0 if none)
  ulong GetLastPingRTT_us() const { return m_last_rtt_us; }

  //--- Reset all timers (call on connect/reconnect)
  void  Reset();
};

//+------------------------------------------------------------------+
//| CKeepAlive constructor                                           |
//+------------------------------------------------------------------+
CKeepAlive::CKeepAlive() {
  m_keep_alive_sec   = 60;  // Standalone default; CMqttClient overrides to 10 s via SetKeepAlive()
  m_last_send_ms     = 0;
  m_pingreq_sent_ms  = 0;
  m_pingresp_timeout = 5;   // 5s PINGRESP deadline; 0 = fall back to keep-alive interval
  m_pingreq_sent_us  = 0;
  m_last_rtt_us      = 0;
}

//+------------------------------------------------------------------+
//| Reset - Clear all timers                                         |
//+------------------------------------------------------------------+
void CKeepAlive::Reset() {
  m_last_send_ms    = GetMicrosecondCount() / 1000;
  m_pingreq_sent_ms = 0;
  m_pingreq_sent_us = 0;
}

//+------------------------------------------------------------------+
//| SetPingRespTimeout - Configure PINGRESP watchdog                 |
//+------------------------------------------------------------------+
void CKeepAlive::SetPingRespTimeout(uint seconds) {
  m_pingresp_timeout = seconds;
  if (seconds == 0 && m_keep_alive_sec > 30) {
    MQTT_LOG_WARN("PINGRESP timeout inherits the keep-alive interval of " + (string)m_keep_alive_sec
                  + "s when SetPingRespTimeout(0) is used");
  }
}

//+------------------------------------------------------------------+
//| OnPacketSent - Notify that any packet was sent                   |
//+------------------------------------------------------------------+
void CKeepAlive::OnPacketSent() {
  m_last_send_ms = GetMicrosecondCount() / 1000;
  //--- Do NOT clear m_pingreq_sent_ms here. Sending traffic to the broker
  //--- does not prove the connection is alive; only receiving a response
  //--- (PINGRESP or any other packet) does. The PINGRESP deadline started
  //--- by OnPingreqSent() must remain active until OnPingRespReceived()
  //--- or a broker packet is received, so that half-open connections are
  //--- detected even when the EA keeps publishing data.
}

//+------------------------------------------------------------------+
//| OnPingreqSent - Notify that a PINGREQ specifically was sent      |
//+------------------------------------------------------------------+
void CKeepAlive::OnPingreqSent() {
  ulong now_us      = GetMicrosecondCount();
  ulong now_ms      = now_us / 1000;
  m_last_send_ms    = now_ms;
  m_pingreq_sent_ms = now_ms;  // Start the PINGRESP deadline timer
  m_pingreq_sent_us = now_us;  // Capture microsecond timestamp for RTT
}

//+------------------------------------------------------------------+
//| OnPingRespReceived - PINGRESP arrived, cancel deadline           |
//+------------------------------------------------------------------+
void CKeepAlive::OnPingRespReceived() {
  //--- Compute RTT before clearing the PINGREQ timestamp
  if (m_pingreq_sent_us > 0) {
    ulong now_us  = GetMicrosecondCount();
    m_last_rtt_us = (now_us >= m_pingreq_sent_us) ? (now_us - m_pingreq_sent_us) : 0;
  }
  m_pingreq_sent_ms = 0;
  m_pingreq_sent_us = 0;
  m_last_send_ms    = GetMicrosecondCount() / 1000;
}

//+------------------------------------------------------------------+
//| NeedsPing - Returns true if PINGREQ should be sent now           |
//| Logic: If keep-alive is active and the elapsed time since last   |
//|        send is >= keep-alive interval, signal a PINGREQ.         |
//+------------------------------------------------------------------+
bool CKeepAlive::NeedsPing() const {
  if (IsDisabled()) {
    return false;
  }
  if (m_pingreq_sent_ms != 0) {
    return false;  // Already waiting for a PINGRESP
  }

  ulong now_ms  = GetMicrosecondCount() / 1000;
  ulong idle_ms = (now_ms >= m_last_send_ms) ? (now_ms - m_last_send_ms) : 0;

  //--- Per MQTT 5.0 §3.1.2.10: Send PINGREQ before interval expires
  //--- to allow margin for network latency and timer jitter.
  //--- Fire at 80% of the keep-alive interval.
  return idle_ms >= (ulong)(m_keep_alive_sec * 800);
}

//+------------------------------------------------------------------+
//| IsPingTimedOut - Returns true if PINGRESP deadline was missed    |
//| Purpose: Watchdog mechanism to detect "half-open" or dead        |
//|          connections that SocketRead() cannot detect.            |
//+------------------------------------------------------------------+
bool CKeepAlive::IsPingTimedOut() const {
  if (IsDisabled()) {
    return false;
  }
  if (m_pingreq_sent_ms == 0) {
    return false;  // No outstanding PINGREQ to monitor
  }

  //--- Use custom timeout if set, otherwise default to keep-alive interval
  uint  timeout_sec = (m_pingresp_timeout > 0) ? m_pingresp_timeout : m_keep_alive_sec;
  ulong now_ms      = GetMicrosecondCount() / 1000;
  ulong elapsed_ms  = (now_ms >= m_pingreq_sent_ms) ? (now_ms - m_pingreq_sent_ms) : 0;
  return elapsed_ms >= (ulong)(timeout_sec * 1000);
}

//+------------------------------------------------------------------+
//| CAsyncConnector                                                  |
//|                                                                  |
//| Purpose: Non-blocking socket connection state machine.           |
//|          Each Poll() call attempts a short-timeout               |
//|          SocketConnect(), allowing the MQL5 event loop to stay   |
//|          responsive during connection establishment.             |
//|                                                                  |
//| Usage:                                                           |
//|   1. Call Begin() with host, port and timing constraints.        |
//|   2. On each OnTimer() / OnTick(): call Poll(socket).            |
//|      - TRANSPORT_CONNECTING : still in progress, call again.     |
//|      - TRANSPORT_OK         : TCP connected; socket is passed    |
//|                               out via out_socket (caller owns).  |
//|      - TRANSPORT_ERROR_*    : fatal, connection abandoned.       |
//|                                                                  |
//| Design notes:                                                    |
//|   MQL5's SocketConnect() is always blocking. "Non-blocking"      |
//|   behaviour is achieved by using a configurable per-attempt      |
//|   timeout so each Poll() call returns quickly without holding    |
//|   up the event loop. The state machine tracks an overall         |
//|   deadline across retries.                                       |
//+------------------------------------------------------------------+
class CAsyncConnector {
 private:
  string m_host;                // Target hostname or IP
  uint   m_port;                // Target port
  bool   m_tls;                 // true = TLS handshake required after TCP connect
  uint   m_attempt_timeout_ms;  // Per-attempt timeout passed to SocketConnect()
  ulong  m_deadline_ms;         // Absolute wall-clock budget across all retries started by Begin().
  ulong  m_next_attempt_ms;     // Backoff gate; Poll() returns CONNECTING until this retry time is reached.
  uint   m_attempt_count;       // Number of failed attempts in the current Begin() window.
  int    m_socket;              // Working socket handle replaced on each retry to avoid stale connect state.
  bool   m_active;              // true while an async connect is in progress.

  //--- (Re-)allocate a fresh working socket; returns false on SocketCreate failure
  bool   _NewSocket();

 public:
  CAsyncConnector();
  ~CAsyncConnector();

  //--- Start a non-blocking connection attempt.
  //--- attempt_timeout_ms: wall-clock ms allowed per SocketConnect() call.
  //--- overall_timeout_ms: total budget before TRANSPORT_ERROR_TIMEOUT is raised.
  void Begin(const string host, uint port, bool tls, uint attempt_timeout_ms = 500, uint overall_timeout_ms = 15000);

  //--- Advance the state machine.  Must be called on every timer/tick.
  //--- out_socket is set to the open handle only when TRANSPORT_OK is returned;
  //--- caller takes ownership and must NOT call Cancel() afterward.
  ENUM_TRANSPORT_ERROR Poll(int &out_socket);

  //--- Abort an in-progress attempt and release the socket
  void                 Cancel();

  bool                 IsActive() const { return m_active; }
  bool                 IsTLS() const { return m_tls; }
  string               GetHost() const { return m_host; }
  uint                 GetPort() const { return m_port; }
  uint                 GetAttemptTimeoutMs() const { return m_attempt_timeout_ms; }
  uint                 GetRemainingBudgetMs() const {
    ulong now_ms = GetMicrosecondCount() / 1000;
    return (m_deadline_ms > now_ms) ? (uint)(m_deadline_ms - now_ms) : 0;
  }
};

//+------------------------------------------------------------------+
//| CAsyncConnector constructor                                      |
//+------------------------------------------------------------------+
CAsyncConnector::CAsyncConnector() {
  m_host               = "";
  m_port               = 0;
  m_tls                = false;
  m_attempt_timeout_ms = 500;
  m_deadline_ms        = 0;
  m_next_attempt_ms    = 0;
  m_attempt_count      = 0;
  m_socket             = INVALID_HANDLE;
  m_active             = false;
}

//+------------------------------------------------------------------+
//| CAsyncConnector destructor                                       |
//+------------------------------------------------------------------+
CAsyncConnector::~CAsyncConnector() { Cancel(); }

//+------------------------------------------------------------------+
//| _NewSocket - Allocate a fresh working socket                     |
//| Returns false (and sets m_active=false) if SocketCreate fails    |
//+------------------------------------------------------------------+
bool CAsyncConnector::_NewSocket() {
  if (m_socket != INVALID_HANDLE) {
    SocketClose(m_socket);
    m_socket = INVALID_HANDLE;
  }
  m_socket = SocketCreate();
  if (m_socket == INVALID_HANDLE) {
    MQTT_LOG_ERROR("SocketCreate() failed — error " + (string)GetLastError());
    m_active = false;
    return false;
  }
  return true;
}

//+------------------------------------------------------------------+
//| Begin - Initiate a non-blocking connection                       |
//| Parameters: host               - target host / IP                |
//|             port               - target port                     |
//|             tls                - true to require TLS after TCP   |
//|             attempt_timeout_ms - per-attempt SocketConnect limit |
//|             overall_timeout_ms - total budget across all retries |
//+------------------------------------------------------------------+
void CAsyncConnector::Begin(const string host, uint port, bool tls, uint attempt_timeout_ms, uint overall_timeout_ms) {
  Cancel();  // Release any previous in-progress attempt
  m_host               = host;
  m_port               = port;
  m_tls                = tls;
  m_attempt_timeout_ms = (attempt_timeout_ms > 0) ? attempt_timeout_ms : 500;
  m_deadline_ms        = (GetMicrosecondCount() / 1000) + overall_timeout_ms;
  m_next_attempt_ms    = GetMicrosecondCount() / 1000;
  m_attempt_count      = 0;
  m_active             = _NewSocket();
  if (m_active) {
    MQTT_LOG_DEBUG("CAsyncConnector: async connect started to " + host + ":" + (string)port + " (attempt_timeout="
                   + (string)m_attempt_timeout_ms + "ms, overall=" + (string)overall_timeout_ms + "ms)");
  }
}

//+------------------------------------------------------------------+
//| Poll - Advance the async-connect state machine                   |
//| Returns: TRANSPORT_CONNECTING - attempt in progress, call again  |
//|          TRANSPORT_OK         - connected; out_socket is valid   |
//|          TRANSPORT_ERROR_*    - fatal failure                    |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CAsyncConnector::Poll(int &out_socket) {
  out_socket = INVALID_HANDLE;
  if (!m_active) {
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- Check overall deadline
  ulong now_ms = GetMicrosecondCount() / 1000;
  if (now_ms >= m_deadline_ms) {
    MQTT_LOG_ERROR("Overall timeout exceeded for " + m_host + ":" + (string)m_port);
    Cancel();
    return TRANSPORT_ERROR_TIMEOUT;
  }

  if (now_ms < m_next_attempt_ms) {
    return TRANSPORT_CONNECTING;
  }

  //--- Clamp per-attempt timeout to remaining budget
  uint remaining_ms      = (uint)(m_deadline_ms - now_ms);
  uint effective_timeout = (m_attempt_timeout_ms < remaining_ms) ? m_attempt_timeout_ms : remaining_ms;

  //--- Attempt the TCP connection
  if (SocketConnect(m_socket, m_host, m_port, effective_timeout)) {
    //--- Success: transfer socket ownership to caller
    out_socket = m_socket;
    m_socket   = INVALID_HANDLE;
    m_active   = false;
    MQTT_LOG_DEBUG("TCP connected to " + m_host + ":" + (string)m_port);
    return TRANSPORT_OK;
  }

  //--- This attempt timed out; pace retries to reduce DNS/SYN churn during outages.
  int  last_err        = GetLastError();
  uint remaining_after = (uint)(m_deadline_ms - now_ms);
  m_attempt_count++;
  uint backoff_shift  = (m_attempt_count > 4) ? 4 : m_attempt_count;
  uint base_delay_ms  = 50u << backoff_shift;  // 100, 200, 400, 800, 800...
  uint jitter_ms      = (uint)((GetMicrosecondCount() / 1000) % 125);
  uint retry_delay_ms = base_delay_ms + jitter_ms;
  if (retry_delay_ms > remaining_after) {
    retry_delay_ms = remaining_after;
  }
  if (m_attempt_count <= 3 || (m_attempt_count % 5) == 0) {
    MQTT_LOG_WARN("Attempt failed (error " + (string)last_err + "), retrying in " + (string)retry_delay_ms
                  + "ms, remaining budget " + (string)remaining_after + "ms");
  } else {
    MQTT_LOG_DEBUG("Attempt failed (error " + (string)last_err + "), retrying in " + (string)retry_delay_ms + "ms");
  }
  m_next_attempt_ms = now_ms + retry_delay_ms;
  _NewSocket();  // Closes old socket, opens new one; sets m_active=false on failure
  return m_active ? TRANSPORT_CONNECTING : TRANSPORT_ERROR_SOCKET;
}

//+------------------------------------------------------------------+
//| Cancel - Abort any in-progress attempt and release the socket    |
//+------------------------------------------------------------------+
void CAsyncConnector::Cancel() {
  if (m_socket != INVALID_HANDLE) {
    SocketClose(m_socket);
    m_socket = INVALID_HANDLE;
  }
  m_active          = false;
  m_next_attempt_ms = 0;
  m_attempt_count   = 0;
}

//+------------------------------------------------------------------+
//| CMqttTransport                                                   |
//|                                                                  |
//| Purpose: Unified send/receive abstraction over a raw MQL5 socket.|
//| Supports both plaintext and TLS sockets via the same API.        |
//|                                                                  |
//| The Poll() method:                                               |
//|   - Reads available bytes from socket in non-blocking fashion    |
//|   - Feeds bytes to the internal CPacketFramer                    |
//|   - Checks keep-alive timers (sends PINGREQ when idle)           |
//|   - Populates an output array of complete MQTT packets           |
//|                                                                  |
//| Reference: MQL5 TLS API                                          |
//+------------------------------------------------------------------+
class CMqttTransport : public IMqttTransport {
 private:
  int   m_socket;                  // MQL5 socket handle (INVALID_HANDLE if disconnected)
  bool  m_tls;                     // true = TLS socket; false = plaintext
  bool  m_tls_handshake_complete;  // true after explicit TLS completes or we defer final negotiation to the first TLS
                                   // application write
  bool  m_tls_appdata_ready;       // true after the socket is ready for the first TLS application write
  bool  m_tls_skip_explicit_handshake;  // true when a fresh TLS socket should defer negotiation directly to the first
                                        // application write
  ulong m_tls_phase_started_us;         // Monotonic timestamp when the current TLS phase began.
  ulong
       m_tls_next_write_probe_us;  // Backoff gate for deferred application-write probes during pending TLS negotiation.
  uint m_tls_handshake_restart_count;        // Bounded restart count when explicit TLS handshaking stalls repeatedly.
  uint m_tls_pending_count;                  // Consecutive 5274 "still pending" polls in the current TLS phase.
  CPacketFramer                m_framer;     // TCP stream packet framer
  CKeepAlive                   m_keepalive;  // Keep-alive timer manager
  CAsyncConnector              m_async_connect;  // Non-blocking connection state machine
  ENUM_TRANSPORT_CONNECT_PHASE m_connect_phase;  // Fine-grained connect phase for diagnostics/state reporting.
  bool                         m_connected;      // True only when the socket is usable for MQTT application traffic.
  uint
    m_read_timeout;  // SocketRead timeout in ms (default 100 ms — balances responsiveness with high-latency tolerance)
  uint  m_blocking_warn_threshold_ms;  // Warn when a blocking transport phase exceeds this duration (0 = disabled)
  ulong m_last_blocking_operation_us;  // Duration of the last blocking connect/TLS phase in microseconds.
  uchar m_pingreq_buf[];               // Pre-built PINGREQ packet bytes
  uchar m_recv_buf[];  // Pre-allocated socket receive buffer — reused each Poll() to eliminate GC alloc/free

#ifdef MQTT_UNIT_TESTS
  bool m_test_write_ready_stub_enabled;
  bool m_test_write_ready;
#endif

  //--- Pre-build the PINGREQ bytes once at construction time
  void _BuildPingreq();
  bool _RunTlsHandshake(const string host, uint port);
  bool _ShouldDeferPendingTlsHandshake(ulong tls_elapsed_us, uint remaining_budget_ms, uint attempt_timeout_ms) const;
  bool _IsSocketWriteReady() const;
  void _LogBlockingOperation(const string operation, const string target, ulong elapsed_us) const;

  //--- Internal Connect handler to prevent ambiguous implicit uint->bool casting
  ENUM_TRANSPORT_ERROR _Connect(const string host, uint port, bool use_tls, uint timeout_ms);

 public:
  CMqttTransport(uint read_timeout_ms = 100);
  ~CMqttTransport();

  //--- Connect to broker over plaintext TCP (blocking — waits up to timeout_ms)
  ENUM_TRANSPORT_ERROR Connect(const string host, uint port, uint timeout_ms = 5000);

  //--- Connect to broker over TLS (blocking — waits up to timeout_ms)
  ENUM_TRANSPORT_ERROR ConnectTLS(const string host, uint port, uint timeout_ms = 5000);

  //--- Begin a non-blocking TCP connection attempt.
  //--- Returns TRANSPORT_CONNECTING immediately.
  //--- Call Poll() repeatedly; it will complete the TCP (and optional TLS)
  //--- handshake and transition IsConnected() to true when ready.
  //--- attempt_timeout_ms: per-Poll() SocketConnect limit (default 500 ms).
  //--- overall_timeout_ms: total budget across all retries (default 15 000 ms).
  ENUM_TRANSPORT_ERROR ConnectAsync(const string host, uint port, uint attempt_timeout_ms = 500,
                                    uint overall_timeout_ms = 15000);

  //--- Same as ConnectAsync() but initiates TLS after TCP connect succeeds.
  ENUM_TRANSPORT_ERROR ConnectTLSAsync(const string host, uint port, uint attempt_timeout_ms = 500,
                                       uint overall_timeout_ms = 15000);

  //--- Close the socket and reset all state
  virtual void         Disconnect() override;

  //--- True while the socket is open and no fatal error has been detected
  virtual bool         IsConnected() const override { return m_connected && m_socket != INVALID_HANDLE; }

  //--- True while a non-blocking connection attempt is in progress
  virtual bool         IsConnecting() const override {
    return m_async_connect.IsActive() || m_connect_phase == TRANSPORT_PHASE_TLS_HANDSHAKING;
  }

  virtual ENUM_TRANSPORT_CONNECT_PHASE GetConnectPhase() const override { return m_connect_phase; }

  //--- Transmit raw packet bytes; also resets the keep-alive idle timer
  virtual ENUM_TRANSPORT_ERROR         Send(const uchar &pkt[], int len = -1) override;

  //--- Poll: drain the socket, frame packets, service keep-alive.
  //--- Also advances any pending async connection attempt.
  //--- out_packets[] receives zero or more complete MQTT packets.
  //--- Returns TRANSPORT_OK or TRANSPORT_CONNECTING (async in progress) or a fatal error.
  virtual ENUM_TRANSPORT_ERROR         Poll(PacketBuffer &out_packets[], uint &out_count) override;

  //--- Framer and keep-alive configuration
  virtual void                         SetMaxPacketSize(uint max_size) override { m_framer.SetMaxPacketSize(max_size); }
  //--- Set maximum framer backing-buffer size in bytes (0 = unlimited)
  virtual void                         SetMaxBufferSize(uint max_size) override { m_framer.SetMaxBufferSize(max_size); }
  virtual void                         SetKeepAlive(uint seconds) override { m_keepalive.SetKeepAlive(seconds); }
  virtual void                         SetPingRespTimeout(uint sec) override { m_keepalive.SetPingRespTimeout(sec); }
  virtual void                         SetReadTimeout(uint ms) override { m_read_timeout = ms; }
  virtual void  SetBlockingOperationWarnThreshold(uint ms) override { m_blocking_warn_threshold_ms = ms; }

  //--- Read-only access to keep-alive state for diagnostics
  bool          NeedsPing() const { return m_keepalive.NeedsPing(); }
  bool          IsPingTimedOut() const { return m_keepalive.IsPingTimedOut(); }

  //--- Last PINGREQ→PINGRESP round-trip in microseconds (0 = no measurement yet)
  virtual ulong GetLastPingRTT_us() const override { return m_keepalive.GetLastPingRTT_us(); }
  virtual ulong GetLastBlockingOperationDuration_us() const override { return m_last_blocking_operation_us; }

  //--- Raw socket handle access for TOFU certificate inspection
  virtual int   GetSocket() const override { return m_socket; }

#ifdef MQTT_UNIT_TESTS
  void TestSetTls(bool use_tls) { m_tls = use_tls; }
  void TestSetTlsHandshakeComplete(bool complete) { m_tls_handshake_complete = complete; }
  void TestSetTlsAppdataReady(bool ready) { m_tls_appdata_ready = ready; }
  void TestSetTlsSkipExplicitHandshake(bool skip) { m_tls_skip_explicit_handshake = skip; }
  void TestSetSocket(int socket) { m_socket = socket; }
  void TestSetConnected(bool connected) { m_connected = connected; }
  void TestSetConnectPhase(ENUM_TRANSPORT_CONNECT_PHASE phase) { m_connect_phase = phase; }
  void TestSetTlsPhaseStartedUs(ulong started_us) { m_tls_phase_started_us = started_us; }
  void TestSetTlsNextWriteProbeUs(ulong probe_us) { m_tls_next_write_probe_us = probe_us; }
  void TestSetTlsHandshakeRestartCount(uint count) { m_tls_handshake_restart_count = count; }
  bool TestShouldDeferPendingTlsHandshake(ulong tls_elapsed_us, uint remaining_budget_ms,
                                          uint attempt_timeout_ms) const {
    return _ShouldDeferPendingTlsHandshake(tls_elapsed_us, remaining_budget_ms, attempt_timeout_ms);
  }
  void TestSetWriteReady(bool ready) {
    m_test_write_ready_stub_enabled = true;
    m_test_write_ready              = ready;
  }
  void TestClearWriteReadyStub() { m_test_write_ready_stub_enabled = false; }
#endif
};

//+------------------------------------------------------------------+
//| CMqttTransport constructor                                       |
//| Parameters: read_timeout_ms - SocketRead timeout in milliseconds |
//|             (default 100 ms; increase for high-latency networks) |
//+------------------------------------------------------------------+
CMqttTransport::CMqttTransport(uint read_timeout_ms) {
  m_socket                      = INVALID_HANDLE;
  m_tls                         = false;
  m_tls_handshake_complete      = false;
  m_tls_appdata_ready           = false;
  m_tls_skip_explicit_handshake = false;
  m_tls_phase_started_us        = 0;
  m_tls_next_write_probe_us     = 0;
  m_tls_handshake_restart_count = 0;
  m_tls_pending_count           = 0;
  m_connect_phase               = TRANSPORT_PHASE_IDLE;
  m_connected                   = false;
  m_read_timeout                = read_timeout_ms;
  m_blocking_warn_threshold_ms  = 250;
  m_last_blocking_operation_us  = 0;
  _BuildPingreq();
  ArrayResize(m_recv_buf, 4096, 4096);  // Pre-allocate once — SocketRead/TlsRead resize in-place

#ifdef MQTT_UNIT_TESTS
  m_test_write_ready_stub_enabled = false;
  m_test_write_ready              = true;
#endif
}

//+------------------------------------------------------------------+
//| CMqttTransport destructor                                        |
//+------------------------------------------------------------------+
CMqttTransport::~CMqttTransport() {
  if (m_socket != INVALID_HANDLE) {
    SocketClose(m_socket);
  }
  ArrayFree(m_pingreq_buf);
}

//+------------------------------------------------------------------+
//| _LogBlockingOperation                                            |
//| Purpose: Warn when a blocking handshake exceeds the threshold    |
//+------------------------------------------------------------------+
void CMqttTransport::_LogBlockingOperation(const string operation, const string target, ulong elapsed_us) const {
  if (m_blocking_warn_threshold_ms == 0) {
    return;
  }

  ulong threshold_us = (ulong)m_blocking_warn_threshold_ms * 1000UL;
  if (elapsed_us >= threshold_us) {
    MQTT_LOG_WARN(operation + " blocked the chart thread for " + (string)(elapsed_us / 1000UL)
                  + " ms while connecting to " + target + ".");
  }
}

//+------------------------------------------------------------------+
//| _IsSocketWriteReady                                              |
//| Purpose: Treat a not-yet-writable TLS socket as still connecting |
//|          instead of probing application-data writes too early.   |
//+------------------------------------------------------------------+
bool CMqttTransport::_IsSocketWriteReady() const {
#ifdef MQTT_UNIT_TESTS
  if (m_test_write_ready_stub_enabled) {
    return m_test_write_ready;
  }
#endif
  return m_socket != INVALID_HANDLE && SocketIsWritable(m_socket) > 0;
}

//+------------------------------------------------------------------+
//| _RunTlsHandshake                                                 |
//| Purpose: Measure and warn on the blocking TLS handshake phase    |
//+------------------------------------------------------------------+
bool CMqttTransport::_RunTlsHandshake(const string host, uint port) {
  if (port == 443) {
    //--- The raw 443 MT5 probe showed that explicit SocketTlsHandshake() on
    //--- implicit HTTPS/TLS port 443 never completes on MT5 build 5698 and
    //--- breaks later SocketTlsSend() calls. Preserve the socket and let the
    //--- first TLS send/read drive the platform's implicit negotiation.
    m_last_blocking_operation_us = 0;
    MQTT_LOG_DEBUG("Skipping explicit TLS handshake on implicit port 443 for " + host);
    return true;
  }

  ulong started_us             = GetMicrosecondCount();
  bool  ok                     = SocketTlsHandshake(m_socket, host);
  m_last_blocking_operation_us = GetMicrosecondCount() - started_us;
  _LogBlockingOperation("TLS handshake", host, m_last_blocking_operation_us);
  return ok;
}

//+------------------------------------------------------------------+
//| _ShouldDeferPendingTlsHandshake                                  |
//| Purpose: Keep restarted non-443 TLS sockets from burning the     |
//|          remaining overall connect budget on repeated 5274 polls.|
//+------------------------------------------------------------------+
bool CMqttTransport::_ShouldDeferPendingTlsHandshake(ulong tls_elapsed_us, uint remaining_budget_ms,
                                                     uint attempt_timeout_ms) const {
  const ulong tls_defer_after_pending_us  = 20000000UL;
  const uint  tls_defer_budget_reserve_ms = 5000u;
  const uint  max_tls_handshake_restarts  = 2u;

  if (m_tls_handshake_restart_count > 0 && remaining_budget_ms <= attempt_timeout_ms + tls_defer_budget_reserve_ms) {
    return true;
  }

  if (m_tls_handshake_restart_count >= max_tls_handshake_restarts && tls_elapsed_us >= tls_defer_after_pending_us) {
    return true;
  }

  return false;
}

//+------------------------------------------------------------------+
//| _BuildPingreq - Cache the 2-byte PINGREQ packet                  |
//+------------------------------------------------------------------+
void CMqttTransport::_BuildPingreq() {
  CPingreq pq;
  pq.Build(m_pingreq_buf);
}

//+------------------------------------------------------------------+
//| Connect - Establish a plaintext TCP connection                   |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttTransport::Connect(const string host, uint port, uint timeout_ms) {
  return _Connect(host, port, false, timeout_ms);
}

//+------------------------------------------------------------------+
//| ConnectTLS - Establish a TLS connection                          |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttTransport::ConnectTLS(const string host, uint port, uint timeout_ms) {
  return _Connect(host, port, true, timeout_ms);
}

//+------------------------------------------------------------------+
//| ConnectAsync - Begin a non-blocking plaintext connection         |
//| Returns TRANSPORT_CONNECTING immediately.                        |
//| Call Poll() on each tick; it will complete the handshake and     |
//| flip IsConnected() to true when the TCP session is established.  |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttTransport::ConnectAsync(const string host, uint port, uint attempt_timeout_ms,
                                                  uint overall_timeout_ms) {
  Disconnect();
  m_async_connect.Begin(host, port, false, attempt_timeout_ms, overall_timeout_ms);
  m_connect_phase = m_async_connect.IsActive() ? TRANSPORT_PHASE_TCP_CONNECTING : TRANSPORT_PHASE_IDLE;
  return m_async_connect.IsActive() ? TRANSPORT_CONNECTING : TRANSPORT_ERROR_SOCKET;
}

//+------------------------------------------------------------------+
//| ConnectTLSAsync - Begin a non-blocking TLS connection            |
//| TCP is connected asynchronously; TLS handshake is deferred to a  |
//| dedicated Poll() phase after TCP connect succeeds.               |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttTransport::ConnectTLSAsync(const string host, uint port, uint attempt_timeout_ms,
                                                     uint overall_timeout_ms) {
  Disconnect();
  m_async_connect.Begin(host, port, true, attempt_timeout_ms, overall_timeout_ms);
  m_connect_phase = m_async_connect.IsActive() ? TRANSPORT_PHASE_TCP_CONNECTING : TRANSPORT_PHASE_IDLE;
  return m_async_connect.IsActive() ? TRANSPORT_CONNECTING : TRANSPORT_ERROR_SOCKET;
}

//+------------------------------------------------------------------+
//| _Connect - Internal unified connection handler                   |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttTransport::_Connect(const string host, uint port, bool use_tls, uint timeout_ms) {
  Disconnect();
  m_connect_phase = TRANSPORT_PHASE_TCP_CONNECTING;

  m_socket        = SocketCreate();
  if (m_socket == INVALID_HANDLE) {
    MQTT_LOG_ERROR("SocketCreate() failed — error " + (string)GetLastError());
    m_connect_phase = TRANSPORT_PHASE_IDLE;
    return TRANSPORT_ERROR_SOCKET;
  }

  if (!SocketConnect(m_socket, host, port, timeout_ms)) {
    MQTT_LOG_ERROR("SocketConnect(" + host + ":" + (string)port + ") failed — error " + (string)GetLastError());
    SocketClose(m_socket);
    m_socket        = INVALID_HANDLE;
    m_connect_phase = TRANSPORT_PHASE_IDLE;
    return TRANSPORT_ERROR_SOCKET;
  }

  if (use_tls) {
    m_connect_phase = TRANSPORT_PHASE_TLS_HANDSHAKING;
    if (!_RunTlsHandshake(host, port)) {
      MQTT_LOG_ERROR("TLS handshake failed for " + host + " — error " + (string)GetLastError());
      SocketClose(m_socket);
      m_socket        = INVALID_HANDLE;
      m_connect_phase = TRANSPORT_PHASE_IDLE;
      return TRANSPORT_ERROR_TLS;
    }
  }

  m_tls                         = use_tls;
  m_tls_handshake_complete      = use_tls;
  m_tls_appdata_ready           = !use_tls;
  m_tls_skip_explicit_handshake = false;
  m_tls_phase_started_us        = 0;
  m_tls_next_write_probe_us     = 0;
  m_tls_handshake_restart_count = 0;
  m_tls_pending_count           = 0;
  m_connected                   = true;
  m_connect_phase               = TRANSPORT_PHASE_CONNECTED;
  m_framer.Reset();
  m_keepalive.Reset();
  MQTT_LOG_INFO("Connected (" + (use_tls ? "TLS" : "plaintext") + ") to " + host + ":" + (string)port);
  return TRANSPORT_OK;
}

//+------------------------------------------------------------------+
//| Disconnect - Close socket and reset all state                    |
//+------------------------------------------------------------------+
void CMqttTransport::Disconnect() {
  m_async_connect.Cancel();  // Abort any in-progress non-blocking attempt
  if (m_socket != INVALID_HANDLE) {
    SocketClose(m_socket);
    m_socket = INVALID_HANDLE;
  }
  m_tls                         = false;
  m_tls_handshake_complete      = false;
  m_tls_appdata_ready           = false;
  m_tls_skip_explicit_handshake = false;
  m_tls_phase_started_us        = 0;
  m_tls_next_write_probe_us     = 0;
  m_tls_handshake_restart_count = 0;
  m_tls_pending_count           = 0;
  m_connected                   = false;
  m_connect_phase               = TRANSPORT_PHASE_IDLE;
  m_framer.Reset();
}

//+------------------------------------------------------------------+
//| Send - Transmit raw packet bytes over the active socket          |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttTransport::Send(const uchar &pkt[], int len) {
  if (!IsConnected()) {
    MQTT_LOG_WARN("Not connected.");
    return TRANSPORT_ERROR_SOCKET;
  }

  int send_len = (len < 0) ? ArraySize(pkt) : len;
  if (send_len <= 0) {
    return TRANSPORT_OK;
  }

  //--- Send loop: retries until all bytes are transmitted (handles TCP partial writes).
  //--- MQL-001: A single send_buf[] is allocated lazily (only on partial write) to
  //--- avoid creating a fresh temporary array on every retry iteration.
  uint  total_sent = 0;
  uchar send_buf[];
  while (total_sent < (uint)send_len) {
    int remaining = send_len - (int)total_sent;
    int chunk;
    if (total_sent == 0) {
      //--- First attempt: send directly from the input array (zero-copy)
      chunk = m_tls ? SocketTlsSend(m_socket, pkt, remaining) : SocketSend(m_socket, pkt, remaining);
    } else {
      //--- Partial write: copy remaining bytes into send_buf once, then reuse
      if (ArraySize(send_buf) == 0) {
        ArrayResize(send_buf, remaining);
        ArrayCopy(send_buf, pkt, 0, total_sent, remaining);
      }
      chunk = m_tls ? SocketTlsSend(m_socket, send_buf, remaining) : SocketSend(m_socket, send_buf, remaining);
      //--- Advance send_buf for next iteration by shifting remaining data forward
      if (chunk > 0 && chunk < remaining) {
        ArrayCopy(send_buf, send_buf, 0, chunk, remaining - chunk);
      }
    }
    if (chunk <= 0) {
      int send_err = GetLastError();
      if (m_tls && !m_tls_appdata_ready && total_sent == 0 && (send_err == 5274 || send_err == 5273)) {
        if (!m_tls_handshake_complete && _RunTlsHandshake(m_async_connect.GetHost(), m_async_connect.GetPort())) {
          m_tls_handshake_complete  = true;
          m_tls_next_write_probe_us = GetMicrosecondCount() + 250000UL;
          m_tls_pending_count       = 0;
          return TRANSPORT_CONNECTING;
        }

        m_tls_next_write_probe_us = GetMicrosecondCount() + 1000000UL;
        return TRANSPORT_CONNECTING;
      }

      MQTT_LOG_ERROR("Failed after " + (string)total_sent + "/" + (string)send_len + " bytes, error "
                     + (string)send_err);
      Disconnect();  // Close socket cleanly on send error; prevents handle leak
      return TRANSPORT_ERROR_SEND;
    }
    total_sent += (uint)chunk;
  }

  if (m_tls) {
    m_tls_appdata_ready       = true;
    m_tls_next_write_probe_us = 0;
  }
  m_keepalive.OnPacketSent();
  return TRANSPORT_OK;
}

//+------------------------------------------------------------------+
//| Poll - Read from socket, extract complete packets, manage keeps  |
//| Parameters: out_packets - [OUT] complete MQTT packets received   |
//|             out_count   - [OUT] number of packets in out_packets |
//| Returns: TRANSPORT_OK or TRANSPORT_CONNECTING (async in-progress)|
//|          or a fatal error code                                   |
//| Note: Also advances any pending non-blocking connection attempt. |
//|       TLS is exposed as a distinct connect phase so callers can  |
//|       surface that blocking platform limitation explicitly.      |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttTransport::Poll(PacketBuffer &out_packets[], uint &out_count) {
  out_count = 0;
  ArrayResize(out_packets, 0);

  if (m_connect_phase == TRANSPORT_PHASE_TLS_HANDSHAKING) {
    if (m_tls_skip_explicit_handshake) {
      m_tls                     = true;
      m_tls_handshake_complete  = true;
      m_tls_appdata_ready       = false;
      m_connected               = true;
      m_connect_phase           = TRANSPORT_PHASE_CONNECTED;
      m_tls_next_write_probe_us = GetMicrosecondCount() + 250000UL;
      m_framer.Reset();
      m_keepalive.Reset();
      MQTT_LOG_DEBUG("Skipping explicit TLS handshake for " + m_async_connect.GetHost() + ":"
                     + (string)m_async_connect.GetPort()
                     + " after a bounded restart; deferring negotiation to the first TLS application write");
      return TRANSPORT_CONNECTING;
    }

    if (!m_tls_handshake_complete) {
      if (!_RunTlsHandshake(m_async_connect.GetHost(), m_async_connect.GetPort())) {
        int tls_err = GetLastError();
        if (tls_err == 5274) {
          m_tls_pending_count++;

          if (m_async_connect.GetPort() != 443) {
            const ulong tls_restart_after_pending_us  = 15000000UL;
            const uint  tls_restart_budget_reserve_ms = 7500u;
            const uint  max_tls_handshake_restarts    = 2u;
            ulong tls_elapsed_us = (m_tls_phase_started_us > 0 && GetMicrosecondCount() >= m_tls_phase_started_us) ?
                                     (GetMicrosecondCount() - m_tls_phase_started_us) :
                                     0;
            uint  remaining_budget_ms = m_async_connect.GetRemainingBudgetMs();
            uint  attempt_timeout_ms  = m_async_connect.GetAttemptTimeoutMs();
            if (tls_elapsed_us >= tls_restart_after_pending_us
                && m_tls_handshake_restart_count < max_tls_handshake_restarts) {
              if (remaining_budget_ms > attempt_timeout_ms + tls_restart_budget_reserve_ms) {
                if (m_socket != INVALID_HANDLE) {
                  SocketClose(m_socket);
                  m_socket = INVALID_HANDLE;
                }
                m_tls                         = false;
                m_tls_handshake_complete      = false;
                m_tls_appdata_ready           = false;
                m_connected                   = false;
                m_connect_phase               = TRANSPORT_PHASE_IDLE;
                m_tls_phase_started_us        = 0;
                m_tls_next_write_probe_us     = 0;
                m_tls_pending_count           = 0;
                m_tls_skip_explicit_handshake = false;
                m_tls_handshake_restart_count++;
                m_async_connect.Begin(m_async_connect.GetHost(), m_async_connect.GetPort(), true, attempt_timeout_ms,
                                      remaining_budget_ms);
                m_connect_phase = m_async_connect.IsActive() ? TRANSPORT_PHASE_TCP_CONNECTING : TRANSPORT_PHASE_IDLE;
                if (m_async_connect.IsActive()) {
                  MQTT_LOG_WARN(
                    "TLS handshake remained pending for " + m_async_connect.GetHost() + ":"
                    + (string)m_async_connect.GetPort()
                    + "; restarting the socket and retrying the explicit TLS handshake on a fresh connection"
                    + " (restart #" + (string)m_tls_handshake_restart_count + ")");
                  return TRANSPORT_CONNECTING;
                }
                return TRANSPORT_ERROR_SOCKET;
              }
            }

            if (_ShouldDeferPendingTlsHandshake(tls_elapsed_us, remaining_budget_ms, attempt_timeout_ms)) {
              m_tls                     = true;
              m_tls_handshake_complete  = false;
              m_tls_appdata_ready       = false;
              m_connected               = true;
              m_connect_phase           = TRANSPORT_PHASE_CONNECTED;
              m_tls_next_write_probe_us = GetMicrosecondCount() + 1000000UL;
              m_framer.Reset();
              m_keepalive.Reset();
              if (m_tls_handshake_restart_count > 0 && remaining_budget_ms <= attempt_timeout_ms + 5000u) {
                MQTT_LOG_DEBUG("Restarted TLS handshake still pending for " + m_async_connect.GetHost() + ":"
                               + (string)m_async_connect.GetPort()
                               + "; remaining connect budget is low, so final negotiation is deferred to the first TLS "
                                 "application write");
              } else {
                MQTT_LOG_DEBUG("TLS handshake still pending for " + m_async_connect.GetHost() + ":"
                               + (string)m_async_connect.GetPort()
                               + "; deferring final negotiation to the first TLS application write");
              }
              return TRANSPORT_CONNECTING;
            }
          }

          if (m_async_connect.GetPort() == 443) {
            MQTT_LOG_DEBUG("TLS handshake still pending on implicit port 443 for " + m_async_connect.GetHost());
          } else {
            MQTT_LOG_DEBUG("TLS handshake still pending for " + m_async_connect.GetHost() + ":"
                           + (string)m_async_connect.GetPort());
          }
          return TRANSPORT_CONNECTING;
        }

        MQTT_LOG_ERROR("TLS handshake failed for " + m_async_connect.GetHost() + " — error " + (string)tls_err);
        SocketClose(m_socket);
        m_socket        = INVALID_HANDLE;
        m_connect_phase = TRANSPORT_PHASE_IDLE;
        return TRANSPORT_ERROR_TLS;
      }

      m_tls_handshake_complete = true;
      m_tls_pending_count      = 0;
    }

    m_tls                     = true;
    m_tls_appdata_ready       = false;
    m_connected               = true;
    m_connect_phase           = TRANSPORT_PHASE_CONNECTED;
    m_tls_next_write_probe_us = GetMicrosecondCount() + 250000UL;
    m_framer.Reset();
    m_keepalive.Reset();
    MQTT_LOG_DEBUG("Async connect completed (TLS) to " + m_async_connect.GetHost() + ":"
                   + (string)m_async_connect.GetPort());
    return TRANSPORT_CONNECTING;
  }

  //--- ── Async connect phase ─────────────────────────────────────
  //--- Advance the non-blocking connection state machine if active.
  //--- This must come before the IsConnected() guard below.
  if (m_async_connect.IsActive()) {
    int                  new_socket  = INVALID_HANDLE;
    ENUM_TRANSPORT_ERROR conn_result = m_async_connect.Poll(new_socket);

    if (conn_result == TRANSPORT_CONNECTING) {
      //--- Still waiting — event loop stays unblocked
      return TRANSPORT_CONNECTING;
    }

    if (conn_result != TRANSPORT_OK) {
      //--- Permanent failure: TRANSPORT_ERROR_TIMEOUT or TRANSPORT_ERROR_SOCKET
      MQTT_LOG_ERROR("Async connect failed (" + (string)conn_result + ").");
      m_connect_phase = TRANSPORT_PHASE_IDLE;
      return conn_result;
    }

    //--- TCP is up: store socket and either finalize immediately or transition
    //--- into a distinct TLS phase for the next Poll() call.
    m_socket = new_socket;
    if (m_async_connect.IsTLS()) {
      m_tls_handshake_complete  = false;
      m_tls_appdata_ready       = false;
      m_tls_phase_started_us    = GetMicrosecondCount();
      m_tls_next_write_probe_us = 0;
      m_tls_pending_count       = 0;
      m_connect_phase           = TRANSPORT_PHASE_TLS_HANDSHAKING;
      return TRANSPORT_CONNECTING;
    } else {
      m_tls                         = false;
      m_tls_handshake_complete      = false;
      m_tls_appdata_ready           = true;
      m_tls_skip_explicit_handshake = false;
      m_tls_phase_started_us        = 0;
      m_tls_next_write_probe_us     = 0;
      m_tls_handshake_restart_count = 0;
      m_tls_pending_count           = 0;
      m_connected                   = true;
      m_connect_phase               = TRANSPORT_PHASE_CONNECTED;
      m_framer.Reset();
      m_keepalive.Reset();
      MQTT_LOG_DEBUG("Async connect completed (plaintext) to " + m_async_connect.GetHost() + ":"
                     + (string)m_async_connect.GetPort());
      return TRANSPORT_CONNECTING;
    }
  }

  //--- ── Normal I/O phase ─────────────────────────────────────────
  if (!IsConnected()) {
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- When TLS negotiation is being driven by the first application write,
  //--- do not probe reads yet. The broker cannot send MQTT data before CONNECT,
  //--- and MT5 can surface a spurious 5273 if SocketTlsReadAvailable() is used
  //--- before the first successful SocketTlsSend().
  if (m_tls && !m_tls_appdata_ready) {
    if (!m_tls_handshake_complete) {
      if (_RunTlsHandshake(m_async_connect.GetHost(), m_async_connect.GetPort())) {
        m_tls_handshake_complete  = true;
        m_tls_next_write_probe_us = GetMicrosecondCount() + 250000UL;
        m_tls_pending_count       = 0;
        MQTT_LOG_DEBUG("Deferred TLS handshake completed for " + m_async_connect.GetHost() + ":"
                       + (string)m_async_connect.GetPort());
      } else {
        int tls_err = GetLastError();
        if (tls_err != 5274) {
          MQTT_LOG_ERROR("TLS handshake failed for " + m_async_connect.GetHost() + " — error " + (string)tls_err);
          Disconnect();
          return TRANSPORT_ERROR_TLS;
        }
      }
    }

    if (!m_tls_handshake_complete) {
      if (m_tls_next_write_probe_us > 0 && GetMicrosecondCount() < m_tls_next_write_probe_us) {
        return TRANSPORT_CONNECTING;
      }
      return TRANSPORT_OK;
    }

    if (_IsSocketWriteReady()) {
      m_tls_next_write_probe_us = 0;
      return TRANSPORT_OK;
    }

    ulong now_us = GetMicrosecondCount();
    if (m_tls_next_write_probe_us == 0) {
      m_tls_next_write_probe_us = now_us + 250000UL;
    }
    if (now_us < m_tls_next_write_probe_us) {
      return TRANSPORT_CONNECTING;
    }

    return TRANSPORT_OK;
  }

  //--- Service keep-alive: send PINGREQ if the connection has been idle
  if (m_keepalive.NeedsPing()) {
    ENUM_TRANSPORT_ERROR send_err = Send(m_pingreq_buf);
    if (send_err != TRANSPORT_OK) {
      return send_err;
    }
    //--- Send() calls OnPacketSent() which only updates m_last_send_ms (not the
    //--- PINGRESP deadline). OnPingreqSent() arms the PINGRESP deadline timer.
    m_keepalive.OnPingreqSent();  // Arm PINGRESP deadline
    MQTT_LOG_DEBUG("PINGREQ sent.");
  }

  //--- Detect PINGRESP timeout (connection unresponsive)
  if (m_keepalive.IsPingTimedOut()) {
    MQTT_LOG_ERROR("PINGRESP timeout — connection dead, closing socket.");
    Disconnect();  // Closes socket and resets state; prevents OS handle leak
    return TRANSPORT_ERROR_RECV;
  }

  //--- Read available bytes from the socket (m_recv_buf reused each call — no GC alloc/free)
  int bytes_read = 0;

  if (m_tls) {
    //--- On MT5, probing TLS reads too eagerly right after a deferred first-write
    //--- recovery can surface a spurious 5273 before the secure socket is stably
    //--- readable. Gate SocketTlsReadAvailable() behind SocketIsReadable() the same
    //--- way plain sockets already do.
    uint readable = (uint)SocketIsReadable(m_socket);
    if (readable > 0) {
      bytes_read = SocketTlsReadAvailable(m_socket, m_recv_buf, 4096);
    }
  } else {
    uint readable = (uint)SocketIsReadable(m_socket);
    if (readable > 0) {
      //--- Read only the bytes MT5 reports as immediately available.
      //--- Asking SocketRead() for a larger buffer with a non-zero wait can
      //--- spuriously surface 5273 on public plaintext listeners even though
      //--- the broker accepted the connection and is still waiting.
      int read_len = (int)((readable < 4096u) ? readable : 4096u);
      bytes_read   = SocketRead(m_socket, m_recv_buf, read_len, 0);
    }
  }

  if (bytes_read < 0) {
    int err = GetLastError();
    //--- 5274 = no data available in non-blocking polling; not fatal.
    if (err == 5274) {
      bytes_read = 0;
    } else if (err == 5273) {
      MQTT_LOG_ERROR("Socket I/O error 5273 (connection broken)");
      Disconnect();
      return TRANSPORT_ERROR_RECV;
    } else {
      MQTT_LOG_ERROR("Recv error " + (string)err);
      Disconnect();  // Close socket cleanly on hard recv error; prevents handle leak
      return TRANSPORT_ERROR_RECV;
    }
  }

  if (bytes_read > 0) {
    m_framer.Feed(m_recv_buf, bytes_read);
  }

  //--- Extract every complete MQTT packet that the framer has assembled
  uint                 pkt_capacity = 16;
  ENUM_TRANSPORT_ERROR framer_err   = TRANSPORT_OK;
  ArrayResize(out_packets, pkt_capacity);

  uchar pkt[];
  while (m_framer.NextPacket(pkt, framer_err)) {
    //--- Detect PINGRESP so we can cancel the keep-alive deadline timer
    if (CPingresp::IsPingresp(pkt)) {
      m_keepalive.OnPingRespReceived();
    }

    //--- Grow output array on demand
    if (out_count >= pkt_capacity) {
      pkt_capacity *= 2;
      ArrayResize(out_packets, pkt_capacity);
    }

    int pkt_size = ArraySize(pkt);
    ArrayResize(out_packets[out_count].data, pkt_size);
    ArrayCopy(out_packets[out_count].data, pkt, 0, 0, pkt_size);
    out_count++;
    ArrayFree(pkt);
  }

  //--- Shrink to actual count
  ArrayResize(out_packets, out_count);

  if (framer_err == TRANSPORT_ERROR_BAD_FRAME) {
    //--- STAB-001: Skip the corrupt byte, then scan forward to the next byte
    //--- whose upper nibble looks like a valid MQTT packet type (1-15).
    //--- This limits the blast radius of corruption to a few bytes instead of
    //--- causing a cascade of BAD_FRAME errors from misinterpreting data as headers.
    m_framer.SkipOneByte();
    uint skipped   = m_framer.SkipToNextValidHeader();
    uint remaining = m_framer.Available();
    MQTT_LOG_WARN("Malformed MQTT packet — skipped " + (string)(skipped + 1) + " byte(s) for resync ("
                  + (string)remaining + " bytes remain in framer).");
    return TRANSPORT_ERROR_BAD_FRAME;
  }

  return TRANSPORT_OK;
}

#endif  // MQTT_TRANSPORT_MQH
