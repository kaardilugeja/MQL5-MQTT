//+------------------------------------------------------------------+
//|                                           WebSocketTransport.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| WebSocket Transport for MQTT 5.0 per RFC 6455 and MQTT 5.0 §3.1. |
//|                                                                  |
//| Implements WebSocket framing as a transport layer for MQTT,      |
//| allows MQTT packets to be sent over ws:// or wss:// connections. |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_TRANSPORT_WEBSOCKETTRANSPORT_MQH
#define MQTT_INTERNAL_TRANSPORT_WEBSOCKETTRANSPORT_MQH

#include "Transport.mqh"

#ifdef MQTT_UNIT_TESTS
enum ENUM_WS_TEST_POLL_READ_MODE {
  WS_TEST_POLL_READ_NONE          = 0,
  WS_TEST_POLL_READ_PLAIN         = 1,
  WS_TEST_POLL_READ_TLS_AVAILABLE = 2
};
#endif

//+------------------------------------------------------------------+
//| Class CWebSocketTransport                                        |
//| Purpose: Unified send/receive over WebSocket (ws:// or wss://)   |
//|          Implements WebSocket framing per RFC 6455               |
//| Usage:   Use ConnectWS() for plaintext or ConnectWSS() for TLS   |
//|          Then use Send()/Poll() for MQTT packet I/O              |
//| Note:    WebSocket frames are automatically masked per RFC 6455. |
//|          Masking is protocol compliance, not confidentiality.    |
//|          Prefer WSS for all production deployments.              |
//+------------------------------------------------------------------+
class CWebSocketTransport : public IMqttTransport {
 private:
  //--- Socket handle and connection state
  int                          m_socket;         // MQL5 socket handle (INVALID_HANDLE if disconnected)
  bool                         m_tls;            // true = TLS socket (wss://); false = plaintext (ws://)
  ENUM_TRANSPORT_CONNECT_PHASE m_connect_phase;  // Fine-grained async connect phase

  //--- MQTT framing and keep-alive
  CPacketFramer                m_framer;         // MQTT packet framer (reassembles MQTT packets from WS payloads)
  CKeepAlive                   m_keepalive;      // Keep-alive timer manager (PINGREQ/PINGRESP)
  CAsyncConnector              m_async_connect;  // Non-blocking TCP connect state machine (STAB-002)
  bool                         m_connected;      // Connection state flag
  uint                         m_read_timeout;   // SocketRead timeout in ms (default 50 ms)
  uint   m_blocking_warn_threshold_ms;           // Warn when a blocking transport phase exceeds this duration
  ulong  m_last_blocking_operation_us;           // Duration of the last blocking handshake phase
  uint   m_handshake_timeout_ms;                 // HTTP upgrade handshake timeout
  uchar  m_pingreq_buf[];                        // Pre-built PINGREQ packet bytes
  string m_pending_path;                         // WS path remembered for async handshake phase
  uchar  m_ws_handshake_req[];                   // HTTP upgrade request bytes for async handshake phase
  uint   m_ws_handshake_sent;                    // Bytes of the upgrade request already sent
  uint   m_ws_handshake_send_failures;           // Consecutive async upgrade-write stalls/failures
  uint   m_ws_handshake_socket_restarts;         // Socket-level retries after zero-progress upgrade stalls
  ulong  m_ws_last_send_attempt_us;              // Last async upgrade write attempt timestamp
  string m_ws_expected_accept;                   // Expected Sec-WebSocket-Accept header value
  string m_ws_handshake_resp;                    // Incremental HTTP response header buffer
  ulong  m_ws_handshake_deadline_us;             // Absolute deadline for async HTTP upgrade
  uint   m_max_ws_header_size;                   // Hard cap for HTTP upgrade response headers

  //--- WebSocket framing read state
  uchar  m_ws_buf[];         // Internal buffer for incoming WebSocket frames
  uint   m_ws_head;          // Read cursor (start of unprocessed data)
  uint   m_ws_tail;          // Write cursor (end of accumulated data)
  uint   m_max_ws_buf_size;  // Max WebSocket buffer size in bytes (0 = unlimited)
  bool   m_allow_masked_server_frames;  // Compatibility escape hatch for non-compliant server frames

  //--- Pre-allocated send frame buffer (7.2.3: avoids per-send allocation)
  uchar  m_send_frame[];  // Reusable buffer for outgoing WebSocket frames
  uchar  m_recv_buf[];    // Reusable socket receive buffer (avoids per-Poll allocation)

#ifdef MQTT_UNIT_TESTS
  int   m_test_last_poll_read_mode;
  bool  m_test_poll_read_stub_enabled;
  uchar m_test_poll_read_stub[];
  bool  m_test_write_ready_stub_enabled;
  bool  m_test_write_ready;
  bool  m_test_handshake_send_stub_enabled;
  int   m_test_handshake_send_result;
  int   m_test_handshake_send_error;
  uint  m_test_handshake_send_call_count;
#endif

  //--- Private helper methods
  ENUM_TRANSPORT_ERROR _Connect(const string host, uint port, const string path, bool use_tls, uint timeout_ms);
  ENUM_TRANSPORT_ERROR _ConnectAsync(const string host, uint port, const string path, bool use_tls,
                                     uint attempt_timeout_ms, uint overall_timeout_ms);
  void                 _BuildPingreq();
  int                  _ReadSocketChunkForPoll(uchar &buffer[], uint max_len);
  bool                 _RunTlsHandshake(const string host, uint port);
  void                 _LogBlockingOperation(const string operation, const string target, ulong elapsed_us) const;
  bool                 _DoHandshake(const string host, uint port, const string path, bool use_tls, uint timeout_ms);
  bool                 _PrepareHandshakeRequest(const string host, uint port, const string path, bool use_tls);
  void                 _ResetHandshakeState();
  bool                 _IsSocketWriteReady() const;
  bool                 _RestartAsyncHandshakeSocket(uint remaining_ms);
  void                 _SendHandshakeRequestChunk(const uchar &buffer[], int len, int &chunk, int &send_err);
  bool                 _AppendHandshakeResponseChunk(const string chunk, bool &headers_complete);
  bool                 _ValidateHandshakeResponse(const string response, const string expected_accept) const;
  static string        _TrimAscii(const string value);
  static bool _HeaderHasToken(const string response_lower, const string header_name, const string expected_token);
  void        _FillRandomBytes(uchar &dest[], uint count);
  void        _FillMaskKey(uchar &dest[]);
  bool        _NextWsFramePayload(uchar &out_payload[]);
  void        _SendWsPong(const uchar &ping_payload[], uint payload_len);  // Reply to WS Ping per RFC 6455 §5.5.3
  string      _GenerateWsKey();  // Generate random base64 nonce per RFC 6455 §4.1

 public:
  //--- Constructor and Destructor
  CWebSocketTransport();
  ~CWebSocketTransport();

  //--- Connection methods
  ENUM_TRANSPORT_ERROR ConnectWS(const string host, uint port, const string path = "/mqtt", uint timeout_ms = 5000);
  ENUM_TRANSPORT_ERROR ConnectWSS(const string host, uint port, const string path = "/mqtt", uint timeout_ms = 5000);
  //--- Non-blocking async connect variants.
  //--- Returns TRANSPORT_CONNECTING immediately; connection completes via Poll().
  ENUM_TRANSPORT_ERROR ConnectWSAsync(const string host, uint port, const string path = "/mqtt",
                                      uint attempt_timeout_ms = 500, uint overall_timeout_ms = 30000);
  ENUM_TRANSPORT_ERROR ConnectWSSAsync(const string host, uint port, const string path = "/mqtt",
                                       uint attempt_timeout_ms = 500, uint overall_timeout_ms = 30000);
  virtual void         Disconnect() override;
  virtual bool         IsConnected() const override { return m_connected && m_socket != INVALID_HANDLE; }
  virtual bool         IsConnecting() const override {
    return m_async_connect.IsActive()
        || (m_connect_phase != TRANSPORT_PHASE_IDLE && m_connect_phase != TRANSPORT_PHASE_CONNECTED);
  }  // Async WS in progress
  virtual ENUM_TRANSPORT_CONNECT_PHASE GetConnectPhase() const override { return m_connect_phase; }

  //--- I/O methods
  virtual ENUM_TRANSPORT_ERROR         Send(const uchar &pkt[], int len = -1) override;
  virtual ENUM_TRANSPORT_ERROR         Poll(PacketBuffer &out_packets[], uint &out_count) override;

  //--- Configuration methods
  virtual void                         SetMaxPacketSize(uint max_size) override { m_framer.SetMaxPacketSize(max_size); }
  //--- Set max buffer sizes: ws_max limits the raw WebSocket frame buffer;
  //--- the same value is forwarded to the MQTT framer backing buffer.
  virtual void                         SetMaxBufferSize(uint max_size) override {
    m_max_ws_buf_size = max_size;
    m_framer.SetMaxBufferSize(max_size);
  }
  void          SetAllowMaskedServerFrames(bool allow = true) { m_allow_masked_server_frames = allow; }
  virtual void  SetKeepAlive(uint seconds) override { m_keepalive.SetKeepAlive(seconds); }
  virtual void  SetPingRespTimeout(uint sec) override { m_keepalive.SetPingRespTimeout(sec); }
  virtual void  SetReadTimeout(uint ms) override { m_read_timeout = ms; }
  virtual void  SetBlockingOperationWarnThreshold(uint ms) override { m_blocking_warn_threshold_ms = ms; }

  //--- Socket handle access (for TOFU certificate inspection)
  virtual int   GetSocket() const override { return m_socket; }

  //--- Last PINGREQ→PINGRESP round-trip in microseconds
  virtual ulong GetLastPingRTT_us() const override { return m_keepalive.GetLastPingRTT_us(); }
  virtual ulong GetLastBlockingOperationDuration_us() const override { return m_last_blocking_operation_us; }

  //--- Diagnostic methods
  bool          NeedsPing() const { return m_keepalive.NeedsPing(); }
  bool          IsPingTimedOut() const { return m_keepalive.IsPingTimedOut(); }

#ifdef MQTT_UNIT_TESTS
  //--- Test hooks: inject raw bytes into the WS frame buffer and call _NextWsFramePayload()
  void TestInjectWsBytes(const uchar &bytes[], uint len) {
    if (m_ws_tail + len > (uint)ArraySize(m_ws_buf)) {
      ArrayResize(m_ws_buf, m_ws_tail + len + 256);
    }
    ArrayCopy(m_ws_buf, bytes, m_ws_tail, 0, len);
    m_ws_tail += len;
  }
  bool TestNextWsFramePayload(uchar &out_payload[]) { return _NextWsFramePayload(out_payload); }
  bool TestValidateHandshakeResponse(const string response, const string expected_accept) const {
    return _ValidateHandshakeResponse(response, expected_accept);
  }
  bool TestPrepareHandshakeRequest(const string host, uint port, const string path, bool use_tls) {
    return _PrepareHandshakeRequest(host, port, path, use_tls);
  }
  string TestGetExpectedAccept() const { return m_ws_expected_accept; }
  string TestGetHandshakeRequestString() const {
    if (ArraySize(m_ws_handshake_req) == 0) {
      return "";
    }
    return CharArrayToString(m_ws_handshake_req, 0, ArraySize(m_ws_handshake_req));
  }
  bool TestAppendHandshakeResponseChunk(const string chunk, bool &headers_complete) {
    return _AppendHandshakeResponseChunk(chunk, headers_complete);
  }
  void TestQueuePollReadString(const string chunk) {
    ArrayResize(m_test_poll_read_stub, 0);
    if (StringLen(chunk) > 0) {
      StringToCharArray(chunk, m_test_poll_read_stub, 0, StringLen(chunk));
      ArrayResize(m_test_poll_read_stub, StringLen(chunk));
    }
    m_test_poll_read_stub_enabled = true;
    m_test_last_poll_read_mode    = WS_TEST_POLL_READ_NONE;
  }
  void TestSetWriteReady(bool ready) {
    m_test_write_ready_stub_enabled = true;
    m_test_write_ready              = ready;
  }
  void                         TestClearWriteReadyStub() { m_test_write_ready_stub_enabled = false; }
  void                         TestResetHandshakeState() { _ResetHandshakeState(); }
  ENUM_TRANSPORT_CONNECT_PHASE TestGetConnectPhase() const { return m_connect_phase; }
  void                         TestSetConnectPhase(ENUM_TRANSPORT_CONNECT_PHASE phase) { m_connect_phase = phase; }
  bool                         TestIsConnected() const { return m_connected; }
  bool TestRunTlsHandshake(const string host, uint port) { return _RunTlsHandshake(host, port); }
  void TestSetTls(bool use_tls) { m_tls = use_tls; }
  void TestSetSocket(int socket) { m_socket = socket; }
  int  TestGetLastPollReadMode() const { return m_test_last_poll_read_mode; }
  uint TestGetHandshakeSent() const { return m_ws_handshake_sent; }
  uint TestGetHandshakeSendFailures() const { return m_ws_handshake_send_failures; }
  uint TestGetHandshakeSocketRestarts() const { return m_ws_handshake_socket_restarts; }
  void TestSetHandshakeSocketRestarts(uint value) { m_ws_handshake_socket_restarts = value; }
  void TestSetLastHandshakeSendAttemptUs(ulong value) { m_ws_last_send_attempt_us = value; }
  void TestSetHandshakeSendStub(int result, int err) {
    m_test_handshake_send_stub_enabled = true;
    m_test_handshake_send_result       = result;
    m_test_handshake_send_error        = err;
    m_test_handshake_send_call_count   = 0;
  }
  void TestClearHandshakeSendStub() {
    m_test_handshake_send_stub_enabled = false;
    m_test_handshake_send_result       = 0;
    m_test_handshake_send_error        = 0;
    m_test_handshake_send_call_count   = 0;
  }
  uint TestGetHandshakeSendCallCount() const { return m_test_handshake_send_call_count; }
  void TestSeedAsyncEndpoint(const string host, uint port, bool tls, uint overall_timeout_ms = 5000) {
    m_async_connect.Begin(host, port, tls, 50, overall_timeout_ms);
    m_async_connect.Cancel();
  }
  void TestSetConnected(bool connected) {
    m_connected = connected;
    m_socket    = INVALID_HANDLE;
  }
#endif
};

//+------------------------------------------------------------------+
//| Constructor - Initialize all member variables                    |
//+------------------------------------------------------------------+
CWebSocketTransport::CWebSocketTransport() {
  m_socket                       = INVALID_HANDLE;
  m_tls                          = false;
  m_connect_phase                = TRANSPORT_PHASE_IDLE;
  m_connected                    = false;
  m_read_timeout                 = 50;  // 50ms default for non-blocking polling behavior
  m_blocking_warn_threshold_ms   = 250;
  m_last_blocking_operation_us   = 0;
  m_handshake_timeout_ms         = 5000;
  m_ws_handshake_sent            = 0;
  m_ws_handshake_send_failures   = 0;
  m_ws_handshake_socket_restarts = 0;
  m_ws_last_send_attempt_us      = 0;
  m_ws_handshake_deadline_us     = 0;
  m_max_ws_header_size           = 8192;
  m_ws_head                      = 0;
  m_ws_tail                      = 0;
  m_max_ws_buf_size              = 0;   // Unlimited by default
  m_allow_masked_server_frames   = false;
  m_pending_path                 = "/mqtt";
  ArrayResize(m_ws_buf, 4096, 4096);
  ArrayResize(m_recv_buf, 4096, 4096);  // Pre-allocate to avoid per-Poll heap allocation
  _BuildPingreq();

#ifdef MQTT_UNIT_TESTS
  m_test_last_poll_read_mode         = WS_TEST_POLL_READ_NONE;
  m_test_poll_read_stub_enabled      = false;
  m_test_write_ready_stub_enabled    = false;
  m_test_write_ready                 = true;
  m_test_handshake_send_stub_enabled = false;
  m_test_handshake_send_result       = 0;
  m_test_handshake_send_error        = 0;
  m_test_handshake_send_call_count   = 0;
#endif
}

//+------------------------------------------------------------------+
//| _LogBlockingOperation                                            |
//| Purpose: Warn when a blocking handshake exceeds the threshold    |
//+------------------------------------------------------------------+
void CWebSocketTransport::_LogBlockingOperation(const string operation, const string target, ulong elapsed_us) const {
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
//| Purpose: Treat a not-yet-writable socket as in-progress instead  |
//|          of as a failed WebSocket upgrade write.                 |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_IsSocketWriteReady() const {
#ifdef MQTT_UNIT_TESTS
  if (m_test_write_ready_stub_enabled) {
    return m_test_write_ready;
  }
#endif
  return m_socket != INVALID_HANDLE && SocketIsWritable(m_socket) > 0;
}

//+------------------------------------------------------------------+
//| _SendHandshakeRequestChunk                                       |
//| Purpose: Centralize async WS upgrade writes for live code and    |
//|          deterministic unit-test stubbing.                       |
//+------------------------------------------------------------------+
void CWebSocketTransport::_SendHandshakeRequestChunk(const uchar &buffer[], int len, int &chunk, int &send_err) {
#ifdef MQTT_UNIT_TESTS
  if (m_test_handshake_send_stub_enabled) {
    chunk    = m_test_handshake_send_result;
    send_err = (chunk <= 0) ? m_test_handshake_send_error : 0;
    m_test_handshake_send_call_count++;
    return;
  }
#endif

  chunk    = m_tls ? SocketTlsSend(m_socket, buffer, len) : SocketSend(m_socket, buffer, len);
  send_err = (chunk <= 0) ? GetLastError() : 0;
}

//+------------------------------------------------------------------+
//| _RestartAsyncHandshakeSocket                                     |
//| Purpose: Replace a wedged WS/WSS socket after repeated           |
//|          zero-progress upgrade-write stalls without extending    |
//|          the caller's overall connect budget.                    |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_RestartAsyncHandshakeSocket(uint remaining_ms) {
  const uint max_socket_restarts = m_tls ? 6 : 3;
  string     host                = m_async_connect.GetHost();
  uint       port                = m_async_connect.GetPort();

  if (host == "" || port == 0 || remaining_ms == 0 || m_ws_handshake_socket_restarts >= max_socket_restarts) {
    return false;
  }

  if (m_socket != INVALID_HANDLE) {
    SocketClose(m_socket);
    m_socket = INVALID_HANDLE;
  }

  m_connected     = false;
  m_connect_phase = TRANSPORT_PHASE_IDLE;
  m_ws_handshake_socket_restarts++;
  _ResetHandshakeState();
  m_handshake_timeout_ms = remaining_ms;
  m_async_connect.Begin(host, port, m_tls, 50, remaining_ms);
  if (!m_async_connect.IsActive()) {
    MQTT_LOG_ERROR("Failed to restart async " + (m_tls ? "WSS" : "WS") + " connect after upgrade-write stalls.");
    return false;
  }

  m_connect_phase = TRANSPORT_PHASE_TCP_CONNECTING;
  MQTT_LOG_WARN("Restarting async " + (m_tls ? "WSS" : "WS") + " connect after repeated zero-progress upgrade stalls"
                + " (restart #" + (string)m_ws_handshake_socket_restarts + ", remaining budget=" + (string)remaining_ms
                + "ms).");
  return true;
}

//+------------------------------------------------------------------+
//| _RunTlsHandshake                                                 |
//| Purpose: Measure and warn on the blocking TLS handshake phase    |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_RunTlsHandshake(const string host, uint port) {
  if (port == 443) {
    //--- The raw 443 MT5 probe showed that explicit SocketTlsHandshake()
    //--- on implicit HTTPS/WSS port 443 never completes on MT5 build 5698 and
    //--- leaves the socket unusable for subsequent SocketTlsSend(). On this
    //--- port we must let SocketTlsSend()/SocketTlsReadAvailable() drive the
    //--- platform's implicit TLS negotiation instead of calling the explicit
    //--- handshake API first.
    m_last_blocking_operation_us = 0;
    MQTT_LOG_DEBUG("Skipping explicit WSS TLS handshake on implicit port 443 for " + host);
    return true;
  }

  ulong started_us             = GetMicrosecondCount();
  bool  ok                     = SocketTlsHandshake(m_socket, host);
  m_last_blocking_operation_us = GetMicrosecondCount() - started_us;
  _LogBlockingOperation("WSS TLS handshake", host, m_last_blocking_operation_us);
  return ok;
}

//+------------------------------------------------------------------+
//| _ReadSocketChunkForPoll                                          |
//| Purpose: Read only data that is immediately available so Poll()  |
//|          stays non-blocking for both WS and WSS transports.      |
//+------------------------------------------------------------------+
int CWebSocketTransport::_ReadSocketChunkForPoll(uchar &buffer[], uint max_len) {
#ifdef MQTT_UNIT_TESTS
  m_test_last_poll_read_mode = m_tls ? WS_TEST_POLL_READ_TLS_AVAILABLE : WS_TEST_POLL_READ_PLAIN;
  if (m_test_poll_read_stub_enabled) {
    int available = ArraySize(m_test_poll_read_stub);
    if (available <= 0) {
      return 0;
    }

    int read_len = (available < (int)max_len) ? available : (int)max_len;
    ArrayCopy(buffer, m_test_poll_read_stub, 0, 0, read_len);

    if (read_len < available) {
      ArrayCopy(m_test_poll_read_stub, m_test_poll_read_stub, 0, read_len, available - read_len);
      ArrayResize(m_test_poll_read_stub, available - read_len);
    } else {
      ArrayResize(m_test_poll_read_stub, 0);
    }

    return read_len;
  }
#endif

  if (m_tls) {
    return SocketTlsReadAvailable(m_socket, buffer, max_len);
  }
  uint readable = (uint)SocketIsReadable(m_socket);
  if (readable > 0) {
    int read_len = (int)((readable < max_len) ? readable : max_len);
    return SocketRead(m_socket, buffer, (uint)read_len, 0);
  }
  return 0;
}

//+------------------------------------------------------------------+
//| Destructor - Clean up resources                                  |
//+------------------------------------------------------------------+
CWebSocketTransport::~CWebSocketTransport() {
  Disconnect();
  ArrayFree(m_ws_buf);
  ArrayFree(m_pingreq_buf);
}

//+------------------------------------------------------------------+
//| _BuildPingreq - Pre-build the 2-byte PINGREQ packet             |
//| Purpose: Cache the PINGREQ packet to avoid rebuilding each time  |
//+------------------------------------------------------------------+
void CWebSocketTransport::_BuildPingreq() {
  CPingreq pq;
  pq.Build(m_pingreq_buf);
}

//+------------------------------------------------------------------+
//| _ResetHandshakeState - Clear async HTTP upgrade state            |
//+------------------------------------------------------------------+
void CWebSocketTransport::_ResetHandshakeState() {
  ArrayResize(m_ws_handshake_req, 0);
  m_ws_handshake_sent          = 0;
  m_ws_handshake_send_failures = 0;
  m_ws_last_send_attempt_us    = 0;
  m_ws_expected_accept         = "";
  m_ws_handshake_resp          = "";
  m_ws_handshake_deadline_us   = 0;
}

//+------------------------------------------------------------------+
//| _AppendHandshakeResponseChunk - Accumulate bounded HTTP headers  |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_AppendHandshakeResponseChunk(const string chunk, bool &headers_complete) {
  headers_complete = false;
  if (chunk == "") {
    return true;
  }

  if ((uint)(StringLen(m_ws_handshake_resp) + StringLen(chunk)) > m_max_ws_header_size) {
    MQTT_LOG_ERROR("WebSocket handshake headers exceeded " + (string)m_max_ws_header_size + " bytes");
    return false;
  }

  m_ws_handshake_resp += chunk;
  headers_complete     = (StringFind(m_ws_handshake_resp, "\r\n\r\n") >= 0);
  return true;
}

//+------------------------------------------------------------------+
//| _PrepareHandshakeRequest - Build async HTTP upgrade request      |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_PrepareHandshakeRequest(const string host, uint port, const string path, bool use_tls) {
  _ResetHandshakeState();

  string ws_key       = _GenerateWsKey();
  bool   default_port = (!use_tls && port == 80) || (use_tls && port == 443);
  string host_header  = default_port ? host : host + ":" + (string)port;
  string req = "GET " + path + " HTTP/1.1\r\n" + "Host: " + host_header + "\r\n" + "Upgrade: websocket\r\n"
             + "Connection: Upgrade\r\n" + "Sec-WebSocket-Key: " + ws_key + "\r\n" + "Sec-WebSocket-Version: 13\r\n"
             + "Sec-WebSocket-Protocol: mqtt\r\n\r\n";

  string accept_magic = ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  uchar  sha_in[], sha_hash[], empty_key[], b64_buf[];
  StringToCharArray(accept_magic, sha_in, 0, StringLen(accept_magic));
  if (CryptEncode(CRYPT_HASH_SHA1, sha_in, empty_key, sha_hash) <= 0
      || CryptEncode(CRYPT_BASE64, sha_hash, empty_key, b64_buf) <= 0) {
    MQTT_LOG_ERROR("Failed to compute Sec-WebSocket-Accept value");
    return false;
  }

  int b64_len = ArraySize(b64_buf);
  if (b64_len > 0 && b64_buf[b64_len - 1] == 0) {
    b64_len--;
  }
  m_ws_expected_accept = CharArrayToString(b64_buf, 0, b64_len);
  if (m_ws_expected_accept == "") {
    MQTT_LOG_ERROR("Failed to compute Sec-WebSocket-Accept value");
    return false;
  }

  StringToCharArray(req, m_ws_handshake_req, 0, StringLen(req));
  m_ws_last_send_attempt_us  = GetMicrosecondCount();
  m_ws_handshake_deadline_us = GetMicrosecondCount() + ((ulong)m_handshake_timeout_ms * 1000UL);
  return true;
}

//+------------------------------------------------------------------+
//| _GenerateWsKey - Generate random base64 WebSocket handshake key  |
//| Purpose: Produces a fresh 16-byte random nonce, base64-encoded,  |
//|          per RFC 6455 §4.1 requirement for unique per-connection |
//|          Sec-WebSocket-Key header values.                        |
//| Return: Base64-encoded 24-character string                       |
//| MathRand() is a non-cryptographic LCG PRNG. RFC 6455 recommends  |
//| strong entropy for the client-generated nonce, but MQL5 does not |
//| expose CryptGenRandom() or equivalent. The implementation mixes  |
//| MathRand() with high-resolution timers, chart identity, account  |
//| identity, and a rolling salt to reduce predictability as much as |
//| the platform allows.                                             |
//+------------------------------------------------------------------+
string CWebSocketTransport::_GenerateWsKey() {
  uchar bytes[16];
  _FillRandomBytes(bytes, 16);

  //--- Base64 encode per RFC 4648: convert every 3 bytes to 4 characters
  const string b64    = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  string       result = "";

  for (int i = 0; i < 16; i += 3) {
    uint b0  = bytes[i];
    uint b1  = (i + 1 < 16) ? bytes[i + 1] : 0;
    uint b2  = (i + 2 < 16) ? bytes[i + 2] : 0;

    result  += StringSubstr(b64, (b0 >> 2) & 0x3F, 1);
    result  += StringSubstr(b64, ((b0 << 4) | (b1 >> 4)) & 0x3F, 1);
    result  += (i + 1 < 16) ? StringSubstr(b64, ((b1 << 2) | (b2 >> 6)) & 0x3F, 1) : "=";
    result  += (i + 2 < 16) ? StringSubstr(b64, b2 & 0x3F, 1) : "=";
  }

  return result;
}

//+------------------------------------------------------------------+
//| _FillRandomBytes                                                 |
//+------------------------------------------------------------------+
void CWebSocketTransport::_FillRandomBytes(uchar &dest[], uint count) {
  ArrayResize(dest, count);
  if (count == 0) {
    return;
  }

  static uint rolling_counter = 0;
  uchar       empty_key[];
  uint        written = 0;

  while (written < count) {
    string material =
      StringFormat("%I64u|%u|%I64d|%I64d|%I64d|%u|%u|%u", GetMicrosecondCount(), GetTickCount(), ChartID(),
                   AccountInfoInteger(ACCOUNT_LOGIN), TerminalInfoInteger(TERMINAL_MEMORY_PHYSICAL), (uint)m_socket,
                   rolling_counter, written);
    uchar seed[];
    uchar hash[];
    StringToCharArray(material, seed, 0, StringLen(material));

    if (CryptEncode(CRYPT_HASH_SHA256, seed, empty_key, hash) <= 0 || ArraySize(hash) == 0) {
      if (written == 0) {
        MathSrand((uint)(GetMicrosecondCount() ^ GetTickCount() ^ rolling_counter));
      }
      dest[written++] = (uchar)((MathRand() ^ (GetMicrosecondCount() & 0xFF) ^ (rolling_counter & 0xFF)) & 0xFF);
      rolling_counter++;
      continue;
    }

    int hash_len = ArraySize(hash);
    //--- SHA256 output is 32 bytes of raw binary data. The last byte is 0x00 with
    //--- probability 1/256; treating it as a C-string null terminator discards a
    //--- valid entropy byte. Iterate over all bytes unconditionally.
    for (int i = 0; i < hash_len && written < count; i++) {
      dest[written++] = hash[i];
    }
    rolling_counter++;
  }
}

//+------------------------------------------------------------------+
//| _FillMaskKey                                                     |
//+------------------------------------------------------------------+
void                 CWebSocketTransport::_FillMaskKey(uchar &dest[]) { _FillRandomBytes(dest, 4); }

//+------------------------------------------------------------------+
//| ConnectWS - Establish a plaintext WebSocket connection           |
//| Parameters: host      - MQTT broker hostname or IP address       |
//|             port      - WebSocket port (typically 80 or 8083)    |
//|             path      - WebSocket path (default "/mqtt")         |
//|             timeout_ms - Connection timeout in milliseconds      |
//| Return: TRANSPORT_OK on success, or appropriate error code       |
//| Note: Plain ws:// is intended for development or private network |
//|       use. Prefer WSS for all production deployments.            |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::ConnectWS(const string host, uint port, const string path, uint timeout_ms) {
  return _Connect(host, port, path, false, timeout_ms);
}

//+------------------------------------------------------------------+
//| ConnectWSS - Establish a TLS WebSocket connection                |
//| Parameters: host      - MQTT broker hostname or IP address       |
//|             port      - WebSocket port (typically 443 or 8084)   |
//|             path      - WebSocket path (default "/mqtt")         |
//|             timeout_ms - Connection timeout in milliseconds      |
//| Return: TRANSPORT_OK on success, or appropriate error code       |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::ConnectWSS(const string host, uint port, const string path, uint timeout_ms) {
  return _Connect(host, port, path, true, timeout_ms);
}

//+------------------------------------------------------------------+
//| ConnectWSAsync - Begin non-blocking plaintext WS connection      |
//| Purpose: Start async TCP connect; TLS and the HTTP upgrade       |
//|          advance across explicit Poll() phases after TCP is up.  |
//|          Prevents WebSocket handshake loops from blocking Poll() |
//| Returns: TRANSPORT_CONNECTING immediately                        |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::ConnectWSAsync(const string host, uint port, const string path,
                                                         uint attempt_timeout_ms, uint overall_timeout_ms) {
  return _ConnectAsync(host, port, path, false, attempt_timeout_ms, overall_timeout_ms);
}

//+------------------------------------------------------------------+
//| ConnectWSSAsync - Begin non-blocking TLS WS connection           |
//| Returns: TRANSPORT_CONNECTING immediately                        |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::ConnectWSSAsync(const string host, uint port, const string path,
                                                          uint attempt_timeout_ms, uint overall_timeout_ms) {
  return _ConnectAsync(host, port, path, true, attempt_timeout_ms, overall_timeout_ms);
}

//+------------------------------------------------------------------+
//| _ConnectAsync - Internal async connect launcher (STAB-002)       |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::_ConnectAsync(const string host, uint port, const string path, bool use_tls,
                                                        uint attempt_timeout_ms, uint overall_timeout_ms) {
  Disconnect();
  m_pending_path                 = path;
  m_tls                          = use_tls;
  m_handshake_timeout_ms         = (overall_timeout_ms >= 2000) ? overall_timeout_ms : 2000;
  m_ws_handshake_socket_restarts = 0;
  _ResetHandshakeState();
  m_async_connect.Begin(host, port, use_tls, attempt_timeout_ms, overall_timeout_ms);
  if (!m_async_connect.IsActive()) {
    m_connect_phase = TRANSPORT_PHASE_IDLE;
    return TRANSPORT_ERROR_SOCKET;
  }
  m_connect_phase = TRANSPORT_PHASE_TCP_CONNECTING;
  MQTT_LOG_DEBUG("Async connect started to " + host + ":" + (string)port + path + " (" + (use_tls ? "WSS" : "WS")
                 + ")");
  return TRANSPORT_CONNECTING;
}

//+------------------------------------------------------------------+
//| _Connect - Internal unified connection handler                   |
//| Purpose: Establish TCP/TLS connection and perform WebSocket      |
//|          upgrade handshake per RFC 6455                          |
//| Parameters: host      - MQTT broker hostname or IP address       |
//|             port      - WebSocket port                           |
//|             path      - WebSocket path                           |
//|             use_tls   - true for wss://, false for ws://         |
//|             timeout_ms - Connection timeout in milliseconds      |
//| Return: TRANSPORT_OK on success, or appropriate error code       |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::_Connect(const string host, uint port, const string path, bool use_tls,
                                                   uint timeout_ms) {
  Disconnect();
  m_connect_phase                = TRANSPORT_PHASE_TCP_CONNECTING;
  m_ws_handshake_socket_restarts = 0;

  //--- Create socket
  m_socket                       = SocketCreate();
  if (m_socket == INVALID_HANDLE) {
    MQTT_LOG_ERROR("SocketCreate() failed — error " + (string)GetLastError());
    m_connect_phase = TRANSPORT_PHASE_IDLE;
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- Connect to host
  if (!SocketConnect(m_socket, host, port, timeout_ms)) {
    MQTT_LOG_ERROR("SocketConnect(" + host + ":" + (string)port + ") failed — error " + (string)GetLastError());
    SocketClose(m_socket);
    m_socket        = INVALID_HANDLE;
    m_connect_phase = TRANSPORT_PHASE_IDLE;
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- Perform TLS handshake if required
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

  m_tls                  = use_tls;

  //--- Perform WebSocket upgrade handshake per RFC 6455
  m_handshake_timeout_ms = (timeout_ms >= 2000) ? timeout_ms : 2000;
  m_connect_phase        = TRANSPORT_PHASE_WS_SENDING_REQUEST;
  //--- Reset the WS frame buffer before the handshake so that any WebSocket
  //--- frame bytes arriving in the same TCP segment as the 101 response are
  //--- preserved in the buffer and not discarded after _DoHandshake returns.
  m_ws_head              = 0;
  m_ws_tail              = 0;
  if (!_DoHandshake(host, port, path, use_tls, m_handshake_timeout_ms)) {
    SocketClose(m_socket);
    m_socket = INVALID_HANDLE;
    MQTT_LOG_ERROR("WebSocket handshake failed");
    m_connect_phase = TRANSPORT_PHASE_IDLE;
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- Connection established successfully
  m_connected     = true;
  m_connect_phase = TRANSPORT_PHASE_CONNECTED;
  m_framer.Reset();
  m_keepalive.Reset();
  //--- m_ws_head / m_ws_tail were reset before _DoHandshake; do NOT reset here
  //--- to preserve any WebSocket frame bytes already captured by _DoHandshake.
  _ResetHandshakeState();
  MQTT_LOG_INFO("Connected (" + (use_tls ? "WSS" : "WS") + ") to " + host + ":" + (string)port + path);
  return TRANSPORT_OK;
}

//+------------------------------------------------------------------+
//| _DoHandshake - Perform WebSocket upgrade handshake               |
//| Purpose: Send HTTP upgrade request and validate server response  |
//|          per RFC 6455 Section 4                                  |
//| Details: Client sends an HTTP GET with 'Upgrade: websocket' and  |
//|          'Connection: Upgrade' headers. Server must reply with   |
//|          '101 Switching Protocols' to confirm the upgrade.       |
//| Parameters: host - Host header value (required by HTTP/1.1)      |
//|             path - WebSocket endpoint path (e.g., /mqtt)         |
//| Return: true if handshake successful, false otherwise            |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_DoHandshake(const string host, uint port, const string path, bool use_tls, uint timeout_ms) {
  ulong started_us       = GetMicrosecondCount();
  m_handshake_timeout_ms = timeout_ms;
  if (!_PrepareHandshakeRequest(host, port, path, use_tls)) {
    return false;
  }

  //--- Send the upgrade request via retry loop (handles TLS/plain + TCP partial writes)
  //--- Consistent with CWebSocketTransport::Send() and CMqttTransport::Send() patterns.
  int   req_size   = ArraySize(m_ws_handshake_req);
  uint  total_sent = 0;
  uchar send_buf[];
  while (total_sent < (uint)req_size) {
    int remaining = req_size - (int)total_sent;
    int chunk;
    if (total_sent == 0) {
      chunk = m_tls ? SocketTlsSend(m_socket, m_ws_handshake_req, remaining) :
                      SocketSend(m_socket, m_ws_handshake_req, remaining);
    } else {
      if (ArraySize(send_buf) == 0) {
        ArrayResize(send_buf, remaining);
        ArrayCopy(send_buf, m_ws_handshake_req, 0, total_sent, remaining);
      }
      chunk = m_tls ? SocketTlsSend(m_socket, send_buf, remaining) : SocketSend(m_socket, send_buf, remaining);
      if (chunk > 0 && chunk < remaining) {
        ArrayCopy(send_buf, send_buf, 0, chunk, remaining - chunk);
      }
    }
    if (chunk <= 0) {
      MQTT_LOG_ERROR("Failed to send upgrade request after " + (string)total_sent + "/" + (string)req_size + " bytes");
      return false;
    }
    total_sent += (uint)chunk;
  }

  //--- Read and validate server response using a deadline-based loop.
  //--- Use SocketTlsReadAvailable() for TLS sockets so the polling loop never
  //--- blocks waiting for decrypted application data.
  uchar buf[1024];
  ulong deadline_us = GetMicrosecondCount() + ((ulong)timeout_ms * 1000UL);

  while (GetMicrosecondCount() < deadline_us) {
    int r = _ReadSocketChunkForPoll(buf, 1024);
    if (r > 0) {
      bool headers_complete = false;
      if (!_AppendHandshakeResponseChunk(CharArrayToString(buf, 0, r), headers_complete)) {
        return false;
      }
      if (headers_complete) {
        bool ok                      = _ValidateHandshakeResponse(m_ws_handshake_resp, m_ws_expected_accept);
        m_last_blocking_operation_us = GetMicrosecondCount() - started_us;
        _LogBlockingOperation(use_tls ? "WSS HTTP upgrade" : "WS HTTP upgrade", host + ":" + (string)port + path,
                              m_last_blocking_operation_us);
        if (ok) {
          //--- Any bytes in buf[] beyond the HTTP header boundary are WebSocket frame
          //--- data that arrived in the same TCP segment as the 101 response.
          //--- Copy them into the frame buffer so they are not silently discarded.
          //--- Scan raw buf[] directly for \r\n\r\n to find the exact byte
          //--- boundary. String conversion is lossy for WebSocket data bytes
          //--- that may contain 0x00 which terminates MQL5 strings early.
          int hdr_boundary_in_buf = -1;
          for (int _bi = 0; _bi <= r - 4; _bi++) {
            if (buf[_bi] == '\r' && buf[_bi + 1] == '\n' && buf[_bi + 2] == '\r' && buf[_bi + 3] == '\n') {
              hdr_boundary_in_buf = _bi;
              break;  // Stop at the first occurrence — it marks the true HTTP/WS boundary
            }
          }
          if (hdr_boundary_in_buf >= 0) {
            int ws_start    = hdr_boundary_in_buf + 4;
            int ws_data_len = r - ws_start;
            if (ws_data_len > 0) {
              if (m_ws_tail + (uint)ws_data_len > (uint)ArraySize(m_ws_buf)) {
                ArrayResize(m_ws_buf, m_ws_tail + (uint)ws_data_len + 256);
              }
              ArrayCopy(m_ws_buf, buf, m_ws_tail, ws_start, ws_data_len);
              m_ws_tail += (uint)ws_data_len;
            }
          }
        }
        return ok;
      }
    } else if (r < 0) {
      int err = GetLastError();
      //--- 5274 = no data available yet; continue polling.
      if (err != 5274) {
        if (err == 5273) {
          MQTT_LOG_ERROR("Socket I/O error 5273 (connection broken)");
          return false;
        }
        MQTT_LOG_ERROR("Read error " + (string)err);
        return false;
      }
    }
  }

  m_last_blocking_operation_us = GetMicrosecondCount() - started_us;
  _LogBlockingOperation(use_tls ? "WSS HTTP upgrade" : "WS HTTP upgrade", host + ":" + (string)port + path,
                        m_last_blocking_operation_us);
  MQTT_LOG_ERROR("Timeout waiting for server response");
  return false;
}

//+------------------------------------------------------------------+
//| _ValidateHandshakeResponse                                       |
//| Purpose: Validate HTTP 101 response and MQTT WS headers          |
//| Parameters: response - raw HTTP response                         |
//|             expected_accept - computed accept header             |
//| Return: true if the response is valid                            |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_ValidateHandshakeResponse(const string response, const string expected_accept) const {
  if (StringFind(response, "101 Switching Protocols") < 0) {
    MQTT_LOG_ERROR("Server rejected upgrade — " + response);
    return false;
  }

  string response_lower = response;
  StringToLower(response_lower);
  if (StringFind(response_lower, "upgrade: websocket") < 0) {
    MQTT_LOG_ERROR("Missing Upgrade: websocket header in handshake response");
    return false;
  }
  if (!_HeaderHasToken(response_lower, "connection", "upgrade")) {
    MQTT_LOG_ERROR("Missing Connection: Upgrade header in handshake response");
    return false;
  }
  if (StringFind(response_lower, "sec-websocket-protocol: mqtt") < 0) {
    MQTT_LOG_ERROR("Missing Sec-WebSocket-Protocol: mqtt header in handshake response");
    return false;
  }

  string accept_hdr_lower = "sec-websocket-accept: ";
  int    hdr_pos          = StringFind(response_lower, accept_hdr_lower);
  if (hdr_pos < 0) {
    MQTT_LOG_ERROR("Missing Sec-WebSocket-Accept header");
    return false;
  }

  int    val_start     = hdr_pos + StringLen(accept_hdr_lower);
  int    crlf_pos      = StringFind(response, "\r\n", val_start);
  string server_accept = (crlf_pos > val_start) ? StringSubstr(response, val_start, crlf_pos - val_start) :
                                                  StringSubstr(response, val_start);
  if (server_accept != expected_accept) {
    MQTT_LOG_ERROR("Sec-WebSocket-Accept mismatch. Expected: " + expected_accept + "  Got: " + server_accept);
    return false;
  }

  return true;
}

//+------------------------------------------------------------------+
//| _TrimAscii - Trim ASCII spaces and tabs from both ends           |
//+------------------------------------------------------------------+
string CWebSocketTransport::_TrimAscii(const string value) {
  int start = 0;
  int end   = StringLen(value) - 1;

  while (start <= end) {
    ushort ch = StringGetCharacter(value, start);
    if (ch != ' ' && ch != '\t') {
      break;
    }
    start++;
  }

  while (end >= start) {
    ushort ch = StringGetCharacter(value, end);
    if (ch != ' ' && ch != '\t') {
      break;
    }
    end--;
  }

  return (end >= start) ? StringSubstr(value, start, end - start + 1) : "";
}

//+------------------------------------------------------------------+
//| _HeaderHasToken - Validate a comma-separated HTTP header token   |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_HeaderHasToken(const string response_lower, const string header_name,
                                          const string expected_token) {
  string needle      = header_name + ":";
  int    search_from = 0;

  while (true) {
    int header_pos = StringFind(response_lower, needle, search_from);
    if (header_pos < 0) {
      return false;
    }

    if (header_pos > 0) {
      ushort prev = StringGetCharacter(response_lower, header_pos - 1);
      if (prev != '\n') {
        search_from = header_pos + StringLen(needle);
        continue;
      }
    }

    int    value_start = header_pos + StringLen(needle);
    int    line_end    = StringFind(response_lower, "\r\n", value_start);
    string line        = (line_end >= value_start) ? StringSubstr(response_lower, value_start, line_end - value_start) :
                                                     StringSubstr(response_lower, value_start);

    string tokens[];
    int    token_count = StringSplit(line, ',', tokens);
    if (token_count <= 0) {
      return _TrimAscii(line) == expected_token;
    }

    for (int i = 0; i < token_count; i++) {
      if (_TrimAscii(tokens[i]) == expected_token) {
        return true;
      }
    }

    search_from = value_start;
  }

  return false;
}

//+------------------------------------------------------------------+
//| Disconnect - Close WebSocket connection gracefully               |
//| Purpose: Send WebSocket close frame (opcode 0x8) and close socket|
//|          per RFC 6455 Section 5.5.1                              |
//| Details: A Close frame consists of Opcode 0x8. Client frames     |
//|          must be masked. Status 1000 = Normal Closure.           |
//+------------------------------------------------------------------+
void CWebSocketTransport::Disconnect() {
  //--- Abort any in-progress async TCP connect attempt
  m_async_connect.Cancel();
  m_connect_phase                = TRANSPORT_PHASE_IDLE;
  m_ws_handshake_socket_restarts = 0;
  _ResetHandshakeState();

  if (m_socket != INVALID_HANDLE) {
    //--- Send WebSocket close frame if connection was established
    //--- Frame layout:
    //--- Byte 0: 0x88 (FIN=1, Opcode=8 Close)
    //--- Byte 1: 0x80 (Mask=1, Payload=0)
    //--- Byte 2-5: Random masking key (payload is empty so XOR doesn't affect data)
    //--- Build Close frame with status code 1000 (Normal Closure) per RFC 6455 §5.5.1
    //--- Frame layout: [opcode][len+mask_bit][mask_key(4 bytes)][masked_payload(2 bytes)]
    //---   Byte 0: 0x88 = FIN=1, Opcode=8 (Close)
    //---   Byte 1: 0x82 = Mask=1, Payload=2 (2-byte status code)
    //---   Bytes 2-5: 4-byte masking key (random)
    //---   Bytes 6-7: Status 1000 (0x03E8) XOR'd with mask_key[0..1]
    uchar mask_key[];
    _FillMaskKey(mask_key);

    uchar close_frame[8];
    close_frame[0] = 0x88;                         // FIN + Close opcode
    close_frame[1] = 0x82;                         // Mask=1, Payload length=2
    close_frame[2] = mask_key[0];                  // Mask key byte 0
    close_frame[3] = mask_key[1];                  // Mask key byte 1
    close_frame[4] = mask_key[2];                  // Mask key byte 2
    close_frame[5] = mask_key[3];                  // Mask key byte 3
    close_frame[6] = 0x03 ^ mask_key[0];           // Status hi-byte (0x03) masked
    close_frame[7] = (uchar)(0xE8 ^ mask_key[1]);  // Status lo-byte (0xE8) masked

    if (m_connected) {
      int close_sent;
      if (m_tls) {
        close_sent = SocketTlsSend(m_socket, close_frame, sizeof(close_frame));
      } else {
        close_sent = SocketSend(m_socket, close_frame, sizeof(close_frame));
      }
      if (close_sent <= 0) {
        MQTT_LOG_DEBUG("WebSocket close frame send failed — connection may already be broken");
      }
    }

    //--- Physically close the OS socket
    SocketClose(m_socket);
    m_socket = INVALID_HANDLE;
  }

  //--- Clear internal states
  m_connected = false;
  m_framer.Reset();
  m_ws_head = 0;
  m_ws_tail = 0;
}

//+------------------------------------------------------------------+
//| Send - Transmit MQTT packet wrapped in WebSocket frame           |
//| Purpose: Wrap MQTT packet in binary WebSocket frame and transmit |
//| Parameters: pkt - MQTT packet bytes to send                      |
//|             len - packet length (default: ArraySize(pkt))        |
//| Return: TRANSPORT_OK on success, or appropriate error code       |
//| Note: Per RFC 6455 Section 5.3, client frames MUST be masked     |
//|       via _FillMaskKey(). Masking is not a substitute for TLS.   |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::Send(const uchar &pkt[], int len) {
  //--- Ensure connection is active
  if (!IsConnected()) {
    MQTT_LOG_WARN("Not connected.");
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- Validate payload length
  int payload_len = (len < 0) ? ArraySize(pkt) : len;
  if (payload_len <= 0) {
    return TRANSPORT_OK;
  }

  //--- Build WebSocket frame per RFC 6455 Section 5.2
  //---   Byte 0: 0x82 (FIN=1 + Opcode=2 Binary)
  //---   Byte 1: Mask bit set (0x80) + Payload Length indicator (7-bit)
  //---   Byte 2+: Extended length (if indicator is 126 or 127)
  //---   Next 4 bytes: Masking Key (Required for client-to-server)
  //---   Next bytes: Masked Payload

  //--- 7.2.3: Reuse pre-allocated m_send_frame buffer; only resize when needed
  int frame_len = 0;

  if (payload_len < 126) {
    //--- Case 1: Short payload (length fits in 7 bits)
    uint needed = (uint)(6 + payload_len);
    if ((uint)ArraySize(m_send_frame) < needed) {
      ArrayResize(m_send_frame, needed, needed / 2);
    }
    m_send_frame[0] = 0x82;                         // FIN=1, Opcode=2 (binary)
    m_send_frame[1] = (uchar)(0x80 | payload_len);  // MASK bit set
    frame_len       = 2;
  } else if (payload_len <= 65535) {
    //--- Case 2: Medium payload (16-bit extended length)
    uint needed = (uint)(8 + payload_len);
    if ((uint)ArraySize(m_send_frame) < needed) {
      ArrayResize(m_send_frame, needed, needed / 2);
    }
    m_send_frame[0] = 0x82;                         // FIN=1, Opcode=2 (binary)
    m_send_frame[1] = 0x80 | 126;                   // MASK bit set, length indicator = 126
    m_send_frame[2] = (uchar)(payload_len >> 8);    // Extended length MSB
    m_send_frame[3] = (uchar)(payload_len & 0xFF);  // Extended length LSB
    frame_len       = 4;
  } else {
    //--- Case 3: Large payload (64-bit extended length)
    uint needed = (uint)(14 + payload_len);
    if ((uint)ArraySize(m_send_frame) < needed) {
      ArrayResize(m_send_frame, needed, needed / 2);
    }
    m_send_frame[0] = 0x82;        // FIN=1, Opcode=2 (binary)
    m_send_frame[1] = 0x80 | 127;  // MASK bit set, length indicator = 127
    //--- Reset 64-bit length (only using lower 32 bits for now)
    for (int i = 0; i < 8; i++) {
      m_send_frame[2 + i] = 0;
    }
    m_send_frame[6] = (uchar)((payload_len >> 24) & 0xFF);
    m_send_frame[7] = (uchar)((payload_len >> 16) & 0xFF);
    m_send_frame[8] = (uchar)((payload_len >> 8) & 0xFF);
    m_send_frame[9] = (uchar)(payload_len & 0xFF);
    frame_len       = 10;
  }

  uchar mask_key[];
  _FillMaskKey(mask_key);
  m_send_frame[frame_len]     = mask_key[0];  // Masking key byte 0
  m_send_frame[frame_len + 1] = mask_key[1];  // Masking key byte 1
  m_send_frame[frame_len + 2] = mask_key[2];  // Masking key byte 2
  m_send_frame[frame_len + 3] = mask_key[3];  // Masking key byte 3

  //--- XOR-mask each MQTT payload byte into the WS frame after the masking key
  for (int i = 0; i < payload_len; i++) {
    m_send_frame[frame_len + 4 + i] = pkt[i] ^ mask_key[i % 4];
  }

  //--- Transmit the complete WebSocket frame with retry loop (handles TCP partial writes)
  int   frame_size = frame_len + 4 + payload_len;
  uint  total_sent = 0;
  uchar send_buf[];
  while (total_sent < (uint)frame_size) {
    int remaining = frame_size - (int)total_sent;
    int chunk;
    if (total_sent == 0) {
      chunk = m_tls ? SocketTlsSend(m_socket, m_send_frame, remaining) : SocketSend(m_socket, m_send_frame, remaining);
    } else {
      if (ArraySize(send_buf) == 0) {
        ArrayResize(send_buf, remaining);
        ArrayCopy(send_buf, m_send_frame, 0, total_sent, remaining);
      }
      chunk = m_tls ? SocketTlsSend(m_socket, send_buf, remaining) : SocketSend(m_socket, send_buf, remaining);
      if (chunk > 0 && chunk < remaining) {
        ArrayCopy(send_buf, send_buf, 0, chunk, remaining - chunk);
      }
    }
    if (chunk <= 0) {
      MQTT_LOG_ERROR("Failed after " + (string)total_sent + "/" + (string)frame_size + " bytes, error "
                     + (string)GetLastError());
      Disconnect();  // Close socket to prevent OS handle leak
      return TRANSPORT_ERROR_SEND;
    }
    total_sent += (uint)chunk;
  }

  //--- Update keep-alive timestamp
  m_keepalive.OnPacketSent();
  return TRANSPORT_OK;
}

//+------------------------------------------------------------------+
//| _NextWsFramePayload - Extract next WebSocket frame payload       |
//| Purpose: Parse and extract payload from next complete WS frame   |
//| Parameters: out_payload - output buffer for extracted payload    |
//| Return: true if payload extracted, false if need more data       |
//| Note: Strict mode rejects masked server frames; compatibility    |
//|       mode can still unmask them for non-compliant peers.        |
//+------------------------------------------------------------------+
bool CWebSocketTransport::_NextWsFramePayload(uchar &out_payload[]) {
  //--- Calculate bytes available in the internal buffer
  uint avail = (m_ws_tail >= m_ws_head) ? (m_ws_tail - m_ws_head) : 0;
  if (avail < 2) {
    return false;  // Need at least 2 bytes for the base frame header
  }

  //--- Parse frame header per RFC 6455 Section 5.2
  //--- Byte 0: [FIN(1), RSV1(1), RSV2(1), RSV3(1), Opcode(4)]
  //--- Byte 1: [MASK(1), Payload length (7)]
  uchar b0     = m_ws_buf[m_ws_head];
  uchar b1     = m_ws_buf[m_ws_head + 1];
  uchar opcode = b0 & 0x0F;  // Extract opcode per RFC 6455 §5.2
  //--- Per RFC 6455 §5.2, RSV bits MUST be 0 unless an extension was negotiated.
  //--- This implementation never negotiates extensions (no Sec-WebSocket-Extensions header
  //--- is sent), so any non-zero RSV value indicates a broken or non-compliant peer.
  if ((b0 & 0x70) != 0) {
    MQTT_LOG_ERROR("WS frame has non-zero RSV bits (0x" + StringFormat("%02X", b0 & 0x70)
                   + ") — no extensions negotiated; closing connection per RFC 6455 §5.2.");
    Disconnect();
    return false;
  }
  bool masked      = (b1 & 0x80) != 0;  // Server-to-client frames should NOT be masked per spec
  if (masked && !m_allow_masked_server_frames) {
    MQTT_LOG_ERROR("WS server frame used the MASK bit — protocol violation per RFC 6455 §5.1. "
                   "Disconnecting. Use SetAllowMaskedServerFrames(true) only for non-compliant compatibility.");
    Disconnect();
    return false;
  }
  uint payload_len = b1 & 0x7F;
  uint header_len  = 2;

  //--- Handle extended payload length (126 = 16-bit, 127 = 64-bit)
  if (payload_len == 126) {
    //--- 16-bit payload length (Bytes 2-3)
    if (avail < 4) {
      return false;
    }
    payload_len = (m_ws_buf[m_ws_head + 2] << 8) | m_ws_buf[m_ws_head + 3];
    header_len  = 4;
  } else if (payload_len == 127) {
    //--- 64-bit payload length (Bytes 2-9, big-endian)
    //--- MQTT packets are capped at ~256 MB so only the lower 32 bits are used.
    //--- Upper 32 bits (bytes 2-5) MUST be zero; a non-zero value indicates a
    //--- frame far too large for MQTT or a maliciously crafted/corrupt frame.
    if (avail < 10) {
      return false;
    }
    //--- Reject frames whose upper 32 bits are non-zero
    if (m_ws_buf[m_ws_head + 2] != 0 || m_ws_buf[m_ws_head + 3] != 0 || m_ws_buf[m_ws_head + 4] != 0
        || m_ws_buf[m_ws_head + 5] != 0) {
      MQTT_LOG_ERROR("64-bit frame length exceeds 32-bit range — disconnecting (potential MITM or corrupt stream).");
      //--- Remaining payload bytes are unknown in size; stream cannot be recovered. Force disconnect.
      Disconnect();
      return false;
    }
    payload_len = ((uint)m_ws_buf[m_ws_head + 6] << 24) | ((uint)m_ws_buf[m_ws_head + 7] << 16)
                | ((uint)m_ws_buf[m_ws_head + 8] << 8) | (uint)m_ws_buf[m_ws_head + 9];
    header_len  = 10;
  }

  //--- Reject payloads that exceed the MQTT protocol maximum (268,435,455 bytes).
  //--- Without this check a near-UINT_MAX payload_len causes total_len to wrap,
  //--- the avail<total_len guard passes, and ArrayResize allocates ~4 GB => OOM crash.
  if (payload_len > 268435455) {
    MQTT_LOG_ERROR("WS frame payload exceeds MQTT maximum — disconnecting (potential MITM or corrupt stream).");
    //--- Payload bytes unknown/untrustworthy; stream cannot be recovered. Force disconnect.
    Disconnect();
    return false;
  }

  //--- Calculate total bytes required for a complete frame
  uint mask_len  = masked ? 4 : 0;
  uint total_len = header_len + mask_len + payload_len;

  if (avail < total_len) {
    return false;  // Incomplete frame; wait for more data from socket
  }

  //--- Logic to extract and unmask payload
  if (payload_len > 0) {
    ArrayResize(out_payload, payload_len);
    ArrayCopy(out_payload, m_ws_buf, 0, m_ws_head + header_len + mask_len, payload_len);

    //--- Unmask payload only in explicit compatibility mode for non-compliant peers.
    if (masked) {
      uchar m0 = m_ws_buf[m_ws_head + header_len];      // Mask key byte 0
      uchar m1 = m_ws_buf[m_ws_head + header_len + 1];  // Mask key byte 1
      uchar m2 = m_ws_buf[m_ws_head + header_len + 2];  // Mask key byte 2
      uchar m3 = m_ws_buf[m_ws_head + header_len + 3];  // Mask key byte 3

      for (uint i = 0; i < payload_len; i++) {
        //--- Unmask octet: transformed_octet = original_octet XOR masking_key[i MOD 4]
        out_payload[i] ^= (i % 4 == 0) ? m0 : (i % 4 == 1) ? m1 : (i % 4 == 2) ? m2 : m3;
      }
    }
  } else {
    ArrayResize(out_payload, 0);
  }

  //--- Advance read cursor past the processed frame
  m_ws_head += total_len;

  //--- Dispatch based on WebSocket opcode per RFC 6455 §5.5
  //--- Control frames (Ping 0x9, Pong 0xA, Close 0x8) must NOT be fed into the MQTT framer.
  //--- Only binary data frames (0x2) and continuation frames (0x0) carry MQTT data.
  if (opcode == 0x9) {
    //--- Ping: respond with Pong carrying the same payload per RFC 6455 §5.5.3
    _SendWsPong(out_payload, payload_len);
    MQTT_LOG_DEBUG("WS Ping received — Pong sent (" + (string)payload_len + " bytes payload).");
    ArrayResize(out_payload, 0);
    return false;  // Re-enter loop — no MQTT data from this frame
  }
  if (opcode == 0x8) {
    //--- Close: log the status code, set disconnect state per RFC 6455 §5.5.1
    if (payload_len >= 2) {
      ushort close_code = (ushort)((out_payload[0] << 8) | out_payload[1]);
      MQTT_LOG_INFO("WS Close frame received — status " + (string)close_code);
    } else {
      MQTT_LOG_INFO("WS Close frame received — no status code");
    }
    //--- RFC 6455 §5.5.1: send a Close frame in response before closing
    uint  _cecho_len = (payload_len >= 2) ? 2 : 0;
    uchar _cecho_frame[];
    ArrayResize(_cecho_frame, (int)(6 + _cecho_len));
    _cecho_frame[0] = 0x88;                        // FIN=1, Opcode=0x8 (Close)
    _cecho_frame[1] = (uchar)(0x80 | _cecho_len);  // MASK=1
    uchar _cmask[];
    _FillMaskKey(_cmask);
    _cecho_frame[2] = _cmask[0];
    _cecho_frame[3] = _cmask[1];
    _cecho_frame[4] = _cmask[2];
    _cecho_frame[5] = _cmask[3];
    for (uint _ci = 0; _ci < _cecho_len; _ci++) {
      _cecho_frame[6 + _ci] = out_payload[_ci] ^ _cmask[_ci % 4];
    }
    int _cecho_size = (int)ArraySize(_cecho_frame);
    (m_tls) ? SocketTlsSend(m_socket, _cecho_frame, _cecho_size) : SocketSend(m_socket, _cecho_frame, _cecho_size);
    m_connected = false;
    ArrayResize(out_payload, 0);
    return false;
  }
  if (opcode == 0xA) {
    //--- Pong: silently ignore per RFC 6455 §5.5.3
    ArrayResize(out_payload, 0);
    return false;
  }
  if (opcode != 0x2 && opcode != 0x0) {
    //--- Reject non-binary frames per MQTT-over-WebSocket §6.1 (OASIS)
    MQTT_LOG_WARN("Unexpected WS opcode 0x" + StringFormat("%X", opcode)
                  + " — frame discarded (MQTT requires binary frames)");
    ArrayResize(out_payload, 0);
    return false;
  }

  return true;
}

//+------------------------------------------------------------------+
//| _SendWsPong - Send WebSocket Pong frame in response to Ping      |
//| Purpose: Sends a masked Pong (opcode 0xA) with the same payload  |
//|          as the received Ping per RFC 6455 §5.5.3                |
//| Parameters: ping_payload - payload bytes from the Ping frame     |
//|             payload_len  - number of payload bytes (max 125)     |
//+------------------------------------------------------------------+
void CWebSocketTransport::_SendWsPong(const uchar &ping_payload[], uint payload_len) {
  //--- Control frames cannot have payload > 125 bytes per RFC 6455 §5.5
  if (payload_len > 125) {
    payload_len = 125;
  }

  //--- Frame: [0x8A (FIN+Pong)][0x80|len][mask_key(4)][masked_payload]
  uint  frame_size = 2 + 4 + payload_len;  // header + mask key + payload
  uchar frame[];
  ArrayResize(frame, frame_size);
  frame[0] = 0x8A;                         // FIN=1, Opcode=0xA (Pong)
  frame[1] = (uchar)(0x80 | payload_len);  // MASK=1, payload length

  uchar mask_key[];
  _FillMaskKey(mask_key);
  frame[2] = mask_key[0];
  frame[3] = mask_key[1];
  frame[4] = mask_key[2];
  frame[5] = mask_key[3];

  //--- XOR-mask the payload
  for (uint i = 0; i < payload_len; i++) {
    frame[6 + i] = ping_payload[i] ^ mask_key[i % 4];
  }

  //--- Send Pong frame with retry loop and error checking.
  //--- The original code used a single bare SocketSend/SocketTlsSend with no
  //--- return value check. Per RFC 6455 §5.5.3, failing to respond to a Ping
  //--- may cause the server to close the connection. We now use the same
  //--- retry-until-all-sent pattern as Send() to handle partial writes.
  uint  total_sent  = 0;
  uint  remaining   = frame_size;
  uint  max_retries = 10;
  uint  retry_count = 0;
  uchar send_buf[];
  while (remaining > 0 && retry_count < max_retries) {
    int sent;
    if (total_sent == 0) {
      sent = m_tls ? SocketTlsSend(m_socket, frame, (int)remaining) : SocketSend(m_socket, frame, (int)remaining);
    } else {
      if (ArraySize(send_buf) == 0) {
        ArrayResize(send_buf, remaining);
        ArrayCopy(send_buf, frame, 0, total_sent, remaining);
      }
      sent = m_tls ? SocketTlsSend(m_socket, send_buf, (int)remaining) : SocketSend(m_socket, send_buf, (int)remaining);
      if (sent > 0 && (uint)sent < remaining) {
        ArrayCopy(send_buf, send_buf, 0, sent, (int)(remaining - (uint)sent));
        //--- Shrink the buffer to the valid remainder so the platform socket
        //--- implementation cannot read stale bytes beyond the active data.
        ArrayResize(send_buf, (int)(remaining - (uint)sent));
      }
    }
    if (sent > 0) {
      total_sent  += sent;
      remaining   -= sent;
      retry_count  = 0;  // Reset retry counter on progress
    } else {
      retry_count++;
    }
  }
  if (remaining > 0) {
    MQTT_LOG_WARN("WebSocket Pong send incomplete — " + (string)remaining + " bytes unsent. "
                  + "Server may close the connection per RFC 6455 §5.5.3.");
  }
}

//+------------------------------------------------------------------+
//| Poll - Read from socket, extract MQTT packets, manage keep-alive |
//| Purpose: Drain socket, advance bounded async connect phases,     |
//|          parse WebSocket frames, and service keep-alive timers   |
//| Parameters: out_packets - [OUT] array of complete MQTT packets   |
//|             out_count   - [OUT] number of packets in out_packets |
//| Return: TRANSPORT_OK or a fatal error code                       |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CWebSocketTransport::Poll(PacketBuffer &out_packets[], uint &out_count) {
  out_count = 0;
  ArrayResize(out_packets, 0);

  if (m_connect_phase == TRANSPORT_PHASE_TLS_HANDSHAKING) {
    if (!_RunTlsHandshake(m_async_connect.GetHost(), m_async_connect.GetPort())) {
      int tls_err = GetLastError();
      if (tls_err == 5274) {
        if (m_async_connect.GetPort() == 443) {
          MQTT_LOG_DEBUG("WSS TLS handshake still pending on implicit port 443 for " + m_async_connect.GetHost());
        } else {
          MQTT_LOG_DEBUG("WSS TLS handshake still pending for " + m_async_connect.GetHost() + ":"
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

    if (!_PrepareHandshakeRequest(m_async_connect.GetHost(), m_async_connect.GetPort(), m_pending_path, true)) {
      SocketClose(m_socket);
      m_socket        = INVALID_HANDLE;
      m_connect_phase = TRANSPORT_PHASE_IDLE;
      return TRANSPORT_ERROR_SOCKET;
    }

    m_connect_phase = TRANSPORT_PHASE_WS_SENDING_REQUEST;
    return TRANSPORT_CONNECTING;
  }

  if (m_connect_phase == TRANSPORT_PHASE_WS_SENDING_REQUEST) {
    ulong now_us = GetMicrosecondCount();
    if (now_us >= m_ws_handshake_deadline_us) {
      MQTT_LOG_ERROR("Timeout sending WebSocket upgrade request after " + (string)m_ws_handshake_sent + "/"
                     + (string)ArraySize(m_ws_handshake_req) + " bytes"
                     + " (stalls=" + (string)m_ws_handshake_send_failures + ")");
      SocketClose(m_socket);
      m_socket        = INVALID_HANDLE;
      m_connect_phase = TRANSPORT_PHASE_IDLE;
      return TRANSPORT_ERROR_TIMEOUT;
    }

    int remaining = ArraySize(m_ws_handshake_req) - (int)m_ws_handshake_sent;
    if (remaining <= 0) {
      m_connect_phase = TRANSPORT_PHASE_WS_WAITING_HEADERS;
      return TRANSPORT_CONNECTING;
    }

    if (!_IsSocketWriteReady()) {
      bool allow_probe_send =
        (m_ws_handshake_sent == 0 && m_ws_last_send_attempt_us > 0 && now_us > m_ws_last_send_attempt_us
         && (now_us - m_ws_last_send_attempt_us) >= 250000UL);
      if (!allow_probe_send) {
        return TRANSPORT_CONNECTING;
      }
    }

    uchar tail[];
    ArrayResize(tail, remaining);
    ArrayCopy(tail, m_ws_handshake_req, 0, m_ws_handshake_sent, remaining);
    m_ws_last_send_attempt_us = now_us;
    int chunk                 = 0;
    int send_err              = 0;
    _SendHandshakeRequestChunk(tail, remaining, chunk, send_err);
    if (chunk <= 0) {
      if (send_err != 0 && send_err != 5273 && send_err != 5274) {
        MQTT_LOG_ERROR("WebSocket upgrade request send failed after " + (string)m_ws_handshake_sent + "/"
                       + (string)ArraySize(m_ws_handshake_req) + " bytes, error " + (string)send_err);
        SocketClose(m_socket);
        m_socket        = INVALID_HANDLE;
        m_connect_phase = TRANSPORT_PHASE_IDLE;
        return TRANSPORT_ERROR_SEND;
      }

      m_ws_handshake_send_failures++;
      if (m_ws_handshake_sent == 0 && (send_err == 5273 || m_ws_handshake_send_failures >= 4)) {
        uint remaining_ms = 0;
        if (m_ws_handshake_deadline_us > now_us) {
          remaining_ms = (uint)((m_ws_handshake_deadline_us - now_us + 999ULL) / 1000ULL);
        }
        if (_RestartAsyncHandshakeSocket(remaining_ms)) {
          return TRANSPORT_CONNECTING;
        }
      }
      if (m_ws_handshake_send_failures <= 3 || (m_ws_handshake_send_failures % 10) == 0) {
        MQTT_LOG_WARN("WebSocket upgrade request write stalled after " + (string)m_ws_handshake_sent + "/"
                      + (string)ArraySize(m_ws_handshake_req) + " bytes; stall #" + (string)m_ws_handshake_send_failures
                      + (send_err != 0 ? ", last_error=" + (string)send_err : ""));
      }
      return TRANSPORT_CONNECTING;
    }

    m_ws_handshake_send_failures  = 0;
    m_ws_handshake_sent          += (uint)chunk;
    if (m_ws_handshake_sent < (uint)ArraySize(m_ws_handshake_req)) {
      return TRANSPORT_CONNECTING;
    }

    m_connect_phase = TRANSPORT_PHASE_WS_WAITING_HEADERS;
    return TRANSPORT_CONNECTING;
  }

  if (m_connect_phase == TRANSPORT_PHASE_WS_WAITING_HEADERS) {
    if (GetMicrosecondCount() >= m_ws_handshake_deadline_us) {
      MQTT_LOG_ERROR("Timeout waiting for WebSocket upgrade response");
      SocketClose(m_socket);
      m_socket        = INVALID_HANDLE;
      m_connect_phase = TRANSPORT_PHASE_IDLE;
      return TRANSPORT_ERROR_TIMEOUT;
    }

    uchar buf[1024];
    int   r = _ReadSocketChunkForPoll(buf, 1024);
    if (r > 0) {
      bool headers_complete = false;
      if (!_AppendHandshakeResponseChunk(CharArrayToString(buf, 0, r), headers_complete)) {
        SocketClose(m_socket);
        m_socket        = INVALID_HANDLE;
        m_connect_phase = TRANSPORT_PHASE_IDLE;
        return TRANSPORT_ERROR_PKT_TOO_BIG;
      }
      if (headers_complete) {
        if (!_ValidateHandshakeResponse(m_ws_handshake_resp, m_ws_expected_accept)) {
          MQTT_LOG_ERROR("WebSocket handshake failed");
          SocketClose(m_socket);
          m_socket        = INVALID_HANDLE;
          m_connect_phase = TRANSPORT_PHASE_IDLE;
          return TRANSPORT_ERROR_SOCKET;
        }

        m_connected     = true;
        m_connect_phase = TRANSPORT_PHASE_CONNECTED;
        m_framer.Reset();
        m_keepalive.Reset();
        m_ws_head               = 0;
        m_ws_tail               = 0;
        //--- Preserve any trailing WebSocket frame data that arrived in the
        //--- same TCP segment as the 101 response (e.g. CONNACK)
        //--- Scan raw buf[] directly for \r\n\r\n to find the exact byte
        //--- boundary. String conversion is lossy for WebSocket data bytes
        //--- that may contain 0x00 which terminates MQL5 strings early.
        int _async_hdr_boundary = -1;
        for (int _bi2 = 0; _bi2 <= r - 4; _bi2++) {
          if (buf[_bi2] == '\r' && buf[_bi2 + 1] == '\n' && buf[_bi2 + 2] == '\r' && buf[_bi2 + 3] == '\n') {
            _async_hdr_boundary = _bi2;
            break;  // Stop at the first occurrence — it marks the true HTTP/WS boundary
          }
        }
        if (_async_hdr_boundary >= 0) {
          int _async_ws_start    = _async_hdr_boundary + 4;
          int _async_ws_data_len = r - _async_ws_start;
          if (_async_ws_data_len > 0) {
            if (m_ws_tail + (uint)_async_ws_data_len > (uint)ArraySize(m_ws_buf)) {
              ArrayResize(m_ws_buf, m_ws_tail + (uint)_async_ws_data_len + 256);
            }
            ArrayCopy(m_ws_buf, buf, m_ws_tail, _async_ws_start, _async_ws_data_len);
            m_ws_tail += (uint)_async_ws_data_len;
          }
        }
        _ResetHandshakeState();
        MQTT_LOG_DEBUG("Async connect completed (" + (m_async_connect.IsTLS() ? "WSS" : "WS") + ") to "
                       + m_async_connect.GetHost() + ":" + (string)m_async_connect.GetPort() + m_pending_path);
      }
    } else if (r < 0) {
      int err = GetLastError();
      if (err != 5274) {
        MQTT_LOG_ERROR("Read error " + (string)err + " during WebSocket handshake");
        SocketClose(m_socket);
        m_socket        = INVALID_HANDLE;
        m_connect_phase = TRANSPORT_PHASE_IDLE;
        return TRANSPORT_ERROR_RECV;
      }
    }

    if (m_connect_phase == TRANSPORT_PHASE_WS_WAITING_HEADERS) {
      return TRANSPORT_CONNECTING;
    }
  }

  //--- Async connect phase
  //--- Advance the non-blocking TCP state machine; must come before the
  //--- IsConnected() guard so the first successful Poll() can complete
  //--- the TLS handshake and WS upgrade in the same call.
  if (m_async_connect.IsActive()) {
    int                  new_socket  = INVALID_HANDLE;
    ENUM_TRANSPORT_ERROR conn_result = m_async_connect.Poll(new_socket);

    if (conn_result == TRANSPORT_CONNECTING) {
      m_connect_phase = TRANSPORT_PHASE_TCP_CONNECTING;
      return TRANSPORT_CONNECTING;  // TCP still in progress
    }
    if (conn_result != TRANSPORT_OK) {
      MQTT_LOG_ERROR("Async TCP connect failed (" + (string)conn_result + ").");
      m_connect_phase = TRANSPORT_PHASE_IDLE;
      return conn_result;
    }

    //--- TCP is up. Transition into the next bounded phase and let the next Poll()
    //--- call advance TLS or HTTP upgrade work explicitly.
    m_socket = new_socket;
    if (m_async_connect.IsTLS()) {
      m_connect_phase = TRANSPORT_PHASE_TLS_HANDSHAKING;
      return TRANSPORT_CONNECTING;
    }

    m_tls = false;
    if (!_PrepareHandshakeRequest(m_async_connect.GetHost(), m_async_connect.GetPort(), m_pending_path, false)) {
      SocketClose(m_socket);
      m_socket        = INVALID_HANDLE;
      m_connect_phase = TRANSPORT_PHASE_IDLE;
      return TRANSPORT_ERROR_SOCKET;
    }
    m_connect_phase = TRANSPORT_PHASE_WS_SENDING_REQUEST;
    return TRANSPORT_CONNECTING;
  }

  //--- Must be connected to perform I/O
  if (!IsConnected()) {
    return TRANSPORT_ERROR_SOCKET;
  }

  //--- 1. Service keep-alive: Check if connection has been idle too long
  //--- Per MQTT 5.0 §3.1.2.10: Client must send PINGREQ if no other packets sent
  if (m_keepalive.NeedsPing()) {
    ENUM_TRANSPORT_ERROR send_err = Send(m_pingreq_buf);
    if (send_err != TRANSPORT_OK) {
      return send_err;
    }
    m_keepalive.OnPingreqSent();
    MQTT_LOG_DEBUG("PINGREQ sent.");
  }

  //--- 2. Detect PINGRESP timeout (connection unresponsive)
  //--- If server fails to reply within the timeout, we consider the connection dead.
  if (m_keepalive.IsPingTimedOut()) {
    MQTT_LOG_ERROR("PINGRESP timeout — connection dead.");
    Disconnect();  // Close socket to prevent OS handle leak
    return TRANSPORT_ERROR_RECV;
  }

  //--- 3. Read available bytes from the socket (non-blocking)
  int bytes_read = 0;
  bytes_read     = _ReadSocketChunkForPoll(m_recv_buf, 4096);

  if (bytes_read < 0) {
    int err = GetLastError();
    //--- 5274 = no data available yet; NOT an error in polling
    if (err == 5274) {
      bytes_read = 0;
    } else if (err == 5273) {
      MQTT_LOG_ERROR("Socket I/O error 5273 (connection broken)");
      Disconnect();  // Close socket to prevent OS handle leak
      return TRANSPORT_ERROR_RECV;
    } else {
      MQTT_LOG_ERROR("Recv error " + (string)err);
      Disconnect();  // Close socket to prevent OS handle leak
      return TRANSPORT_ERROR_RECV;
    }
  }

  //--- 4. Accumulate WebSocket frame data into internal buffer
  if (bytes_read > 0) {
    //--- Compact buffer: if head has advanced past a fixed threshold, move data to start.
    //--- Use fixed threshold (2048 bytes) instead of proportional heuristic
    //--- to avoid frequent compactions at high throughput with many small messages.
    //--- MQL5's ArrayCopy supports overlapping regions when the
    //--- destination offset is lower than the source, so this is safe and avoids
    //--- a heap allocation on every compaction cycle.
    if (m_ws_head >= 2048) {
      uint avail = m_ws_tail - m_ws_head;
      if (avail > 0) {
        ArrayCopy(m_ws_buf, m_ws_buf, 0, m_ws_head, avail);
      }
      m_ws_tail = avail;
      m_ws_head = 0;
    }

    //--- Grow buffer if incoming data exceeds current capacity
    if (m_ws_tail + bytes_read > (uint)ArraySize(m_ws_buf)) {
      //--- Guard against unbounded growth
      if (m_max_ws_buf_size > 0 && m_ws_tail + bytes_read > m_max_ws_buf_size) {
        MQTT_LOG_ERROR("WebSocket buffer limit (" + (string)m_max_ws_buf_size
                       + " bytes) exceeded — data dropped, disconnecting.");
        m_connected = false;
        return TRANSPORT_ERROR_PKT_TOO_BIG;
      }
      ArrayResize(m_ws_buf, m_ws_tail + bytes_read + 4096);
    }

    //--- Copy new bytes into position after existing untailed data
    ArrayCopy(m_ws_buf, m_recv_buf, m_ws_tail, 0, bytes_read);
    m_ws_tail += bytes_read;
  }

  //--- 5. Parse complete WebSocket frames and feed payloads to MQTT framer
  uchar ws_payload[];
  while (_NextWsFramePayload(ws_payload)) {
    if (ArraySize(ws_payload) > 0) {
      //--- Feed raw MQTT bytes to the framer for reassembly
      m_framer.Feed(ws_payload, ArraySize(ws_payload));
    }
    ArrayFree(ws_payload);
  }

  //--- 6. Extract complete MQTT packets from the reassembly framer
  uint                 pkt_capacity = 16;
  ENUM_TRANSPORT_ERROR framer_err   = TRANSPORT_OK;
  ArrayResize(out_packets, pkt_capacity);

  uchar pkt[];
  while (m_framer.NextPacket(pkt, framer_err)) {
    //--- Detect PINGRESP to reset the keep-alive watchdog
    if (CPingresp::IsPingresp(pkt)) {
      m_keepalive.OnPingRespReceived();
    }

    //--- Dynamically grow output array if needed
    if (out_count >= pkt_capacity) {
      pkt_capacity *= 2;
      ArrayResize(out_packets, pkt_capacity);
    }

    //--- Copy complete packet data to output array
    int pkt_size = ArraySize(pkt);
    ArrayResize(out_packets[out_count].data, pkt_size);
    ArrayCopy(out_packets[out_count].data, pkt, 0, 0, pkt_size);
    out_count++;
    ArrayFree(pkt);
  }

  //--- Shrink output array to reflect actual number of packets found
  ArrayResize(out_packets, out_count);

  //--- Handle logic errors in MQTT framing (e.g. invalid length)
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

#endif  // MQTT_WEBSOCKETTRANSPORT_MQH
