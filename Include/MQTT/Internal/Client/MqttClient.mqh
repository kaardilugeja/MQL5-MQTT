//+------------------------------------------------------------------+
//|                                                   MqttClient.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Internal CMqttClient implementation header.                      |
//| Public consumers should include MQTT.mqh only.                   |
//|                                                                  |
//| Orchestrates the full connection lifecycle so EA developers do   |
//| not have to manually wire together CConnect, CConnack,           |
//| CMqttTransport, CWebSocketTransport, CAutoReconnect,             |
//| CFlowControl, and CRetransmissionManager.                        |
//|                                                                  |
//| Features                                                         |
//|   - Transport polymorphism (TCP/TLS and WebSocket/WSS)           |
//|   - Non-blocking async TCP / TLS connect                         |
//|   - Automatic CONNACK processing (server keep-alive, max QoS…)   |
//|   - Function-pointer callback event system                       |
//|   - Last Will & Testament (LWT) configuration                    |
//|   - Graceful disconnect with configurable reason code            |
//|   - Subscription replay on every reconnection                    |
//|   - QoS 1 / 2 retransmission of unacknowledged messages          |
//|   - Jittered exponential back-off reconnection via               |
//|     CAutoReconnect                                               |
//|   - Publish queue with backpressure                              |
//|   - CONNACK timeout                                              |
//|   - Configurable default QoS                                     |
//|   - Connection health metrics                                    |
//|                                                                  |
//| Typical EA usage                                                 |
//|   CMqttClient mqtt;                                              |
//|   mqtt.SetHost("broker.example.com", 8883);                      |
//|   mqtt.SetTLS(true);                                             |
//|   mqtt.SetRequireTLS(true);                                      |
//|   mqtt.SetTofuPinning(true);                                     |
//|   mqtt.SetTofuThumbprint("0011223344...CDDEEFF00112233");        |
//|   mqtt.AddRedirectAllowHost("broker.example.com");               |
//|   mqtt.SetCredentials("user", "pass");                           |
//|   mqtt.SetWill("status/EURUSD", "offline", QoS_1, true);         |
//|   mqtt.SetOnMessage(OnMqttMessage);                              |
//|   mqtt.SetOnConnect(OnMqttConnect);                              |
//|   mqtt.Subscribe("signals/#", QoS_1);                            |
//|   mqtt.Connect();                                                |
//|   // In OnTimer():                                               |
//|   mqtt.Poll();                                                   |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_CLIENT_MQH
#define MQTT_INTERNAL_CLIENT_MQH

#include "..\Util\PropertyReader.mqh"
#include "..\Util\TopicMatcher.mqh"
#include "..\Transport\Transport.mqh"
#include "..\Transport\WebSocketTransport.mqh"

//--- Override MQTT_LOG_* macros so every log call inside CMqttClient routes through
//--- m_context.logger directly rather than through a shared global slot.
//--- Logger.mqh resolves MQTT_LOG_* through a chart-scoped registry so that
//--- transport and packet-layer logs follow the active client on that chart.
#undef MQTT_LOG_ERROR
#undef MQTT_LOG_WARN
#undef MQTT_LOG_INFO
#undef MQTT_LOG_DEBUG

#define MQTT_LOG_ERROR(msg)                                                                                     \
  do {                                                                                                          \
    if (MQTT_LEVEL_ERROR <= m_context.logger.m_log_level)                                                       \
      _MqttLogWithLogger(m_context.logger, MQTT_LEVEL_ERROR, "ERROR", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)
#define MQTT_LOG_WARN(msg)                                                                                    \
  do {                                                                                                        \
    if (MQTT_LEVEL_WARN <= m_context.logger.m_log_level)                                                      \
      _MqttLogWithLogger(m_context.logger, MQTT_LEVEL_WARN, "WARN", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)
#define MQTT_LOG_INFO(msg)                                                                                    \
  do {                                                                                                        \
    if (MQTT_LEVEL_INFO <= m_context.logger.m_log_level)                                                      \
      _MqttLogWithLogger(m_context.logger, MQTT_LEVEL_INFO, "INFO", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)
#define MQTT_LOG_DEBUG(msg)                                                                                     \
  do {                                                                                                          \
    if (MQTT_LEVEL_DEBUG <= m_context.logger.m_log_level)                                                       \
      _MqttLogWithLogger(m_context.logger, MQTT_LEVEL_DEBUG, "DEBUG", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)

//+------------------------------------------------------------------+
//| MqttConnectionInfo                                               |
//| Snapshot of client health metrics returned by GetConnectionInfo  |
//+------------------------------------------------------------------+
struct MqttConnectionInfo {
  ulong connection_duration_ms;  // ms since last successful CONNACK (0 if not connected)
  ulong messages_sent;           // Total PUBLISH packets sent (lifetime, not reset on reconnect)
  ulong messages_received;       // Total PUBLISH packets received (lifetime, not reset on reconnect)
  uint  in_flight_count;         // Currently unacknowledged QoS 1/2 outgoing messages
  uint  reconnect_count;         // Total reconnect attempts across all sessions
};

//+------------------------------------------------------------------+
//| ENUM_MQTT_PUBLISH_ERROR                                          |
//| Return codes specific to Publish operations                      |
//+------------------------------------------------------------------+
enum ENUM_MQTT_PUBLISH_ERROR {
  MQTT_PUB_OK                = 0,    // Success: publish accepted (sent or queued as appropriate)
  MQTT_PUB_NOT_CONNECTED     = -1,   // Client is not connected; cannot perform publish
  MQTT_PUB_NO_PACKET_ID      = -2,   // No available packet identifier for QoS>0 publish
  MQTT_PUB_QUEUE_FULL        = -3,   // Publish queue is full; message rejected
  MQTT_PUB_SEND_FAILED       = -4,   // Transport send failed (socket/TLS or I/O error)
  MQTT_PUB_PACKET_TOO_BIG    = -5,   // Payload/packet exceeds allowed maximum size
  MQTT_PUB_INVALID_TOPIC     = -6,   // Topic is invalid (format/wildcard rules violated)
  MQTT_PUB_FLOW_CONTROL_FULL = -7,   // Flow-control window full; no resources consumed
  MQTT_PUB_RECONNECTING      = -8,   // Publish attempted during reconnect/drain; retry later
  MQTT_PUB_QUEUED            = -9,   // Accepted for deferred delivery while disconnected
  MQTT_PUB_EXPIRY_REQUIRED   = -10,  // QoS1 publish rejected: message needs expiry set
};

//+------------------------------------------------------------------+
//| ENUM_MQTT_TRUST_MODE                                             |
//| Effective transport trust posture visible to EA code.            |
//| NOTE: TOFU_FIRST_USE remains for backwards compatibility and now |
//|       means TOFU is enabled but no thumbprint has been provided; |
//|       connection policy will fail closed until one is pinned.    |
//+------------------------------------------------------------------+
enum ENUM_MQTT_TRUST_MODE {
  MQTT_TRUST_MODE_PLAINTEXT      = 0,  // Plaintext transport (no TLS)
  MQTT_TRUST_MODE_TLS            = 1,  // TLS transport (standard TLS with cert verification)
  MQTT_TRUST_MODE_TOFU_FIRST_USE = 2,  // TOFU enabled; no thumbprint provisioned yet (first-use)
  MQTT_TRUST_MODE_TOFU_PINNED    = 3,  // TOFU pinned; thumbprint has been provisioned/pinned
  MQTT_TRUST_MODE_TOFU_DEGRADED  = 4,  // TOFU degraded; cert inspection unavailable but TOFU allowed
};

//+------------------------------------------------------------------+
//| ENUM_MQTT_FAILURE_CLASS                                          |
//| Coarse-grained operator-facing classification of the last error. |
//+------------------------------------------------------------------+
enum ENUM_MQTT_FAILURE_CLASS {
  MQTT_FAILURE_NONE           = 0,  // No failure
  MQTT_FAILURE_TRANSPORT      = 1,  // Transport/network-level error (socket/TLS)
  MQTT_FAILURE_PROTOCOL       = 2,  // MQTT protocol error (malformed/invalid packet)
  MQTT_FAILURE_AUTHENTICATION = 3,  // Authentication failure (invalid credentials)
  MQTT_FAILURE_AUTHORIZATION  = 4,  // Authorization failure (permission denied)
  MQTT_FAILURE_POLICY         = 5,  // Policy enforcement (client/broker policy)
  MQTT_FAILURE_BROKER         = 6,  // Broker-side internal error or rejection
  MQTT_FAILURE_APPLICATION    = 7,  // Application-level error (EA logic or callback)
};

#define MQTT_DEFAULT_MAX_RECONNECT_ATTEMPTS               12
#define MQTT_DEFAULT_MAX_DEFERRED_TRANSPORT_PACKETS       500
#define MQTT_DEFAULT_MAX_DEFERRED_TRANSPORT_BYTES         (1 * 1024 * 1024)
#define MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_EVENTS         500
#define MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_PAYLOAD_BYTES  (1 * 1024 * 1024)
#define MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_PROPERTY_BYTES (1 * 1024 * 1024)

//+------------------------------------------------------------------+
//| Server Redirection callback                                      |
//| Fires when broker sends reason code 0x9C (Use Another Server)    |
//| or 0x9D (Server Moved) with a Server Reference property.         |
//| reason_code: 0x9C or 0x9D                                        |
//| server_reference: the broker-provided URI (e.g. "host:port")     |
//+------------------------------------------------------------------+
typedef void (*MqttOnServerRedirectCallback)(int reason_code, const string server_reference);

//+------------------------------------------------------------------+
//| Heartbeat latency threshold callback                             |
//| Fires when PINGREQ→PINGRESP RTT exceeds a configurable threshold |
//| rtt_us: measured round-trip time in microseconds                 |
//| threshold_us: configured threshold that was exceeded             |
//+------------------------------------------------------------------+
typedef void (*MqttOnRttThresholdCallback)(ulong rtt_us, ulong threshold_us);

//+------------------------------------------------------------------+
//| QoS retransmission failure callback                              |
//| Fires when a QoS 1/2 message is dropped after max retransmits.   |
//| Lets the application decide: retry as QoS 0, re-queue, or discard|
//| packet_id: the dropped message's packet identifier               |
//| qos_level: original QoS level (1 or 2)                           |
//| topic: the topic the message was published to                    |
//| retransmit_count: number of retransmits attempted                |
//+------------------------------------------------------------------+
typedef void (*MqttOnQoSDropCallback)(ushort packet_id, uchar qos_level, const string topic, uint retransmit_count);

//--- Fixed overhead for one SUBSCRIBE packet: Fixed-header(1) + Remaining-Length VBI(4) +
//--- Packet-ID(2) + Property-Length VBI(1) + Subscription-Identifier property(5).
#define MQTT_SUBSCRIBE_FIXED_OVERHEAD     13

//--- Retransmit check interval = retransmit_timeout_s * MQTT_RETRANSMIT_CHECK_FRACTION_MS ms/sec
//--- = 25% of timeout, bounding worst-case retransmit latency to ~1.25× timeout.
#define MQTT_RETRANSMIT_CHECK_FRACTION_MS 250

//+------------------------------------------------------------------+
//| MqttSubscribeOptions                                             |
//| Full §3.8.3.1 subscribe options for the Subscribe() overload.    |
//| Exposes No Local, Retain-As-Published, and Retain Handling bits. |
//+------------------------------------------------------------------+
struct MqttSubscribeOptions {
  uchar qos;              // QoS level (0–2); 0xFF = use client default QoS
  bool  no_local;         // §3.8.3.1 bit 2: do not deliver own publishes back
  bool  rap;              // §3.8.3.1 bit 3: retain-as-published
  uchar retain_handling;  // §3.8.3.1 bits 4-5: 0=send retained on subscribe, 1=only if new sub, 2=never
};

//+------------------------------------------------------------------+
//| CMqttClient                                                      |
//+------------------------------------------------------------------+
class CMqttClient : public IMqttPublishQueueDrainSink {
 private:
  //--- Transport selection
  enum ENUM_TRANSPORT_TYPE {
    TRANSPORT_TCP = 0,
    TRANSPORT_WS  = 1,
  };
  ENUM_TRANSPORT_TYPE m_transport_type;

  //--- Transports (only one active at a time, selected by SetHost/SetHostWS)
  CMqttTransport      m_tcp_transport;                      // TCP/TLS socket transport implementation
  CWebSocketTransport m_ws_transport;                       // WebSocket (ws/wss) transport implementation
  IMqttTransport*     m_transport;                          // Points to the active transport (m_tcp_transport or m_ws_transport)

  //--- WebSocket-specific connection parameters
  string              m_ws_path;

  //--- Broker connection parameters
  string              m_host;                               // Broker hostname or IP (eg. broker.example.com)
  uint                m_port;                               // Broker port (default 1883; 8883 commonly used for TLS)
  bool                m_use_tls;                            // true = use TLS/WSS transport; false = plaintext TCP/WS
  bool                m_require_tls;                        // When true, Connect() refuses if TLS is not enabled
  bool                m_has_successful_connection;          // Distinguishes first-connect policy/bootstrap from reconnects.
  uint                m_connect_timeout_ms;                 // Baseline timeout budget for a fresh connection attempt.
  uint                m_reconnect_connect_timeout_ms;       // Optional timeout override used during reconnect loops.
  uint                m_active_connect_timeout_ms;          // Timeout currently applied to the in-flight connect attempt.
  uint                m_blocking_transport_hard_limit_ms;   // Fail-safe cap if transport connect falls back to blocking work.
  ulong               m_connect_blocking_duration_seen_us;  // Longest blocking transport section seen on the current attempt.

  //--- MQTT session parameters
  string              m_client_id;                          // MQTT client identifier
  string              m_username;                           // CONNECT username (optional)
  string              m_password;                           // CONNECT password (string form)
  uchar               m_password_binary[];                  // Binary password for auth tokens with null bytes
  bool                m_use_binary_password;                // True when using `m_password_binary` instead of `m_password`
  string              m_connect_auth_method;                // CONNECT Authentication Method property (MQTT 5)
  uchar               m_connect_auth_data[];                // Initial Authentication Data bytes sent with CONNECT/AUTH
  bool                m_use_connect_auth_data;              // True to include auth data even if zero-length
  string              m_active_auth_method;                 // Negotiated AUTH method currently active
  ushort              m_keepalive_s;                        // Keep-alive interval in seconds
  bool                m_clean_start;                        // Clean start flag for new session
  bool                m_always_replay_subscriptions;        // false = skip replay when session_present=true
  uint                m_session_expiry;                     // Requested session expiry (seconds)
  uint                m_effective_session_expiry;           // Session expiry actually in force after CONNACK

  //--- Presence flags keep "property absent" distinct from "property present with value 0".
  bool                m_has_connect_request_response_info;  // True when CONNECT includes Request Response Information property
  uchar               m_connect_request_response_info;      // Request Response Information value for CONNECT (0=disabled, 1=enabled)
  bool                m_has_connect_request_problem_info;   // True when CONNECT includes Request Problem Information property
  uchar               m_connect_request_problem_info;       // Request Problem Information value for CONNECT (0=disabled, 1=enabled)

  //--- Last Will & Testament
  bool                m_will_enabled;                       // True when a Will is configured and included in CONNECT
  string              m_will_topic;                         // Topic for the Last Will message
  uchar               m_will_payload[];                     // Will payload bytes (binary)
  uchar               m_will_qos;                           // Will QoS level (0, 1, or 2)
  bool                m_will_retain;                        // Retain flag for the Will message
  uint                m_will_delay_s;                       // Will Delay Interval property in seconds.
  uint                m_will_expiry_s;                      // Will Message Expiry Interval in seconds.
  bool                m_has_will_payload_format;            // Distinguishes omitted Payload Format Indicator from explicit value 0.
  uchar               m_will_payload_format;                // Payload Format Indicator property value.
  string              m_will_content_type;                  // Content-Type property for the Will payload (optional)
  string              m_will_response_topic;                // MQTT 5 Response Topic property for Will request/response patterns.
  uchar               m_will_correlation_data[];            // MQTT 5 Correlation Data paired with m_will_response_topic.
  string              m_will_user_prop_keys[];              // Will User Property keys
  string              m_will_user_prop_vals[];              // Will User Property values
  uint                m_will_user_prop_count;               // Number of populated key/value pairs in the Will User Property arrays.

  //--- Default QoS for Publish/Subscribe when not explicitly specified
  uchar  m_default_qos;

  //--- Client-side Topic Alias Maximum (advertised in CONNECT per §3.1.2.11.8)
  ushort m_client_topic_alias_max;

  //--- Persistent subscription registry (replayed after every reconnect)
  //--- Parallel arrays replace struct array (MQL5 disallows arrays of structs with object members)
  string                m_sub_topic[];                      // Subscription topic filters
  uchar                 m_sub_qos[];                        // QoS level per subscription
  MqttOnMessageCallback m_sub_cb[];        // Per-topic callback (NULL = global m_on_message)
  bool                  m_sub_no_local[];  // §3.8.3.1 No Local option per subscription
  bool                  m_sub_rap[];       // §3.8.3.1 Retain-As-Published option per subscription
  uchar                 m_sub_rh[];        // §3.8.3.1 Retain Handling option (0, 1, or 2)
  uint                  m_sub_id[];        // Auto-assigned subscription identifier per subscription (§3.8.2.1.2)
  uint                  m_sub_utf8_len[];  // Cached UTF-8 byte lengths — populated at Subscribe() time
  uint                  m_sub_id_counter;  // Monotonically increasing sub_id generator
  bool                  m_shared_sub_ids;  // When true, all subscriptions share sub_id=1 for replay batching.
  uint                  m_sub_count;       // Number of live subscriptions tracked in the parallel arrays.

  //--- Compact open-addressing topic→index table aligned with the subscription
  //--- parallel arrays. States: 0=empty, 1=occupied, 2=tombstone.
  string                m_sub_index_keys[];
  uint                  m_sub_index_vals[];
  uchar                 m_sub_index_state[];
  uint                  m_sub_index_capacity;    // Total slots allocated in the open-addressing index.
  uint                  m_sub_index_size;        // Occupied slots in the topic→index table.
  uint                  m_sub_index_tombstones;  // Deleted slots retained so probe chains stay valid.
  CTopicMatcher         m_topic_matcher;         // O(topic-depth) trie dispatch (replaces O(n) scan).

  //--- Publish queue (backpressure buffer)
  CMqttPublishQueue     m_publish_queue;
  CMqttPublishQueueCoordinator
       m_publish_queue_coordinator;             // Schedules queue drain back through the normal publish path.
  bool m_queue_qos0_while_disconnected;         // false = drop QoS 0 while offline unless enabled
  bool m_draining_queue;                        // Guard: prevent re-queue during drain.
  bool m_last_queued_publish_handoff_complete;  // Sticky durable-handoff answer for the last drained publish.

  //--- Deferred message callback queue
  //--- Message delivery callbacks are queued during packet parsing and drained
  //--- after protocol work completes, reducing the chance that user callback
  //--- work delays ACK/keep-alive handling inside the same hot path.
  MqttOnMessageCallback  m_msg_evt_cb[];        // Callback to invoke for each deferred delivery event.
  string                 m_msg_evt_topic[];     // Topic per deferred delivery event.
  uchar                  m_msg_evt_qos[];       // QoS per deferred delivery event.
  bool                   m_msg_evt_retain[];    // Retain flag per deferred delivery event.
  ushort                 m_msg_evt_pktid[];     // Packet identifier per deferred delivery event.
  uint                   m_msg_evt_subid[];     // Subscription Identifier per event; 0 when absent.
  uint                   m_msg_evt_poff[];      // Byte offset into m_msg_evt_pbuf for each payload slice.
  uint                   m_msg_evt_plen[];      // Payload slice length inside m_msg_evt_pbuf.
  uchar                  m_msg_evt_pbuf[];      // Shared payload buffer backing all deferred message events.
  uint                   m_msg_evt_prop_off[];  // Byte offset into m_msg_evt_prop_buf for each property slice.
  uint                   m_msg_evt_prop_len[];  // Property slice length inside m_msg_evt_prop_buf.
  uchar                  m_msg_evt_prop_buf[];  // Shared MQTT 5 property buffer backing deferred message events.
  uint                   m_msg_evt_count;       // Number of deferred message events queued for delivery.
  uint                   m_max_deferred_callback_events;          // 0 = unlimited
  uint                   m_max_deferred_callback_payload_bytes;   // 0 = unlimited
  uint                   m_max_deferred_callback_property_bytes;  // 0 = unlimited

  //--- Pending subscription replay tracking
  //--- Parallel arrays + flat topics replace struct array
  ushort                 m_prs_pkt_id[];          // SUBSCRIBE packet IDs awaiting SUBACK
  uint                   m_prs_tcount[];          // Number of topics per pending entry
  uint                   m_prs_toff[];            // Start offset into m_prs_topics for each entry
  string                 m_prs_topics[];          // Flat topic filter array (all pending batches)
  uint                   m_pending_replay_count;  // Number of replay SUBSCRIBE batches awaiting SUBACK.
  bool                   m_replay_in_progress;    // True while reconnect-time subscription replay is active.
  uint                   m_replay_next_index;     // Next subscription index to batch into replay.

  //--- Pending UNSUBSCRIBE tracking
  ushort                 m_punsub_pkt_id[];
  string                 m_punsub_topic[];
  uint                   m_pending_unsub_count;  // Number of UNSUBSCRIBE packets awaiting UNSUBACK.

  //--- Infrastructure
  CMqttContext           m_context;
  CMqttReconnectPolicy   m_reconnect_policy;

  //--- Reusable PUBLISH builder — cached as a member to avoid per-call heap
  //--- allocation of ~20 fields and multiple dynamic arrays.
  //--- Always call m_pub_builder.Reset() before use.
  CPublish               m_pub_builder;
  PacketBuffer           m_deferred_transport_pkts[];  // Extracted packets deferred to the next Poll()
  uint                   m_deferred_transport_bytes;   // Total bytes retained in the deferred transport backlog.
  uint                   m_max_deferred_transport_packets;  // 0 = unlimited
  uint                   m_max_deferred_transport_bytes;    // 0 = unlimited
  uint                   m_deferred_transport_count;   // Number of deferred packets buffered for the next Poll().

  //--- Pre-allocated match results (reused every Poll() cycle — avoids GC at 200 msg/s)
  uint                   m_match_scratch[];
  uchar                  m_retransmit_batch_buf[];  // Reused TCP retransmit coalescing buffer

  //--- Last successfully validated publish topic (skips StringToCharArray on repeated topics)
  string                 m_pub_last_valid_topic;

  //--- 4-byte ack templates with only bytes [2]/[3] patched per send —
  //--- evicts CPuback/CPubrec/CPubrel object + dynamic array per ack from the hot path.
  uchar                  m_puback_tmpl[];   // { 0x40, 0x02, hi, lo } PUBACK reason=0x00
  uchar                  m_pubrec_tmpl[];   // { 0x50, 0x02, hi, lo } PUBREC reason=0x00
  uchar                  m_pubrel_tmpl[];   // { 0x62, 0x02, hi, lo } PUBREL reason=0x00
  uchar                  m_pubcomp_tmpl[];  // { 0x70, 0x02, hi, lo } PUBCOMP reason=0x00

  //--- State machine
  ENUM_MQTT_CLIENT_STATE m_state;

  //--- CONNACK timeout
  ulong                  m_connect_deadline_ms;  // Absolute ms timestamp for transport/TLS setup; 0 = no deadline
  ulong                  m_connack_deadline_ms;  // Absolute ms timestamp; 0 = no deadline
  uint                   m_connack_timeout_ms;   // Configurable timeout for waiting CONNACK

  //--- Maximum retransmission attempts before a QoS 1/2 message is discarded
  uint                   m_max_retransmit_count;
  uint                   m_retransmit_timeout_s;
  uint m_pubrel_retry_timeout_s;  // Independent PUBREL step-2 retry timeout (0 = same as m_retransmit_timeout_s)

  //--- Per-Poll() packet processing budget (0 = unlimited; legacy behaviour)
  //--- When set, Poll() processes at most this many packets per call and defers
  //--- any remainder to the next timer tick, bounding worst-case event-loop hold time.
  uint m_max_packets_per_poll;

  //--- Callbacks
  MqttOnConnectCallback        m_on_connect;
  MqttOnDisconnectCallback     m_on_disconnect;
  MqttOnMessageCallback        m_on_message;
  MqttOnSubscribeAckCallback   m_on_suback;
  MqttOnUnsubscribeAckCallback m_on_unsuback;
  MqttOnErrorExCallback        m_on_error;
  MqttOnStateChangeCallback    m_on_state_change;
  MqttOnServerRedirectCallback m_on_redirect;
  MqttOnAuthCallback           m_on_auth;
  MqttOnAuthExCallback         m_on_auth_ex;
  MqttOnRttThresholdCallback   m_on_rtt_threshold;
  MqttOnQoSDropCallback        m_on_qos_drop;
  MqttOnPublishResultCallback  m_on_publish_result;
  MqttOnAckCallback            m_on_ack;

  MqttOnPacketIdLowCallback    m_on_packetid_low;         // Fires when available packet IDs < threshold
  uint                         m_packetid_low_threshold;  // 0 = disabled
  ulong                        m_rtt_threshold_us;

  //--- Connection health metrics
  ulong                        m_connected_since_ms;
  uint                         m_reconnect_count;
  ulong                        m_messages_sent;
  ulong                        m_messages_received;
  ulong                        m_last_ping_rtt_us;          // Last PINGREQ→PINGRESP round-trip in microseconds
  ulong                        m_last_retransmit_check_ms;  // Throttle retransmission scans
  bool                         m_in_poll;
  bool                         m_abort_current_poll;  // Stop stale work after a helper or callback closes the session
  int                          m_last_failure_code;
  string                       m_last_failure_description;
  ENUM_MQTT_FAILURE_CLASS      m_last_failure_class;

  //--- Error callback rate limiting
  ulong                        m_last_error_window_us;   // Start of current 1-second window
  uint                         m_error_count_in_window;  // Errors fired in current window
  uint                         m_error_suppressed_in_window;

  //--- Incoming storage circuit breaker
  //--- Guards against infinite reconnect loops when the session DB can't persist
  //--- incoming QoS 2 messages (e.g. disk full). After N consecutive failures
  //--- the circuit trips, all reconnection stops, and m_on_error is fired.
  uint                         m_incoming_storage_error_count;
  uint                         m_incoming_storage_error_max;
  //--- CONNECT User Properties (§3.1.2.11.7)
  //--- Key/value pairs appended to every CONNECT packet sent by this client.
  //--- Common use: commercial brokers that require auth metadata in CONNECT.
  string                       m_connect_user_prop_keys[];
  string                       m_connect_user_prop_vals[];
  uint                         m_connect_user_prop_count;

  //--- CONNACK User Properties cache
  //--- Stores the broker's CONNACK User Properties for EA-level inspection
  //--- after OnConnect fires. Keyed by string for O(n) lookup which is
  //--- acceptable given typical property counts of < 20.
  bool                         m_connack_session_present;
  uchar                        m_connack_reason_code;
  string                       m_connack_reason_string;
  uint                         m_connack_session_expiry;
  string                       m_connack_assigned_client_identifier;
  string                       m_connack_response_information;
  string                       m_connack_server_reference;
  ushort                       m_connack_server_keep_alive;
  ushort                       m_connack_receive_maximum;
  string                       m_connack_auth_method;
  uchar                        m_connack_auth_data[];
  string                       m_connack_user_prop_keys[];
  string                       m_connack_user_prop_vals[];
  uint                         m_connack_user_prop_count;

  //--- Last simple ACK diagnostics cache (PUBACK/PUBREC/PUBREL/PUBCOMP)
  uchar                        m_last_ack_packet_type;
  ushort                       m_last_ack_packet_id;
  uchar                        m_last_ack_reason_code;
  string                       m_last_ack_reason_string;
  string                       m_last_ack_user_prop_keys[];
  string                       m_last_ack_user_prop_vals[];
  uint                         m_last_ack_user_prop_count;

  //--- Outgoing MQTT 5 diagnostics for auto-generated simple ACK packets.
  MqttAckProperties            m_puback_props;
  MqttAckProperties            m_pubrec_props;
  MqttAckProperties            m_pubrel_props;
  MqttAckProperties            m_pubcomp_props;

  //--- Last SUBACK / UNSUBACK diagnostics cache
  ushort                       m_last_suback_packet_id;
  string                       m_last_suback_reason_string;
  string                       m_last_suback_user_prop_keys[];
  string                       m_last_suback_user_prop_vals[];
  uint                         m_last_suback_user_prop_count;
  ushort                       m_last_unsuback_packet_id;
  string                       m_last_unsuback_reason_string;
  string                       m_last_unsuback_user_prop_keys[];
  string                       m_last_unsuback_user_prop_vals[];
  uint                         m_last_unsuback_user_prop_count;

  //--- Last broker DISCONNECT diagnostics cache
  uchar                        m_last_disconnect_reason_code;
  string                       m_last_disconnect_reason_string;
  string                       m_last_disconnect_server_reference;
  string                       m_last_disconnect_user_prop_keys[];
  string                       m_last_disconnect_user_prop_vals[];
  uint                         m_last_disconnect_user_prop_count;

  //--- Last inbound AUTH diagnostics cache
  uchar                        m_last_auth_reason_code;
  string                       m_last_auth_reason_string;
  string                       m_last_auth_method;
  uchar                        m_last_auth_data[];
  string                       m_last_auth_user_prop_keys[];
  string                       m_last_auth_user_prop_vals[];
  uint                         m_last_auth_user_prop_count;

  //--- Server capabilities (from CONNACK)
  uchar                        m_server_max_qos;
  bool                         m_server_retain_available;
  bool                         m_server_wildcard_available;
  bool                         m_server_sub_id_available;
  bool                         m_server_shared_available;

  //--- Server redirection state
  string                       m_server_reference;  // Last received Server Reference URI
  bool                         m_auto_redirect;     // Automatically reconnect to redirected server
  bool                         m_require_redirect_allowlist;
  string                       m_redirect_allow_hosts[];
  uint                         m_redirect_allow_host_count;
  bool   m_redirect_pending;      // Deferred redirect flag — set by _HandleRedirection, processed at end of Poll()
  int    m_redirect_reason_code;  // Pending redirect reason code (0x9C or 0x9D)
  string m_redirect_server_ref;   // Pending redirect server reference URI

  //--- TOFU certificate pinning state
  bool   m_tofu_enabled;      // TOFU pinning active
  string m_tofu_fingerprint;  // Normalized MT5 certificate thumbprint (SHA-1 hex)
  bool   m_tofu_pinned;       // true after a thumbprint has been provisioned or loaded
  bool   m_tofu_strict;       // fail the connection if certificate inspection is unavailable
  string m_session_key;       // Stable key used for session DB and TOFU pin file naming
  ENUM_MQTT_TRUST_MODE m_effective_trust_mode;

  //--- Transport / credential hardening
  bool m_allow_insecure_plaintext_transport;  // explicit escape hatch for ws:// or plaintext TCP on trusted test nets
  bool m_allow_insecure_plaintext_auth;       // explicit escape hatch for plaintext username/password or AUTH

#ifdef MQTT_UNIT_TESTS
  bool m_test_transport_injected;
#endif

  //--- Strict UTF-8 validation flag
  //--- When true: DISCONNECT(0x99) on payload UTF-8 validation failure (spec SHOULD — §3.3.2.3.2).
  //--- When false (default): log a warning but deliver the message to remain compatible with
  //---   brokers/publishers that send marginally non-conformant payloads (e.g. containing U+0000).
  bool                 m_strict_utf8_validation;

  //+------------------------------------------------------------------+
  //| _SyncLogger                                                      |
  //| Purpose: Copy this instance's m_context.logger into the          |
  //|   chart-scoped logger registry so that all subsequent            |
  //|   MQTT_LOG_* calls in this call-chain route to the correct sink. |
  //|   Must be called:                                                |
  //|     1. At the entry of every public method.                      |
  //|     2. After every user callback (the callback may have called   |
  //|        another CMqttClient's public method, overwriting the      |
  //|        slot and leaving it configured for the other instance).   |
  //|                                                                  |
  //| Log configuration is now owned by m_context.logger (a CLogger    |
  //| in CMqttContext) rather than by bare globals. Logger routing is  |
  //| keyed by ChartID(), so public entry points only need to refresh  |
  //| the current chart's active logger before work begins.            |
  //+------------------------------------------------------------------+
  void                 _SyncLogger() { _MqttSetActiveLogger(m_context.logger); }

  //--- Private helpers
  void                 _SetState(ENUM_MQTT_CLIENT_STATE new_state);
  uint                 _ResolveConnectTimeoutMs(bool is_manual_connect) const;
  string               _DescribeConnectTimeout() const;
  void                 _HandleBacklogOverflow(const string desc);
  void                 _AppendPacketCopy(PacketBuffer& dest[], uint& dest_count, const uchar& src[]);
  bool                 _TryAppendDeferredTransportPacket(const uchar& pkt[]);
  void                 _AppendDeferredPackets(PacketBuffer& src[], uint start_idx, uint count);
  void                 _TakeDeferredPackets(PacketBuffer& out[], uint max_count, uint& out_count);
  ENUM_TRANSPORT_ERROR _HandleConnectSetupFailure(ENUM_TRANSPORT_ERROR err, const string desc);
  bool                 _EnforceBlockingTransportHardLimit();
  bool _TryComputeArrayAppendSize(uint current_size, uint append_size, int& new_size, const string buffer_name) const;
  uint _RemainingExpirySecondsFromDeadlineUs(ulong expiry_deadline_us, ulong now_us) const;
  virtual uint            RemainingExpirySecondsFromDeadlineUs(ulong expiry_deadline_us, ulong now_us) const override;
  void                    _SendConnect();
  void                    _PollInternal();
  void                    _OnConnackReceived(uchar& pkt[]);
  void                    _OnPublishReceived(uchar& pkt[]);
  void                    _OnDisconnectReceived(uchar& pkt[]);
  void                    _OnSubackReceived(uchar& pkt[]);
  void                    _OnUnsubackReceived(uchar& pkt[]);
  void                    _OnTransportError(int err_code = 0, string err_desc = "");
  ENUM_MQTT_FAILURE_CLASS _ClassifyFailure(int code, const string desc) const;
  void                    _RememberFailure(int code, const string desc);
  bool                    _IsRedirectHostAllowed(const string host) const;
  void                    _ReplaySubscriptions();
  void                    _RunRetransmissions(uint timeout_seconds = 30);
  void                    _PurgeExpiredQueuedPublishes();
  void                    _DrainPublishQueue();
  void                    _RestorePersistedPublishQueue();
  void _QueueMessageCallback(MqttOnMessageCallback cb, const string topic, const uchar& payload[], int payload_len,
                             uchar qos, bool retain_f, ushort packet_id, uint matched_sub_id,
                             const uchar& publish_properties[]);
  void _DrainMessageCallbacks();
  void _ClearMessageCallbacks();
  void _ResetIncomingMessageMetadata(MqttIncomingMessageMetadata& metadata);
  bool _DecodeIncomingPublishMetadata(uchar& publish_properties[], uint matched_sub_id,
                                      MqttIncomingMessageMetadata& metadata);
  bool _EncodePublishProperties(const MqttPublishProperties& props, uchar& out_props[], uint& out_expiry_interval,
                                bool& out_allow_outgoing_sub_id);
  void _BuildPersistedPublishProperties(const uchar& encoded_props[], uint expiry_interval, uchar& persisted_props[]);
  void _BuildPersistedPublishProperties(const uchar& encoded_props[], int prop_offset, int prop_length,
                                        uint expiry_interval, uchar& persisted_props[]);
  void _ApplyEncodedPublishProperties(CPublish& publish, const uchar& encoded_props[], datetime expiry_time,
                                      bool allow_outgoing_sub_id);
  void _ApplyEncodedPublishProperties(CPublish& publish, const uchar& encoded_props[], int prop_offset, int prop_length,
                                      datetime expiry_time, bool allow_outgoing_sub_id);
  virtual int  PublishQueuedEntry(const string topic, const uchar& payload_buffer[], uint payload_offset,
                                  uint payload_length, uchar qos, bool retain, const uchar& encoded_props_buffer[],
                                  uint prop_offset, uint prop_length, uint remaining_expiry,
                                  bool allow_outgoing_sub_id) override;
  virtual bool LastQueuedPublishDurablyHandedOff() const override { return m_last_queued_publish_handoff_complete; }
  virtual void ReportQueueError(int code, const string description) override;
  void _DispatchAckDiagnostics(uchar packet_type, ushort packet_id, uchar reason_code, const string reason_string,
                               const string& user_prop_keys[], const string& user_prop_vals[], uint user_prop_count);
  void _CacheDisconnectMetadata(uchar reason_code, const string reason_string, const string server_reference,
                                const string& user_prop_keys[], const string& user_prop_vals[], uint user_prop_count);
  void _DisconnectInternal(uchar reason_code, bool has_session_expiry_override, uint session_expiry_interval,
                           const string reason_string, const string server_reference, const string& user_prop_keys[],
                           const string& user_prop_vals[], int user_prop_count);
  ENUM_MQTT_PUBLISH_ERROR _PublishPrepared(const string topic, const uchar& payload[], int len, uchar qos, bool retain,
                                           const uchar& encoded_props[], uint expiry_interval,
                                           bool allow_outgoing_sub_id);
  ENUM_MQTT_PUBLISH_ERROR _PublishPreparedRange(const string topic, const uchar& payload[], int payload_offset, int len,
                                                uchar qos, bool retain, const uchar& encoded_props[], int prop_offset,
                                                int prop_length, uint expiry_interval, bool allow_outgoing_sub_id);
  bool                    _SendImmediateSubscribe(const string topic_filter, uchar opts_byte, uint sub_id);
  void                    _TrackPendingSubscribe(ushort packet_id, const string topic_filter);
  void                    _TrackPendingUnsubscribe(ushort packet_id, const string topic_filter);
  bool                    _RemoveSubscriptionLocal(const string topic_filter);
  bool _ValidateSubscribeRequest(const string topic_filter, bool& use_sub_id, bool fire_error = true);
  void _ResetServerCapabilities();
  void _ClearLocalSessionState();
  void _SetEffectiveSessionExpiry(bool has_connack_override, uint connack_session_expiry);
  void _HandleConnectionClosed(bool has_session_expiry_override = false, uint session_expiry_override = 0);
  void _CacheConnackMetadata(CConnack& connack);
  void _ResetAckProperties(MqttAckProperties& props);
  bool _HasAckProperties(const MqttAckProperties& props) const;
  ENUM_TRANSPORT_ERROR _SendPubackPacket(ushort packet_id, uchar reason_code = 0x00);
  ENUM_TRANSPORT_ERROR _SendPubrecPacket(ushort packet_id, uchar reason_code = 0x00);
  ENUM_TRANSPORT_ERROR _SendPubrelPacket(ushort packet_id, uchar reason_code = 0x00);
  ENUM_TRANSPORT_ERROR _SendPubcompPacket(ushort packet_id, uchar reason_code = 0x00);
  void                 _FireError(int code, const string desc);
  void _FireErrorEx(int code, const string desc, const string src_file, int src_line, const string func_name);
  void _SecureEraseString(string& value);
  uint _NextSubscriptionIdentifier();
  uint _SubIndexHash(const string key) const;
  bool _SubIndexRehash(uint min_capacity);
  bool _SubIndexEnsureCapacity(uint desired_size);
  bool _SubIndexLookup(const string key, uint& out_idx) const;
  bool _SubIndexSet(const string key, uint idx);
  bool _SubIndexRemove(const string key);
  void _ClearDeferredTransportPackets();
  bool _ReadSimpleAckDiagnostics(uchar& pkt[], const string pkt_name, string& out_reason_string,
                                 string& out_user_prop_keys[], string& out_user_prop_vals[], uint& out_user_prop_count);
  bool _HasLocalSessionState() const;
  bool _ParseServerReference(const string ref, string& out_host, uint& out_port);
  bool _IsSimpleAckReasonValid(uchar packet_type, uchar reason_code);
  bool _ParseSimpleAckPacket(const uchar& pkt[], int pkt_size, ushort& out_pktid, uchar& out_reason);
  bool _TryGetOutgoingAckMessage(const string ack_name, ushort packet_id, uchar expected_qos, bool require_state,
                                 ENUM_QOS2_STATE expected_state, SessionMessage& out_msg);
  bool _QoS1PublishRequiresExpiry(uchar qos, uint expiry_interval) const;
  void _ProtocolDisconnect(uchar reason_code, const string desc);
  bool _HandleRedirection(int reason_code, const string server_ref);
  bool _HasSensitiveAuth() const;
  void _UpdateEffectiveTrustMode();
  bool _NormalizeCertificateThumbprint(const string raw_thumbprint, string& normalized) const;
  void _PersistTofuFingerprint(const string cert_thumb);
  bool _EvaluateTofuCertificate(bool cert_available, const string cert_thumb);
  string _GetIndexedStringOrEmpty(const string& values[], uint idx, uint count) const {
    return (idx < count) ? values[idx] : "";
  }
  void _CopyByteArray(const uchar& src[], uchar& dest[]) const {
    ArrayResize(dest, ArraySize(src));
    if (ArraySize(src) > 0) {
      ArrayCopy(dest, src);
    }
  }
#ifdef MQTT_UNIT_TESTS
  bool _TopicMatchesFilter(const string topic, const string filter);
#endif
  bool   _IsPermanentFailure(uchar reason_code);
  string _SanitizeSessionKey(const string key) const;

 public:
  CMqttClient();
  ~CMqttClient();

  //--- ═══════════════════════════════════════════════════════════
  //--- Configuration (call before Connect)
  //--- ═══════════════════════════════════════════════════════════

  //--- Core connection
  CMqttClient* SetHost(const string host, uint port = 1883);
  CMqttClient* SetHostWS(const string host, uint port = 80, const string path = "/mqtt");
  CMqttClient* SetTLS(bool enable = true);
  CMqttClient* SetRequireTLS(bool require = true);                              // Enforce TLS requirement
  CMqttClient* SetClientId(const string client_id);
  CMqttClient* SetCredentials(const string username, const string password);
  CMqttClient* SetCredentials(const string username, const uchar& password[]);  // Binary password overload
  CMqttClient* SetAuthMethod(const string auth_method);                         // CONNECT Authentication Method
  CMqttClient* SetAuthData(const string auth_data);                             // CONNECT Authentication Data (string)
  CMqttClient* SetAuthData(const uchar& auth_data[]);                           // CONNECT Authentication Data (binary)
  CMqttClient* SetConnectTimeout(uint ms = 5000);
  CMqttClient* SetReconnectConnectTimeout(uint ms = 0);

  //--- Session
  CMqttClient* SetCleanStart(bool clean = true);
  CMqttClient* SetAlwaysReplaySubscriptions(bool always = false);
  CMqttClient* SetSessionExpiry(uint seconds = 0);
  CMqttClient* SetSessionEncryptionPassphrase(const string passphrase);
  CMqttClient* SetKeepAlive(ushort seconds = 60);
  CMqttClient* SetRequestResponseInformation(bool enable = true);
  CMqttClient* SetRequestProblemInformation(bool enable = true);

  //--- Last Will & Testament
  CMqttClient* SetWill(const string topic, const string payload, uchar qos = QoS_0, bool retain = false);
  CMqttClient* SetWillBytes(const string topic, const uchar& payload[], uchar qos = QoS_0, bool retain = false);
  CMqttClient* SetWillDelay(uint seconds);
  CMqttClient* SetWillExpiry(uint seconds);
  CMqttClient* SetWillPayloadFormat(PAYLOAD_FORMAT_INDICATOR format);
  CMqttClient* SetWillProperties(const string content_type = "", const string response_topic = "");
  CMqttClient* SetWillCorrelationData(const string corr_data);
  CMqttClient* SetWillCorrelationData(const uchar& corr_data[]);
  CMqttClient* SetWillUserProperty(const string key, const string val);

  //--- Defaults
  CMqttClient* SetDefaultQoS(uchar qos = QoS_0);

  //--- Topic Alias
  CMqttClient* SetClientTopicAliasMaximum(ushort max = 0);  // 0 = disabled

  //--- Reliability
  CMqttClient* SetAutoReconnect(bool enable = true, uint min_backoff_ms = 1000, uint max_backoff_ms = 60000);
  CMqttClient* SetMaxRetransmitCount(uint count = 10);
  CMqttClient* SetRetransmitTimeout(uint seconds = 30);
  CMqttClient* SetMaxQueuedMessages(uint count = 500);
  CMqttClient* SetMaxQueuedPayloadBytes(uint bytes = 0);        // 0 = unlimited
  CMqttClient* SetMaxQueuedPropertyBytes(uint bytes = 0);       // 0 = unlimited
  CMqttClient* SetMaxSingleQueuedPublishBytes(uint bytes = 0);  // 0 = unlimited
  CMqttClient* SetQueueQoS0WhenDisconnected(bool enable = false);
  CMqttClient* SetConnackTimeout(uint ms = 10000);
  CMqttClient* SetMaxReconnectAttempts(uint count = MQTT_DEFAULT_MAX_RECONNECT_ATTEMPTS);
  // 0 = unlimited (circuit breaker off)
  CMqttClient* SetMaxPacketsPerPoll(uint n = 50);  // 0 = unlimited; set e.g. 20 to cap event-loop hold time
  CMqttClient* SetMaxDeferredTransportPackets(uint count = MQTT_DEFAULT_MAX_DEFERRED_TRANSPORT_PACKETS);
  CMqttClient* SetMaxDeferredTransportBytes(uint bytes = MQTT_DEFAULT_MAX_DEFERRED_TRANSPORT_BYTES);
  CMqttClient* SetMaxDeferredCallbackEvents(uint count = MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_EVENTS);
  CMqttClient* SetMaxDeferredCallbackPayloadBytes(uint bytes = MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_PAYLOAD_BYTES);
  CMqttClient* SetMaxDeferredCallbackPropertyBytes(uint bytes = MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_PROPERTY_BYTES);
  CMqttClient*
  SetMaxIncomingPacketSize(uint bytes);  // Cap incoming packet memory (default: 1 MB); override after construction
  CMqttClient* SetPingRespTimeout(uint seconds);  // Independent PINGRESP deadline (0 = same as keep-alive)
  CMqttClient* SetBlockingTransportWarnThreshold(uint ms = 250);
  CMqttClient* SetBlockingTransportHardLimit(uint ms = 0);

  //--- Callbacks
  CMqttClient* SetOnConnect(MqttOnConnectCallback cb);
  CMqttClient* SetOnDisconnect(MqttOnDisconnectCallback cb);
  CMqttClient* SetOnMessage(MqttOnMessageCallback cb);
  CMqttClient* SetOnSubscribeAck(MqttOnSubscribeAckCallback cb);
  CMqttClient* SetOnUnsubscribeAck(MqttOnUnsubscribeAckCallback cb);
  CMqttClient* SetOnError(MqttOnErrorExCallback cb);
  CMqttClient* SetOnStateChange(MqttOnStateChangeCallback cb);
  CMqttClient* SetOnServerRedirect(MqttOnServerRedirectCallback cb);

  //--- Enhanced authentication callback (§4.12)
  CMqttClient* SetOnAuth(MqttOnAuthCallback cb);
  CMqttClient* SetOnAuthEx(MqttOnAuthExCallback cb);

  //--- Heartbeat latency monitoring
  CMqttClient* SetOnRttThreshold(MqttOnRttThresholdCallback cb, ulong threshold_us);

  //--- QoS retransmission failure
  CMqttClient* SetOnQoSDrop(MqttOnQoSDropCallback cb);

  //--- Publish delivery result (PUBACK/PUBCOMP reason code notification)
  CMqttClient* SetOnPublishResult(MqttOnPublishResultCallback cb);

  //--- MQTT 5 diagnostics callback for PUBACK/PUBREC/PUBREL/PUBCOMP
  CMqttClient* SetOnAck(MqttOnAckCallback cb);

  //--- Outgoing MQTT 5 diagnostic properties for auto-generated PUBACK/PUBREC/PUBREL/PUBCOMP.
  CMqttClient* SetAckReasonString(uchar packet_type, const string reason_string);
  CMqttClient* AddAckUserProperty(uchar packet_type, const string key, const string val);
  CMqttClient* ClearAckProperties(uchar packet_type);

  //--- Packet ID pool low-water-mark monitoring (0 = disabled)
  CMqttClient* SetOnPacketIdLow(MqttOnPacketIdLowCallback cb, uint threshold = 1000);

  //--- PUBREL independent retry timeout (0 = use m_retransmit_timeout_s)
  CMqttClient* SetPubrelRetryTimeout(uint seconds);

  //--- Server redirection policy
  CMqttClient* SetAutoRedirect(bool enable = true);
  CMqttClient* SetRequireRedirectAllowlist(bool require_match = true);
  CMqttClient* AddRedirectAllowHost(const string host);
  CMqttClient* ClearRedirectAllowHosts();
  CMqttClient*
  SetSharedSubscriptionIds(bool shared = true);  // true = all topics share sub_id=1 → single SUBSCRIBE packet on replay

  //--- CONNECT User Properties (§3.1.2.11.7)
  //--- Appended to every CONNECT packet. Useful for auth metadata required by
  //--- commercial brokers (e.g. client type, version, account ID).
  CMqttClient* SetConnectUserProperty(const string key, const string val);

  //--- CONNACK User Properties query (§3.2.2.3.8)
  //--- Returned by broker in CONNACK. May contain rate limits, feature flags, etc.
  //--- Valid after OnConnect fires. Returns "" if key not found.
  bool         GetConnackSessionPresent() const { return m_connack_session_present; }
  uchar        GetConnackReasonCode() const { return m_connack_reason_code; }
  string       GetConnackReasonString() const { return m_connack_reason_string; }
  uint         GetConnackSessionExpiryInterval() const { return m_connack_session_expiry; }
  string       GetConnackAssignedClientIdentifier() const { return m_connack_assigned_client_identifier; }
  string       GetConnackResponseInformation() const { return m_connack_response_information; }
  string       GetConnackServerReference() const { return m_connack_server_reference; }
  ushort       GetConnackServerKeepAlive() const { return m_connack_server_keep_alive; }
  ushort       GetConnackReceiveMaximum() const { return m_connack_receive_maximum; }
  uint         GetConnackMaximumPacketSize() const { return m_context.flow_control.GetMaximumPacketSize(); }
  uchar        GetConnackMaximumQoS() const { return m_server_max_qos; }
  ushort       GetConnackTopicAliasMaximum() const { return m_context.topic_alias_manager.GetTopicAliasMaximum(); }
  bool         GetConnackRetainAvailable() const { return m_server_retain_available; }
  bool         GetConnackWildcardSubscriptionAvailable() const { return m_server_wildcard_available; }
  bool         GetConnackSubscriptionIdentifierAvailable() const { return m_server_sub_id_available; }
  bool         GetConnackSharedSubscriptionAvailable() const { return m_server_shared_available; }
  string       GetConnackAuthenticationMethod() const { return m_connack_auth_method; }
  void         GetConnackAuthenticationData(uchar& dest[]) const { _CopyByteArray(m_connack_auth_data, dest); }
  string       GetConnackUserProperty(const string key) const;
  uint         GetConnackUserPropertyCount() const { return m_connack_user_prop_count; }
  string       GetConnackUserPropertyKey(uint idx) const;
  string       GetConnackUserPropertyValue(uint idx) const;

  //--- Incoming storage circuit-breaker
  //--- After this many consecutive StoreIncomingMessage failures (e.g. disk full),
  //--- the circuit breaks: reconnection stops and m_on_error fires.
  //--- Default: 5. Set to 0 to disable (allow infinite loops).
  CMqttClient* SetIncomingStorageErrorMax(uint max);

  //--- Strict UTF-8 payload validation (§3.3.2.3.2)
  //--- When strict=true (default): DISCONNECT(0x99) on UTF-8 validation failure.
  //--- When strict=false: warn and deliver for permissive broker interoperability.
  CMqttClient* SetStrictUtf8Validation(bool strict = true);

  //--- TOFU certificate pinning
  CMqttClient* SetTofuPinning(bool enable = true);
  CMqttClient* SetTofuThumbprint(const string thumbprint);
  CMqttClient* SetTofuFingerprint(const string thumbprint) { return SetTofuThumbprint(thumbprint); }
  CMqttClient* SetTofuStrictMode(bool strict = true);

  //--- Security policy
  CMqttClient* SetAllowInsecurePlaintextTransport(bool allow = true);
  CMqttClient* SetAllowInsecurePlaintextAuth(bool allow = true);
  CMqttClient* SetAllowMaskedServerFrames(bool allow = true);

  //--- Logging
  CMqttClient* SetLogLevel(ENUM_MQTT_LOG_LEVEL level);
  CMqttClient* SetLogSink(MqttLogSinkCallback sink);

  //--- ═══════════════════════════════════════════════════════════
  //--- Operations (Connect/Subscribe before or after Connect)
  //--- ═══════════════════════════════════════════════════════════

  ENUM_TRANSPORT_ERROR Connect();
  void                 Disconnect(uchar reason_code = 0x00);
  void                 Disconnect(uchar reason_code, uint session_expiry_interval);
  void                 Disconnect(uchar reason_code, uint session_expiry_interval, const string reason_string,
                                  const string server_reference = "");
  void Disconnect(uchar reason_code, uint session_expiry_interval, const string reason_string,
                  const string server_reference, const string& user_prop_keys[], const string& user_prop_vals[]);
  void Poll();

  //--- Publish
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const string payload);
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const string payload, uchar qos, bool retain = false);
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const string payload, const MqttPublishProperties& props);
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const string payload, uchar qos, bool retain,
                                  const MqttPublishProperties& props);
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const uchar& payload[], int len = -1);
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const uchar& payload[], int len, uchar qos, bool retain = false);
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const uchar& payload[], int len,
                                  const MqttPublishProperties& props);
  ENUM_MQTT_PUBLISH_ERROR Publish(const string topic, const uchar& payload[], int len, uchar qos, bool retain,
                                  const MqttPublishProperties& props);

  //--- Subscribe / Unsubscribe (queued and replayed on reconnect)
  void                    Subscribe(const string topic_filter, uchar qos = 0xFF);         // 0xFF = use default QoS
  void Subscribe(const string topic_filter, MqttOnMessageCallback cb, uchar qos = 0xFF);  // per-topic callback overload
  void Subscribe(
    const string                topic_filter,
    const MqttSubscribeOptions& opts);  // Full §3.8.3.1 options (No Local, Retain-As-Published, Retain Handling)
  void Unsubscribe(const string topic_filter);

  //--- Enhanced Authentication (§4.12) — send AUTH packet for multi-step exchange
  void SendAuth(uchar reason_code, const string method, const uchar& data[], const string reason_string = "");

  //--- ═══════════════════════════════════════════════════════════
  //--- State queries
  //--- ═══════════════════════════════════════════════════════════

  bool IsConnected() const { return m_state == MQTT_CLIENT_CONNECTED; }
  bool IsConnecting() const {
    return m_state == MQTT_CLIENT_CONNECTING || m_state == MQTT_CLIENT_WAITING_CONNACK
        || m_state == MQTT_CLIENT_TLS_HANDSHAKING;  // TLS upgrade phase
  }
  bool IsSafeToPublish() const { return m_state == MQTT_CLIENT_CONNECTED; }
  bool FlushSessionStateNow() { return !m_context.session_db.IsDirty() || m_context.session_db.FlushIfDirty(0); }
  ENUM_MQTT_CLIENT_STATE GetState() const { return m_state; }

  //--- Health metrics
  uint                   GetReconnectCount() const { return m_reconnect_count; }
  bool                   IsReconnectInProgress() const { return m_reconnect_policy.IsReconnectInProgress(); }
  uint                   GetReconnectAttemptCount() const { return m_reconnect_policy.GetCurrentAttemptCount(); }
  uint                   GetMaxReconnectAttempts() const { return m_reconnect_policy.GetMaxAttempts(); }
  bool                   IsSessionEncryptionEnabled() const { return m_context.session_db.IsEncryptionEnabled(); }
  ulong                  GetMessagesSent() const { return m_messages_sent; }
  ulong                  GetMessagesReceived() const { return m_messages_received; }
  ulong                  GetLastPingRTT() const { return m_last_ping_rtt_us; }  // microseconds
  ulong  GetLastTransportBlockingDuration() const { return m_transport.GetLastBlockingOperationDuration_us(); }
  uint   GetInFlightCount() const { return m_context.flow_control.GetInFlightCount(); }
  ushort GetIncomingInFlightCount() const { return m_context.flow_control.GetIncomingInFlightCount(); }
  uint   GetInFlightQoS1Count() const { return m_context.flow_control.GetInFlightQoS1Count(); }
  uint   GetInFlightQoS2Count() const { return m_context.flow_control.GetInFlightQoS2Count(); }
  uint   GetQueuedMessageCount() const { return m_publish_queue.GetQueuedMessageCount(); }
  uint   GetDurableQueuedMessageCount() const { return m_context.session_db.GetOfflineQueuedMessageCount(); }
  uint   GetQueuedPayloadBytes() const { return m_publish_queue.GetPayloadBytes(); }
  uint   GetQueuedPropertyBytes() const { return m_publish_queue.GetPropertyBytes(); }
  ulong  GetOldestQueuedMessageAgeMs() const;
  uint   GetCallbackBacklogCount() const { return m_msg_evt_count; }
  uint   GetCallbackBacklogPayloadBytes() const { return (uint)ArraySize(m_msg_evt_pbuf); }
  uint   GetCallbackBacklogPropertyBytes() const { return (uint)ArraySize(m_msg_evt_prop_buf); }
  uint   GetDeferredTransportBacklogCount() const { return m_deferred_transport_count; }
  uint   GetDeferredTransportBacklogBytes() const { return m_deferred_transport_bytes; }
  int    GetLastFailureCode() const { return m_last_failure_code; }
  string GetLastFailureDescription() const { return m_last_failure_description; }
  ENUM_MQTT_FAILURE_CLASS GetLastFailureClass() const { return m_last_failure_class; }
  uint                    GetMaxRetransmitCount() const { return m_max_retransmit_count; }
  string                  GetServerReference() const { return m_server_reference; }
  uchar                   GetLastAckPacketType() const { return m_last_ack_packet_type; }
  ushort                  GetLastAckPacketId() const { return m_last_ack_packet_id; }
  uchar                   GetLastAckReasonCode() const { return m_last_ack_reason_code; }
  string                  GetLastAckReasonString() const { return m_last_ack_reason_string; }
  uint                    GetLastAckUserPropertyCount() const { return m_last_ack_user_prop_count; }
  string                  GetLastAckUserPropertyKey(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_ack_user_prop_keys, idx, m_last_ack_user_prop_count);
  }
  string GetLastAckUserPropertyValue(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_ack_user_prop_vals, idx, m_last_ack_user_prop_count);
  }
  ushort GetLastSubackPacketId() const { return m_last_suback_packet_id; }
  string GetLastSubackReasonString() const { return m_last_suback_reason_string; }
  uint   GetLastSubackUserPropertyCount() const { return m_last_suback_user_prop_count; }
  string GetLastSubackUserPropertyKey(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_suback_user_prop_keys, idx, m_last_suback_user_prop_count);
  }
  string GetLastSubackUserPropertyValue(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_suback_user_prop_vals, idx, m_last_suback_user_prop_count);
  }
  ushort GetLastUnsubackPacketId() const { return m_last_unsuback_packet_id; }
  string GetLastUnsubackReasonString() const { return m_last_unsuback_reason_string; }
  uint   GetLastUnsubackUserPropertyCount() const { return m_last_unsuback_user_prop_count; }
  string GetLastUnsubackUserPropertyKey(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_unsuback_user_prop_keys, idx, m_last_unsuback_user_prop_count);
  }
  string GetLastUnsubackUserPropertyValue(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_unsuback_user_prop_vals, idx, m_last_unsuback_user_prop_count);
  }
  uchar  GetLastDisconnectReasonCode() const { return m_last_disconnect_reason_code; }
  string GetLastDisconnectReasonString() const { return m_last_disconnect_reason_string; }
  string GetLastDisconnectServerReference() const { return m_last_disconnect_server_reference; }
  uint   GetLastDisconnectUserPropertyCount() const { return m_last_disconnect_user_prop_count; }
  string GetLastDisconnectUserPropertyKey(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_disconnect_user_prop_keys, idx, m_last_disconnect_user_prop_count);
  }
  string GetLastDisconnectUserPropertyValue(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_disconnect_user_prop_vals, idx, m_last_disconnect_user_prop_count);
  }
  uchar  GetLastAuthReasonCode() const { return m_last_auth_reason_code; }
  string GetLastAuthReasonString() const { return m_last_auth_reason_string; }
  string GetLastAuthMethod() const { return m_last_auth_method; }
  void   GetLastAuthData(uchar& dest[]) const { _CopyByteArray(m_last_auth_data, dest); }
  uint   GetLastAuthUserPropertyCount() const { return m_last_auth_user_prop_count; }
  string GetLastAuthUserPropertyKey(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_auth_user_prop_keys, idx, m_last_auth_user_prop_count);
  }
  string GetLastAuthUserPropertyValue(uint idx) const {
    return _GetIndexedStringOrEmpty(m_last_auth_user_prop_vals, idx, m_last_auth_user_prop_count);
  }
  ENUM_MQTT_TRUST_MODE GetEffectiveTrustMode() const { return m_effective_trust_mode; }

  //--- Connection info snapshot
  void                 GetConnectionInfo(MqttConnectionInfo& info) const;

#ifdef MQTT_UNIT_TESTS
  void TestInjectTransport(IMqttTransport* transport) {
    if (transport != NULL) {
      m_transport               = transport;
      m_test_transport_injected = true;
    }
  }
  void TestSetState(ENUM_MQTT_CLIENT_STATE state) { m_state = state; }
  void TestStartReconnect() { m_reconnect_policy.StartLoopIfNeeded(); }
  bool TestIsReconnecting() const { return m_reconnect_policy.IsReconnecting(); }
  uint TestGetReconnectBackoff() const { return m_reconnect_policy.GetCurrentBackoff(); }
  uint TestGetReconnectAttemptCount() const { return m_reconnect_policy.GetCurrentAttemptCount(); }
  void TestSetReconnectAttemptCount(uint value) { m_reconnect_policy.SetCurrentAttemptCount(value); }
  void TestSetHasSuccessfulConnection(bool value) { m_has_successful_connection = value; }
  uint TestResolveConnectTimeout(bool reconnecting) const { return _ResolveConnectTimeoutMs(!reconnecting); }
  bool TestTryComputeArrayAppendSize(uint current_size, uint append_size, int& new_size) const {
    return _TryComputeArrayAppendSize(current_size, append_size, new_size, "test buffer");
  }
  void TestSetServerCapabilities(bool wildcard_available, bool sub_id_available, bool shared_available) {
    m_server_wildcard_available = wildcard_available;
    m_server_sub_id_available   = sub_id_available;
    m_server_shared_available   = shared_available;
  }
  void                 TestSetActiveAuthMethod(const string method) { m_active_auth_method = method; }
  ENUM_TRANSPORT_ERROR TestHandleConnectSetupFailure(ENUM_TRANSPORT_ERROR err, const string desc) {
    return _HandleConnectSetupFailure(err, desc);
  }
  CMqttContext* TestContext() { return &m_context; }
  void          TestSendConnect() { _SendConnect(); }
  void          TestOnPublishReceived(uchar& pkt[]) {
    _OnPublishReceived(pkt);
    _DrainMessageCallbacks();
  }
  void   TestDrainMessageCallbacks() { _DrainMessageCallbacks(); }
  void   TestOnSubackReceived(uchar& pkt[]) { _OnSubackReceived(pkt); }
  void   TestReplaySubscriptions() { _ReplaySubscriptions(); }
  void   TestRunRetransmissions(uint timeout_seconds = 0) { _RunRetransmissions(timeout_seconds); }
  void   TestDrainPublishQueue() { _DrainPublishQueue(); }
  void   TestRestorePersistedPublishQueue() { _RestorePersistedPublishQueue(); }
  uint   TestGetSubscriptionCount() const { return m_sub_count; }
  string TestGetSubscriptionTopic(uint idx) const { return (idx < m_sub_count) ? m_sub_topic[idx] : ""; }
  bool   TestGetSubscriptionNoLocal(uint idx) const { return (idx < m_sub_count) ? m_sub_no_local[idx] : false; }
  bool   TestGetSubscriptionRap(uint idx) const { return (idx < m_sub_count) ? m_sub_rap[idx] : false; }
  uchar  TestGetSubscriptionRh(uint idx) const { return (idx < m_sub_count) ? m_sub_rh[idx] : 0; }
  uint   TestGetSubscriptionUtf8Len(uint idx) const { return (idx < m_sub_count) ? m_sub_utf8_len[idx] : 0; }
  bool   TestIsReplayInProgress() const { return m_replay_in_progress; }
  uint   TestGetReplayCursor() const { return m_replay_next_index; }
  ulong  TestGetConnackDeadlineMs() const { return m_connack_deadline_ms; }
  uint   TestGetIncomingStorageErrorCount() const { return m_incoming_storage_error_count; }
  bool   TestHandleRedirection(int reason_code, const string server_ref) {
    return _HandleRedirection(reason_code, server_ref);
  }
  bool   TestIsRedirectPending() const { return m_redirect_pending; }
  string TestGetHost() const { return m_host; }
  uint   TestGetPort() const { return m_port; }
  void   TestSetQueuedEnqueueTimeUs(uint idx, ulong enqueue_time_us) {
    m_publish_queue.SetEnqueuedAtUs(idx, enqueue_time_us);
  }
  bool TestTopicMatchesFilter(const string topic, const string filter) { return _TopicMatchesFilter(topic, filter); }
  bool TestEvaluateTofuCertificate(bool cert_available, const string cert_thumb) {
    return _EvaluateTofuCertificate(cert_available, cert_thumb);
  }
  ENUM_MQTT_TRUST_MODE TestGetEffectiveTrustMode() const { return m_effective_trust_mode; }
  void                 TestQueueDeferredTransportPacket(const uchar& pkt[]) {
    _TryAppendDeferredTransportPacket(pkt);
  }
  uint TestGetDeferredTransportCount() const { return m_deferred_transport_count; }
  uint TestGetDeferredTransportBytes() const { return m_deferred_transport_bytes; }
#endif
};

//+------------------------------------------------------------------+
//| Constructor                                                      |
//+------------------------------------------------------------------+
CMqttClient::CMqttClient() {
  m_transport_type                       = TRANSPORT_TCP;
  m_transport                            = GetPointer(m_tcp_transport);
  m_ws_path                              = "/mqtt";
  m_host                                 = "";
  m_port                                 = 1883;
  m_use_tls                              = false;
  m_require_tls                          = false;
  m_has_successful_connection            = false;
  m_connect_timeout_ms                   = 5000;
  m_reconnect_connect_timeout_ms         = 0;
  m_active_connect_timeout_ms            = 0;
  m_blocking_transport_hard_limit_ms     = 0;
  m_connect_blocking_duration_seen_us    = 0;
  m_client_id                            = "";
  m_username                             = "";
  m_password                             = "";
  m_use_binary_password                  = false;
  m_connect_auth_method                  = "";
  m_use_connect_auth_data                = false;
  m_active_auth_method                   = "";
  m_keepalive_s                          = 10;  // 10 s default — detects dead connections within ~15 s.
                                                // Many NAT/firewall gateways kill idle connections after 30–60 s;
                                                // 10 s keep-alive is safe across all deployment scenarios.
                                                // Use SetKeepAlive(60) to revert to a conservative value if needed.
  m_clean_start                          = true;
  m_always_replay_subscriptions          = false;
  m_session_expiry                       = 0;
  m_effective_session_expiry             = 0;
  m_has_connect_request_response_info    = false;
  m_connect_request_response_info        = 0;
  m_has_connect_request_problem_info     = false;
  m_connect_request_problem_info         = 0;
  m_will_enabled                         = false;
  m_will_topic                           = "";
  m_will_qos                             = QoS_0;
  m_will_retain                          = false;
  m_will_delay_s                         = 0;
  m_will_expiry_s                        = 0;
  m_has_will_payload_format              = false;
  m_will_payload_format                  = 0;
  m_will_content_type                    = "";
  m_will_response_topic                  = "";
  m_will_user_prop_count                 = 0;
  m_default_qos                          = QoS_0;
  m_client_topic_alias_max               = 0;  // Disabled by default — must be explicitly set
  m_sub_id_counter                       = 0;
  m_shared_sub_ids                       = false;
  m_sub_count                            = 0;
  m_sub_index_capacity                   = 0;
  m_sub_index_size                       = 0;
  m_sub_index_tombstones                 = 0;
  m_queue_qos0_while_disconnected        = false;
  m_draining_queue                       = false;
  m_last_queued_publish_handoff_complete = false;
  m_msg_evt_count                        = 0;
  m_max_deferred_callback_events         = MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_EVENTS;
  m_max_deferred_callback_payload_bytes  = MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_PAYLOAD_BYTES;
  m_max_deferred_callback_property_bytes = MQTT_DEFAULT_MAX_DEFERRED_CALLBACK_PROPERTY_BYTES;
  m_pending_replay_count                 = 0;
  m_replay_in_progress                   = false;
  m_replay_next_index                    = 0;
  m_pending_unsub_count                  = 0;
  m_deferred_transport_count             = 0;
  m_state                                = MQTT_CLIENT_DISCONNECTED;
  m_connect_deadline_ms                  = 0;
  m_connack_deadline_ms                  = 0;
  m_connack_timeout_ms                   = 10000;
  m_max_retransmit_count                 = 10;
  m_retransmit_timeout_s                 = 15;  // 15 s before retransmitting unacked QoS 1/2 packets.
  m_max_packets_per_poll                 = 50;  // Cap burst processing to keep OnTimer responsive by default
  m_on_connect                           = NULL;
  m_on_disconnect                        = NULL;
  m_on_message                           = NULL;
  m_on_suback                            = NULL;
  m_on_unsuback                          = NULL;
  m_on_error                             = NULL;
  m_on_state_change                      = NULL;
  m_on_redirect                          = NULL;
  m_on_auth                              = NULL;
  m_on_auth_ex                           = NULL;
  m_on_rtt_threshold                     = NULL;
  m_on_qos_drop                          = NULL;
  m_on_publish_result                    = NULL;
  m_on_ack                               = NULL;
  m_on_packetid_low                      = NULL;
  m_packetid_low_threshold               = 0;  // 0 = disabled
  m_pubrel_retry_timeout_s               = 0;  // 0 = use m_retransmit_timeout_s
  m_rtt_threshold_us                     = 0;  // 0 = disabled
  m_connected_since_ms                   = 0;
  m_reconnect_count                      = 0;
  m_messages_sent                        = 0;
  m_messages_received                    = 0;
  m_last_ping_rtt_us                     = 0;
  m_last_retransmit_check_ms             = 0;
  m_in_poll                              = false;
  m_abort_current_poll                   = false;
  m_last_failure_code                    = 0;
  m_last_failure_description             = "";
  m_last_failure_class                   = MQTT_FAILURE_NONE;
  m_last_error_window_us                 = 0;
  m_error_count_in_window                = 0;
  m_error_suppressed_in_window           = 0;
  m_server_max_qos                       = 2;
  m_server_retain_available              = true;
  m_server_wildcard_available            = true;
  m_server_sub_id_available              = true;
  m_server_shared_available              = true;
  m_server_reference                     = "";
  m_auto_redirect                        = false;
  m_require_redirect_allowlist           = true;
  m_redirect_allow_host_count            = 0;
  m_redirect_pending                     = false;
  m_redirect_reason_code                 = 0;
  m_redirect_server_ref                  = "";
  m_tofu_enabled                         = false;
  m_tofu_fingerprint                     = "";
  m_tofu_pinned                          = false;
  m_tofu_strict                          = false;
  m_session_key                          = "";
  m_effective_trust_mode                 = MQTT_TRUST_MODE_PLAINTEXT;
  m_allow_insecure_plaintext_transport   = false;
  m_allow_insecure_plaintext_auth        = false;
#ifdef MQTT_UNIT_TESTS
  m_test_transport_injected = false;
#endif
  //--- m_context.logger is initialised to MQTT_DEFAULT_LOG_LEVEL / NULL sink
  //--- by the CLogger default constructor.
  m_pub_last_valid_topic               = "";
  //--- Incoming storage circuit breaker defaults
  m_incoming_storage_error_count       = 0;
  m_incoming_storage_error_max         = 5;
  //--- CONNECT/CONNACK user properties
  m_connect_user_prop_count            = 0;
  m_connack_session_present            = false;
  m_connack_reason_code                = 0;
  m_connack_reason_string              = "";
  m_connack_session_expiry             = 0;
  m_connack_assigned_client_identifier = "";
  m_connack_response_information       = "";
  m_connack_server_reference           = "";
  m_connack_server_keep_alive          = 0;
  m_connack_receive_maximum            = 65535;
  m_connack_auth_method                = "";
  m_connack_user_prop_count            = 0;
  m_last_ack_packet_type               = 0;
  m_last_ack_packet_id                 = 0;
  m_last_ack_reason_code               = 0;
  m_last_ack_reason_string             = "";
  m_last_ack_user_prop_count           = 0;
  _ResetAckProperties(m_puback_props);
  _ResetAckProperties(m_pubrec_props);
  _ResetAckProperties(m_pubrel_props);
  _ResetAckProperties(m_pubcomp_props);
  m_last_suback_packet_id            = 0;
  m_last_suback_reason_string        = "";
  m_last_suback_user_prop_count      = 0;
  m_last_unsuback_packet_id          = 0;
  m_last_unsuback_reason_string      = "";
  m_last_unsuback_user_prop_count    = 0;
  m_last_disconnect_reason_code      = 0;
  m_last_disconnect_reason_string    = "";
  m_last_disconnect_server_reference = "";
  m_last_disconnect_user_prop_count  = 0;
  m_last_auth_reason_code            = 0;
  m_last_auth_reason_string          = "";
  m_last_auth_method                 = "";
  m_last_auth_user_prop_count        = 0;
  //--- Default to strict fail-closed handling for MQTT 5 payload format compliance.
  m_strict_utf8_validation           = true;
  //--- Pre-size match scratch; grows by 8 on demand; 64 covers typical subscription tables
  ArrayResize(m_match_scratch, 64);
  //--- Pre-initialise 4-byte PUBACK/PUBREC/PUBREL templates (reason code = 0x00)
  ArrayResize(m_puback_tmpl, 4);
  m_puback_tmpl[0] = 0x40;
  m_puback_tmpl[1] = 0x02;
  m_puback_tmpl[2] = 0x00;
  m_puback_tmpl[3] = 0x00;
  ArrayResize(m_pubrec_tmpl, 4);
  m_pubrec_tmpl[0] = 0x50;
  m_pubrec_tmpl[1] = 0x02;
  m_pubrec_tmpl[2] = 0x00;
  m_pubrec_tmpl[3] = 0x00;
  ArrayResize(m_pubrel_tmpl, 4);
  m_pubrel_tmpl[0] = 0x62;
  m_pubrel_tmpl[1] = 0x02;
  m_pubrel_tmpl[2] = 0x00;
  m_pubrel_tmpl[3] = 0x00;
  ArrayResize(m_pubcomp_tmpl, 4);
  m_pubcomp_tmpl[0] = 0x70;
  m_pubcomp_tmpl[1] = 0x02;
  m_pubcomp_tmpl[2] = 0x00;
  m_pubcomp_tmpl[3] = 0x00;
  m_deferred_transport_bytes = 0;
  m_max_deferred_transport_packets = MQTT_DEFAULT_MAX_DEFERRED_TRANSPORT_PACKETS;
  m_max_deferred_transport_bytes = MQTT_DEFAULT_MAX_DEFERRED_TRANSPORT_BYTES;
  //--- Cap incoming packet size at 1 MB by default.
  //--- The spec maximum (268,435,455 bytes) would allow a rogue broker to
  //--- send a 268 MB packet and crash MT5. 1 MB is ample for any trading payload.
  m_reconnect_policy.Configure(true, 1000, 60000);
  m_reconnect_policy.SetMaxAttempts(MQTT_DEFAULT_MAX_RECONNECT_ATTEMPTS);
  m_tcp_transport.SetMaxPacketSize(1 * 1024 * 1024);
  m_ws_transport.SetMaxPacketSize(1 * 1024 * 1024);
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CMqttClient::~CMqttClient() {
  m_reconnect_policy.Stop();
  if (m_transport != NULL) {
    m_transport.Disconnect();
  }
  _ClearMessageCallbacks();
  _HandleConnectionClosed();
  _SecureEraseString(m_password);
  //--- SecureZeroArray is a best-effort erase. The MQL5 compiler may optimise away
  //--- writes to arrays that are about to go out of scope (dead-store elimination).
  //--- A double-write pattern (0xAA then 0x00) can make elision less likely if a
  //--- future hardening pass needs stronger best-effort scrubbing for terminal audits.
  SecureZeroArray(m_password_binary);
}

//+------------------------------------------------------------------+
//| _ResetAckProperties                                              |
//+------------------------------------------------------------------+
void CMqttClient::_ResetAckProperties(MqttAckProperties& props) {
  props.has_reason_string = false;
  props.reason_string     = "";
  ArrayResize(props.user_property_keys, 0);
  ArrayResize(props.user_property_vals, 0);
}

//+------------------------------------------------------------------+
//| _HasAckProperties                                                |
//+------------------------------------------------------------------+
bool CMqttClient::_HasAckProperties(const MqttAckProperties& props) const {
  return props.has_reason_string || ArraySize(props.user_property_keys) > 0;
}

//+------------------------------------------------------------------+
//| _SendPubackPacket                                                |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttClient::_SendPubackPacket(ushort packet_id, uchar reason_code) {
  if (reason_code == 0x00 && !_HasAckProperties(m_puback_props)) {
    m_puback_tmpl[2] = (uchar)(packet_id >> 8);
    m_puback_tmpl[3] = (uchar)(packet_id & 0xFF);
    return m_transport.Send(m_puback_tmpl);
  }

  CPuback puback;
  uchar   pkt[];
  puback.SetPacketId(packet_id);
  puback.SetReasonCode(reason_code);
  if (m_puback_props.has_reason_string) {
    puback.SetReasonString(m_puback_props.reason_string);
  }
  int user_prop_count = ArraySize(m_puback_props.user_property_keys);
  for (int i = 0; i < user_prop_count; i++) {
    puback.SetUserProperty(m_puback_props.user_property_keys[i], m_puback_props.user_property_vals[i]);
  }
  puback.Build(pkt);
  return m_transport.Send(pkt);
}

//+------------------------------------------------------------------+
//| _SendPubrecPacket                                                |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttClient::_SendPubrecPacket(ushort packet_id, uchar reason_code) {
  if (reason_code == 0x00 && !_HasAckProperties(m_pubrec_props)) {
    m_pubrec_tmpl[2] = (uchar)(packet_id >> 8);
    m_pubrec_tmpl[3] = (uchar)(packet_id & 0xFF);
    return m_transport.Send(m_pubrec_tmpl);
  }

  CPubrec pubrec;
  uchar   pkt[];
  pubrec.SetPacketId(packet_id);
  pubrec.SetReasonCode(reason_code);
  if (m_pubrec_props.has_reason_string) {
    pubrec.SetReasonString(m_pubrec_props.reason_string);
  }
  int user_prop_count = ArraySize(m_pubrec_props.user_property_keys);
  for (int i = 0; i < user_prop_count; i++) {
    pubrec.SetUserProperty(m_pubrec_props.user_property_keys[i], m_pubrec_props.user_property_vals[i]);
  }
  pubrec.Build(pkt);
  return m_transport.Send(pkt);
}

//+------------------------------------------------------------------+
//| _SendPubrelPacket                                                |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttClient::_SendPubrelPacket(ushort packet_id, uchar reason_code) {
  if (reason_code == 0x00 && !_HasAckProperties(m_pubrel_props)) {
    m_pubrel_tmpl[2] = (uchar)(packet_id >> 8);
    m_pubrel_tmpl[3] = (uchar)(packet_id & 0xFF);
    return m_transport.Send(m_pubrel_tmpl);
  }

  CPubrel pubrel;
  uchar   pkt[];
  pubrel.SetPacketId(packet_id);
  pubrel.SetReasonCode(reason_code);
  if (m_pubrel_props.has_reason_string) {
    pubrel.SetReasonString(m_pubrel_props.reason_string);
  }
  int user_prop_count = ArraySize(m_pubrel_props.user_property_keys);
  for (int i = 0; i < user_prop_count; i++) {
    pubrel.SetUserProperty(m_pubrel_props.user_property_keys[i], m_pubrel_props.user_property_vals[i]);
  }
  pubrel.Build(pkt);
  return m_transport.Send(pkt);
}

//+------------------------------------------------------------------+
//| _SendPubcompPacket                                               |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttClient::_SendPubcompPacket(ushort packet_id, uchar reason_code) {
  //--- Fast path: reason code 0x00 with no properties — send pre-built 4-byte template
  //--- without allocating a CPubcomp object or dynamic uchar[] array.
  if (reason_code == 0x00 && !_HasAckProperties(m_pubcomp_props)) {
    m_pubcomp_tmpl[2] = (uchar)(packet_id >> 8);
    m_pubcomp_tmpl[3] = (uchar)(packet_id & 0xFF);
    return m_transport.Send(m_pubcomp_tmpl);
  }
  CPubcomp pubcomp;
  uchar    pkt[];
  pubcomp.SetPacketId(packet_id);
  pubcomp.SetReasonCode(reason_code);
  if (m_pubcomp_props.has_reason_string) {
    pubcomp.SetReasonString(m_pubcomp_props.reason_string);
  }
  int user_prop_count = ArraySize(m_pubcomp_props.user_property_keys);
  for (int i = 0; i < user_prop_count; i++) {
    pubcomp.SetUserProperty(m_pubcomp_props.user_property_keys[i], m_pubcomp_props.user_property_vals[i]);
  }
  pubcomp.Build(pkt);
  return m_transport.Send(pkt);
}

//+------------------------------------------------------------------+
//| _SetState — transition state with callback notification          |
//+------------------------------------------------------------------+
void CMqttClient::_SetState(ENUM_MQTT_CLIENT_STATE new_state) {
  if (m_state == new_state) {
    return;
  }
  ENUM_MQTT_CLIENT_STATE old_state = m_state;
  m_state                          = new_state;
  if (m_on_state_change != NULL) {
    m_on_state_change(old_state, new_state);
    _SyncLogger();  // ARCH-003: restore this instance's log config after callback
  }
}

//+------------------------------------------------------------------+
//| _FireError — invoke error callback if registered                 |
//| Rate-limited to avoid flooding the application callback          |
//| during bursts of malformed packets.                              |
//+------------------------------------------------------------------+
void CMqttClient::_FireError(int code, const string desc) {
  _RememberFailure(code, desc);
  MQTT_LOG_ERROR(desc + " (code=" + (string)code + ")");
  if (m_on_error != NULL) {
    //--- Critical error codes always bypass rate limiting.
    //--- Protocol violations and transport failures must never be suppressed.
    bool is_critical =
      (code == MQTT_REASON_CODE_PROTOCOL_ERROR || code == MQTT_REASON_CODE_PACKET_TOO_LARGE
       || code == MQTT_REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED || code == MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR
       || code <= (int)TRANSPORT_ERROR_TIMEOUT);
    if (!is_critical) {
      //--- Rate limit non-critical errors: max 10 callbacks per second
      ulong now_us = GetMicrosecondCount();
      if (now_us - m_last_error_window_us >= 1000000) {
        if (m_error_suppressed_in_window > 0) {
          MQTT_LOG_WARN((string)m_error_suppressed_in_window
                        + " non-critical errors suppressed in the previous 1-second window");
        }
        m_last_error_window_us       = now_us;
        m_error_count_in_window      = 0;
        m_error_suppressed_in_window = 0;
      }
      if (m_error_count_in_window >= 10) {
        m_error_suppressed_in_window++;
        return;  // Rate limited
      }
      m_error_count_in_window++;
    }
    MqttErrorContext ctx;
    ctx.error_code    = code;
    ctx.description   = desc;
    ctx.source_file   = "";
    ctx.source_line   = 0;
    ctx.function_name = "";
    m_on_error(ctx);
    _SyncLogger();  // ARCH-003: restore this instance's log config after callback
  }
}
//+------------------------------------------------------------------+
//| _SecureEraseString — best-effort sensitive string scrubbing      |
//+------------------------------------------------------------------+
void CMqttClient::_SecureEraseString(string& value) {
  int len = StringLen(value);
  if (len <= 0) {
    value = "";
    return;
  }
  uchar scrub[];
  uchar zeros[];
  ArrayResize(scrub, len);
  ArrayResize(zeros, len);
  ArrayInitialize(scrub, 0xAA);
  ArrayInitialize(zeros, 0);
  value = CharArrayToString(scrub, 0, len, CP_UTF8);
  value = CharArrayToString(zeros, 0, len, CP_UTF8);
  value = "";
  ArrayFree(scrub);
  ArrayFree(zeros);
}

//+------------------------------------------------------------------+
//| _HandleConnectSetupFailure                                       |
//| Purpose: Route immediate connect setup failures through the      |
//|          normal transport-error path so auto-reconnect state is  |
//|          preserved consistently across TCP and WebSocket paths.  |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttClient::_HandleConnectSetupFailure(ENUM_TRANSPORT_ERROR err, const string desc) {
  MQTT_LOG_ERROR(desc + " (err=" + (string)(int)err + ")");
  _OnTransportError((int)err, desc);
  return err;
}

//+------------------------------------------------------------------+
//| _EnforceBlockingTransportHardLimit                               |
//| Purpose: Abort connection setup when a blocking transport phase  |
//|          exceeds the caller-configured fail-closed threshold.    |
//+------------------------------------------------------------------+
bool CMqttClient::_EnforceBlockingTransportHardLimit() {
  if (m_blocking_transport_hard_limit_ms == 0) {
    return false;
  }

  ulong elapsed_us = m_transport.GetLastBlockingOperationDuration_us();
  if (elapsed_us == 0 || elapsed_us == m_connect_blocking_duration_seen_us) {
    return false;
  }

  m_connect_blocking_duration_seen_us = elapsed_us;

  ulong threshold_us                  = (ulong)m_blocking_transport_hard_limit_ms * 1000UL;
  if (elapsed_us <= threshold_us) {
    return false;
  }

  string                       phase_desc = m_use_tls ? "TLS handshake" : "blocking transport phase";
  ENUM_TRANSPORT_CONNECT_PHASE phase      = m_transport.GetConnectPhase();
  if (phase == TRANSPORT_PHASE_WS_SENDING_REQUEST || phase == TRANSPORT_PHASE_WS_WAITING_HEADERS
      || m_transport_type == TRANSPORT_WS) {
    phase_desc = m_use_tls ? "WSS connect phase" : "WebSocket connect phase";
  }

  string msg = phase_desc + " exceeded configured hard limit of " + (string)m_blocking_transport_hard_limit_ms
             + " ms (measured " + (string)(elapsed_us / 1000UL) + " ms)";
  MQTT_LOG_ERROR(msg + ".");
  _OnTransportError(TRANSPORT_ERROR_TIMEOUT, msg);
  return true;
}

//+------------------------------------------------------------------+
//| _TryComputeArrayAppendSize                                       |
//| Guard flat-buffer growth against 32-bit wrap/cast overflow.      |
//+------------------------------------------------------------------+
bool CMqttClient::_TryComputeArrayAppendSize(uint current_size, uint append_size, int& new_size,
                                             const string buffer_name) const {
  const uint max_array_size = 2147483647u;
  if (current_size > max_array_size || append_size > max_array_size || current_size > max_array_size - append_size) {
    MQTT_LOG_ERROR(buffer_name + " exceeds 32-bit array capacity.");
    return false;
  }

  new_size = (int)(current_size + append_size);
  return true;
}

//+------------------------------------------------------------------+
//| _HandleBacklogOverflow                                           |
//| Convert local backlog exhaustion into a fail-closed disconnect   |
//| with a clear implementation-specific error reason.               |
//+------------------------------------------------------------------+
void CMqttClient::_HandleBacklogOverflow(const string desc) {
  if (m_abort_current_poll) {
    return;
  }
  _ProtocolDisconnect(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, desc);
}

//+------------------------------------------------------------------+
//| _RemainingExpirySecondsFromDeadlineUs                            |
//| Convert a monotonic microsecond deadline into MQTT whole seconds |
//| while preserving sub-second remainder as 1 second.               |
//+------------------------------------------------------------------+
uint CMqttClient::_RemainingExpirySecondsFromDeadlineUs(ulong expiry_deadline_us, ulong now_us) const {
  if (expiry_deadline_us == 0 || expiry_deadline_us <= now_us) {
    return 0;
  }

  ulong remaining_us = expiry_deadline_us - now_us;
  ulong remaining_s  = (remaining_us + 999999ULL) / 1000000ULL;
  if (remaining_s > 4294967295ULL) {
    return 4294967295u;
  }
  return (uint)remaining_s;
}

//+------------------------------------------------------------------+
//| RemainingExpirySecondsFromDeadlineUs                             |
//| Adapter for helper-owned queue draining.                         |
//+------------------------------------------------------------------+
uint CMqttClient::RemainingExpirySecondsFromDeadlineUs(ulong expiry_deadline_us, ulong now_us) const {
  return _RemainingExpirySecondsFromDeadlineUs(expiry_deadline_us, now_us);
}

//+------------------------------------------------------------------+
//| _DescribeConnectTimeout                                          |
//| Build a phase-specific timeout description for connect failures  |
//+------------------------------------------------------------------+
string CMqttClient::_DescribeConnectTimeout() const {
  string                       desc  = "Transport setup timeout — TCP/TLS/WebSocket connect did not complete in time";
  ENUM_TRANSPORT_CONNECT_PHASE phase = m_transport.GetConnectPhase();

  if (m_transport_type == TRANSPORT_WS && m_use_tls && m_port == 443 && phase == TRANSPORT_PHASE_TLS_HANDSHAKING) {
    return desc
         + " (SocketTlsHandshake remained pending on implicit WSS port 443 before the HTTP upgrade reached the broker)";
  }

  if (phase == TRANSPORT_PHASE_WS_SENDING_REQUEST) {
    return desc + " (WebSocket upgrade request did not finish sending)";
  }

  if (phase == TRANSPORT_PHASE_WS_WAITING_HEADERS) {
    return desc + " (WebSocket upgrade response headers were not received)";
  }

  if (phase == TRANSPORT_PHASE_TLS_HANDSHAKING) {
    return desc + " (TLS handshake did not complete)";
  }

  if (phase == TRANSPORT_PHASE_TCP_CONNECTING) {
    return desc + " (TCP connect did not complete)";
  }

  return desc;
}

//+------------------------------------------------------------------+
//| _AppendPacketCopy - Deep-copy packet bytes into a buffer array   |
//+------------------------------------------------------------------+
void CMqttClient::_AppendPacketCopy(PacketBuffer& dest[], uint& dest_count, const uchar& src[]) {
  ArrayResize(dest, dest_count + 1);
  int len = ArraySize(src);
  ArrayResize(dest[dest_count].data, len);
  if (len > 0) {
    ArrayCopy(dest[dest_count].data, src);
  }
  dest_count++;
}

//+------------------------------------------------------------------+
//| _TryAppendDeferredTransportPacket                                |
//| Preflight deferred transport growth before mutating backlog      |
//| state so per-Poll clipping cannot grow without bounds.           |
//+------------------------------------------------------------------+
bool CMqttClient::_TryAppendDeferredTransportPacket(const uchar& pkt[]) {
  uint pkt_bytes = (uint)ArraySize(pkt);

  if (m_max_deferred_transport_packets > 0 && m_deferred_transport_count >= m_max_deferred_transport_packets) {
    _HandleBacklogOverflow("Deferred transport backlog packet limit reached while clipping a Poll() burst");
    return false;
  }

  int new_backlog_bytes = 0;
  if (!_TryComputeArrayAppendSize(m_deferred_transport_bytes, pkt_bytes, new_backlog_bytes,
                                  "Deferred transport backlog bytes")) {
    _HandleBacklogOverflow("Deferred transport backlog exceeded local byte capacity");
    return false;
  }
  if (m_max_deferred_transport_bytes > 0 && (uint)new_backlog_bytes > m_max_deferred_transport_bytes) {
    _HandleBacklogOverflow("Deferred transport backlog byte limit reached while clipping a Poll() burst");
    return false;
  }

  _AppendPacketCopy(m_deferred_transport_pkts, m_deferred_transport_count, pkt);
  m_deferred_transport_bytes = (uint)new_backlog_bytes;
  return true;
}

//+------------------------------------------------------------------+
//| _AppendDeferredPackets - Queue extracted packets for next Poll   |
//| when the per-call packet budget clips a busy burst after the     |
//| transport already framed those packets. Preserves order without  |
//| dropping data or forcing the transport to re-read the socket.    |
//+------------------------------------------------------------------+
void CMqttClient::_AppendDeferredPackets(PacketBuffer& src[], uint start_idx, uint count) {
  for (uint i = 0; i < count; i++) {
    if (!_TryAppendDeferredTransportPacket(src[start_idx + i].data)) {
      break;
    }
  }
}

//+------------------------------------------------------------------+
//| _TakeDeferredPackets - Drain deferred packets into out[] first   |
//| so packet ordering stays deterministic across timer ticks when   |
//| m_max_packets_per_poll is limiting one busy connection.          |
//+------------------------------------------------------------------+
void CMqttClient::_TakeDeferredPackets(PacketBuffer& out[], uint max_count, uint& out_count) {
  out_count = 0;
  uint take = m_deferred_transport_count;
  uint taken_bytes = 0;
  if (max_count > 0 && take > max_count) {
    take = max_count;
  }

  for (uint i = 0; i < take; i++) {
    taken_bytes += (uint)ArraySize(m_deferred_transport_pkts[i].data);
    _AppendPacketCopy(out, out_count, m_deferred_transport_pkts[i].data);
  }

  uint remaining = m_deferred_transport_count - take;
  if (remaining > 0) {
    for (uint i = 0; i < remaining; i++) {
      int len = ArraySize(m_deferred_transport_pkts[take + i].data);
      ArrayResize(m_deferred_transport_pkts[i].data, len);
      if (len > 0) {
        ArrayCopy(m_deferred_transport_pkts[i].data, m_deferred_transport_pkts[take + i].data);
      }
    }
  }

  for (uint i = remaining; i < m_deferred_transport_count; i++) {
    ArrayFree(m_deferred_transport_pkts[i].data);
  }

  m_deferred_transport_count = remaining;
  m_deferred_transport_bytes = (taken_bytes > m_deferred_transport_bytes) ? 0 : (m_deferred_transport_bytes - taken_bytes);
  ArrayResize(m_deferred_transport_pkts, m_deferred_transport_count);
}

//+------------------------------------------------------------------+
//| _ClearDeferredTransportPackets                                   |
//+------------------------------------------------------------------+
void CMqttClient::_ClearDeferredTransportPackets() {
  for (uint i = 0; i < m_deferred_transport_count; i++) {
    ArrayFree(m_deferred_transport_pkts[i].data);
  }
  m_deferred_transport_count = 0;
  m_deferred_transport_bytes = 0;
  ArrayResize(m_deferred_transport_pkts, 0);
}

//+------------------------------------------------------------------+
//| _HasLocalSessionState                                            |
//+------------------------------------------------------------------+
bool CMqttClient::_HasLocalSessionState() const {
  return m_context.session_db.HasPendingMessages() || m_context.session_db.GetInUsePacketIdCount() > 0
      || m_sub_count > 0 || m_pending_unsub_count > 0 || m_pending_replay_count > 0 || GetQueuedMessageCount() > 0;
}

//+------------------------------------------------------------------+
//| _NextSubscriptionIdentifier                                      |
//| Purpose: Allocate the next valid Subscription Identifier value   |
//|          within the MQTT Variable Byte Integer range.            |
//+------------------------------------------------------------------+
uint CMqttClient::_NextSubscriptionIdentifier() {
  if (m_shared_sub_ids) {
    return 1;
  }

  if (m_sub_id_counter >= VARINT_MAX_FOUR_BYTES) {
    m_sub_id_counter = 0;
  }

  m_sub_id_counter++;
  if (m_sub_id_counter == 0 || m_sub_id_counter > VARINT_MAX_FOUR_BYTES) {
    m_sub_id_counter = 1;
  }
  return m_sub_id_counter;
}

//+------------------------------------------------------------------+
//| _SubIndexHash                                                    |
//| Purpose: Stable FNV-1a-style hash for the compact subscription   |
//|          topic index. The hash is terminal-independent so        |
//|          reconnect replay and tests stay deterministic.          |
//+------------------------------------------------------------------+
uint CMqttClient::_SubIndexHash(const string key) const {
  uint hash = 2166136261;
  int  len  = StringLen(key);
  for (int i = 0; i < len; i++) {
    uint ch  = (uint)StringGetCharacter(key, i);
    hash    ^= (ch & 0xFF);
    hash    *= 16777619;
    hash    ^= ((ch >> 8) & 0xFF);
    hash    *= 16777619;
  }
  return hash;
}

//+------------------------------------------------------------------+
//| _SubIndexRehash                                                  |
//| Purpose: Resize/compact the subscription topic index table.      |
//+------------------------------------------------------------------+
bool CMqttClient::_SubIndexRehash(uint min_capacity) {
  //--- Keep the table power-of-two sized so lookups can use a cheap bitmask.
  //--- Rehashing also compacts tombstones after unsubscribe-heavy churn.
  uint new_capacity = 16;
  while (new_capacity < min_capacity) {
    new_capacity <<= 1;
  }

  string old_keys[];
  uint   old_vals[];
  uchar  old_state[];
  uint   old_capacity = m_sub_index_capacity;

  ArrayCopy(old_keys, m_sub_index_keys);
  ArrayCopy(old_vals, m_sub_index_vals);
  ArrayCopy(old_state, m_sub_index_state);

  ArrayResize(m_sub_index_keys, new_capacity);
  ArrayResize(m_sub_index_vals, new_capacity);
  ArrayResize(m_sub_index_state, new_capacity);
  ArrayInitialize(m_sub_index_state, 0);
  m_sub_index_capacity   = new_capacity;
  m_sub_index_size       = 0;
  m_sub_index_tombstones = 0;

  for (uint i = 0; i < old_capacity; i++) {
    if (i < (uint)ArraySize(old_state) && old_state[i] == 1) {
      if (!_SubIndexSet(old_keys[i], old_vals[i])) {
        return false;
      }
    }
  }
  return true;
}

//+------------------------------------------------------------------+
//| _SubIndexEnsureCapacity                                          |
//| Purpose: Keep the subscription topic index below ~70% load.      |
//+------------------------------------------------------------------+
bool CMqttClient::_SubIndexEnsureCapacity(uint desired_size) {
  if (m_sub_index_capacity == 0) {
    return _SubIndexRehash(16);
  }

  //--- Count tombstones in the load calculation so probe chains stay short even
  //--- after lots of subscribe/unsubscribe churn across reconnects.
  if ((desired_size + m_sub_index_tombstones) * 10 >= m_sub_index_capacity * 7) {
    return _SubIndexRehash(m_sub_index_capacity << 1);
  }
  return true;
}

//+------------------------------------------------------------------+
//| _SubIndexLookup                                                  |
//| Purpose: Lookup a subscription topic in the compact index.       |
//+------------------------------------------------------------------+
bool CMqttClient::_SubIndexLookup(const string key, uint& out_idx) const {
  if (m_sub_index_capacity == 0) {
    return false;
  }

  uint mask = m_sub_index_capacity - 1;
  uint pos  = _SubIndexHash(key) & mask;
  for (uint probe = 0; probe < m_sub_index_capacity; probe++) {
    uchar state = m_sub_index_state[pos];
    if (state == 0) {
      return false;
    }
    if (state == 1 && m_sub_index_keys[pos] == key) {
      out_idx = m_sub_index_vals[pos];
      return true;
    }
    pos = (pos + 1) & mask;
  }
  return false;
}

//+------------------------------------------------------------------+
//| _SubIndexSet                                                     |
//| Purpose: Insert/update a topic→index mapping.                    |
//+------------------------------------------------------------------+
bool CMqttClient::_SubIndexSet(const string key, uint idx) {
  if (!_SubIndexEnsureCapacity(m_sub_index_size + 1)) {
    return false;
  }

  uint mask            = m_sub_index_capacity - 1;
  uint pos             = _SubIndexHash(key) & mask;
  uint first_tombstone = UINT_MAX;

  for (uint probe = 0; probe < m_sub_index_capacity; probe++) {
    uchar state = m_sub_index_state[pos];
    if (state == 0) {
      uint target = (first_tombstone != UINT_MAX) ? first_tombstone : pos;
      if (first_tombstone != UINT_MAX) {
        m_sub_index_tombstones--;
      }
      m_sub_index_keys[target]  = key;
      m_sub_index_vals[target]  = idx;
      m_sub_index_state[target] = 1;
      m_sub_index_size++;
      return true;
    }
    if (state == 2) {
      if (first_tombstone == UINT_MAX) {
        first_tombstone = pos;
      }
    } else if (m_sub_index_keys[pos] == key) {
      m_sub_index_vals[pos] = idx;
      return true;
    }
    pos = (pos + 1) & mask;
  }

  if (first_tombstone != UINT_MAX) {
    m_sub_index_tombstones--;
    m_sub_index_keys[first_tombstone]  = key;
    m_sub_index_vals[first_tombstone]  = idx;
    m_sub_index_state[first_tombstone] = 1;
    m_sub_index_size++;
    return true;
  }

  return false;
}

//+------------------------------------------------------------------+
//| _SubIndexRemove                                                  |
//| Purpose: Remove a topic from the compact subscription index.     |
//+------------------------------------------------------------------+
bool CMqttClient::_SubIndexRemove(const string key) {
  if (m_sub_index_capacity == 0) {
    return false;
  }

  uint mask = m_sub_index_capacity - 1;
  uint pos  = _SubIndexHash(key) & mask;
  for (uint probe = 0; probe < m_sub_index_capacity; probe++) {
    uchar state = m_sub_index_state[pos];
    if (state == 0) {
      return false;
    }
    if (state == 1 && m_sub_index_keys[pos] == key) {
      m_sub_index_state[pos] = 2;
      m_sub_index_keys[pos]  = "";
      m_sub_index_size--;
      m_sub_index_tombstones++;

      if (m_sub_index_size == 0) {
        return _SubIndexRehash(16);
      }
      if (m_sub_index_tombstones * 4 >= m_sub_index_capacity || m_sub_index_size * 5 < m_sub_index_capacity) {
        uint target_capacity = (m_sub_index_size < 8) ? 16 : (m_sub_index_size << 1);
        return _SubIndexRehash(target_capacity);
      }
      return true;
    }
    pos = (pos + 1) & mask;
  }
  return false;
}

//+------------------------------------------------------------------+
//| Configuration setters                                            |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetHost(const string host, uint port) {
  m_host           = host;
  m_port           = port;
  m_transport_type = TRANSPORT_TCP;
  m_transport      = GetPointer(m_tcp_transport);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetHostWS                                                        |
//| Purpose: Configure broker host and port for WebSocket connections|
//| Parameters: host - [IN] broker hostname or IP                    |
//|             port - [IN] port number                              |
//|             path - [IN] WebSocket path (e.g. "/mqtt")            |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetHostWS(const string host, uint port, const string path) {
  m_host           = host;
  m_port           = port;
  m_ws_path        = path;
  m_transport_type = TRANSPORT_WS;
  m_transport      = GetPointer(m_ws_transport);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetTLS                                                           |
//| Purpose: Enable or disable TLS for TCP connections               |
//| Parameters: enable - [IN] true to enable TLS                     |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetTLS(bool enable) {
  m_use_tls = enable;
  if (enable) {
    MQTT_LOG_WARN("TLS/WSS handshakes use blocking MQL5 socket APIs on the chart thread. "
                  "Use a dedicated MQTT chart or terminal for production trading workloads.");
  }
  _UpdateEffectiveTrustMode();
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetRequireTLS                                                    |
//| Purpose: Enforce TLS requirement                                 |
//| Parameters: require - [IN] true to require TLS                   |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetRequireTLS(bool require) {
  m_require_tls = require;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetClientId                                                      |
//| Purpose: Set client identifier                                   |
//| Parameters: client_id - [IN] MQTT client ID string               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetClientId(const string client_id) {
  m_client_id = client_id;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetCredentials                                                   |
//| Purpose: Set username and password for authentication            |
//| Parameters: username - [IN] username string                      |
//|             password - [IN] password string                      |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetCredentials(const string username, const string password) {
  _SecureEraseString(m_password);
  SecureZeroArray(m_password_binary);
  m_username            = username;
  m_password            = password;
  m_use_binary_password = false;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetCredentials                                                   |
//| Purpose: Set username and binary password                        |
//| Parameters: username - [IN] username string                      |
//|             password - [IN] binary password array                |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetCredentials(const string username, const uchar& password[]) {
  _SecureEraseString(m_password);
  SecureZeroArray(m_password_binary);
  m_username = username;
  m_password = "";
  ArrayResize(m_password_binary, ArraySize(password));
  if (ArraySize(password) > 0) {
    ArrayCopy(m_password_binary, password);
  }
  m_use_binary_password = true;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAuthMethod                                                    |
//| Purpose: Set CONNECT Authentication Method property              |
//| Parameters: auth_method - [IN] authentication method string      |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAuthMethod(const string auth_method) {
  m_connect_auth_method = auth_method;
  if (auth_method == "") {
    m_use_connect_auth_data = false;
    ArrayFree(m_connect_auth_data);
  }
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAuthData                                                      |
//| Purpose: Set CONNECT Authentication Data property                |
//| Parameters: auth_data - [IN] authentication data string          |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAuthData(const string auth_data) {
  uchar data[];
  int   len = StringToCharArray(auth_data, data, 0, WHOLE_ARRAY, CP_UTF8);
  if (len > 0 && data[len - 1] == 0) {
    ArrayResize(data, len - 1);
  }
  return SetAuthData(data);
}

//+------------------------------------------------------------------+
//| SetAuthData                                                      |
//| Purpose: Set CONNECT Authentication Data property                |
//| Parameters: auth_data - [IN] authentication data bytes           |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAuthData(const uchar& auth_data[]) {
  ArrayCopy(m_connect_auth_data, auth_data);
  m_use_connect_auth_data = (ArraySize(auth_data) > 0);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetConnectTimeout                                                |
//| Purpose: Set connection timeout in milliseconds                  |
//| Parameters: ms - [IN] timeout in ms                              |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetConnectTimeout(uint ms) {
  m_connect_timeout_ms = ms;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetReconnectConnectTimeout                                       |
//| Purpose: Set a dedicated transport-setup budget for reconnects   |
//| Parameters: ms - [IN] timeout in ms; 0 reuses SetConnectTimeout  |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetReconnectConnectTimeout(uint ms) {
  m_reconnect_connect_timeout_ms = ms;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| _ResolveConnectTimeoutMs                                         |
//| Purpose: Use a separate timeout budget for reconnects when set   |
//+------------------------------------------------------------------+
uint CMqttClient::_ResolveConnectTimeoutMs(bool is_manual_connect) const {
  if (m_reconnect_connect_timeout_ms > 0 && (!is_manual_connect || m_has_successful_connection)) {
    return m_reconnect_connect_timeout_ms;
  }
  return m_connect_timeout_ms;
}

//+------------------------------------------------------------------+
//| SetCleanStart                                                    |
//| Purpose: Set Clean Start flag per §3.1.2.4                       |
//| Parameters: clean - [IN] true for clean start                    |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetCleanStart(bool clean) {
  m_clean_start = clean;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAlwaysReplaySubscriptions                                     |
//| Purpose: Control whether subscriptions are replayed on every     |
//|          reconnect, even when session_present=true.              |
//| Parameters: always - [IN] true = unconditional replay            |
//|                           false = skip when session_present=true |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAlwaysReplaySubscriptions(bool always) {
  m_always_replay_subscriptions = always;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetSessionExpiry                                                 |
//| Purpose: Set Session Expiry Interval per §3.1.2.11.2             |
//| Parameters: seconds - [IN] session expiry in seconds             |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetSessionExpiry(uint seconds) {
  m_session_expiry = seconds;
  if (m_state == MQTT_CLIENT_DISCONNECTED || m_state == MQTT_CLIENT_CONNECTING
      || m_state == MQTT_CLIENT_WAITING_CONNACK) {
    m_effective_session_expiry = seconds;
  }
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetSessionEncryptionPassphrase                                   |
//| Purpose: Enable optional AES-256 at-rest encryption for the      |
//|          persistent session database using a single-pass         |
//|          SHA-256 passphrase hash as the AES-256 key plus a       |
//|          SHA-256 integrity envelope. Pass an empty string to     |
//|          disable it.                                             |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetSessionEncryptionPassphrase(const string passphrase) {
  m_context.session_db.SetEncryptionPassphrase(passphrase);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetKeepAlive                                                     |
//| Purpose: Set Keep Alive interval per §3.1.2.10                   |
//| Parameters: seconds - [IN] interval in seconds                   |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetKeepAlive(ushort seconds) {
  m_keepalive_s = seconds;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetRequestResponseInformation                                    |
//| Purpose: Set CONNECT Request Response Information flag           |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetRequestResponseInformation(bool enable) {
  m_has_connect_request_response_info = true;
  m_connect_request_response_info     = enable ? 1 : 0;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetRequestProblemInformation                                     |
//| Purpose: Set CONNECT Request Problem Information flag            |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetRequestProblemInformation(bool enable) {
  m_has_connect_request_problem_info = true;
  m_connect_request_problem_info     = enable ? 1 : 0;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWill                                                          |
//| Purpose: Set Last Will and Testament per §3.1.2.5 (String)       |
//| Parameters: topic - [IN] will topic filter                       |
//|             payload - [IN] will message string                   |
//|             qos - [IN] will QoS level                            |
//|             retain - [IN] true to retain will message            |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWill(const string topic, const string payload, uchar qos, bool retain) {
  //--- Will topics must not contain wildcards; a wildcard here causes the CONNECT builder
  //--- to produce an empty-topic packet that the broker rejects with a protocol error.
  if (StringFind(topic, "#") >= 0 || StringFind(topic, "+") >= 0) {
    MQTT_LOG_ERROR("Will topic must not contain wildcard characters (§3.1.3.2 of the MQTT 5.0 spec)");
    return GetPointer(this);  // m_will_enabled remains false
  }
  m_will_enabled = true;
  m_will_topic   = topic;
  m_will_qos     = qos;
  m_will_retain  = retain;
  int len        = StringToUTF8Bytes(payload, m_will_payload);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWillBytes                                                     |
//| Purpose: Set Last Will and Testament per §3.1.2.5 (Binary)       |
//| Parameters: topic - [IN] will topic filter                       |
//|             payload - [IN] binary payload data                   |
//|             qos - [IN] will QoS level                            |
//|             retain - [IN] true to retain will message            |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillBytes(const string topic, const uchar& payload[], uchar qos, bool retain) {
  //--- Will topics must not contain wildcards; a wildcard here causes the CONNECT builder
  //--- to produce an empty-topic packet that the broker rejects with a protocol error.
  if (StringFind(topic, "#") >= 0 || StringFind(topic, "+") >= 0) {
    MQTT_LOG_ERROR("Will topic must not contain wildcard characters (§3.1.3.2 of the MQTT 5.0 spec)");
    return GetPointer(this);  // m_will_enabled remains false
  }
  m_will_enabled = true;
  m_will_topic   = topic;
  m_will_qos     = qos;
  m_will_retain  = retain;
  ArrayResize(m_will_payload, ArraySize(payload));
  ArrayCopy(m_will_payload, payload);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWillDelay                                                     |
//| Purpose: Set Will Delay Interval per §3.1.3.2.1                  |
//| Parameters: seconds - [IN] delay in seconds                      |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillDelay(uint seconds) {
  m_will_delay_s = seconds;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWillExpiry                                                    |
//| Purpose: Set Will Expiry Interval per §3.1.3.2.2                 |
//| Parameters: seconds - [IN] interval in seconds                   |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillExpiry(uint seconds) {
  m_will_expiry_s = seconds;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWillPayloadFormat                                             |
//| Purpose: Set Will Payload Format Indicator                       |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillPayloadFormat(PAYLOAD_FORMAT_INDICATOR format) {
  m_has_will_payload_format = true;
  m_will_payload_format     = (uchar)format;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWillProperties                                                |
//| Purpose: Set additional Last Will properties                     |
//| Parameters: content_type - [IN] MIME type of will message        |
//|             response_topic - [IN] response topic for will        |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillProperties(const string content_type, const string response_topic) {
  m_will_content_type   = content_type;
  m_will_response_topic = response_topic;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWillCorrelationData                                           |
//| Purpose: Set Will Correlation Data from a string                 |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillCorrelationData(const string corr_data) {
  uchar data[];
  int   len = StringToCharArray(corr_data, data, 0, WHOLE_ARRAY, CP_UTF8);
  if (len > 0 && data[len - 1] == 0) {
    ArrayResize(data, len - 1);
  }
  return SetWillCorrelationData(data);
}

//+------------------------------------------------------------------+
//| SetWillCorrelationData                                           |
//| Purpose: Set Will Correlation Data from raw bytes                |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillCorrelationData(const uchar& corr_data[]) {
  ArrayResize(m_will_correlation_data, ArraySize(corr_data));
  if (ArraySize(corr_data) > 0) {
    ArrayCopy(m_will_correlation_data, corr_data);
  }
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetWillUserProperty                                              |
//| Purpose: Append a Will User Property                             |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetWillUserProperty(const string key, const string val) {
  uint idx = m_will_user_prop_count++;
  ArrayResize(m_will_user_prop_keys, m_will_user_prop_count, 8);
  ArrayResize(m_will_user_prop_vals, m_will_user_prop_count, 8);
  m_will_user_prop_keys[idx] = key;
  m_will_user_prop_vals[idx] = val;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetDefaultQoS                                                    |
//| Purpose: Set default QoS for publish/subscribe                   |
//| Parameters: qos - [IN] default QoS level (0, 1, or 2)            |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetDefaultQoS(uchar qos) {
  if (qos > 2) {
    MQTT_LOG_WARN("SetDefaultQoS: invalid QoS " + (string)qos + " — clamped to 2.");
    qos = 2;
  }
  m_default_qos = qos;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetClientTopicAliasMaximum                                       |
//| Purpose: Set max topic aliases we accept from server             |
//| Parameters: max - [IN] maximum alias value (0=disabled)          |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetClientTopicAliasMaximum(ushort max) {
  m_client_topic_alias_max = max;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAutoReconnect                                                 |
//| Purpose: Configure automatic reconnection strategy               |
//| Parameters: enable - [IN] true to enable auto-reconnect          |
//|             min_backoff_ms - [IN] minimum wait before retry      |
//|             max_backoff_ms - [IN] maximum base wait before       |
//|                               symmetric ±25% jitter is applied   |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAutoReconnect(bool enable, uint min_backoff_ms, uint max_backoff_ms) {
  m_reconnect_policy.Configure(enable, min_backoff_ms, max_backoff_ms);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxRetransmitCount                                            |
//| Purpose: Set max retransmit attempts for QoS 1/2                 |
//| Parameters: count - [IN] max number of retries                   |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxRetransmitCount(uint count) {
  m_max_retransmit_count = count;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetRetransmitTimeout                                             |
//| Purpose: Set timeout for QoS 1/2 retransmissions                 |
//| Parameters: seconds - [IN] timeout in seconds                    |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetRetransmitTimeout(uint seconds) {
  m_retransmit_timeout_s = seconds;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxQueuedMessages                                             |
//| Purpose: Set max size of publish queue (backpressure)            |
//| Parameters: count - [IN] max number of messages to queue         |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxQueuedMessages(uint count) {
  m_publish_queue.SetMaxMessages(count);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxQueuedPayloadBytes                                         |
//| Purpose: Cap total queued payload bytes while disconnected       |
//| Parameters: bytes - [IN] total payload-byte budget; 0 disables   |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxQueuedPayloadBytes(uint bytes) {
  m_publish_queue.SetMaxPayloadBytes(bytes);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxQueuedPropertyBytes                                        |
//| Purpose: Cap total queued encoded-property bytes while offline   |
//| Parameters: bytes - [IN] property-byte budget; 0 disables        |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxQueuedPropertyBytes(uint bytes) {
  m_publish_queue.SetMaxPropertyBytes(bytes);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxSingleQueuedPublishBytes                                   |
//| Purpose: Cap one queued publish's payload+property byte size     |
//| Parameters: bytes - [IN] per-message budget; 0 disables          |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxSingleQueuedPublishBytes(uint bytes) {
  m_publish_queue.SetMaxSingleBytes(bytes);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetQueueQoS0WhenDisconnected                                     |
//| Purpose: Control whether disconnected QoS 0 publishes are queued |
//| Parameters: enable - [IN] true to queue QoS 0 while offline      |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetQueueQoS0WhenDisconnected(bool enable) {
  m_queue_qos0_while_disconnected = enable;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxPacketsPerPoll                                             |
//| Purpose: Limit packets processed per Poll() call to cap the      |
//|          worst-case event-loop hold time during burst arrivals.  |
//|          Residual packets are deferred to the next timer tick.   |
//| Parameters: n - [IN] max packets; 0 = unlimited (default)        |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxPacketsPerPoll(uint n) {
  m_max_packets_per_poll = n;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxDeferredTransportPackets                                   |
//| Purpose: Cap deferred transport packets buffered across Poll()   |
//|          calls when the per-call packet budget clips a burst.    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxDeferredTransportPackets(uint count) {
  m_max_deferred_transport_packets = count;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxDeferredTransportBytes                                     |
//| Purpose: Cap deferred transport bytes buffered across Poll()     |
//|          calls when the per-call packet budget clips a burst.    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxDeferredTransportBytes(uint bytes) {
  m_max_deferred_transport_bytes = bytes;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxDeferredCallbackEvents                                     |
//| Purpose: Cap deferred callback events buffered across Poll()     |
//|          calls while protocol work continues.                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxDeferredCallbackEvents(uint count) {
  m_max_deferred_callback_events = count;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxDeferredCallbackPayloadBytes                               |
//| Purpose: Cap deferred callback payload bytes retained across     |
//|          Poll() calls while callbacks are pending.               |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxDeferredCallbackPayloadBytes(uint bytes) {
  m_max_deferred_callback_payload_bytes = bytes;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxDeferredCallbackPropertyBytes                              |
//| Purpose: Cap deferred callback property bytes retained across    |
//|          Poll() calls while callbacks are pending.               |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxDeferredCallbackPropertyBytes(uint bytes) {
  m_max_deferred_callback_property_bytes = bytes;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetConnackTimeout                                                |
//| Purpose: Set max time to wait for CONNACK after CONNECT          |
//| Parameters: ms - [IN] timeout in milliseconds                    |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetConnackTimeout(uint ms) {
  m_connack_timeout_ms = ms;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxReconnectAttempts — circuit breaker configuration          |
//| count=0 disables the circuit breaker (unlimited retries).        |
//| The constructor default is MQTT_DEFAULT_MAX_RECONNECT_ATTEMPTS.  |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxReconnectAttempts(uint count) {
  m_reconnect_policy.SetMaxAttempts(count);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetMaxIncomingPacketSize                                         |
//| Purpose: Override the 1 MB default cap on incoming packet memory.|
//|         Use a lower value (e.g., 64 KB) for constrained EAs, or  |
//|         higher for large payloads. Must be called before Connect.|
//| Parameters: bytes - [IN] maximum number of bytes for one packet  |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetMaxIncomingPacketSize(uint bytes) {
  m_tcp_transport.SetMaxPacketSize(bytes);
  m_ws_transport.SetMaxPacketSize(bytes);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetPingRespTimeout                                               |
//| Purpose: Set independent PINGRESP deadline, decoupled from the   |
//|          keep-alive interval. Allows tighter dead-connection     |
//|          detection without shortening the keep-alive period.     |
//| Parameters: seconds - [IN] seconds to wait (0 = use keep-alive)  |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetPingRespTimeout(uint seconds) {
  m_tcp_transport.SetPingRespTimeout(seconds);
  m_ws_transport.SetPingRespTimeout(seconds);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetBlockingTransportWarnThreshold                                |
//| Purpose: Warn when blocking TLS/WSS connect phases exceed the    |
//|          configured duration. Set 0 to disable warnings.         |
//| Parameters: ms - [IN] warning threshold in milliseconds          |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetBlockingTransportWarnThreshold(uint ms) {
  m_tcp_transport.SetBlockingOperationWarnThreshold(ms);
  m_ws_transport.SetBlockingOperationWarnThreshold(ms);
  if (m_transport != NULL) {
    m_transport.SetBlockingOperationWarnThreshold(ms);
  }
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetBlockingTransportHardLimit                                    |
//| Purpose: Abort connection setup when a blocking TLS/WSS connect  |
//|          phase exceeds the configured duration. 0 disables.      |
//| Parameters: ms - [IN] hard limit in milliseconds                 |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetBlockingTransportHardLimit(uint ms) {
  m_blocking_transport_hard_limit_ms = ms;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnConnect                                                     |
//| Purpose: Set callback for successful connection                  |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnConnect(MqttOnConnectCallback cb) {
  m_on_connect = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnDisconnect                                                  |
//| Purpose: Set callback for disconnect diagnostics                 |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnDisconnect(MqttOnDisconnectCallback cb) {
  m_on_disconnect = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnMessage                                                     |
//| Purpose: Set global callback for incoming PUBLISH packets        |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnMessage(MqttOnMessageCallback cb) {
  m_on_message = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnSubscribeAck                                                |
//| Purpose: Set callback for SUBACK diagnostics                     |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnSubscribeAck(MqttOnSubscribeAckCallback cb) {
  m_on_suback = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnUnsubscribeAck                                              |
//| Purpose: Set callback for UNSUBACK diagnostics                   |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnUnsubscribeAck(MqttOnUnsubscribeAckCallback cb) {
  m_on_unsuback = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnError                                                       |
//| Purpose: Set callback for general MQTT/Transport errors          |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnError(MqttOnErrorExCallback cb) {
  m_on_error = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnStateChange                                                 |
//| Purpose: Set callback for client state transitions               |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnStateChange(MqttOnStateChangeCallback cb) {
  m_on_state_change = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnServerRedirect                                              |
//| Purpose: Set callback for broker redirection (§3.2.2.3.8)        |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnServerRedirect(MqttOnServerRedirectCallback cb) {
  m_on_redirect = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnAuth                                                        |
//| Purpose: Set callback for AUTH packet reception (§3.15)          |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnAuth(MqttOnAuthCallback cb) {
  m_on_auth = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnAuthEx                                                      |
//| Purpose: Set extended callback for AUTH diagnostics (§3.15)      |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnAuthEx(MqttOnAuthExCallback cb) {
  m_on_auth_ex = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnRttThreshold                                                |
//| Purpose: Set callback for heartbeat latency threshold exceeded   |
//| Parameters: cb - [IN] function pointer to callback               |
//|             threshold_us - [IN] threshold in microseconds        |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnRttThreshold(MqttOnRttThresholdCallback cb, ulong threshold_us) {
  m_on_rtt_threshold = cb;
  m_rtt_threshold_us = threshold_us;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnQoSDrop                                                     |
//| Purpose: Set callback for QoS 1/2 messages dropped after retry   |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnQoSDrop(MqttOnQoSDropCallback cb) {
  m_on_qos_drop = cb;
  return GetPointer(this);
}
//+------------------------------------------------------------------+
//| SetOnPublishResult                                               |
//| Purpose: Set callback for publish acknowledgment result          |
//| Parameters: cb - [IN] function pointer to callback               |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnPublishResult(MqttOnPublishResultCallback cb) {
  m_on_publish_result = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnAck                                                         |
//| Purpose: Set callback for parsed PUBACK/PUBREC/PUBREL/PUBCOMP   |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnAck(MqttOnAckCallback cb) {
  m_on_ack = cb;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAckReasonString                                               |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAckReasonString(uchar packet_type, const string reason_string) {
  switch (packet_type) {
    case PUBACK:
      m_puback_props.has_reason_string = true;
      m_puback_props.reason_string     = reason_string;
      break;
    case PUBREC:
      m_pubrec_props.has_reason_string = true;
      m_pubrec_props.reason_string     = reason_string;
      break;
    case PUBREL:
      m_pubrel_props.has_reason_string = true;
      m_pubrel_props.reason_string     = reason_string;
      break;
    case PUBCOMP:
      m_pubcomp_props.has_reason_string = true;
      m_pubcomp_props.reason_string     = reason_string;
      break;
    default:
      MQTT_LOG_ERROR("Ack diagnostics only support PUBACK/PUBREC/PUBREL/PUBCOMP packet types");
      break;
  }
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| AddAckUserProperty                                               |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::AddAckUserProperty(uchar packet_type, const string key, const string val) {
  switch (packet_type) {
    case PUBACK:
      ArrayResize(m_puback_props.user_property_keys, ArraySize(m_puback_props.user_property_keys) + 1);
      ArrayResize(m_puback_props.user_property_vals, ArraySize(m_puback_props.user_property_vals) + 1);
      m_puback_props.user_property_keys[ArraySize(m_puback_props.user_property_keys) - 1] = key;
      m_puback_props.user_property_vals[ArraySize(m_puback_props.user_property_vals) - 1] = val;
      break;
    case PUBREC:
      ArrayResize(m_pubrec_props.user_property_keys, ArraySize(m_pubrec_props.user_property_keys) + 1);
      ArrayResize(m_pubrec_props.user_property_vals, ArraySize(m_pubrec_props.user_property_vals) + 1);
      m_pubrec_props.user_property_keys[ArraySize(m_pubrec_props.user_property_keys) - 1] = key;
      m_pubrec_props.user_property_vals[ArraySize(m_pubrec_props.user_property_vals) - 1] = val;
      break;
    case PUBREL:
      ArrayResize(m_pubrel_props.user_property_keys, ArraySize(m_pubrel_props.user_property_keys) + 1);
      ArrayResize(m_pubrel_props.user_property_vals, ArraySize(m_pubrel_props.user_property_vals) + 1);
      m_pubrel_props.user_property_keys[ArraySize(m_pubrel_props.user_property_keys) - 1] = key;
      m_pubrel_props.user_property_vals[ArraySize(m_pubrel_props.user_property_vals) - 1] = val;
      break;
    case PUBCOMP:
      ArrayResize(m_pubcomp_props.user_property_keys, ArraySize(m_pubcomp_props.user_property_keys) + 1);
      ArrayResize(m_pubcomp_props.user_property_vals, ArraySize(m_pubcomp_props.user_property_vals) + 1);
      m_pubcomp_props.user_property_keys[ArraySize(m_pubcomp_props.user_property_keys) - 1] = key;
      m_pubcomp_props.user_property_vals[ArraySize(m_pubcomp_props.user_property_vals) - 1] = val;
      break;
    default:
      MQTT_LOG_ERROR("Ack diagnostics only support PUBACK/PUBREC/PUBREL/PUBCOMP packet types");
      break;
  }
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| ClearAckProperties                                               |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::ClearAckProperties(uchar packet_type) {
  switch (packet_type) {
    case PUBACK:
      _ResetAckProperties(m_puback_props);
      break;
    case PUBREC:
      _ResetAckProperties(m_pubrec_props);
      break;
    case PUBREL:
      _ResetAckProperties(m_pubrel_props);
      break;
    case PUBCOMP:
      _ResetAckProperties(m_pubcomp_props);
      break;
    default:
      MQTT_LOG_ERROR("Ack diagnostics only support PUBACK/PUBREC/PUBREL/PUBCOMP packet types");
      break;
  }
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetOnPacketIdLow                                                 |
//| Purpose: Set callback for low packet ID pool monitoring          |
//| Parameters: cb - [IN] function pointer to callback               |
//|             threshold - [IN] low-water-mark threshold            |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetOnPacketIdLow(MqttOnPacketIdLowCallback cb, uint threshold) {
  m_on_packetid_low        = cb;
  m_packetid_low_threshold = threshold;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetPubrelRetryTimeout                                            |
//| Purpose: Set independent retry timeout for QoS 2 PUBREL          |
//| Parameters: seconds - [IN] timeout in seconds                    |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetPubrelRetryTimeout(uint seconds) {
  m_pubrel_retry_timeout_s = seconds;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAutoRedirect                                                  |
//| Purpose: Set automatic server redirection policy                 |
//| Parameters: enable - [IN] true to follow Server Reference        |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAutoRedirect(bool enable) {
  m_auto_redirect = enable;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetRequireRedirectAllowlist                                      |
//| Purpose: Compatibility toggle retained for existing callers.     |
//|          Redirect auto-follow is now always gated by the         |
//|          explicit allowlist to fail closed.                      |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetRequireRedirectAllowlist(bool require_match) {
  if (!require_match) {
    MQTT_LOG_WARN("Redirect allowlist enforcement remains mandatory. "
                  "SetRequireRedirectAllowlist(false) is ignored.");
  }
  m_require_redirect_allowlist = true;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| AddRedirectAllowHost                                             |
//| Purpose: Add one exact-match hostname to the redirect allowlist  |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::AddRedirectAllowHost(const string host) {
  string normalized = host;
  StringToLower(normalized);
  if (normalized == "") {
    return GetPointer(this);
  }

  for (uint i = 0; i < m_redirect_allow_host_count; i++) {
    if (m_redirect_allow_hosts[i] == normalized) {
      return GetPointer(this);
    }
  }

  ArrayResize(m_redirect_allow_hosts, (int)(m_redirect_allow_host_count + 1));
  m_redirect_allow_hosts[m_redirect_allow_host_count] = normalized;
  m_redirect_allow_host_count++;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| ClearRedirectAllowHosts                                          |
//| Purpose: Remove all configured redirect allowlist hosts          |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::ClearRedirectAllowHosts() {
  ArrayFree(m_redirect_allow_hosts);
  m_redirect_allow_host_count = 0;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetSharedSubscriptionIds                                         |
//| When enabled, all subscriptions share sub_id=1 so                |
//| _ReplaySubscriptions can send all topics in a single SUBSCRIBE   |
//| packet instead of one packet per unique sub_id.                  |
//| Default: false (per-topic IDs for EAs that dispatch on sub_id).  |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetSharedSubscriptionIds(bool shared) {
  if (m_sub_count > 0 && shared != m_shared_sub_ids) {
    MQTT_LOG_WARN("SetSharedSubscriptionIds called after subscriptions added — existing subscription IDs unchanged; "
                  "call before Subscribe()");
  }
  m_shared_sub_ids = shared;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetTofuPinning                                                   |
//| Purpose: Enable/disable certificate pinning against a            |
//|          provisioned or persisted MT5 thumbprint                 |
//| Parameters: enable - [IN] true to enable the TOFU pinning policy |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetTofuPinning(bool enable) {
  m_tofu_enabled = enable;
  if (!enable) {
    m_tofu_fingerprint = "";
    m_tofu_pinned      = false;
  }
  _UpdateEffectiveTrustMode();
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetTofuThumbprint                                                |
//| Purpose: Pre-provision the MT5 certificate thumbprint used for   |
//|          TOFU verification. The platform exposes SHA-1           |
//|          thumbprints; separators and case are normalized away.   |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetTofuThumbprint(const string thumbprint) {
  if (thumbprint == "") {
    m_tofu_fingerprint = "";
    m_tofu_pinned      = false;
    _UpdateEffectiveTrustMode();
    return GetPointer(this);
  }

  string normalized = "";
  if (!_NormalizeCertificateThumbprint(thumbprint, normalized)) {
    MQTT_LOG_ERROR("Invalid TOFU thumbprint. Expected an MT5 SHA-1 certificate thumbprint as 40 hex characters "
                   "(separators ':' or '-' are allowed and will be normalized).");
    m_tofu_fingerprint = "";
    m_tofu_pinned      = false;
    _UpdateEffectiveTrustMode();
    return GetPointer(this);
  }

  m_tofu_fingerprint = normalized;
  m_tofu_pinned      = true;
  _UpdateEffectiveTrustMode();
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetTofuStrictMode                                                |
//| Purpose: Make thumbprint verification failure connection-fatal   |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetTofuStrictMode(bool strict) {
  m_tofu_strict = strict;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAllowInsecurePlaintextAuth                                    |
//| Purpose: Explicit opt-in for sending auth over plaintext         |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAllowInsecurePlaintextTransport(bool allow) {
  m_allow_insecure_plaintext_transport = allow;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAllowInsecurePlaintextAuth                                    |
//| Purpose: Explicit opt-in for sending auth over plaintext         |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAllowInsecurePlaintextAuth(bool allow) {
  m_allow_insecure_plaintext_auth = allow;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetAllowMaskedServerFrames                                       |
//| Purpose: Compatibility opt-in for non-compliant masked WS frames |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetAllowMaskedServerFrames(bool allow) {
  m_ws_transport.SetAllowMaskedServerFrames(allow);
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetLogLevel                                                      |
//| Purpose: Set per-instance logging verbosity                      |
//| Parameters: level - [IN] log level enum                          |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetLogLevel(ENUM_MQTT_LOG_LEVEL level) {
  m_context.logger.m_log_level = (int)level;
  _SyncLogger();
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetLogSink                                                       |
//| Purpose: Set per-instance log output callback                    |
//| Parameters: sink - [IN] function pointer to log sink             |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetLogSink(MqttLogSinkCallback sink) {
  m_context.logger.m_log_sink = sink;
  _SyncLogger();
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetConnectUserProperty                                           |
//| Purpose: Append a User Property to every CONNECT packet          |
//| Parameters: key - [IN] property name                             |
//|             val - [IN] property value                            |
//| Return: Pointer to this instance for chaining                    |
//| Note:  Call before Connect(). Duplicate keys are allowed per     |
//|        §3.1.2.11.7 — the broker receives both entries.           |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetConnectUserProperty(const string key, const string val) {
  uint idx = m_connect_user_prop_count++;
  ArrayResize(m_connect_user_prop_keys, m_connect_user_prop_count, 8);
  ArrayResize(m_connect_user_prop_vals, m_connect_user_prop_count, 8);
  m_connect_user_prop_keys[idx] = key;
  m_connect_user_prop_vals[idx] = val;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetIncomingStorageErrorMax                                       |
//| Purpose: Configure the incoming-storage circuit-breaker limit    |
//| Parameters: max - [IN] consecutive failures before circuit trips |
//|                        (0 = disabled, infinite tolerance)        |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetIncomingStorageErrorMax(uint max) {
  m_incoming_storage_error_max = max;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| SetStrictUtf8Validation                                          |
//| Purpose: Control UTF-8 validation behaviour for incoming PUBLISH |
//| Parameters: strict - [IN] true = DISCONNECT(0x99) on failure     |
//|                           false = warn only                      |
//| Return: Pointer to this instance for chaining                    |
//+------------------------------------------------------------------+
CMqttClient* CMqttClient::SetStrictUtf8Validation(bool strict) {
  m_strict_utf8_validation = strict;
  return GetPointer(this);
}

//+------------------------------------------------------------------+
//| GetConnackUserProperty                                           |
//| Purpose: Retrieve a CONNACK User Property by key                 |
//| Parameters: key - [IN] property name to look up                  |
//| Return: property value, or "" if not found                       |
//| Note: Valid after OnConnect callback fires.                      |
//+------------------------------------------------------------------+
string CMqttClient::GetConnackUserProperty(const string key) const {
  for (uint i = 0; i < m_connack_user_prop_count; i++) {
    if (m_connack_user_prop_keys[i] == key) {
      return m_connack_user_prop_vals[i];
    }
  }
  return "";
}

//+------------------------------------------------------------------+
//| GetConnackUserPropertyKey                                        |
//| Purpose: Retrieve a CONNACK User Property key by index           |
//| Return: key string at idx, or "" if out of range                 |
//+------------------------------------------------------------------+
string CMqttClient::GetConnackUserPropertyKey(uint idx) const {
  return (idx < m_connack_user_prop_count) ? m_connack_user_prop_keys[idx] : "";
}

//+------------------------------------------------------------------+
//| GetConnackUserPropertyValue                                      |
//| Purpose: Retrieve a CONNACK User Property value by index         |
//| Return: value string at idx, or "" if out of range               |
//+------------------------------------------------------------------+
string CMqttClient::GetConnackUserPropertyValue(uint idx) const {
  return (idx < m_connack_user_prop_count) ? m_connack_user_prop_vals[idx] : "";
}

//+------------------------------------------------------------------+
//| _CacheConnackMetadata                                            |
//| Purpose: Persist parsed CONNACK metadata on the facade           |
//| Note: Called immediately after parse so rejected CONNACK packets |
//|       remain inspectable to EA code.                             |
//+------------------------------------------------------------------+
void CMqttClient::_CacheConnackMetadata(CConnack& connack) {
  m_connack_session_present            = connack.IsSessionPresent();
  m_connack_reason_code                = connack.GetReasonCode();
  m_connack_reason_string              = connack.GetReasonString();
  m_connack_session_expiry             = connack.GetSessionExpiryInterval();
  m_connack_assigned_client_identifier = connack.GetAssignedClientIdentifier();
  m_connack_response_information       = connack.GetResponseInformation();
  m_connack_server_reference           = connack.GetServerReference();
  m_connack_server_keep_alive          = connack.GetServerKeepAlive();
  m_connack_receive_maximum            = connack.GetReceiveMaximum();
  m_connack_auth_method                = connack.GetAuthenticationMethod();
  connack.GetAuthenticationData(m_connack_auth_data);

  m_connack_user_prop_count = connack.GetUserPropertyCount();
  ArrayResize(m_connack_user_prop_keys, m_connack_user_prop_count);
  ArrayResize(m_connack_user_prop_vals, m_connack_user_prop_count);
  for (uint upi = 0; upi < m_connack_user_prop_count; upi++) {
    m_connack_user_prop_keys[upi] = connack.GetUserPropertyKey(upi);
    m_connack_user_prop_vals[upi] = connack.GetUserPropertyValue(upi);
  }
}

//+------------------------------------------------------------------+
//| _ClassifyConnackParseReason                                      |
//| Purpose: Map CONNACK parser failures to MQTT 5 disconnect codes |
//+------------------------------------------------------------------+
uchar _ClassifyConnackParseReason(const int err) {
  return (err == MQTT_ERROR_PROTOCOL_VIOLATION || err == MQTT_ERROR_INVALID_REASON_CODE) ?
           MQTT_REASON_CODE_PROTOCOL_ERROR :
           MQTT_REASON_CODE_MALFORMED_PACKET;
}

//+------------------------------------------------------------------+
//| _ClearLocalSessionState                                          |
//| Purpose: Drop packet IDs and stored QoS state for the local view |
//|          of the current broker session.                          |
//+------------------------------------------------------------------+
void CMqttClient::_ClearLocalSessionState() {
  m_context.session_db.ClearAllMessages();
  m_context.session_db.ResetPacketIds();
  m_context.flow_control.ResetTransientState();
}

//+------------------------------------------------------------------+
//| _SetEffectiveSessionExpiry                                       |
//| Purpose: Apply the broker-effective Session Expiry Interval to   |
//|          local persistence and reconnect behavior.               |
//+------------------------------------------------------------------+
void CMqttClient::_SetEffectiveSessionExpiry(bool has_connack_override, uint connack_session_expiry) {
  m_effective_session_expiry = has_connack_override ? connack_session_expiry : m_session_expiry;
  m_context.session_db.SetPersistence(m_effective_session_expiry > 0);
}

//+------------------------------------------------------------------+
//| _HandleConnectionClosed                                          |
//| Purpose: Flush or discard local session state based on the       |
//|          effective closing Session Expiry Interval.              |
//+------------------------------------------------------------------+
void CMqttClient::_HandleConnectionClosed(bool has_session_expiry_override, uint session_expiry_override) {
  uint closing_session_expiry = has_session_expiry_override ? session_expiry_override : m_effective_session_expiry;

  if (closing_session_expiry == 0) {
    _ClearLocalSessionState();
    m_context.session_db.SetPersistence(false);
    return;
  }

  m_context.session_db.FlushIfDirty(0);
}

//+------------------------------------------------------------------+
//| _FireErrorEx                                                     |
//| Purpose: Fire structured error with source context               |
//| Parameters: code - [IN] error code                               |
//|             desc - [IN] error description                        |
//|             src_file - [IN] source file (__FILE__)               |
//|             src_line - [IN] source line (__LINE__)               |
//|             func_name - [IN] function name (__FUNCTION__)        |
//+------------------------------------------------------------------+
void CMqttClient::_FireErrorEx(int code, const string desc, const string src_file, int src_line,
                               const string func_name) {
  _RememberFailure(code, desc);
  MQTT_LOG_ERROR(desc + " (code=" + (string)code + ")");
  if (m_on_error != NULL) {
    bool is_critical =
      (code == MQTT_REASON_CODE_PROTOCOL_ERROR || code == MQTT_REASON_CODE_PACKET_TOO_LARGE
       || code == MQTT_REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED || code == MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR
       || code <= (int)TRANSPORT_ERROR_TIMEOUT);
    if (!is_critical) {
      ulong now_us = GetMicrosecondCount();
      if (now_us - m_last_error_window_us >= 1000000) {
        if (m_error_suppressed_in_window > 0) {
          MQTT_LOG_WARN((string)m_error_suppressed_in_window
                        + " non-critical errors suppressed in the previous 1-second window");
        }
        m_last_error_window_us       = now_us;
        m_error_count_in_window      = 0;
        m_error_suppressed_in_window = 0;
      }
      if (m_error_count_in_window >= 10) {
        m_error_suppressed_in_window++;
        return;
      }
      m_error_count_in_window++;
    }
    MqttErrorContext ctx;
    ctx.error_code    = code;
    ctx.description   = desc;
    ctx.source_file   = src_file;
    ctx.source_line   = src_line;
    ctx.function_name = func_name;
    m_on_error(ctx);
    _SyncLogger();  // Restore this instance's log config after callback
  }
}
//+------------------------------------------------------------------+
//| _ParseServerReference                                            |
//| Purpose: Parse "host:port" from Server Reference (§3.2.2.3.8)    |
//| Parameters: ref - [IN] reference string                          |
//|             out_host - [OUT] parsed hostname                     |
//|             out_port - [OUT] parsed port                         |
//| Return: true if successfully parsed                              |
//+------------------------------------------------------------------+
bool CMqttClient::_ParseServerReference(const string ref, string& out_host, uint& out_port) {
  if (ref == "") {
    return false;
  }
  //--- Handle IPv6 bracket notation: [addr]:port
  if (StringLen(ref) > 0 && StringGetCharacter(ref, 0) == '[') {
    int bracket_end = StringFind(ref, "]");
    if (bracket_end > 1) {
      out_host = StringSubstr(ref, 1, bracket_end - 1);
      if (bracket_end + 1 < StringLen(ref) && StringGetCharacter(ref, bracket_end + 1) == ':') {
        out_port = (uint)StringToInteger(StringSubstr(ref, bracket_end + 2));
        if (out_port == 0 || out_port > 65535) {
          out_port = m_use_tls ? 8883 : 1883;
        }
      } else {
        out_port = m_use_tls ? 8883 : 1883;
      }
      return (out_host != "");
    }
  }
  //--- IPv4/hostname: host:port or host
  int colon_pos = StringFind(ref, ":");
  if (colon_pos > 0) {
    out_host        = StringSubstr(ref, 0, colon_pos);
    string port_str = StringSubstr(ref, colon_pos + 1);
    out_port        = (uint)StringToInteger(port_str);
    if (out_port == 0 || out_port > 65535) {
      out_port = m_use_tls ? 8883 : 1883;
    }
  } else {
    out_host = ref;
    out_port = m_use_tls ? 8883 : 1883;
  }
  //--- Validate hostname characters for [a-zA-Z0-9.-] (RFC 1123 / RFC 3696).
  //--- Prevents log-injection or file-path injection from a malicious server_reference value.
  for (int _hi = 0; _hi < StringLen(out_host); _hi++) {
    ushort _c = StringGetCharacter(out_host, _hi);
    if (!((_c >= 'a' && _c <= 'z') || (_c >= 'A' && _c <= 'Z') || (_c >= '0' && _c <= '9') || _c == '-' || _c == '.')) {
      MQTT_LOG_WARN("_ParseServerReference: hostname contains invalid character 0x" + StringFormat("%02X", _c)
                    + " — ignoring redirect (S-4)");
      out_host = "";
      return false;
    }
  }
  return (out_host != "");
}

//+------------------------------------------------------------------+
//| _IsSimpleAckReasonValid                                          |
//| Purpose: Validate fast-path ACK reason codes by packet type      |
//+------------------------------------------------------------------+
bool CMqttClient::_IsSimpleAckReasonValid(uchar packet_type, uchar reason_code) {
  switch (packet_type) {
    case PUBACK:
    case PUBREC:
      switch (reason_code) {
        case 0x00:
        case 0x10:
        case 0x80:
        case 0x83:
        case 0x87:
        case 0x90:
        case 0x91:
        case 0x97:
        case 0x99:
          return true;
      }
      return false;

    case PUBREL:
    case PUBCOMP:
      return (reason_code == 0x00 || reason_code == 0x92);
  }

  return false;
}

//+------------------------------------------------------------------+
//| _QoS1PublishRequiresExpiry                                       |
//| Purpose: Prevent unlimited QoS 1 retransmit from pinning the     |
//|          flow-control window forever on non-expiring messages.   |
//+------------------------------------------------------------------+
bool CMqttClient::_QoS1PublishRequiresExpiry(uchar qos, uint expiry_interval) const {
  return qos == QoS_1 && m_max_retransmit_count == 0 && expiry_interval == 0;
}

//+------------------------------------------------------------------+
//| _ParseSimpleAckPacket                                            |
//| Purpose: Shared varint decode for PUBACK/PUBREC/PUBREL/PUBCOMP.  |
//| Parameters: pkt      - raw packet bytes                          |
//|             pkt_size - total byte count of pkt                   |
//|             out_pktid  - [OUT] decoded packet identifier         |
//|             out_reason - [OUT] reason code (0x00 if absent)      |
//| Return: false if the packet framing or reason code is malformed. |
//+------------------------------------------------------------------+
bool CMqttClient::_ParseSimpleAckPacket(const uchar& pkt[], int pkt_size, ushort& out_pktid, uchar& out_reason) {
  if (pkt_size <= 0) {
    return false;
  }

  uint ack_idx = 1;
  uint remlen  = DecodeVariableByteInteger(pkt, ack_idx);
  if (remlen == UINT_MAX || remlen < 2 || ack_idx >= (uint)pkt_size || ack_idx + remlen != (uint)pkt_size
      || ack_idx + 1 >= (uint)pkt_size) {
    return false;
  }

  out_pktid = (ushort)((pkt[ack_idx] << 8) | pkt[ack_idx + 1]);
  if (remlen == 2) {
    out_reason = 0x00;
    return true;
  }

  if (ack_idx + 2 >= (uint)pkt_size) {
    return false;
  }

  out_reason        = pkt[ack_idx + 2];
  uchar packet_type = (pkt[0] >> 4) & 0x0F;
  if (!_IsSimpleAckReasonValid(packet_type, out_reason)) {
    return false;
  }

  if (remlen == 3) {
    return true;
  }

  uint props_idx = ack_idx + 3;
  uint props_len = DecodeVariableByteInteger(pkt, props_idx);
  if (props_len == UINT_MAX || props_idx > (uint)pkt_size || props_idx + props_len != (uint)pkt_size) {
    return false;
  }

  return true;
}

//+------------------------------------------------------------------+
//| _ReadSimpleAckDiagnostics                                        |
//| Purpose: Parse Reason String and User Properties from            |
//|          PUBACK/PUBREC/PUBREL/PUBCOMP packets.                   |
//+------------------------------------------------------------------+
bool CMqttClient::_ReadSimpleAckDiagnostics(uchar& pkt[], const string pkt_name, string& out_reason_string,
                                            string& out_user_prop_keys[], string& out_user_prop_vals[],
                                            uint& out_user_prop_count) {
  out_reason_string   = "";
  out_user_prop_count = 0;
  ArrayFree(out_user_prop_keys);
  ArrayFree(out_user_prop_vals);

  uint ack_idx = 1;
  uint remlen  = DecodeVariableByteInteger(pkt, ack_idx);
  if (remlen == UINT_MAX || remlen < 2 || ack_idx + remlen != (uint)ArraySize(pkt)) {
    return false;
  }

  if (remlen <= 3) {
    return true;
  }

  uint props_idx = ack_idx + 3;
  uint props_len = DecodeVariableByteInteger(pkt, props_idx);
  if (props_len == UINT_MAX || props_idx + props_len != (uint)ArraySize(pkt)) {
    return false;
  }

  CPropertyReader reader;
  reader.ReadProperties(pkt, props_len, props_idx, PROP_ALLOW_REASON_STRING | PROP_ALLOW_USER_PROPERTY, pkt_name);
  if (reader.HasError() || props_idx != (uint)ArraySize(pkt)) {
    return false;
  }

  if (reader.HasReasonString()) {
    out_reason_string = reader.GetReasonString();
  }
  out_user_prop_count = reader.GetUserPropertyCount();
  if (out_user_prop_count > 0) {
    ArrayResize(out_user_prop_keys, out_user_prop_count);
    ArrayResize(out_user_prop_vals, out_user_prop_count);
    for (uint i = 0; i < out_user_prop_count; i++) {
      out_user_prop_keys[i] = reader.GetUserPropertyKey(i);
      out_user_prop_vals[i] = reader.GetUserPropertyValue(i);
    }
  }

  return true;
}

//+------------------------------------------------------------------+
//| _ProtocolDisconnect                                              |
//| Purpose: Send MQTT 5 DISCONNECT before closing on protocol error |
//+------------------------------------------------------------------+
void CMqttClient::_ProtocolDisconnect(uchar reason_code, const string desc) {
  CDisconnect disc;
  disc.SetReasonCode(reason_code);
  uchar pkt[];
  disc.Build(pkt);
  m_transport.Send(pkt);
  _OnTransportError((int)reason_code, desc);
}

//+------------------------------------------------------------------+
//| _HandleRedirection                                               |
//| Purpose: Server redirection logic per §3.2.2.3.8                 |
//| Parameters: reason_code - [IN] 0x9C or 0x9D                      |
//|             server_ref - [IN] server pointer URI                 |
//| Note: Fires callback and auto-redirects if enabled.              |
//+------------------------------------------------------------------+
bool CMqttClient::_HandleRedirection(int reason_code, const string server_ref) {
  m_server_reference = server_ref;
  //--- Validate server_reference string length before use.
  //--- A malicious or misconfigured broker could send a very long string or hostname-injection
  //--- characters that corrupt logs or get forwarded to file paths.
  if (StringLen(server_ref) > 255) {
    MQTT_LOG_WARN("Server Reference exceeds 255 characters (" + (string)StringLen(server_ref)
                  + ") — ignoring auto-redirect to prevent injection (S-4)");
    return false;
  }
  MQTT_LOG_INFO("Server redirection — reason=0x" + StringFormat("%02X", reason_code) + " reference=\"" + server_ref
                + "\"" + (reason_code == MQTT_REASON_CODE_SERVER_MOVED ? " (permanent)" : " (temporary)"));

  //--- Fire the redirection callback
  if (m_on_redirect != NULL) {
    m_on_redirect(reason_code, server_ref);
    _SyncLogger();  // Restore this instance's log config after callback
  }

  //--- Auto-redirect if enabled
  if (m_auto_redirect && server_ref != "") {
    string new_host = "";
    uint   new_port = 0;
    if (_ParseServerReference(server_ref, new_host, new_port)) {
      if (!_IsRedirectHostAllowed(new_host)) {
        MQTT_LOG_WARN("Blocking auto-redirect to " + new_host + ":" + (string)new_port
                      + " — host is not in the approved redirect allowlist.");
        return false;
      }
      MQTT_LOG_INFO("Auto-redirecting to " + new_host + ":" + (string)new_port);
      //--- Update host/port for the redirect target
      m_host             = new_host;
      m_port             = new_port;
      //--- Defer the actual reconnect to end of Poll() to avoid calling Connect()
      //--- from within a CONNACK/DISCONNECT handler.
      m_redirect_pending = true;
      return true;
    } else {
      MQTT_LOG_WARN("Could not parse Server Reference: " + server_ref);
      return false;
    }
  }
  return false;
}

//+------------------------------------------------------------------+
//| _ClassifyFailure                                                 |
//| Purpose: Collapse raw error codes into operator-facing buckets   |
//+------------------------------------------------------------------+
ENUM_MQTT_FAILURE_CLASS CMqttClient::_ClassifyFailure(int code, const string desc) const {
  if (code == 0 && desc == "") {
    return MQTT_FAILURE_NONE;
  }

  if (StringFind(desc, "MQTT_FAILURE_BROKER:") == 0
      || (code == TRANSPORT_ERROR_TIMEOUT && StringFind(desc, "CONNACK timeout") >= 0)) {
    return MQTT_FAILURE_BROKER;
  }

  if (code < 0) {
    return MQTT_FAILURE_TRANSPORT;
  }

  switch (code) {
    case MQTT_REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION:
    case MQTT_REASON_CODE_BAD_USER_NAME_OR_PASSWORD:
    case MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD:
      return MQTT_FAILURE_AUTHENTICATION;

    case MQTT_REASON_CODE_NOT_AUTHORIZED:
    case MQTT_REASON_CODE_BANNED:
      return MQTT_FAILURE_AUTHORIZATION;

    case MQTT_REASON_CODE_MALFORMED_PACKET:
    case MQTT_REASON_CODE_PROTOCOL_ERROR:
    case MQTT_REASON_CODE_TOPIC_FILTER_INVALID:
    case MQTT_REASON_CODE_TOPIC_NAME_INVALID:
    case MQTT_REASON_CODE_PACKET_IDENTIFIER_IN_USE:
    case MQTT_REASON_CODE_PACKET_IDENTIFIER_NOT_FOUND:
    case MQTT_REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED:
    case MQTT_REASON_CODE_TOPIC_ALIAS_INVALID:
    case MQTT_REASON_CODE_PACKET_TOO_LARGE:
    case MQTT_REASON_CODE_PAYLOAD_FORMAT_INVALID:
      return MQTT_FAILURE_PROTOCOL;

    case MQTT_REASON_CODE_MESSAGE_RATE_TOO_HIGH:
    case MQTT_REASON_CODE_QUOTA_EXCEEDED:
    case MQTT_REASON_CODE_ADMINISTRATIVE_ACTION:
    case MQTT_REASON_CODE_RETAIN_NOT_SUPPORTED:
    case MQTT_REASON_CODE_QOS_NOT_SUPPORTED:
    case MQTT_REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED:
    case MQTT_REASON_CODE_CONNECTION_RATE_EXCEEDED:
    case MQTT_REASON_CODE_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED:
    case MQTT_REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED:
      return MQTT_FAILURE_POLICY;

    case MQTT_REASON_CODE_SERVER_UNAVAILABLE:
    case MQTT_REASON_CODE_SERVER_BUSY:
    case MQTT_REASON_CODE_SERVER_SHUTTING_DOWN:
    case MQTT_REASON_CODE_KEEP_ALIVE_TIMEOUT:
    case MQTT_REASON_CODE_SESSION_TAKEN_OVER:
    case MQTT_REASON_CODE_USE_ANOTHER_SERVER:
    case MQTT_REASON_CODE_SERVER_MOVED:
    case MQTT_REASON_CODE_MAXIMUM_CONNECT_TIME:
      return MQTT_FAILURE_BROKER;
  }

  return MQTT_FAILURE_APPLICATION;
}

//+------------------------------------------------------------------+
//| _RememberFailure                                                 |
//| Purpose: Update public last-failure telemetry                    |
//+------------------------------------------------------------------+
void CMqttClient::_RememberFailure(int code, const string desc) {
  m_last_failure_code        = code;
  m_last_failure_description = desc;
  m_last_failure_class       = _ClassifyFailure(code, desc);
}

//+------------------------------------------------------------------+
//| _IsRedirectHostAllowed                                           |
//| Purpose: Exact-match allowlist check for broker redirects        |
//+------------------------------------------------------------------+
bool CMqttClient::_IsRedirectHostAllowed(const string host) const {
  string normalized = host;
  StringToLower(normalized);
  for (uint i = 0; i < m_redirect_allow_host_count; i++) {
    if (m_redirect_allow_hosts[i] == normalized) {
      return true;
    }
  }

  return false;
}

//+------------------------------------------------------------------+
//| _HasSensitiveAuth                                                |
//| Purpose: Determine whether CONNECT will carry auth material      |
//+------------------------------------------------------------------+
bool CMqttClient::_HasSensitiveAuth() const {
  return m_username != "" || m_password != "" || ArraySize(m_password_binary) > 0 || m_connect_auth_method != ""
      || m_use_connect_auth_data;
}

//+------------------------------------------------------------------+
//| _UpdateEffectiveTrustMode                                        |
//| Purpose: Keep the externally visible trust posture current       |
//+------------------------------------------------------------------+
void CMqttClient::_UpdateEffectiveTrustMode() {
  if (!m_use_tls) {
    m_effective_trust_mode = MQTT_TRUST_MODE_PLAINTEXT;
    return;
  }

  if (!m_tofu_enabled) {
    m_effective_trust_mode = MQTT_TRUST_MODE_TLS;
    return;
  }

  m_effective_trust_mode = m_tofu_pinned ? MQTT_TRUST_MODE_TOFU_PINNED : MQTT_TRUST_MODE_TOFU_FIRST_USE;
}

//+------------------------------------------------------------------+
//| _NormalizeCertificateThumbprint                                  |
//| Purpose: Canonicalize MT5 certificate thumbprints for stable     |
//|          comparison/persistence. Accepts case-insensitive SHA-1  |
//|          hex with optional ':' or '-' separators.                |
//+------------------------------------------------------------------+
bool CMqttClient::_NormalizeCertificateThumbprint(const string raw_thumbprint, string& normalized) const {
  normalized = "";

  int len    = StringLen(raw_thumbprint);
  for (int i = 0; i < len; i++) {
    ushort c = StringGetCharacter(raw_thumbprint, i);
    if (c == ' ' || c == ':' || c == '-') {
      continue;
    }
    if (c >= 'a' && c <= 'f') {
      c = (ushort)(c - ('a' - 'A'));
    }
    if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F'))) {
      normalized = "";
      return false;
    }
    normalized += ShortToString((short)c);
  }

  if (StringLen(normalized) != 40) {
    normalized = "";
    return false;
  }

  return true;
}

//+------------------------------------------------------------------+
//| _PersistTofuFingerprint                                          |
//| Purpose: Save a normalized TOFU thumbprint for future sessions   |
//+------------------------------------------------------------------+
void CMqttClient::_PersistTofuFingerprint(const string cert_thumb) {
  if (m_session_key == "") {
    return;
  }

  string normalized_thumb = "";
  if (!_NormalizeCertificateThumbprint(cert_thumb, normalized_thumb)) {
    MQTT_LOG_WARN("TOFU: refusing to persist an invalid certificate thumbprint representation.");
    return;
  }

  string tofu_path = "mqtt_tofu_" + m_session_key + ".pin";
  int    tofu_fh   = FileOpen(tofu_path, FILE_WRITE | FILE_TXT | FILE_REWRITE);
  if (tofu_fh != INVALID_HANDLE) {
    FileWriteString(tofu_fh, normalized_thumb);
    FileClose(tofu_fh);
    MQTT_LOG_DEBUG("TOFU: thumbprint persisted to '" + tofu_path + "'");
  } else {
    MQTT_LOG_WARN("TOFU: could not persist thumbprint — FileOpen failed for '" + tofu_path + "'");
  }
}

//+------------------------------------------------------------------+
//| _EvaluateTofuCertificate                                         |
//| Purpose: Apply TOFU verify policy to the peer certificate        |
//+------------------------------------------------------------------+
bool CMqttClient::_EvaluateTofuCertificate(bool cert_available, const string cert_thumb) {
  if (!m_tofu_enabled || !m_use_tls) {
    _UpdateEffectiveTrustMode();
    return true;
  }

  if (!m_tofu_pinned) {
    m_effective_trust_mode = MQTT_TRUST_MODE_TOFU_FIRST_USE;
    string desc            = "TOFU pinning requires a pre-provisioned MT5 certificate thumbprint via "
                             "SetTofuThumbprint() or a previously persisted pin. First-use capture is disabled.";
    MQTT_LOG_ERROR(desc);
    _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, desc);
    Disconnect(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR);
    return false;
  }

  if (!cert_available) {
    m_effective_trust_mode = MQTT_TRUST_MODE_TOFU_DEGRADED;
    string desc            = "TOFU strict mode: could not inspect the peer certificate for thumbprint verification";
    if (m_tofu_strict) {
      MQTT_LOG_ERROR(desc);
      _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, desc);
      Disconnect(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR);
      return false;
    }
    MQTT_LOG_WARN("TOFU — could not read TLS certificate, trust mode downgraded to TOFU_DEGRADED.");
    return true;
  }

  string normalized_thumb = "";
  if (!_NormalizeCertificateThumbprint(cert_thumb, normalized_thumb)) {
    m_effective_trust_mode = MQTT_TRUST_MODE_TOFU_DEGRADED;
    string desc            = "TOFU strict mode: could not normalize the peer certificate thumbprint for verification";
    if (m_tofu_strict) {
      MQTT_LOG_ERROR(desc);
      _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, desc);
      Disconnect(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR);
      return false;
    }
    MQTT_LOG_WARN("TOFU — peer certificate thumbprint could not be normalized, trust mode downgraded to "
                  "TOFU_DEGRADED.");
    return true;
  }

  if (normalized_thumb != m_tofu_fingerprint) {
    MQTT_LOG_ERROR("TOFU VIOLATION — certificate thumbprint changed!");
    MQTT_LOG_ERROR("  Expected: " + m_tofu_fingerprint);
    MQTT_LOG_ERROR("  Got:      " + normalized_thumb);
    _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
               "TOFU: server certificate thumbprint changed (possible MITM)");
    Disconnect(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR);
    return false;
  }

  m_effective_trust_mode = MQTT_TRUST_MODE_TOFU_PINNED;
  MQTT_LOG_DEBUG("TOFU thumbprint verified OK.");
  return true;
}

//+------------------------------------------------------------------+
//| _TopicMatchesFilter                                              |
//| Purpose: MQTT §4.7.1.2 wildcard matching                         |
//| Parameters: topic - [IN] incoming PUBLISH topic name             |
//|             filter - [IN] subscription topic filter              |
//| Return: true if topic matches filter                             |
//| Note: Supports '+' (single-level) and '#' (multi-level)          |
//+------------------------------------------------------------------+
#ifdef MQTT_UNIT_TESTS
bool CMqttClient::_TopicMatchesFilter(const string topic, const string filter) {
  //--- Exact match fast path (covers the common non-wildcard case)
  if (topic == filter) {
    return true;
  }

  int t_len = StringLen(topic);
  int f_len = StringLen(filter);
  int ti    = 0;  // topic index
  int fi    = 0;  // filter index

  while (fi < f_len) {
    ushort fc = StringGetCharacter(filter, fi);

    if (fc == '#') {
      //--- Per §4.7.2: '#' at the first filter segment does not match topics beginning with '$'
      if (fi == 0 && t_len > 0 && StringGetCharacter(topic, 0) == '$') {
        return false;
      }
      //--- '#' must be the last character in a valid filter per §4.7.1.2
      //--- It matches the remainder of the topic (zero or more levels).
      return true;
    }

    if (fc == '+') {
      //--- Per §4.7.2: '+' at the first filter segment does not match topics beginning with '$'
      if (fi == 0 && ti < t_len && StringGetCharacter(topic, 0) == '$') {
        return false;
      }
      //--- '+' matches exactly one topic level (all chars until next '/' or end)
      if (ti >= t_len) {
        return false;  // No topic level left to match
      }
      //--- Advance topic past the current level
      while (ti < t_len && StringGetCharacter(topic, ti) != '/') {
        ti++;
      }
      fi++;
      continue;
    }

    //--- Literal character: must match exactly
    if (ti >= t_len) {
      //--- Topic is consumed. Match still succeeds if the remaining filter
      //--- is exactly "/#" (multi-level wildcard matching zero sub-levels).
      //--- Per MQTT §4.7.1.2: "sport/#" matches "sport".
      if (fc == '/' && fi + 1 < f_len && StringGetCharacter(filter, fi + 1) == '#') {
        return true;
      }
      return false;
    }
    ushort tc = StringGetCharacter(topic, ti);
    if (tc != fc) {
      return false;
    }
    ti++;
    fi++;
  }

  //--- Both topic and filter must be fully consumed for a match
  return (ti == t_len && fi == f_len);
}
#endif

//+------------------------------------------------------------------+
//| _ResetServerCapabilities                                         |
//| Purpose: Reset CONNACK-derived server capability flags           |
//+------------------------------------------------------------------+
void CMqttClient::_ResetServerCapabilities() {
  m_server_max_qos            = 2;
  m_server_retain_available   = true;
  m_server_wildcard_available = true;
  m_server_sub_id_available   = true;
  m_server_shared_available   = true;
  m_context.flow_control.ResetServerLimits();
  m_transport.SetMaxPacketSize(0);
  //--- Per §3.3.2.3.4 Topic Alias Mappings are scoped to the Network Connection.
  //--- The broker discards all alias mappings when the connection closes, so we
  //--- must clear our own on every reconnect.  SetTopicAliasMaximum(0) alone
  //--- left m_client_topic_to_alias/m_client_alias_to_topic intact, causing:
  //---   (a) Build() attaching a stale alias value to the packet, and
  //---   (b) m_highest_client_alias retaining the old high-water-mark, which
  //---       triggers LRU eviction immediately on the first new connection even
  //---       when fewer aliases are in use, or fails Build() when the new
  //---       CONNACK advertises a smaller Topic Alias Maximum than before.
  m_context.topic_alias_manager.ClearAll();
  m_context.topic_alias_manager.SetTopicAliasMaximum(0);
}

//+------------------------------------------------------------------+
//| _ValidateSubscribeRequest                                        |
//| Purpose: Enforce broker SUBSCRIBE capability flags               |
//| Parameters: topic_filter - [IN] requested topic filter           |
//|             use_sub_id  - [OUT] true when sub-id may be emitted  |
//|             fire_error  - [IN] true to surface local rejection   |
//| Return: true if the SUBSCRIBE may be sent                        |
//+------------------------------------------------------------------+
bool CMqttClient::_ValidateSubscribeRequest(const string topic_filter, bool& use_sub_id, bool fire_error) {
  use_sub_id          = m_server_sub_id_available;

  bool uses_wildcards = (StringFind(topic_filter, "#") >= 0 || StringFind(topic_filter, "+") >= 0);
  if (uses_wildcards && !m_server_wildcard_available) {
    if (fire_error) {
      _FireError(MQTT_REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED,
                 "Broker declared Wildcard Subscriptions unavailable; refusing SUBSCRIBE for " + topic_filter);
    }
    return false;
  }

  if (CSubscribe::IsSharedSubscriptionFilter(topic_filter) && !m_server_shared_available) {
    if (fire_error) {
      _FireError(MQTT_REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED,
                 "Broker declared Shared Subscriptions unavailable; refusing SUBSCRIBE for " + topic_filter);
    }
    return false;
  }

  return true;
}

//+------------------------------------------------------------------+
//| _TrackPendingSubscribe                                           |
//| Purpose: Record a live SUBSCRIBE for later SUBACK correlation    |
//+------------------------------------------------------------------+
void CMqttClient::_TrackPendingSubscribe(ushort packet_id, const string topic_filter) {
  uint idx   = m_pending_replay_count++;
  uint tbase = ArraySize(m_prs_topics);
  ArrayResize(m_prs_pkt_id, m_pending_replay_count, 4);
  ArrayResize(m_prs_tcount, m_pending_replay_count, 4);
  ArrayResize(m_prs_toff, m_pending_replay_count, 4);
  ArrayResize(m_prs_topics, tbase + 1, 4);
  m_prs_pkt_id[idx]   = packet_id;
  m_prs_tcount[idx]   = 1;
  m_prs_toff[idx]     = tbase;
  m_prs_topics[tbase] = topic_filter;
}

//+------------------------------------------------------------------+
//| _TrackPendingUnsubscribe                                         |
//| Purpose: Defer local unsubscribe removal until UNSUBACK arrives  |
//+------------------------------------------------------------------+
void CMqttClient::_TrackPendingUnsubscribe(ushort packet_id, const string topic_filter) {
  ArrayResize(m_punsub_pkt_id, m_pending_unsub_count + 1, 4);
  ArrayResize(m_punsub_topic, m_pending_unsub_count + 1, 4);
  m_punsub_pkt_id[m_pending_unsub_count] = packet_id;
  m_punsub_topic[m_pending_unsub_count]  = topic_filter;
  m_pending_unsub_count++;
}

//+------------------------------------------------------------------+
//| _RemoveSubscriptionLocal                                         |
//| Purpose: Remove a subscription from local registry structures    |
//| Parameters: topic_filter - [IN] filter to remove                 |
//| Return: true if the subscription existed                         |
//+------------------------------------------------------------------+
bool CMqttClient::_RemoveSubscriptionLocal(const string topic_filter) {
  uint idx = 0;
  if (!_SubIndexLookup(topic_filter, idx)) {
    return false;
  }

  _SubIndexRemove(topic_filter);
  m_topic_matcher.RemoveFilter(topic_filter);
  uint last = m_sub_count - 1;
  if (idx != last) {
    m_sub_topic[idx]    = m_sub_topic[last];
    m_sub_qos[idx]      = m_sub_qos[last];
    m_sub_cb[idx]       = m_sub_cb[last];
    m_sub_no_local[idx] = m_sub_no_local[last];
    m_sub_rap[idx]      = m_sub_rap[last];
    m_sub_rh[idx]       = m_sub_rh[last];
    m_sub_id[idx]       = m_sub_id[last];
    m_sub_utf8_len[idx] = m_sub_utf8_len[last];
    _SubIndexSet(m_sub_topic[idx], idx);
    m_topic_matcher.RemoveSubIndex(last);
    m_topic_matcher.AddFilter(m_sub_topic[idx], idx);
  }
  //--- Swap-with-last moves the tail entry into position idx. If that position
  //--- was already replayed (idx < m_replay_next_index), the moved entry would
  //--- never be replayed. Decrement the cursor so the replay covers it.
  if (m_replay_in_progress && idx < m_replay_next_index) {
    m_replay_next_index--;
  }
  m_sub_count--;
  ArrayResize(m_sub_topic, m_sub_count);
  ArrayResize(m_sub_qos, m_sub_count);
  ArrayResize(m_sub_cb, m_sub_count);
  ArrayResize(m_sub_no_local, m_sub_count);
  ArrayResize(m_sub_rap, m_sub_count);
  ArrayResize(m_sub_rh, m_sub_count);
  ArrayResize(m_sub_id, m_sub_count);
  ArrayResize(m_sub_utf8_len, m_sub_count);
  return true;
}

//+------------------------------------------------------------------+
//| _SendImmediateSubscribe                                          |
//| Purpose: Build and send one live SUBSCRIBE packet                |
//| Parameters: topic_filter - [IN] topic filter                     |
//|             opts_byte    - [IN] full subscription options byte   |
//|             sub_id       - [IN] subscription identifier          |
//| Return: true on successful send                                  |
//+------------------------------------------------------------------+
bool CMqttClient::_SendImmediateSubscribe(const string topic_filter, uchar opts_byte, uint sub_id) {
  bool use_sub_id = true;
  if (!_ValidateSubscribeRequest(topic_filter, use_sub_id)) {
    return false;
  }

  CSubscribe sub;
  sub.SetTopicFilter(topic_filter, opts_byte);
  if (use_sub_id) {
    sub.SetSubscriptionIdentifier(sub_id);
  }

  uchar pkt[];
  sub.Build(pkt, &m_context.session_db);
  if (ArraySize(pkt) == 0) {
    ushort leaked_id = sub.GetPacketId();
    if (leaked_id != 0) {
      m_context.session_db.ReleasePacketId(leaked_id);
    }
    _FireError(MQTT_REASON_CODE_MALFORMED_PACKET, "Failed to build SUBSCRIBE for " + topic_filter);
    return false;
  }
  if (!m_context.flow_control.ValidateOutgoingPacketSize((uint)ArraySize(pkt))) {
    ushort leaked_id = sub.GetPacketId();
    if (leaked_id != 0) {
      m_context.session_db.ReleasePacketId(leaked_id);
    }
    _FireError(MQTT_REASON_CODE_PACKET_TOO_LARGE, "SUBSCRIBE packet exceeds server Maximum Packet Size");
    return false;
  }

  ENUM_TRANSPORT_ERROR err = m_transport.Send(pkt);
  if (err != TRANSPORT_OK) {
    ushort leaked_id = sub.GetPacketId();
    if (leaked_id != 0) {
      m_context.session_db.ReleasePacketId(leaked_id);
    }
    _FireError((int)err, "Failed to send SUBSCRIBE for " + topic_filter);
    return false;
  }

  _TrackPendingSubscribe(sub.GetPacketId(), topic_filter);
  return true;
}

//+------------------------------------------------------------------+
//| Subscribe                                                        |
//| Purpose: Register a persistent subscription                      |
//| Parameters: topic_filter - [IN] subscription filter              |
//|             qos - [IN] QoS level (0xFF = use default)            |
//+------------------------------------------------------------------+
void CMqttClient::Subscribe(const string topic_filter, uchar qos) { Subscribe(topic_filter, NULL, qos); }

//+------------------------------------------------------------------+
//| Subscribe                                                        |
//| Purpose: Subscribe with per-topic callback                       |
//| Parameters: topic_filter - [IN] subscription filter              |
//|             cb - [IN] function pointer for this topic            |
//|             qos - [IN] QoS level (0xFF = use default)            |
//+------------------------------------------------------------------+
void CMqttClient::Subscribe(const string topic_filter, MqttOnMessageCallback cb, uchar qos) {
  _SyncLogger();  // Ensure this instance's log config is active
  if (!CSubscribe::IsValidTopicFilter(topic_filter)) {
    _FireError(MQTT_REASON_CODE_TOPIC_FILTER_INVALID, "Invalid Topic Filter: " + topic_filter);
    return;
  }
  //--- Resolve 0xFF sentinel to default QoS
  if (qos == 0xFF) {
    qos = m_default_qos;
  }
  if (qos > 2) {
    qos = 2;
  }

  if (m_state == MQTT_CLIENT_CONNECTED) {
    bool use_sub_id = true;
    if (!_ValidateSubscribeRequest(topic_filter, use_sub_id)) {
      return;
    }
  }

  uint existing_idx = 0;
  if (_SubIndexLookup(topic_filter, existing_idx)) {
    if (m_state == MQTT_CLIENT_CONNECTED) {
      //--- Rebuild the full options byte from stored flags so NL/RAP/RH are preserved
      uchar _resub_opts = (uchar)(qos & 0x03) | (m_sub_no_local[existing_idx] ? 0x04 : 0)
                        | (m_sub_rap[existing_idx] ? 0x08 : 0) | ((m_sub_rh[existing_idx] & 0x03) << 4);
      if (!_SendImmediateSubscribe(topic_filter, _resub_opts, m_sub_id[existing_idx])) {
        return;
      }
    }
    m_sub_qos[existing_idx] = qos;
    m_sub_cb[existing_idx]  = cb;
    //--- Update trie entry with new callback (remove old, re-add at same index)
    m_topic_matcher.RemoveFilter(topic_filter);
    m_topic_matcher.AddFilter(topic_filter, existing_idx);
    return;
  }
  //--- Adaptive reserve reduces ArrayResize calls on large subscription tables
  uint _sub_reserve = (m_sub_count < 64) ? 8 : (m_sub_count >> 1);
  ArrayResize(m_sub_topic, m_sub_count + 1, _sub_reserve);
  ArrayResize(m_sub_qos, m_sub_count + 1, _sub_reserve);
  ArrayResize(m_sub_cb, m_sub_count + 1, _sub_reserve);
  ArrayResize(m_sub_no_local, m_sub_count + 1, _sub_reserve);
  ArrayResize(m_sub_rap, m_sub_count + 1, _sub_reserve);
  ArrayResize(m_sub_rh, m_sub_count + 1, _sub_reserve);
  ArrayResize(m_sub_id, m_sub_count + 1, _sub_reserve);
  ArrayResize(m_sub_utf8_len, m_sub_count + 1, _sub_reserve);
  m_sub_topic[m_sub_count]    = topic_filter;
  m_sub_qos[m_sub_count]      = qos;
  m_sub_cb[m_sub_count]       = cb;
  m_sub_no_local[m_sub_count] = false;  // Default: deliver own publishes
  m_sub_rap[m_sub_count]      = false;  // Default: do not retain-as-published
  m_sub_rh[m_sub_count]       = 0;      // Default: send retained on subscribe
  m_sub_id[m_sub_count]       = _NextSubscriptionIdentifier();
  //--- Use StringToUTF8Bytes (already strips null terminator) instead of StringToCharArray
  uchar _clen_buf[];
  int   _clen                 = StringToUTF8Bytes(topic_filter, _clen_buf);
  m_sub_utf8_len[m_sub_count] = (uint)(_clen > 0 ? _clen : 0);
  _SubIndexSet(topic_filter, m_sub_count);
  m_topic_matcher.AddFilter(topic_filter, m_sub_count);  // Register in trie
  m_sub_count++;

  //--- If already connected, send SUBSCRIBE immediately
  if (m_state == MQTT_CLIENT_CONNECTED) {
    if (!_SendImmediateSubscribe(topic_filter, (uchar)(qos & 0x03), m_sub_id[m_sub_count - 1])) {
      return;
    }
  }
}

//+------------------------------------------------------------------+
//| Unsubscribe                                                      |
//| Purpose: Remove a persistent subscription                        |
//| Parameters: topic_filter - [IN] filter to remove                 |
//+------------------------------------------------------------------+
void CMqttClient::Unsubscribe(const string topic_filter) {
  _SyncLogger();  // Ensure this instance's log config is active
  for (uint i = 0; i < m_pending_unsub_count; i++) {
    if (m_punsub_topic[i] == topic_filter) {
      return;
    }
  }

  //--- If connected, send UNSUBSCRIBE
  if (m_state == MQTT_CLIENT_CONNECTED) {
    CUnsubscribe unsub;
    unsub.AddTopicFilter(topic_filter);
    uchar pkt[];
    unsub.Build(pkt, &m_context.session_db);
    if (ArraySize(pkt) == 0) {
      ushort leaked_id = unsub.GetPacketId();
      if (leaked_id != 0) {
        m_context.session_db.ReleasePacketId(leaked_id);
      }
      _FireError(MQTT_REASON_CODE_MALFORMED_PACKET, "Failed to build UNSUBSCRIBE for " + topic_filter);
      return;
    }
    //--- Per §3.2.2.3.5: Validate outgoing packet size
    if (!m_context.flow_control.ValidateOutgoingPacketSize((uint)ArraySize(pkt))) {
      ushort leaked_id = unsub.GetPacketId();
      if (leaked_id != 0) {
        m_context.session_db.ReleasePacketId(leaked_id);
      }
      _FireError(MQTT_REASON_CODE_PACKET_TOO_LARGE, "UNSUBSCRIBE packet exceeds server Maximum Packet Size");
      return;
    }
    ENUM_TRANSPORT_ERROR err = m_transport.Send(pkt);
    if (err != TRANSPORT_OK) {
      ushort leaked_id = unsub.GetPacketId();
      if (leaked_id != 0) {
        m_context.session_db.ReleasePacketId(leaked_id);
      }
      _FireError((int)err, "Failed to send UNSUBSCRIBE for " + topic_filter);
      return;
    }
    _TrackPendingUnsubscribe(unsub.GetPacketId(), topic_filter);
    return;
  }

  _RemoveSubscriptionLocal(topic_filter);
}

//+------------------------------------------------------------------+
//| Subscribe (MqttSubscribeOptions overload)                        |
//| Purpose: Subscribe with full §3.8.3.1 option flags               |
//| Parameters: topic_filter - [IN] subscription filter              |
//|             opts - [IN] subscribe options (QoS, NL, RAP, RH)     |
//+------------------------------------------------------------------+
void CMqttClient::Subscribe(const string topic_filter, const MqttSubscribeOptions& opts) {
  _SyncLogger();
  if (!CSubscribe::IsValidTopicFilter(topic_filter)) {
    _FireError(MQTT_REASON_CODE_TOPIC_FILTER_INVALID, "Invalid Topic Filter: " + topic_filter);
    return;
  }
  if (opts.no_local && CSubscribe::IsSharedSubscriptionFilter(topic_filter)) {
    _FireError(MQTT_REASON_CODE_PROTOCOL_ERROR,
               "'No Local' flag is not permitted for Shared Subscriptions per MQTT §4.8.6");
    return;
  }
  uchar qos = (opts.qos == 0xFF) ? m_default_qos : opts.qos;
  if (qos > 2) {
    qos = 2;
  }
  if (m_state == MQTT_CLIENT_CONNECTED) {
    bool use_sub_id = true;
    if (!_ValidateSubscribeRequest(topic_filter, use_sub_id)) {
      return;
    }
  }
  uchar opts_byte =
    (uchar)(qos & 0x03) | (opts.no_local ? 0x04 : 0) | (opts.rap ? 0x08 : 0) | ((opts.retain_handling & 0x03) << 4);

  uint existing_idx = 0;
  if (_SubIndexLookup(topic_filter, existing_idx)) {
    if (m_state == MQTT_CLIENT_CONNECTED) {
      if (!_SendImmediateSubscribe(topic_filter, opts_byte, m_sub_id[existing_idx])) {
        return;
      }
    }
    m_sub_qos[existing_idx] = qos;
    if (existing_idx < (uint)ArraySize(m_sub_no_local)) {
      m_sub_no_local[existing_idx] = opts.no_local;
    }
    if (existing_idx < (uint)ArraySize(m_sub_rap)) {
      m_sub_rap[existing_idx] = opts.rap;
    }
    if (existing_idx < (uint)ArraySize(m_sub_rh)) {
      m_sub_rh[existing_idx] = (uchar)(opts.retain_handling & 0x03);
    }
    return;
  }

  //--- New subscription: grow all parallel arrays
  //--- Adaptive reserve reduces ArrayResize calls on large subscription tables
  uint _sub2_reserve = (m_sub_count < 64) ? 8 : (m_sub_count >> 1);
  ArrayResize(m_sub_topic, m_sub_count + 1, _sub2_reserve);
  ArrayResize(m_sub_qos, m_sub_count + 1, _sub2_reserve);
  ArrayResize(m_sub_cb, m_sub_count + 1, _sub2_reserve);
  ArrayResize(m_sub_no_local, m_sub_count + 1, _sub2_reserve);
  ArrayResize(m_sub_rap, m_sub_count + 1, _sub2_reserve);
  ArrayResize(m_sub_rh, m_sub_count + 1, _sub2_reserve);
  ArrayResize(m_sub_id, m_sub_count + 1, _sub2_reserve);
  ArrayResize(m_sub_utf8_len, m_sub_count + 1, _sub2_reserve);
  m_sub_topic[m_sub_count]    = topic_filter;
  m_sub_qos[m_sub_count]      = qos;
  m_sub_cb[m_sub_count]       = NULL;
  m_sub_no_local[m_sub_count] = opts.no_local;
  m_sub_rap[m_sub_count]      = opts.rap;
  m_sub_rh[m_sub_count]       = (uchar)(opts.retain_handling & 0x03);
  m_sub_id[m_sub_count]       = _NextSubscriptionIdentifier();
  //--- Use StringToUTF8Bytes (already strips null terminator) instead of StringToCharArray
  uchar _clen2_buf[];
  int   _clen2                = StringToUTF8Bytes(topic_filter, _clen2_buf);
  m_sub_utf8_len[m_sub_count] = (uint)(_clen2 > 0 ? _clen2 : 0);
  _SubIndexSet(topic_filter, m_sub_count);
  m_topic_matcher.AddFilter(topic_filter, m_sub_count);
  m_sub_count++;

  //--- If already connected, send SUBSCRIBE immediately
  if (m_state == MQTT_CLIENT_CONNECTED) {
    if (!_SendImmediateSubscribe(topic_filter, opts_byte, m_sub_id[m_sub_count - 1])) {
      return;
    }
  }
}

//+------------------------------------------------------------------+
//| SendAuth                                                         |
//| Purpose: Send AUTH packet for multi-step authentication §4.12    |
//| Parameters: reason_code - [IN] Success, Continue, or Re-auth     |
//|             method - [IN] Authentication Method string           |
//|             data - [IN] Authentication Data bytes                |
//|             reason_string - [IN] optional AUTH Reason String     |
//+------------------------------------------------------------------+
void CMqttClient::SendAuth(uchar reason_code, const string method, const uchar& data[], const string reason_string) {
  _SyncLogger();
  if (m_state != MQTT_CLIENT_CONNECTED && m_state != MQTT_CLIENT_WAITING_CONNACK) {
    _FireError(MQTT_REASON_CODE_PROTOCOL_ERROR, "Cannot send AUTH in current state");
    return;
  }
  if (m_active_auth_method == "") {
    _FireError(MQTT_REASON_CODE_PROTOCOL_ERROR,
               "Cannot send AUTH because CONNECT did not negotiate an Authentication Method");
    return;
  }
  if (method == "" || method != m_active_auth_method) {
    _FireError(MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD,
               "AUTH method must match CONNECT Authentication Method for the current session");
    return;
  }
  if (m_state == MQTT_CLIENT_WAITING_CONNACK && reason_code == MQTT_REASON_CODE_RE_AUTHENTICATE) {
    _FireError(MQTT_REASON_CODE_PROTOCOL_ERROR,
               "Re-authentication AUTH is not valid before CONNACK completes per MQTT §4.12");
    return;
  }

  CAuth auth;
  auth.SetReasonCode(reason_code);
  auth.SetAuthMethod(method);
  if (reason_string != "") {
    auth.SetReasonString(reason_string);
  }
  if (ArraySize(data) > 0) {
    auth.SetAuthData(data);
  }

  uchar pkt[];
  auth.Build(pkt);
  if (ArraySize(pkt) == 0) {
    _FireError(MQTT_REASON_CODE_MALFORMED_PACKET, "Failed to build AUTH packet");
    return;
  }
  if (!m_context.flow_control.ValidateOutgoingPacketSize((uint)ArraySize(pkt))) {
    _FireError(MQTT_REASON_CODE_PACKET_TOO_LARGE, "AUTH packet exceeds server Maximum Packet Size");
    return;
  }

  ENUM_TRANSPORT_ERROR err = m_transport.Send(pkt);
  if (err != TRANSPORT_OK) {
    _FireError((int)err, "Failed to send AUTH packet");
  }
}

//+------------------------------------------------------------------+
//| Connect                                                          |
//| Purpose: Initiate asynchronous connection to the broker          |
//| Return: TRANSPORT_CONNECTING on success, or error code           |
//| Note: Call Poll() to complete the connection handshake.          |
//+------------------------------------------------------------------+
ENUM_TRANSPORT_ERROR CMqttClient::Connect() {
  _SyncLogger();  // Ensure this instance's log config is active
  if (m_host == "") {
    MQTT_LOG_ERROR("Host not configured — call SetHost() or SetHostWS() first.");
    return TRANSPORT_ERROR_SOCKET;
  }

#ifdef MQTT_UNIT_TESTS
  bool use_injected_transport = m_test_transport_injected;
#else
  bool use_injected_transport = false;
#endif

  //--- A fresh manual Connect() attempt must not inherit deferred transport
  //--- packets from an older session, even if policy validation rejects the
  //--- new attempt before any socket work begins.
  _ClearDeferredTransportPackets();

  //--- Enforce TLS requirement if configured
  if (!use_injected_transport && m_require_tls && !m_use_tls) {
    MQTT_LOG_ERROR("TLS is required (SetRequireTLS) but not enabled. Call SetTLS(true) before connecting.");
    _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
               "Connection refused: TLS is required by client policy but not enabled");
    return TRANSPORT_ERROR_TLS;
  }

  if (!use_injected_transport && !m_use_tls) {
    string transport_desc = (m_transport_type == TRANSPORT_WS) ? "Plain WebSocket (ws://)" : "Plain MQTT/TCP";
    if (!m_allow_insecure_plaintext_transport) {
      MQTT_LOG_ERROR(transport_desc
                     + " is blocked by default. Enable TLS/WSS for production or explicitly allow insecure plaintext "
                       "transport only on trusted private test networks.");
      _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                 "Connection refused: insecure plaintext transport requires explicit opt-in via "
                 "SetAllowInsecurePlaintextTransport(true)");
      return TRANSPORT_ERROR_TLS;
    }
    MQTT_LOG_WARN(transport_desc
                  + " is running without TLS because SetAllowInsecurePlaintextTransport(true) was "
                    "explicitly enabled. Restrict this to trusted private test networks.");
  }

  if (!use_injected_transport && _HasSensitiveAuth() && !m_use_tls) {
    if (!m_allow_insecure_plaintext_auth) {
      MQTT_LOG_ERROR("Plaintext authentication is blocked by default. Enable TLS with SetTLS(true) or explicitly allow "
                     "plaintext auth.");
      _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                 "Connection refused: plaintext username/password or AUTH data requires TLS unless explicitly allowed");
      return TRANSPORT_ERROR_TLS;
    }
#ifndef MQTT_LOG_CREDENTIALS
    MQTT_LOG_WARN("Sending authentication material over plaintext because SetAllowInsecurePlaintextAuth(true) was "
                  "explicitly enabled.");
#endif
  }
  if (!use_injected_transport && m_use_tls) {
    MQTT_LOG_WARN("TLS/WSS handshakes use blocking MQL5 socket APIs on the chart thread. "
                  "Use a dedicated MQTT chart or terminal for production trading workloads.");
  }

  ushort client_recv_max = m_context.flow_control.GetClientReceiveMaximum();
  uint   client_max_pkt  = m_context.flow_control.GetClientMaximumPacketSize();
  _ClearMessageCallbacks();
  if (!use_injected_transport) {
    m_transport.Disconnect();
  }
  m_context.OnDisconnect();
  if (client_recv_max > 0 && client_recv_max < 65535) {
    m_context.flow_control.SetClientReceiveMaximum(client_recv_max);
  }
  if (client_max_pkt >= 5) {
    m_context.flow_control.SetClientMaximumPacketSize(client_max_pkt);
  }
  _ResetServerCapabilities();
  m_active_auth_method              = "";
  //--- Determine whether this is a manual Connect() call or an
  //--- auto-reconnect triggered by Poll(). Manual calls reset the circuit-
  //--- breaker counter so the EA operator gets a fresh N-attempt budget.
  //--- Auto-reconnect calls (IsReconnecting=true) preserve the accumulated count
  //--- so the circuit breaker actually trips after N total attempts.
  bool is_manual_connect            = !m_reconnect_policy.IsReconnecting();
  uint effective_connect_timeout_ms = _ResolveConnectTimeoutMs(is_manual_connect);
  m_active_connect_timeout_ms       = effective_connect_timeout_ms;
  if (is_manual_connect) {
    //--- A user-initiated Connect() starts a fresh attempt budget and leaves any
    //--- previous reconnect loop behind. Auto-reconnect attempts must NOT stop the
    //--- helper here, otherwise an immediate setup failure strands the client offline.
    m_reconnect_policy.OnManualConnect();
  }

  //--- Ensure session DB lifecycle is wired for reconnect/restart durability.
  //--- Keep persistence enabled when using non-clean sessions or explicit expiry.
  string session_key         = _SanitizeSessionKey((m_client_id != "") ? m_client_id : (m_host + "_" + (string)m_port));
  m_session_key              = session_key;  // Persist for TOFU file naming across reconnects
  m_effective_session_expiry = m_session_expiry;
  bool persistent            = (m_effective_session_expiry > 0);
  if (!m_context.session_db.Init("mqtt_" + session_key, persistent)) {
    _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, "Failed to initialize session database");
  }
  //--- Restore persisted circuit-breaker counter for auto-reconnect
  //--- continuity across EA restarts during broker outages.
  if (!is_manual_connect) {
    uint persisted = m_context.session_db.GetReconnectFailureCount();
    if (persisted > m_reconnect_policy.GetCurrentAttemptCount()) {
      m_reconnect_policy.RestorePersistedAttemptCount(persisted);
      MQTT_LOG_DEBUG("Restored reconnect failure count=" + (string)persisted + " from session DB.");
    }
  } else {
    //--- Manual connect — also reset the persisted value.
    m_context.session_db.SetReconnectFailureCount(0);
  }
  if (m_clean_start) {
    m_context.session_db.ResetSession();
  }

  m_incoming_storage_error_count = m_context.session_db.GetIncomingStorageErrorCount();
  if (m_incoming_storage_error_count > 0) {
    MQTT_LOG_DEBUG("Restored incoming storage error count=" + (string)m_incoming_storage_error_count
                   + " from session DB.");
  }

  if (!m_clean_start && persistent) {
    //--- Rebuild incoming in-flight flow-control state from the loaded session.
    //--- CFlowControl::m_incoming_bitfield and m_incoming_inflight_count are not
    //--- persisted to disk. Without this rebuild, the first PUBREL for a
    //--- broker-retransmitted QoS-2 message after an EA restart calls
    //--- ReleaseIncomingQoS on an already-clear bit, underflowing the uint counter
    //--- to UINT_MAX and causing permanent "Receive Maximum exceeded" disconnects.
    SessionMessage loaded_incoming[];
    uint           loaded_count = m_context.session_db.GetIncomingMessages(loaded_incoming);
    for (uint ri = 0; ri < loaded_count; ri++) {
      //--- Re-occupy the incoming flow-control slot for each persisted incoming message.
      //--- qos_level is always > 0 for stored incoming messages (only QoS 2 is stored).
      m_context.flow_control.RegisterIncomingQoS(loaded_incoming[ri].packet_id, loaded_incoming[ri].qos_level,
                                                 loaded_incoming[ri].payload_size);
    }
    if (loaded_count > 0) {
      MQTT_LOG_DEBUG("Rebuilt incoming flow-control state for " + (string)loaded_count
                     + " persisted incoming QoS-2 message(s) (STAB-3).");
    }
    //--- Rebuild outgoing flow-control state from the loaded session so retransmissions
    //--- and new publishes respect the server's Receive Maximum after reconnect.
    SessionMessage loaded_outgoing[];
    uint           loaded_out_count = m_context.session_db.GetPendingMessages(loaded_outgoing, true);
    for (uint ri = 0; ri < loaded_out_count; ri++) {
      m_context.flow_control.RegisterOutgoingQoS(loaded_outgoing[ri].packet_id, loaded_outgoing[ri].qos_level,
                                                 loaded_outgoing[ri].payload_size);
    }
    if (loaded_out_count > 0) {
      MQTT_LOG_DEBUG("Rebuilt outgoing flow-control state for " + (string)loaded_out_count
                     + " persisted outgoing QoS message(s).");
    }
    if (m_publish_queue.IsCompletelyEmpty()) {
      _RestorePersistedPublishQueue();
    }
  }

  //--- TOFU persistence: load a previously pinned thumbprint from the session-local
  //--- pin file when the caller did not pre-provision one in code.
  if (m_tofu_enabled && m_use_tls && !m_tofu_pinned) {
    string tofu_path = "mqtt_tofu_" + m_session_key + ".pin";
    int    tofu_fh   = FileOpen(
      tofu_path, FILE_READ | FILE_TXT);  // terminal-local; not FILE_COMMON (prevents cross-terminal pin collision)
    if (tofu_fh != INVALID_HANDLE) {
      string stored_fp = FileReadString(tofu_fh);
      FileClose(tofu_fh);
      if (stored_fp != "") {
        string normalized_thumb = "";
        if (!_NormalizeCertificateThumbprint(stored_fp, normalized_thumb)) {
          MQTT_LOG_ERROR("TOFU: persisted thumbprint in '" + tofu_path
                         + "' is invalid. Delete the pin file or replace it with a valid MT5 SHA-1 thumbprint.");
          _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                     "Connection refused: persisted TOFU thumbprint is invalid. Delete the pin file or set "
                     "SetTofuThumbprint() before Connect().");
          return TRANSPORT_ERROR_TLS;
        }
        m_tofu_fingerprint = normalized_thumb;
        m_tofu_pinned      = true;
        MQTT_LOG_INFO("TOFU: loaded persisted thumbprint for session '" + m_session_key + "'");
      }
    }
  }
  _UpdateEffectiveTrustMode();
  if (m_tofu_enabled && m_use_tls && !m_tofu_pinned) {
    MQTT_LOG_ERROR("TOFU pinning is enabled but no pre-provisioned or persisted MT5 certificate thumbprint is "
                   "available. First-use capture is disabled.");
    _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
               "Connection refused: TOFU pinning requires a pre-provisioned MT5 certificate thumbprint via "
               "SetTofuThumbprint() or a previously persisted pin.");
    return TRANSPORT_ERROR_TLS;
  }

  //--- Apply keep-alive to the active transport before connect
  m_transport.SetKeepAlive(m_keepalive_s);
  m_connect_deadline_ms               = 0;
  m_connack_deadline_ms               = 0;
  m_connect_blocking_duration_seen_us = m_transport.GetLastBlockingOperationDuration_us();

#ifdef MQTT_UNIT_TESTS
  if (use_injected_transport) {
    MQTT_LOG_DEBUG("Using injected test transport for CONNECT path.");
    if (m_transport.IsConnected()) {
      _SetState(MQTT_CLIENT_CONNECTING);
      _SendConnect();
      if (m_state == MQTT_CLIENT_WAITING_CONNACK || m_state == MQTT_CLIENT_CONNECTED) {
        return TRANSPORT_CONNECTING;
      }
      return TRANSPORT_ERROR_SEND;
    }
    if (m_transport.IsConnecting()) {
      m_connect_deadline_ms = (GetMicrosecondCount() / 1000) + effective_connect_timeout_ms;
      _SetState(MQTT_CLIENT_CONNECTING);
      MQTT_LOG_DEBUG("Injected test transport is still connecting.");
      return TRANSPORT_CONNECTING;
    }
    return _HandleConnectSetupFailure(TRANSPORT_ERROR_SOCKET, "Injected test transport is not connected");
  }
#endif

  ENUM_TRANSPORT_ERROR err;

  if (m_transport_type == TRANSPORT_WS) {
    //--- WebSocket: async connect (prevents EA freeze during connection).
    //--- Poll() completes TLS handshake and WS upgrade once TCP connects, then
    //--- CMqttClient::Poll() detects IsConnected()==true and sends CONNECT.
    if (m_use_tls) {
      err = m_ws_transport.ConnectWSSAsync(m_host, m_port, m_ws_path, 50, effective_connect_timeout_ms);
    } else {
      err = m_ws_transport.ConnectWSAsync(m_host, m_port, m_ws_path, 50, effective_connect_timeout_ms);
    }
    if (err == TRANSPORT_CONNECTING || err == TRANSPORT_OK) {
      m_connect_deadline_ms = (GetMicrosecondCount() / 1000) + effective_connect_timeout_ms;
      _SetState(MQTT_CLIENT_CONNECTING);
      MQTT_LOG_DEBUG("Async WS connect started to " + m_host + ":" + (string)m_port + m_ws_path + " ("
                     + (m_use_tls ? "WSS" : "WS") + ")");
      return TRANSPORT_CONNECTING;
    }
    return _HandleConnectSetupFailure(err, "WebSocket connect failed");
  }

  //--- TCP: async connect
  //--- m_connect_timeout_ms is the TOTAL budget (semantics of SetConnectTimeout).
  //--- The per-attempt socket timeout is fixed at 50 ms so that Poll() remains
  //--- responsive and the overall deadline is not silently extended to 30 s.
  if (m_use_tls) {
    err = m_tcp_transport.ConnectTLSAsync(m_host, m_port, 50, effective_connect_timeout_ms);
  } else {
    err = m_tcp_transport.ConnectAsync(m_host, m_port, 50, effective_connect_timeout_ms);
  }

  if (err == TRANSPORT_CONNECTING || err == TRANSPORT_OK) {
    m_connect_deadline_ms = (GetMicrosecondCount() / 1000) + effective_connect_timeout_ms;
    _SetState(MQTT_CLIENT_CONNECTING);
    MQTT_LOG_DEBUG("Async connect started to " + m_host + ":" + (string)m_port + " (" + (m_use_tls ? "TLS" : "plain")
                   + ")");
    return TRANSPORT_CONNECTING;
  }

  return _HandleConnectSetupFailure(err, "ConnectAsync failed");
}

//+------------------------------------------------------------------+
//| _CacheDisconnectMetadata                                         |
//| Purpose: Keep the public disconnect diagnostics surface aligned  |
//|          for both inbound and client-initiated disconnects.      |
//+------------------------------------------------------------------+
void CMqttClient::_CacheDisconnectMetadata(uchar reason_code, const string reason_string, const string server_reference,
                                           const string& user_prop_keys[], const string& user_prop_vals[],
                                           uint user_prop_count) {
  m_last_disconnect_reason_code      = reason_code;
  m_last_disconnect_reason_string    = reason_string;
  m_last_disconnect_server_reference = server_reference;
  m_last_disconnect_user_prop_count  = user_prop_count;
  ArrayResize(m_last_disconnect_user_prop_keys, user_prop_count);
  ArrayResize(m_last_disconnect_user_prop_vals, user_prop_count);
  for (uint i = 0; i < user_prop_count; i++) {
    m_last_disconnect_user_prop_keys[i] = user_prop_keys[i];
    m_last_disconnect_user_prop_vals[i] = user_prop_vals[i];
  }
}

//+------------------------------------------------------------------+
//| _DispatchAckDiagnostics                                          |
//| Purpose: Cache and surface MQTT 5 simple-ack diagnostics even    |
//|          when the packet does not match a live outgoing message. |
//+------------------------------------------------------------------+
void CMqttClient::_DispatchAckDiagnostics(uchar packet_type, ushort packet_id, uchar reason_code,
                                          const string reason_string, const string& user_prop_keys[],
                                          const string& user_prop_vals[], uint user_prop_count) {
  m_last_ack_packet_type     = packet_type;
  m_last_ack_packet_id       = packet_id;
  m_last_ack_reason_code     = reason_code;
  m_last_ack_reason_string   = reason_string;
  m_last_ack_user_prop_count = user_prop_count;
  ArrayResize(m_last_ack_user_prop_keys, user_prop_count);
  ArrayResize(m_last_ack_user_prop_vals, user_prop_count);
  for (uint i = 0; i < user_prop_count; i++) {
    m_last_ack_user_prop_keys[i] = user_prop_keys[i];
    m_last_ack_user_prop_vals[i] = user_prop_vals[i];
  }

  if (m_on_ack != NULL) {
    m_on_ack(packet_type, packet_id, reason_code, reason_string, user_prop_keys, user_prop_vals, (int)user_prop_count);
    _SyncLogger();
  }
}

//+------------------------------------------------------------------+
//| _DisconnectInternal                                              |
//| Purpose: Shared client-initiated DISCONNECT implementation with  |
//|          optional MQTT 5 metadata and session-expiry override.   |
//+------------------------------------------------------------------+
void CMqttClient::_DisconnectInternal(uchar reason_code, bool has_session_expiry_override, uint session_expiry_interval,
                                      const string reason_string, const string server_reference,
                                      const string& user_prop_keys[], const string& user_prop_vals[],
                                      int user_prop_count) {
  _SyncLogger();  // Ensure this instance's log config is active
  m_abort_current_poll = true;
  m_reconnect_policy.Stop();
  m_active_auth_method        = "";
  m_replay_in_progress        = false;
  m_replay_next_index         = 0;

  uint actual_user_prop_count = 0;
  if (user_prop_count > 0) {
    actual_user_prop_count = (uint)user_prop_count;
  }
  uint key_count = (uint)ArraySize(user_prop_keys);
  uint val_count = (uint)ArraySize(user_prop_vals);
  if (actual_user_prop_count > key_count) {
    actual_user_prop_count = key_count;
  }
  if (actual_user_prop_count > val_count) {
    actual_user_prop_count = val_count;
  }

  if (m_state == MQTT_CLIENT_CONNECTED) {
    CDisconnect disc;
    disc.SetReasonCode(reason_code);
    if (has_session_expiry_override) {
      disc.SetSessionExpiryInterval(session_expiry_interval);
    }
    if (reason_string != "") {
      disc.SetReasonString(reason_string);
    }
    if (server_reference != "") {
      disc.SetServerReference(server_reference);
    }
    for (uint i = 0; i < actual_user_prop_count; i++) {
      disc.SetUserProperty(user_prop_keys[i], user_prop_vals[i]);
    }
    uchar disc_buf[];
    disc.Build(disc_buf);
    m_transport.Send(disc_buf);
  }

  _CacheDisconnectMetadata(reason_code, reason_string != "" ? reason_string : "Client initiated disconnect",
                           server_reference, user_prop_keys, user_prop_vals, actual_user_prop_count);

  m_transport.Disconnect();
  _ClearDeferredTransportPackets();
  _ClearMessageCallbacks();
  _HandleConnectionClosed(has_session_expiry_override, session_expiry_interval);
  m_context.OnDisconnect();

  ENUM_MQTT_CLIENT_STATE prev = m_state;
  _SetState(MQTT_CLIENT_DISCONNECTED);

  if (prev == MQTT_CLIENT_CONNECTED && m_on_disconnect != NULL) {
    m_on_disconnect((int)reason_code, m_last_disconnect_reason_string, m_last_disconnect_server_reference,
                    m_last_disconnect_user_prop_keys, m_last_disconnect_user_prop_vals,
                    (int)m_last_disconnect_user_prop_count);
    _SyncLogger();
  }
}

//+------------------------------------------------------------------+
//| Disconnect                                                       |
//| Purpose: Gracefully disconnect from the broker                   |
//| Parameters: reason_code - [IN] reason for disconnect (§3.14.2.1) |
//+------------------------------------------------------------------+
void CMqttClient::Disconnect(uchar reason_code) {
  string no_user_prop_keys[];
  string no_user_prop_vals[];
  _DisconnectInternal(reason_code, false, 0, "", "", no_user_prop_keys, no_user_prop_vals, 0);
}

//+------------------------------------------------------------------+
//| Disconnect with Session Expiry Interval per §3.14.2.2.2          |
//| Validates the zero→non-zero constraint: if CONNECT used          |
//| session_expiry=0, setting a non-zero value here is a Protocol    |
//| Error per §3.14.2.2.2.                                           |
//+------------------------------------------------------------------+
void CMqttClient::Disconnect(uchar reason_code, uint session_expiry_interval) {
  //--- Validate Session Expiry Interval constraint per §3.14.2.2.2
  if (session_expiry_interval > 0 && m_session_expiry == 0) {
    MQTT_LOG_ERROR("Cannot set non-zero Session Expiry Interval on DISCONNECT "
                   "when CONNECT used zero per §3.14.2.2.2 — clamping to 0");
    session_expiry_interval = 0;
  }

  string no_user_prop_keys[];
  string no_user_prop_vals[];
  _DisconnectInternal(reason_code, true, session_expiry_interval, "", "", no_user_prop_keys, no_user_prop_vals, 0);
}

//+------------------------------------------------------------------+
//| Disconnect with explicit MQTT 5 reason text / server reference   |
//+------------------------------------------------------------------+
void CMqttClient::Disconnect(uchar reason_code, uint session_expiry_interval, const string reason_string,
                             const string server_reference) {
  string no_user_prop_keys[];
  string no_user_prop_vals[];
  Disconnect(reason_code, session_expiry_interval, reason_string, server_reference, no_user_prop_keys,
             no_user_prop_vals);
}

//+------------------------------------------------------------------+
//| Disconnect with full MQTT 5 metadata                             |
//+------------------------------------------------------------------+
void CMqttClient::Disconnect(uchar reason_code, uint session_expiry_interval, const string reason_string,
                             const string server_reference, const string& user_prop_keys[],
                             const string& user_prop_vals[]) {
  //--- Validate Session Expiry Interval constraint per §3.14.2.2.2
  if (session_expiry_interval > 0 && m_session_expiry == 0) {
    MQTT_LOG_ERROR("Cannot set non-zero Session Expiry Interval on DISCONNECT "
                   "when CONNECT used zero per §3.14.2.2.2 — clamping to 0");
    session_expiry_interval = 0;
  }

  _DisconnectInternal(reason_code, true, session_expiry_interval, reason_string, server_reference, user_prop_keys,
                      user_prop_vals, (int)MathMin(ArraySize(user_prop_keys), ArraySize(user_prop_vals)));
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish a message with string payload (default QoS)     |
//| Parameters: topic - [IN] target topic name                       |
//|             payload - [IN] message string                        |
//| Return: MQTT_PUB_OK on success or queueing                       |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const string payload) {
  return Publish(topic, payload, m_default_qos, false);
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish a message with string payload (explicit QoS)    |
//| Parameters: topic - [IN] target topic name                       |
//|             payload - [IN] message string                        |
//|             qos - [IN] QoS level (0, 1, or 2)                    |
//|             retain - [IN] true to retain the message             |
//| Return: MQTT_PUB_OK on success or queueing                       |
//| This overload allocates a uchar[] on every call for the          |
//| UTF-8 encoding of 'payload'. For signal EAs publishing to the    |
//| same format at 10+ msg/s, prefer the uchar[] Publish overload    |
//| with a pre-encoded payload to avoid per-call GC pressure.        |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const string payload, uchar qos, bool retain) {
  uchar payload_bytes[];
  int   len = StringToUTF8Bytes(payload, payload_bytes);
  return Publish(topic, payload_bytes, len, qos, retain);
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish string payload with MQTT 5 PUBLISH properties   |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const string payload,
                                             const MqttPublishProperties& props) {
  return Publish(topic, payload, m_default_qos, false, props);
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish string payload with explicit MQTT 5 properties  |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const string payload, uchar qos, bool retain,
                                             const MqttPublishProperties& props) {
  uchar payload_bytes[];
  int   len = StringToUTF8Bytes(payload, payload_bytes);
  return Publish(topic, payload_bytes, len, qos, retain, props);
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish a message with binary payload (default QoS)     |
//| Parameters: topic - [IN] target topic name                       |
//|             payload - [IN] binary data array                     |
//|             len - [IN] payload length (-1 = full array)          |
//| Return: MQTT_PUB_OK on success or queueing                       |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const uchar& payload[], int len) {
  return Publish(topic, payload, len, m_default_qos, false);
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish binary payload with MQTT 5 PUBLISH properties   |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const uchar& payload[], int len,
                                             const MqttPublishProperties& props) {
  return Publish(topic, payload, len, m_default_qos, false, props);
}

//+------------------------------------------------------------------+
//| _EncodePublishProperties                                         |
//| Purpose: Convert facade properties into encoded PUBLISH bytes    |
//+------------------------------------------------------------------+
bool CMqttClient::_EncodePublishProperties(const MqttPublishProperties& props, uchar& out_props[],
                                           uint& out_expiry_interval, bool& out_allow_outgoing_sub_id) {
  ArrayResize(out_props, 0);
  out_expiry_interval       = 0;
  out_allow_outgoing_sub_id = false;

  int key_count             = ArraySize(props.user_property_keys);
  int val_count             = ArraySize(props.user_property_vals);
  if (key_count != val_count) {
    MQTT_LOG_ERROR("PUBLISH user property key/value count mismatch");
    return false;
  }
  if (props.has_subscription_identifier) {
    MQTT_LOG_ERROR("Client-originated PUBLISH Subscription Identifier is not supported per MQTT §3.3.2.3.8");
    return false;
  }
  if (props.has_topic_alias && props.topic_alias == 0) {
    MQTT_LOG_ERROR("PUBLISH Topic Alias must be greater than zero");
    return false;
  }

  CPublish prop_builder;
  if (props.has_payload_format) {
    prop_builder.SetPayloadFormatIndicator((PAYLOAD_FORMAT_INDICATOR)props.payload_format);
  }
  if (props.has_message_expiry) {
    out_expiry_interval = props.message_expiry_interval;
    prop_builder.SetMessageExpiryInterval(props.message_expiry_interval);
  }
  if (props.has_topic_alias) {
    prop_builder.SetTopicAlias(props.topic_alias);
  }
  if (props.response_topic != "") {
    prop_builder.SetResponseTopic(props.response_topic);
  }
  if (ArraySize(props.correlation_data) > 0) {
    prop_builder.SetCorrelationData(props.correlation_data);
  }
  if (props.content_type != "") {
    prop_builder.SetContentType(props.content_type);
  }
  for (int i = 0; i < key_count; i++) {
    prop_builder.SetUserProperty(props.user_property_keys[i], props.user_property_vals[i]);
  }
  prop_builder.GetEncodedProperties(out_props);
  return true;
}

//+------------------------------------------------------------------+
//| _BuildPersistedPublishProperties                                 |
//| Purpose: Persist raw publish properties without duplicating      |
//|          Message Expiry Interval, which is tracked separately    |
//|          as an absolute expiry timestamp for queue/replay paths. |
//+------------------------------------------------------------------+
void CMqttClient::_BuildPersistedPublishProperties(const uchar& encoded_props[], uint expiry_interval,
                                                   uchar& persisted_props[]) {
  _BuildPersistedPublishProperties(encoded_props, 0, ArraySize(encoded_props), expiry_interval, persisted_props);
}

//+------------------------------------------------------------------+
//| _BuildPersistedPublishProperties                                 |
//| Purpose: Persist a property slice without duplicating expiry     |
//+------------------------------------------------------------------+
void CMqttClient::_BuildPersistedPublishProperties(const uchar& encoded_props[], int prop_offset, int prop_length,
                                                   uint expiry_interval, uchar& persisted_props[]) {
  ArrayResize(persisted_props, 0);

  int total_props = ArraySize(encoded_props);
  if (prop_length <= 0 || total_props == 0 || prop_offset >= total_props) {
    return;
  }

  if (prop_offset < 0) {
    prop_offset = 0;
  }

  uint src_size = (uint)prop_length;
  uint max_size = (uint)(total_props - prop_offset);
  if (src_size > max_size) {
    src_size = max_size;
  }

  if (expiry_interval == 0) {
    ArrayResize(persisted_props, (int)src_size);
    ArrayCopy(persisted_props, encoded_props, 0, prop_offset, (int)src_size);
    return;
  }

  uint idx            = (uint)prop_offset;
  uint src_end        = idx + src_size;
  uint filtered_size  = 0;
  bool removed_expiry = false;

  while (idx < src_end) {
    uint  entry_start = idx;
    uchar prop_id     = encoded_props[idx++];
    uint  value_len   = 0;

    if (!CPropertyEncoder::GetPropertyValueLength(prop_id, encoded_props, idx, value_len)
        || idx + value_len > src_end) {
      MQTT_LOG_WARN("Persisted publish properties were malformed while stripping Message Expiry Interval; keeping the "
                    "original buffer.");
      ArrayResize(persisted_props, (int)src_size);
      ArrayCopy(persisted_props, encoded_props, 0, prop_offset, (int)src_size);
      return;
    }

    idx += value_len;
    if (prop_id == MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL) {
      removed_expiry = true;
      continue;
    }

    uint entry_len = idx - entry_start;
    uint new_size  = filtered_size + entry_len;
    uint reserve   = (new_size > 64) ? (new_size / 2) : 64;
    ArrayResize(persisted_props, (int)new_size, (int)reserve);
    ArrayCopy(persisted_props, encoded_props, (int)filtered_size, (int)entry_start, (int)entry_len);
    filtered_size = new_size;
  }

  if (!removed_expiry) {
    ArrayResize(persisted_props, (int)src_size);
    ArrayCopy(persisted_props, encoded_props, 0, prop_offset, (int)src_size);
  }
}

//+------------------------------------------------------------------+
//| _ApplyEncodedPublishProperties                                   |
//| Purpose: Apply persisted properties and remaining expiry         |
//+------------------------------------------------------------------+
void CMqttClient::_ApplyEncodedPublishProperties(CPublish& publish, const uchar& encoded_props[], datetime expiry_time,
                                                 bool allow_outgoing_sub_id) {
  _ApplyEncodedPublishProperties(publish, encoded_props, 0, ArraySize(encoded_props), expiry_time,
                                 allow_outgoing_sub_id);
}

//+------------------------------------------------------------------+
//| _ApplyEncodedPublishProperties                                   |
//| Purpose: Apply a property slice and remaining expiry             |
//+------------------------------------------------------------------+
void CMqttClient::_ApplyEncodedPublishProperties(CPublish& publish, const uchar& encoded_props[], int prop_offset,
                                                 int prop_length, datetime expiry_time, bool allow_outgoing_sub_id) {
  int total_props = ArraySize(encoded_props);
  if (prop_offset < 0) {
    prop_offset = 0;
  }
  if (prop_length < 0) {
    prop_length = 0;
  }
  if (prop_length > 0 && prop_offset < total_props) {
    publish.SetEncodedProperties(encoded_props, prop_offset, prop_length);
  }
  if (expiry_time > 0) {
    datetime now              = TimeLocal();
    uint     remaining_expiry = (expiry_time > now) ? (uint)(expiry_time - now) : 0;
    publish.SetMessageExpiryInterval(remaining_expiry);
  }
}

//+------------------------------------------------------------------+
//| _RestorePersistedPublishQueue                                    |
//| Purpose: Rehydrate the offline queue from the durable DB lane.   |
//+------------------------------------------------------------------+
void CMqttClient::_RestorePersistedPublishQueue() {
  uint   restored   = 0;
  string error_text = "";
  if (!m_publish_queue_coordinator.RestorePersistedQueue(m_context.session_db, m_publish_queue, GetMicrosecondCount(),
                                                         TimeLocal(), restored, error_text)) {
    _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, error_text);
  }

  if (restored > 0) {
    MQTT_LOG_DEBUG("Restored " + (string)restored + " durable offline queued publish(es) from session DB.");
  }
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish a message with binary payload (explicit QoS)    |
//| Parameters: topic - [IN] target topic name                       |
//|             payload - [IN] binary data array                     |
//|             len - [IN] payload length (-1 = full array)          |
//|             qos - [IN] QoS level (0, 1, or 2)                    |
//|             retain - [IN] true to retain the message             |
//| Return: MQTT_PUB_OK on success or queueing                       |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const uchar& payload[], int len, uchar qos,
                                             bool retain) {
  uchar encoded_props[];
  return _PublishPrepared(topic, payload, len, qos, retain, encoded_props, 0, false);
}

//+------------------------------------------------------------------+
//| Publish                                                          |
//| Purpose: Publish binary payload with explicit MQTT 5 properties  |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::Publish(const string topic, const uchar& payload[], int len, uchar qos,
                                             bool retain, const MqttPublishProperties& props) {
  uchar encoded_props[];
  uint  expiry_interval       = 0;
  bool  allow_outgoing_sub_id = false;
  if (!_EncodePublishProperties(props, encoded_props, expiry_interval, allow_outgoing_sub_id)) {
    return MQTT_PUB_SEND_FAILED;
  }
  return _PublishPrepared(topic, payload, len, qos, retain, encoded_props, expiry_interval, allow_outgoing_sub_id);
}

//+------------------------------------------------------------------+
//| _PublishPrepared                                                 |
//| Purpose: Shared property-aware publish implementation            |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::_PublishPrepared(const string topic, const uchar& payload[], int len, uchar qos,
                                                      bool retain, const uchar& encoded_props[], uint expiry_interval,
                                                      bool allow_outgoing_sub_id) {
  return _PublishPreparedRange(topic, payload, 0, len, qos, retain, encoded_props, 0, ArraySize(encoded_props),
                               expiry_interval, allow_outgoing_sub_id);
}

//+------------------------------------------------------------------+
//| _PublishPreparedRange                                            |
//| Purpose: Shared publish path that can read payload/property      |
//|          slices from larger backing buffers                      |
//+------------------------------------------------------------------+
ENUM_MQTT_PUBLISH_ERROR CMqttClient::_PublishPreparedRange(const string topic, const uchar& payload[],
                                                           int payload_offset, int len, uchar qos, bool retain,
                                                           const uchar& encoded_props[], int prop_offset,
                                                           int prop_length, uint expiry_interval,
                                                           bool allow_outgoing_sub_id) {
  _SyncLogger();  // Ensure this instance's log config is active
  m_last_queued_publish_handoff_complete = false;
  int payload_size                       = ArraySize(payload);
  if (payload_offset < 0) {
    payload_offset = 0;
  }
  if (payload_offset > payload_size) {
    payload_offset = payload_size;
  }

  int payload_len = (len < 0) ? (payload_size - payload_offset) : len;
  if (payload_len < 0) {
    payload_len = 0;
  }
  int payload_available = payload_size - payload_offset;
  if (payload_len > payload_available) {
    payload_len = payload_available;
  }

  int props_size = ArraySize(encoded_props);
  if (prop_offset < 0) {
    prop_offset = 0;
  }
  if (prop_offset > props_size) {
    prop_offset = props_size;
  }
  if (prop_length < 0) {
    prop_length = 0;
  }
  int props_available = props_size - prop_offset;
  if (prop_length > props_available) {
    prop_length = props_available;
  }

  uchar persisted_props[];
  _BuildPersistedPublishProperties(encoded_props, prop_offset, prop_length, expiry_interval, persisted_props);

  //--- Validate topic name per MQTT §4.7.3: MUST be at least one character
  if (StringLen(topic) == 0) {
    MQTT_LOG_ERROR("PUBLISH topic name MUST be at least one character per MQTT §4.7.3");
    return MQTT_PUB_INVALID_TOPIC;
  }

  if (qos > 2) {
    qos = 2;
  }

  if (_QoS1PublishRequiresExpiry(qos, expiry_interval)) {
    _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
               "QoS 1 publishes require Message Expiry Interval when MaxRetransmitCount=0");
    return MQTT_PUB_EXPIRY_REQUIRED;
  }

  //--- Clamp to server's maximum QoS per §3.2.2.3.4
  if (qos > m_server_max_qos) {
    MQTT_LOG_WARN("QoS " + (string)(int)qos + " exceeds broker Maximum QoS " + (string)(int)m_server_max_qos
                  + " — clamping to QoS " + (string)(int)m_server_max_qos);
    qos = m_server_max_qos;
  }

  //--- Check retain availability
  if (retain && !m_server_retain_available) {
    _FireError(MQTT_REASON_CODE_RETAIN_NOT_SUPPORTED, "Broker does not support retain");
    retain = false;
  }

  if (!IsConnected()) {
    uint                           payload_len_u = (uint)payload_len;
    uint                           purged_count  = 0;
    string                         warning_text  = "";
    string                         error_text    = "";
    ENUM_MQTT_OFFLINE_QUEUE_RESULT queue_result  = m_publish_queue_coordinator.QueueWhileDisconnected(
      m_context.session_db, m_publish_queue, *this, m_draining_queue, m_queue_qos0_while_disconnected, topic, payload,
      payload_len_u, qos, retain, expiry_interval, persisted_props, allow_outgoing_sub_id, purged_count, warning_text,
      error_text);
    if (purged_count > 0) {
      MQTT_LOG_WARN("Dropped " + (string)purged_count + " expired queued publish(es) while offline.");
    }
    if (StringLen(warning_text) > 0) {
      MQTT_LOG_WARN(warning_text);
    }
    if (StringLen(error_text) > 0) {
      _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, error_text);
    }

    switch (queue_result) {
      case MQTT_OFFLINE_QUEUE_RESULT_RECONNECTING:
        return MQTT_PUB_RECONNECTING;
      case MQTT_OFFLINE_QUEUE_RESULT_NOT_CONNECTED:
        return MQTT_PUB_NOT_CONNECTED;
      case MQTT_OFFLINE_QUEUE_RESULT_QUEUE_FULL:
        return MQTT_PUB_QUEUE_FULL;
      case MQTT_OFFLINE_QUEUE_RESULT_SEND_FAILED:
        return MQTT_PUB_SEND_FAILED;
      case MQTT_OFFLINE_QUEUE_RESULT_QUEUED:
      default:
        return MQTT_PUB_QUEUED;
    }
  }

  //--- Build and send — reuse the cached builder instance to avoid per-call
  //--- heap allocation of ~20 fields and multiple dynamic arrays.
  datetime expiry_time = (expiry_interval > 0) ? (TimeLocal() + (datetime)expiry_interval) : 0;
  m_pub_builder.Reset();
  //--- Skip topic UTF-8 validation when the topic is the same as last time.
  //--- Trading EAs repeatedly publish to static topics (e.g. "signals/EURUSD");
  //--- this avoids the StringToCharArray allocation inside ValidateTopicName on every publish.
  if (topic == m_pub_last_valid_topic) {
    m_pub_builder.SetTopicNameFast(topic);  // Bypasses ValidateTopicName — already cached as valid
  } else {
    m_pub_builder.SetTopicName(topic);
    if (!m_pub_builder.IsTopicSet()) {
      return MQTT_PUB_INVALID_TOPIC;  // ValidateTopicName rejected the topic (wildcards, length, etc.)
    }
    m_pub_last_valid_topic = topic;   // Cache for next call
  }
  if (payload_len > 0) {
    m_pub_builder.SetPayload(payload, payload_offset,
                             payload_len);  // Direct slice copy, no intermediate tmp[] allocation
  }
  if (retain) {
    m_pub_builder.SetRetain(true);
  }
  _ApplyEncodedPublishProperties(m_pub_builder, encoded_props, prop_offset, prop_length, expiry_time,
                                 allow_outgoing_sub_id);

  //--- Auto-assign a topic alias for hot topics (zero-cost on second+ publish to same topic).
  //--- Fires only when BOTH sides have agreed on alias support:
  //---   * Client advertised Topic Alias Maximum > 0 in CONNECT (m_client_topic_alias_max)
  //---   * Server advertised Topic Alias Maximum > 0 in CONNACK (topic_alias_manager)
  //--- The first PUBLISH carries the full topic name + alias property (establishing the mapping).
  //--- All subsequent publishes to the same topic send a zero-length topic + alias only,
  //--- saving topic_len bytes per packet per MQTT §3.3.2.3.4.
  if (m_client_topic_alias_max > 0 && m_context.topic_alias_manager.GetTopicAliasMaximum() > 0) {
    ushort topic_alias = m_context.topic_alias_manager.GetClientAlias(topic);
    if (topic_alias == 0) {
      //--- First publish for this topic this session: register a new alias.
      //--- Build() will include the full topic name + alias property to establish the
      //--- mapping on the broker before alias-only mode can be used.
      m_context.topic_alias_manager.RegisterClientAliasAuto(topic, topic_alias);
    } else {
      //--- Alias already established on broker in a previous publish.
      //--- Switch to alias-only mode: zero-length topic + alias property only.
      //--- ClearTopicName() sets m_topname to empty so Build() takes the reusing_topic_alias
      //--- path (§3.3.2.1) instead of re-sending the full topic name alongside the alias.
      m_pub_builder.SetTopicAlias(topic_alias);
      m_pub_builder.ClearTopicName();
      //--- Update LRU timestamp so heavily-used aliases are not evicted first when
      //--- the alias pool is full and a new topic needs a slot.
      m_context.topic_alias_manager.TouchClientAlias(topic_alias);
    }
  }

  uchar pkt[];

  if (qos == QoS_0) {
    m_pub_builder.Build(pkt, &m_context.topic_alias_manager, NULL, m_server_max_qos);
    if (ArraySize(pkt) == 0) {
      _FireError(MQTT_REASON_CODE_MALFORMED_PACKET, "Failed to build QoS 0 PUBLISH packet");
      return MQTT_PUB_SEND_FAILED;
    }
    //--- Per §3.2.2.3.5: Client MUST NOT send packets exceeding server's Maximum Packet Size
    if (!m_context.flow_control.ValidateOutgoingPacketSize((uint)ArraySize(pkt))) {
      _FireError(MQTT_REASON_CODE_PACKET_TOO_LARGE, "QoS 0 PUBLISH exceeds server Maximum Packet Size");
      return MQTT_PUB_PACKET_TOO_BIG;
    }
  } else {
    //--- Check flow-control window BEFORE consuming any resources (packet ID, DB write, heap).
    //--- RegisterOutgoingQoS() also guards internally, but by that point resources are already spent.
    if (!m_context.flow_control.CanSendQoSMessage(qos)) {
      return MQTT_PUB_FLOW_CONTROL_FULL;
    }
    ushort pktid = m_context.session_db.AllocatePacketId();
    if (pktid == 0) {
      return MQTT_PUB_NO_PACKET_ID;
    }

    if (!m_context.session_db.StoreOutgoingMessageRange(pktid, qos, topic, payload, (uint)payload_offset,
                                                        (uint)payload_len, retain, 0, expiry_interval, persisted_props,
                                                        false)) {
      m_context.session_db.ReleasePacketId(pktid);
      _FireErrorEx(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                   "Session DB write failed for QoS " + (string)(int)qos + " publish", __FILE__, __LINE__,
                   __FUNCTION__);
      return MQTT_PUB_SEND_FAILED;
    }
    m_pub_builder.SetPacketId(pktid);
    if (qos == QoS_1) {
      m_pub_builder.SetQoS_1(true);
    } else {
      m_pub_builder.SetQoS_2(true);
    }
    m_pub_builder.Build(pkt, &m_context.topic_alias_manager, NULL, m_server_max_qos);
    if (ArraySize(pkt) == 0) {
      m_context.session_db.RemoveMessage(pktid);
      _FireError(MQTT_REASON_CODE_MALFORMED_PACKET, "Failed to build QoS " + (string)(int)qos + " PUBLISH packet");
      return MQTT_PUB_SEND_FAILED;
    }
    //--- Per §3.2.2.3.5, validate packet size for QoS 1/2 BEFORE registering
    //--- in flow control. On failure, clean up the DB entry (RemoveMessage also releases pktid).
    if (!m_context.flow_control.ValidateOutgoingPacketSize((uint)ArraySize(pkt))) {
      m_context.session_db.RemoveMessage(pktid);
      _FireError(MQTT_REASON_CODE_PACKET_TOO_LARGE,
                 "QoS " + (string)(int)qos + " PUBLISH exceeds server Maximum Packet Size");
      return MQTT_PUB_PACKET_TOO_BIG;
    }
    if (!m_context.flow_control.RegisterOutgoingQoS(pktid, qos, (uint)ArraySize(pkt))) {
      //--- RemoveMessage() internally calls ReleasePacketId(), so we
      //--- MUST NOT call ReleasePacketId() here again
      m_context.session_db.RemoveMessage(pktid);
      return MQTT_PUB_SEND_FAILED;
    }

    //--- From this point onward the durable outgoing QoS path owns the message.
    //--- Even if the immediate transport send fails, reconnect retransmission will
    //--- resume from the in-flight store rather than the offline queue.
    m_last_queued_publish_handoff_complete = true;
  }

  ENUM_TRANSPORT_ERROR err = m_transport.Send(pkt);
  if (err != TRANSPORT_OK) {
    _OnTransportError((int)err, "Publish send failed");
    m_active_connect_timeout_ms = 0;
    return MQTT_PUB_SEND_FAILED;
  }
  m_messages_sent++;
  return MQTT_PUB_OK;
}

//+------------------------------------------------------------------+
//| _SendConnect                                                     |
//| Purpose: Build and transmit the MQTT CONNECT packet              |
//+------------------------------------------------------------------+
void CMqttClient::_SendConnect() {
  CConnect conn;
  conn.SetCleanStart(m_clean_start);
  conn.SetKeepAlive(m_keepalive_s);
  m_active_auth_method = m_connect_auth_method;
  if (m_session_expiry > 0) {
    conn.SetSessionExpiryInterval(m_session_expiry);
  }

  //--- Advertise client-side constraints where configured
  ushort client_recv_max = m_context.flow_control.GetClientReceiveMaximum();
  if (client_recv_max > 0 && client_recv_max < 65535) {
    conn.SetReceiveMaximum(client_recv_max);
  }
  uint client_max_pkt = m_context.flow_control.GetClientMaximumPacketSize();
  if (client_max_pkt >= 5) {
    conn.SetMaximumPacketSize(client_max_pkt);
  }
  //--- Advertise client-side Topic Alias Maximum per §3.1.2.11.8
  if (m_client_topic_alias_max > 0) {
    conn.SetTopicAliasMaximum(m_client_topic_alias_max);
    m_context.topic_alias_manager.SetClientTopicAliasMaximum(m_client_topic_alias_max);
  }
  conn.SetClientIdentifier(m_client_id);
  if (m_username != "") {
    conn.SetUserName(m_username);
    conn.SetUserNameFlag(true);
  }
  if (m_password != "" || m_use_binary_password) {
    if (m_use_binary_password) {
      conn.SetPassword(m_password_binary);
    } else {
      conn.SetPassword(m_password);
    }
    conn.SetPasswordFlag(true);
  }
  if (m_connect_auth_method != "") {
    conn.SetAuthMethod(m_connect_auth_method);
    if (m_use_connect_auth_data) {
      conn.SetAuthData(m_connect_auth_data);
    }
  }
  if (m_has_connect_request_response_info) {
    conn.SetRequestResponseInfo(m_connect_request_response_info);
  }
  if (m_has_connect_request_problem_info) {
    conn.SetRequestProblemInfo(m_connect_request_problem_info);
  }

  //--- Apply CONNECT User Properties (§3.1.2.11.7)
  //--- These are set via SetConnectUserProperty(key, val) before Connect().
  for (uint upi = 0; upi < m_connect_user_prop_count; upi++) {
    conn.SetUserProperty(m_connect_user_prop_keys[upi], m_connect_user_prop_vals[upi]);
  }

  //--- Last Will & Testament
  if (m_will_enabled) {
    conn.SetWillFlag(true);
    conn.SetWillTopic(m_will_topic);
    conn.SetWillPayload(m_will_payload);
    //--- Clamp Will QoS to server Maximum QoS on reconnect per §3.1.2.12
    //--- On first connect m_server_max_qos defaults to 2 so no clamping occurs.
    uchar effective_will_qos = (uchar)MathMin((int)m_will_qos, (int)m_server_max_qos);
    if (effective_will_qos != m_will_qos) {
      MQTT_LOG_WARN("Will QoS " + (string)m_will_qos + " clamped to server Maximum QoS " + (string)m_server_max_qos
                    + " per §3.1.2.12");
    }
    if (effective_will_qos == QoS_1) {
      conn.SetWillQoS_1(true);
    } else if (effective_will_qos == QoS_2) {
      conn.SetWillQoS_2(true);
    }
    if (m_will_retain) {
      conn.SetWillRetain(true);
    }
    if (m_will_delay_s > 0) {
      conn.SetWillDelayInterval(m_will_delay_s);
    }
    if (m_will_expiry_s > 0) {
      conn.SetWillMessageExpiryInterval(m_will_expiry_s);
    }
    if (m_has_will_payload_format) {
      conn.SetWillPayloadFormatIndicator(m_will_payload_format);
    }
    if (m_will_content_type != "") {
      conn.SetWillContentType(m_will_content_type);
    }
    if (m_will_response_topic != "") {
      conn.SetWillResponseTopic(m_will_response_topic);
    }
    if (ArraySize(m_will_correlation_data) > 0) {
      conn.SetWillCorrelationData(m_will_correlation_data);
    }
    for (uint wpi = 0; wpi < m_will_user_prop_count; wpi++) {
      conn.SetWillUserProperty(m_will_user_prop_keys[wpi], m_will_user_prop_vals[wpi]);
    }
  }

  uchar pkt[];
  conn.Build(pkt, &m_context.flow_control);
  if (ArraySize(pkt) == 0) {
    MQTT_LOG_ERROR("CONNECT build failed — refusing to wait for CONNACK with an empty packet.");
    _OnTransportError(MQTT_REASON_CODE_MALFORMED_PACKET, "Failed to build CONNECT packet");
    return;
  }

  ENUM_TRANSPORT_ERROR err = m_transport.Send(pkt);
  if (err == TRANSPORT_OK) {
    MQTT_LOG_DEBUG("CONNECT sent.");
    m_connect_deadline_ms = 0;
    m_connack_deadline_ms = (GetMicrosecondCount() / 1000) + m_connack_timeout_ms;
    _SetState(MQTT_CLIENT_WAITING_CONNACK);
  } else if (err == TRANSPORT_CONNECTING) {
    MQTT_LOG_DEBUG("CONNECT send deferred until the TLS transport becomes writable.");
  } else {
    MQTT_LOG_ERROR("Failed to send CONNECT (err=" + (string)(int)err + ").");
    _OnTransportError((int)err, "Failed to send CONNECT packet");
  }
}

//+------------------------------------------------------------------+
//| _OnConnackReceived                                               |
//| Purpose: Process CONNACK and apply server-mandated limits        |
//| Parameters: pkt - [IN/OUT] raw packet buffer                     |
//+------------------------------------------------------------------+
void CMqttClient::_OnConnackReceived(uchar& pkt[]) {
  CConnack connack;
  int      err = connack.Read(pkt);
  if (err != MQTT_OK) {
    uchar mapped_reason = _ClassifyConnackParseReason(err);
    MQTT_LOG_ERROR("CONNACK parse error (" + (string)err + ") — disconnecting.");
    //--- Per §2.2.2.2 and §3.14.1, malformed frames map to 0x81 while
    //--- genuine protocol violations map to 0x82.
    if (err == MQTT_ERROR_PROTOCOL_VIOLATION || err == MQTT_ERROR_INVALID_REASON_CODE
        || err == MQTT_ERROR_MALFORMED_VARINT || err == MQTT_ERROR_MALFORMED_PACKET
        || err == MQTT_ERROR_INVALID_PROPS_LEN || err == MQTT_ERROR_BUFFER_OVERFLOW
        || err == MQTT_ERROR_PACKET_TOO_SHORT) {
      CDisconnect disc;
      disc.SetReasonCode(mapped_reason);
      uchar disc_buf[];
      disc.Build(disc_buf);
      m_transport.Send(disc_buf);
    }
    _OnTransportError((int)mapped_reason, "CONNACK parse error: " + MqttErrorToString((ENUM_MQTT_ERROR)err));
    return;
  }

  _CacheConnackMetadata(connack);

  uchar rc = connack.GetReasonCode();
  if (rc != 0x00) {
    string reason = connack.GetReasonString();
    MQTT_LOG_ERROR("CONNACK rejected by broker — reason code 0x" + StringFormat("%02X", rc) + " (" + reason + ")");

    //--- Check for server redirection reason codes
    if ((rc == MQTT_REASON_CODE_USE_ANOTHER_SERVER || rc == MQTT_REASON_CODE_SERVER_MOVED)) {
      string server_ref = connack.GetServerReference();
      if (server_ref != "" && _HandleRedirection((int)rc, server_ref)) {
        return;  // Redirect set up — Poll() will execute the reconnect
      }
      // Fall through to _OnTransportError if redirect failed or server reference was absent
    }

    _OnTransportError((int)rc, "CONNACK rejected: " + reason);
    //--- Permanent failure — stop any reconnect that _OnTransportError just armed
    if (_IsPermanentFailure(rc)) {
      m_reconnect_policy.Stop();
      MQTT_LOG_ERROR("Permanent CONNACK failure (0x" + StringFormat("%02X", rc)
                     + ") "
                       "— auto-reconnect disabled to prevent broker IP-ban.");
    }
    return;
  }

  //--- Apply Server Keep Alive per §3.2.2.3.14
  ushort server_ka            = connack.GetServerKeepAlive();
  m_connack_server_keep_alive = server_ka;
  if (server_ka > 0) {
    MQTT_LOG_INFO("Server Keep Alive overrides client value: " + (string)m_keepalive_s + "s → " + (string)server_ka
                  + "s (§3.2.2.3.14)");
    m_transport.SetKeepAlive(server_ka);
  }

  //--- Reset connection-scoped limits so omitted CONNACK properties revert
  //--- to the MQTT 5 defaults instead of leaking across reconnects.
  m_context.flow_control.ResetServerLimits();

  //--- Apply Maximum Packet Size
  uint max_pkt = connack.GetMaximumPacketSize();
  m_transport.SetMaxPacketSize(max_pkt);
  if (max_pkt > 0) {
    m_context.flow_control.SetMaximumPacketSize(max_pkt);
  }

  //--- Apply Receive Maximum (flow control window) per §3.2.2.3.3
  ushort recv_max = connack.GetReceiveMaximum();
  //--- Per §3.2.2.3.3, Receive Maximum = 0 is a Protocol Error.
  //--- The server MUST NOT send a value of 0.
  if (recv_max == 0) {
    MQTT_LOG_ERROR("Server sent Receive Maximum = 0 — Protocol Error per §3.2.2.3.3");
    CDisconnect disc;
    disc.SetReasonCode(0x82);  // Protocol Error
    uchar disc_buf[];
    disc.Build(disc_buf);
    m_transport.Send(disc_buf);
    _OnTransportError(0x82, "Protocol Error: server Receive Maximum is 0 per §3.2.2.3.3");
    return;
  }
  if (recv_max > 0) {
    m_context.flow_control.SetReceiveMaximum(recv_max);
    //--- After a reconnect to a server that advertises a LOWER Receive
    //--- Maximum than the previous connection, the existing in-flight message count
    //--- may already exceed the new window. This would cause _RunRetransmissions to
    //--- emit a burst of packets that violates the server's flow control window and
    //--- trigger a Protocol Error disconnect (§4.9).
    //---
    //--- We detect this condition and log a warning here. The retransmission
    //--- manager checks CanSendQoSMessage() per packet, so retransmissions will
    //--- naturally be throttled as the window frees up via incoming ACKs.
    //--- We do NOT forcibly purge in-flight entries because those messages may
    //--- need re-delivery; instead we rely on the flow control guard in
    //--- _RunRetransmissions / RegisterOutgoingQoS to prevent over-sending.
    uint inflight = m_context.flow_control.GetInFlightCount();
    if (inflight > (uint)recv_max) {
      MQTT_LOG_WARN("P1-5: Reconnected server Receive Maximum (" + (string)recv_max
                    + ") is smaller than current in-flight count (" + (string)inflight
                    + "). Retransmissions will be throttled to respect the new window.");
    }
  }
  m_connack_receive_maximum   = recv_max;

  //--- Store server capabilities
  m_server_max_qos            = connack.GetMaximumQoS();
  m_server_retain_available   = connack.IsRetainAvailable();
  m_server_wildcard_available = connack.IsWildcardSubscriptionAvailable();
  m_server_sub_id_available   = connack.IsSubscriptionIdentifierAvailable();
  m_server_shared_available   = connack.IsSharedSubscriptionAvailable();

  //--- Apply Topic Alias Maximum
  ushort topic_alias_max      = connack.GetTopicAliasMaximum();
  m_context.topic_alias_manager.SetTopicAliasMaximum(topic_alias_max);

  string connack_auth_method = connack.GetAuthenticationMethod();
  if (connack_auth_method != "") {
    if (m_connect_auth_method == "") {
      MQTT_LOG_ERROR("CONNACK included Authentication Method without CONNECT Authentication Method per MQTT §4.12");
      CDisconnect disc;
      disc.SetReasonCode(MQTT_REASON_CODE_PROTOCOL_ERROR);
      uchar disc_buf[];
      disc.Build(disc_buf);
      m_transport.Send(disc_buf);
      _OnTransportError(MQTT_REASON_CODE_PROTOCOL_ERROR,
                        "Protocol Error: CONNACK Authentication Method received without CONNECT Authentication Method");
      return;
    }
    if (connack_auth_method != m_connect_auth_method) {
      MQTT_LOG_ERROR("CONNACK Authentication Method does not match CONNECT Authentication Method per MQTT §4.12");
      CDisconnect disc;
      disc.SetReasonCode(MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD);
      uchar disc_buf[];
      disc.Build(disc_buf);
      m_transport.Send(disc_buf);
      _OnTransportError(MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD,
                        "Bad Authentication Method: CONNACK Authentication Method mismatch");
      return;
    }
    m_active_auth_method = connack_auth_method;
  } else {
    m_active_auth_method = m_connect_auth_method;
  }

  m_connect_deadline_ms = 0;
  m_connack_deadline_ms = 0;  // Cancel CONNACK timeout
  bool session_present  = connack.IsSessionPresent();
  bool has_local_state  = _HasLocalSessionState();

  if (session_present && m_clean_start) {
    MQTT_LOG_ERROR(
      "Broker reported Session Present=1 but Clean Start was requested — closing connection per MQTT §3.2.2.4");
    CDisconnect disc;
    disc.SetReasonCode(MQTT_REASON_CODE_PROTOCOL_ERROR);
    uchar disc_buf[];
    disc.Build(disc_buf);
    m_transport.Send(disc_buf);
    _OnTransportError(MQTT_REASON_CODE_PROTOCOL_ERROR,
                      "Protocol Error: Session Present=1 when Clean Start was requested");
    return;
  }
  //--- After an EA restart the in-memory state is empty even though the broker kept the session.
  //--- Disconnecting here would create a permanent reconnect loop until the broker session expires.
  //--- Accept the connection instead — the broker will retransmit any unacknowledged QoS 1/2
  //--- messages so the local state re-syncs naturally per the spec.
  if (session_present && !has_local_state) {
    MQTT_LOG_WARN("Broker has session but no local state — accepting connection and re-syncing");
  }

  //--- Persist server-assigned Client Identifier (§3.2.2.3.7)
  //--- If the broker assigned a client ID (because we sent an empty one), store it
  //--- so future reconnections and session-key lookups use the same identity.
  string assigned_id                   = connack.GetAssignedClientIdentifier();
  m_connack_assigned_client_identifier = assigned_id;
  if (assigned_id != "") {
    MQTT_LOG_INFO("Server assigned Client Identifier: " + assigned_id);
    m_client_id = assigned_id;
  }

  _SetEffectiveSessionExpiry(connack.HasSessionExpiry(), connack.GetSessionExpiryInterval());

  _SetState(MQTT_CLIENT_CONNECTED);
  m_has_successful_connection = true;
  m_connected_since_ms        = GetMicrosecondCount() / 1000;
  m_active_connect_timeout_ms = 0;
  m_reconnect_policy.OnSuccessfulConnect();
  //--- Persist circuit-breaker reset so an EA restart during a broker
  //--- outage begins from 0 consecutive failures, not a stale counter.
  m_context.session_db.SetReconnectFailureCount(0);
  //--- Reset incoming storage error counter on reconnect success.
  m_incoming_storage_error_count = 0;
  m_context.session_db.SetIncomingStorageErrorCount(0);
  MQTT_LOG_INFO("CONNACK success — session_present=" + (string)session_present);
  if (m_connack_user_prop_count > 0) {
    MQTT_LOG_DEBUG("CONNACK carried " + (string)m_connack_user_prop_count + " user propert"
                   + (m_connack_user_prop_count == 1 ? "y" : "ies") + ".");
  }

  //--- Per §3.2.2.1.1, when server returns Session Present = 0 and client
  //--- connected with Clean Start = 0, the server has no session state.
  //--- The client MUST discard its local session state.
  if (!session_present && !m_clean_start) {
    _ClearLocalSessionState();
    MQTT_LOG_INFO("Server has no session — clearing local session state per §3.2.2.1.1");
  }

  _UpdateEffectiveTrustMode();

  //--- TOFU certificate pinning
  //--- Verify the presented TLS certificate against the pre-provisioned or
  //--- previously persisted MT5 certificate thumbprint.
  if (m_tofu_enabled && m_use_tls) {
    string   cert_subject = "";
    string   cert_issuer  = "";
    string   cert_serial  = "";
    string   cert_thumb   = "";
    datetime cert_expire  = 0;
    //--- SocketTlsCertificate retrieves the server's TLS certificate details
    if (!_EvaluateTofuCertificate(SocketTlsCertificate(m_transport.GetSocket(), cert_subject, cert_issuer, cert_serial,
                                                       cert_thumb, cert_expire),
                                  cert_thumb)) {
      return;
    }
  }

  //--- Subscription replay on reconnect.
  //--- Per §4.1, when session_present=true the broker already holds all subscription
  //--- state, so a replay is redundant and wastes bandwidth + packet IDs.
  //--- Replay is only skipped when session_present=true AND clean_start=false.
  //--- Call SetAlwaysReplaySubscriptions(true) to restore unconditional replay
  //--- (e.g., for bridging scenarios or conservative interop with non-compliant brokers).
  //--- Replay must complete before the on_connect callback fires so that any new
  //--- Subscribe() calls inside the callback are not replayed a second time.
  if (!session_present || m_clean_start || m_always_replay_subscriptions) {
    _ReplaySubscriptions();
  } else {
    MQTT_LOG_DEBUG("Session resumed (session_present=true) — skipping subscription replay per §4.1.");
  }

  //--- Fire the connect callback after subscription replay so that Subscribe() calls
  //--- inside the callback are not double-subscribed by a subsequent replay.
  if (m_on_connect != NULL) {
    m_on_connect(session_present);
    _SyncLogger();  // Restore this instance's log config after callback
  }

  //--- Re-transmit any pending QoS 1/2 messages from session store
  _RunRetransmissions(0);

  //--- Drain backpressure queue
  _DrainPublishQueue();
}

//+------------------------------------------------------------------+
//| _OnPublishReceived                                               |
//| Purpose: Parse incoming PUBLISH and dispatch to callbacks        |
//| Parameters: pkt - [IN/OUT] raw packet buffer                     |
//+------------------------------------------------------------------+
void CMqttClient::_OnPublishReceived(uchar& pkt[]) {
  if (ArraySize(pkt) > 0) {
    uchar header_qos = (pkt[0] >> 1) & 0x03;
    if (header_qos == 0x03) {
      _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET,
                          "Incoming PUBLISH used forbidden QoS 3 bits per MQTT §3.3.1.1");
      return;
    }
  }

  CPublish pub;
  int      err = pub.Read(pkt, &m_context.topic_alias_manager);
  if (err != MQTT_OK) {
    uchar reason_code =
      (err == MQTT_ERROR_PROTOCOL_VIOLATION) ? MQTT_REASON_CODE_PROTOCOL_ERROR : MQTT_REASON_CODE_MALFORMED_PACKET;
    _ProtocolDisconnect(reason_code, "Incoming PUBLISH parse error: " + MqttErrorToString((ENUM_MQTT_ERROR)err));
    return;
  }

  string topic     = pub.GetTopicName();
  uchar  qos       = pub.GetQoS();
  bool   retain_f  = pub.GetRetain();
  ushort packet_id = pub.GetPacketId();

  //--- Enforce Topic Alias Maximum boundary per §3.3.2.3.4.
  //--- If the client never advertised Topic Alias support (m_client_topic_alias_max == 0),
  //--- any incoming PUBLISH with a Topic Alias is a Protocol Error.
  //--- If the alias exceeds the client's advertised maximum, that is also a Protocol Error.
  if (pub.HasTopicAlias()) {
    ushort incoming_alias = pub.GetTopicAlias();
    if (m_client_topic_alias_max == 0) {
      _ProtocolDisconnect(MQTT_REASON_CODE_TOPIC_ALIAS_INVALID,
                          "Server sent Topic Alias but client Topic Alias Maximum is 0 per §3.3.2.3.4");
      return;
    }
    if (incoming_alias > m_client_topic_alias_max) {
      _ProtocolDisconnect(MQTT_REASON_CODE_TOPIC_ALIAS_INVALID,
                          "Server Topic Alias " + (string)incoming_alias + " exceeds client maximum "
                            + (string)m_client_topic_alias_max + " per §3.3.2.3.4");
      return;
    }
  }

  //--- Enforce incoming Receive Maximum for QoS1/2 messages
  if (qos > QoS_0) {
    if (packet_id == 0) {
      _ProtocolDisconnect(MQTT_REASON_CODE_PROTOCOL_ERROR, "Incoming QoS publish without packet id");
      return;
    }
    //--- Use tri-state result to distinguish a genuine DUP retransmission
    //--- (REG_DUPLICATE — packet ID already in our incoming in-flight table) from a
    //--- window-full condition (REG_WINDOW_FULL — broker violated our Receive Maximum).
    ENUM_REG_QOS_RESULT reg_result = m_context.flow_control.RegisterIncomingQoS(packet_id, qos, (uint)ArraySize(pkt));
    if (reg_result == REG_DUPLICATE) {
      //--- Genuine DUP retransmission — slot still in-flight from previous delivery.
      //--- Re-send PUBACK (QoS 1) or PUBREC (QoS 2) to satisfy the broker, but
      //--- suppress callback delivery to prevent duplicate trade executions.
      if (qos == QoS_1) {
        _SendPubackPacket(packet_id);
        MQTT_LOG_DEBUG("QoS 1 DUP suppressed for packet ID " + (string)packet_id
                       + " — slot in-flight, re-sent PUBACK without callback delivery.");
      } else {
        //--- QoS 2: re-send PUBREC without re-delivering per §3.3.1.1
        _SendPubrecPacket(packet_id);
        MQTT_LOG_DEBUG("QoS 2 DUP suppressed for packet ID " + (string)packet_id
                       + " — slot in-flight, re-sent PUBREC without callback delivery.");
      }
      return;  // Suppress duplicate delivery
    }
    if (reg_result == REG_WINDOW_FULL) {
      //--- Broker violated the Receive Maximum we advertised in CONNECT.
      //--- Must disconnect with 0x93 per §4.9.1 regardless of DUP flag.
      _ProtocolDisconnect(MQTT_REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED, "Incoming Receive Maximum exceeded");
      return;
    }
    // REG_OK — fall through to normal delivery
  }

  //--- QoS acknowledgement dispatch
  if (qos == QoS_1 && packet_id > 0) {
    ENUM_TRANSPORT_ERROR ack_err = _SendPubackPacket(packet_id);
    if (ack_err != TRANSPORT_OK) {
      _OnTransportError((int)ack_err, "Failed to send PUBACK");
      return;
    }
    m_context.flow_control.ReleaseIncomingQoS(packet_id);
  } else if (qos == QoS_2 && packet_id > 0) {
    //--- Per §3.3.1.1, when a QoS 2 PUBLISH with DUP=1
    //--- is received and the packet ID is already in the session DB (incoming),
    //--- we MUST NOT deliver the message to the application callback again.
    //--- Instead, re-send PUBREC for the existing packet ID.
    SessionMessage existing;
    if (m_context.session_db.GetMessage(packet_id, existing, false)) {
      //--- Duplicate QoS2 PUBLISH (broker retransmit with DUP=1): re-send PUBREC.
      //--- RegisterIncomingQoS returned REG_OK here (the slot WAS incremented),
      //--- but the DB entry already exists from a previous delivery attempt that
      //--- survived a reconnect with non-persistent flow-control state. The slot
      //--- will be released by ReleaseIncomingQoS when PUBREL is received, so
      //--- we do NOT release it here.
      _SendPubrecPacket(packet_id);
      return;  // Suppress duplicate delivery to application
    }

    uchar payload_copy[];
    uchar publish_properties[];
    pub.GetPayloadBytes(payload_copy);
    pub.GetParsedPropertiesRaw(publish_properties);
    if (!m_context.session_db.StoreIncomingMessage(packet_id, qos, topic, payload_copy, (uint)ArraySize(payload_copy),
                                                   retain_f, publish_properties)) {
      m_context.flow_control.ReleaseIncomingQoS(packet_id);
      //--- Incoming storage circuit breaker.
      //--- A simple _OnTransportError here causes disconnect → auto-reconnect → same
      //--- broker PUBLISH → same disk-full failure → infinite reconnect loop.
      //--- Instead, increment a counter and break the circuit after N consecutive
      //--- failures so the EA can alert the trader and recover manually.
      m_incoming_storage_error_count++;
      m_context.session_db.SetIncomingStorageErrorCount(m_incoming_storage_error_count);
      if (m_incoming_storage_error_max > 0 && m_incoming_storage_error_count >= m_incoming_storage_error_max) {
        MQTT_LOG_ERROR("Incoming storage circuit breaker tripped (" + (string)m_incoming_storage_error_count
                       + " consecutive failures). Stopping reconnection. Free disk space and restart EA.");
        m_reconnect_policy.Stop();
        m_reconnect_policy.Disable();  // Prevent further auto-reconnect until EA restarts
        _FireErrorEx(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                     "Incoming QoS2 persistence failed " + (string)m_incoming_storage_error_count
                       + " times — circuit broken. Resolve disk issue and restart EA.",
                     __FILE__, __LINE__, __FUNCTION__);
        _OnTransportError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                          "Incoming QoS2 storage circuit breaker tripped");
      } else {
        MQTT_LOG_WARN("Failed to persist incoming QoS2 message (failure " + (string)m_incoming_storage_error_count + "/"
                      + (m_incoming_storage_error_max > 0 ? (string)m_incoming_storage_error_max : "unlimited")
                      + "). Disconnecting for retry.");
        _OnTransportError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, "Failed to persist incoming QoS2 message");
      }
      return;
    }

    MQTT_LOG_INFO("Stored incoming QoS2 PUBLISH awaiting PUBREL for packet ID " + (string)packet_id + " on topic '"
                  + topic + "'.");

    ENUM_TRANSPORT_ERROR rec_err = _SendPubrecPacket(packet_id);
    if (rec_err != TRANSPORT_OK) {
      _OnTransportError((int)rec_err, "Failed to send PUBREC");
      return;
    }

    MQTT_LOG_INFO("Sent PUBREC for incoming QoS2 packet ID " + (string)packet_id + ".");
    return;  // Deliver QoS2 to app only after PUBREL/PUBCOMP boundary
  }

  //--- Validate UTF-8 payload when Payload Format Indicator = 1 per §3.3.2.3.2
  //--- Per spec, client SHOULD verify payload is valid UTF-8.
  if (pub.HasPayloadFormat() && pub.GetPayloadFormatIndicator() == 1) {
    uchar raw_payload[];
    pub.GetPayloadBytes(raw_payload);
    int raw_len = ArraySize(raw_payload);
    if (raw_len > 0) {
      if (ValidateUtf8Data(raw_payload, 0, (uint)raw_len) != MQTT_OK) {
        //--- Per §3.3.2.3.2 the receiver SHOULD (not MUST) close the connection with 0x99.
        //--- Strict mode is the default. SetStrictUtf8Validation(false) opts into
        //--- warn-and-deliver behaviour for permissive broker interoperability.
        if (m_strict_utf8_validation) {
          MQTT_LOG_WARN("Payload Format Indicator is 1 (UTF-8) but payload failed UTF-8 validation per "
                        "§3.3.2.3.2. Sending DISCONNECT 0x99 (strict mode on).");
          _ProtocolDisconnect(0x99, "Payload Format Invalid per §3.3.2.3.2");
          return;
        } else {
          MQTT_LOG_WARN("Payload Format Indicator is 1 (UTF-8) but payload failed UTF-8 validation per "
                        "§3.3.2.3.2 (strict mode off — delivering anyway). "
                        "Call SetStrictUtf8Validation(true) to disconnect on violation.");
        }
      }
    }
  }

  //--- NOTE — m_messages_received is incremented HERE (after UTF-8 validation) for QoS 0/1
  //--- but at the PUBREL boundary for QoS 2 (see the PUBREL handler in Poll()). Incrementing
  //--- after validation ensures rejected messages (strict UTF-8 mode) are not counted.
  m_messages_received++;

  //--- Fire message callback
  uchar payload[];
  uchar publish_properties[];
  pub.GetPayloadBytes(payload);
  pub.GetParsedPropertiesRaw(publish_properties);
  int  payload_len   = ArraySize(payload);

  //--- Trie-based per-topic callback dispatch (O(topic-depth))
  //--- Delivers to ALL matching subscriptions per §4.7.1.2.
  //--- Per-topic callbacks fire for each matching subscription that has one.
  //--- Global m_on_message fires exactly once for NULL-callback trie matches.
  bool dispatched    = false;
  bool global_fired  = false;
  //--- Use pre-allocated member scratch buffer — avoids GC alloc each message
  uint matched_count = 0;
  m_topic_matcher.Match(topic, m_match_scratch, matched_count);
  for (uint mi = 0; mi < matched_count; mi++) {
    uint i = m_match_scratch[mi];
    if (i < m_sub_count) {
      uint dispatch_sub_id = (i < (uint)ArraySize(m_sub_id)) ? m_sub_id[i] : 0;
      if (m_sub_cb[i] != NULL) {
        _QueueMessageCallback(m_sub_cb[i], topic, payload, payload_len, qos, retain_f, packet_id, dispatch_sub_id,
                              publish_properties);
        dispatched = true;
      } else if (!global_fired && m_on_message != NULL) {
        //--- Subscription uses global handler — fire it exactly once per message
        _QueueMessageCallback(m_on_message, topic, payload, payload_len, qos, retain_f, packet_id, dispatch_sub_id,
                              publish_properties);
        dispatched   = true;
        global_fired = true;
      }
    }
  }

  //--- Fallback: no trie match at all — fire global handler if configured
  if (!dispatched && m_on_message != NULL) {
    _QueueMessageCallback(m_on_message, topic, payload, payload_len, qos, retain_f, packet_id, 0, publish_properties);
  }
}

//+------------------------------------------------------------------+
//| _OnDisconnectReceived                                            |
//| Purpose: Process incoming DISCONNECT from broker (§3.14)         |
//| Parameters: pkt - [IN/OUT] raw packet buffer                     |
//+------------------------------------------------------------------+
void CMqttClient::_OnDisconnectReceived(uchar& pkt[]) {
  CDisconnect disc;
  int         err         = disc.Read(pkt);
  uchar       reason_code = 0x00;
  string      reason_str  = "Broker initiated disconnect";
  string      user_prop_keys[];
  string      user_prop_vals[];
  uint        user_prop_count = 0;

  if (err == MQTT_OK) {
    reason_code = disc.GetReasonCode();
    string rs   = disc.GetReasonString();
    if (rs != "") {
      reason_str = rs;
    }

    user_prop_count = disc.GetUserPropertyCount();
    ArrayResize(user_prop_keys, user_prop_count);
    ArrayResize(user_prop_vals, user_prop_count);
    for (uint i = 0; i < user_prop_count; i++) {
      user_prop_keys[i] = disc.GetUserPropertyKey(i);
      user_prop_vals[i] = disc.GetUserPropertyValue(i);
    }
  }

  _CacheDisconnectMetadata(reason_code, reason_str, (err == MQTT_OK) ? disc.GetServerReference() : "", user_prop_keys,
                           user_prop_vals, user_prop_count);

  MQTT_LOG_INFO("Broker DISCONNECT — reason=0x" + StringFormat("%02X", reason_code) + " (" + reason_str + ")");

  m_transport.Disconnect();
  _ClearDeferredTransportPackets();
  _ClearMessageCallbacks();
  _HandleConnectionClosed(disc.HasSessionExpiry(), disc.GetSessionExpiryInterval());
  m_context.OnDisconnect();

  //--- Set state BEFORE firing callbacks so IsConnected() returns false
  //--- inside the callback, preventing reentrancy / stack-overflow if the
  //--- callback tries to Publish() or Subscribe().
  _SetState(MQTT_CLIENT_DISCONNECTED);

  if (m_on_disconnect != NULL) {
    m_on_disconnect((int)reason_code, reason_str, m_last_disconnect_server_reference, m_last_disconnect_user_prop_keys,
                    m_last_disconnect_user_prop_vals, (int)m_last_disconnect_user_prop_count);
    _SyncLogger();
  }

  //--- Check for server redirection in DISCONNECT
  if (err == MQTT_OK
      && (reason_code == MQTT_REASON_CODE_USE_ANOTHER_SERVER || reason_code == MQTT_REASON_CODE_SERVER_MOVED)) {
    string server_ref = disc.GetServerReference();
    if (server_ref != "" && _HandleRedirection((int)reason_code, server_ref)) {
      return;  // Redirect set up — Poll() will execute the reconnect
    }
    // Fall through to auto-reconnect if redirect failed or server reference was absent
  }

  //--- Enter reconnect mode only for transient failures.
  //--- Permanent failures (bad credentials, banned, etc.) must NOT trigger
  //--- auto-reconnect — repeated attempts would cause broker IP-bans.
  if (m_reconnect_policy.IsEnabled() && !_IsPermanentFailure(reason_code)) {
    m_reconnect_policy.StartLoopIfNeeded();
    //--- If already reconnecting, preserve existing backoff state
  } else if (_IsPermanentFailure(reason_code)) {
    MQTT_LOG_ERROR("Permanent DISCONNECT failure (0x" + StringFormat("%02X", reason_code)
                   + ") — auto-reconnect disabled to prevent broker IP-ban.");
  }
}

//+------------------------------------------------------------------+
//| _IsPermanentFailure                                              |
//| Purpose: Determine if a reason code represents a permanent       |
//|          failure where auto-reconnect MUST be suppressed.        |
//|          Reconnecting on these codes risks broker IP-bans.       |
//+------------------------------------------------------------------+
bool CMqttClient::_IsPermanentFailure(uchar reason_code) {
  switch (reason_code) {
    case MQTT_REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION:  // 0x84
    case MQTT_REASON_CODE_CLIENT_IDENTIFIER_NOT_VALID:   // 0x85
    case MQTT_REASON_CODE_BAD_USER_NAME_OR_PASSWORD:     // 0x86
    case MQTT_REASON_CODE_NOT_AUTHORIZED:                // 0x87
    case MQTT_REASON_CODE_BANNED:                        // 0x8A
    case MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD:     // 0x8C
      return true;
    default:
      return false;
  }
}

//+------------------------------------------------------------------+
//| _SanitizeSessionKey                                              |
//| Purpose: Strip path-traversal and shell-injection characters     |
//|          from broker-derived strings before they are embedded    |
//|          in filenames (session DB, TOFU pin file).               |
//|          Accepts: A-Z a-z 0-9 '-' '_' '.' '@'                    |
//|          Replaces everything else with '_'.                      |
//| Returns: sanitized string, or "default_session" when empty.      |
//+------------------------------------------------------------------+
string CMqttClient::_SanitizeSessionKey(const string key) const {
  string out = "";
  int    len = StringLen(key);
  for (int i = 0; i < len; i++) {
    ushort c = StringGetCharacter(key, i);
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
        || c == '@') {
      out += ShortToString(c);
    } else {
      out += "_";  // Replace path-traversal and control characters
    }
  }
  return (StringLen(out) > 0) ? out : "default_session";
}

//+------------------------------------------------------------------+
//| _TryGetOutgoingAckMessage                                        |
//| Purpose: Accept only ACKs that still belong to a live outgoing   |
//|          message in the expected QoS/state transition            |
//+------------------------------------------------------------------+
bool CMqttClient::_TryGetOutgoingAckMessage(const string ack_name, ushort packet_id, uchar expected_qos,
                                            bool require_state, ENUM_QOS2_STATE expected_state,
                                            SessionMessage& out_msg) {
  if (!m_context.session_db.GetMessage(packet_id, out_msg, true)) {
    MQTT_LOG_WARN(ack_name + " for unknown packet ID " + (string)packet_id + " ignored.");
    return false;
  }
  if (!out_msg.is_outgoing) {
    MQTT_LOG_WARN(ack_name + " for incoming packet ID " + (string)packet_id + " ignored.");
    return false;
  }
  if (out_msg.qos_level != expected_qos) {
    MQTT_LOG_WARN(ack_name + " for packet ID " + (string)packet_id + " ignored due to unexpected QoS "
                  + (string)out_msg.qos_level + ".");
    return false;
  }
  if (require_state && out_msg.qos2_state != expected_state) {
    MQTT_LOG_WARN(ack_name + " for packet ID " + (string)packet_id + " ignored in QoS 2 state "
                  + EnumToString(out_msg.qos2_state) + " (expected " + EnumToString(expected_state) + ").");
    return false;
  }
  return true;
}

//+------------------------------------------------------------------+
//| _OnSubackReceived                                                |
//+------------------------------------------------------------------+
void CMqttClient::_OnSubackReceived(uchar& pkt[]) {
  CSuback suback;
  int     err = suback.Read(pkt);
  if (err != MQTT_OK) {
    uchar reason_code = (err == MQTT_ERROR_PROTOCOL_VIOLATION || err == MQTT_ERROR_INVALID_REASON_CODE) ?
                          MQTT_REASON_CODE_PROTOCOL_ERROR :
                          MQTT_REASON_CODE_MALFORMED_PACKET;
    _ProtocolDisconnect(reason_code, "SUBACK parse error");
    return;
  }

  ushort packet_id              = suback.GetPacketIdentifier();
  m_last_suback_packet_id       = packet_id;
  m_last_suback_reason_string   = suback.GetReasonString();
  m_last_suback_user_prop_count = suback.GetUserPropertyCount();
  ArrayResize(m_last_suback_user_prop_keys, m_last_suback_user_prop_count);
  ArrayResize(m_last_suback_user_prop_vals, m_last_suback_user_prop_count);
  for (uint i = 0; i < m_last_suback_user_prop_count; i++) {
    m_last_suback_user_prop_keys[i] = suback.GetUserPropertyKey(i);
    m_last_suback_user_prop_vals[i] = suback.GetUserPropertyValue(i);
  }
  uchar reason_codes[];
  suback.GetReasonCodes(reason_codes);
  int count = ArraySize(reason_codes);

  //--- Correlate SUBACK with pending replay subscriptions.
  //--- If this packet_id matches a replayed SUBSCRIBE batch, remove
  //--- subscriptions whose reason code indicates failure (>= 0x80).
  for (uint p = 0; p < m_pending_replay_count; p++) {
    if (m_prs_pkt_id[p] == packet_id) {
      if (count != (int)m_prs_tcount[p]) {
        MQTT_LOG_ERROR("SUBACK reason code count " + (string)count + " does not match replayed topic count "
                       + (string)m_prs_tcount[p] + ". Closing connection.");
        m_context.session_db.ReleasePacketId(packet_id);
        _ProtocolDisconnect(MQTT_REASON_CODE_PROTOCOL_ERROR,
                            "SUBACK reason code count does not match replayed topic count");
        return;
      }
      for (int rc = 0; rc < count && rc < (int)m_prs_tcount[p]; rc++) {
        if (reason_codes[rc] >= 0x80) {
          //--- Server refused this subscription — remove from parallel arrays and compact topic index
          string failed_topic = m_prs_topics[m_prs_toff[p] + rc];
          for (uint s = 0; s < m_sub_count; s++) {
            if (m_sub_topic[s] == failed_topic) {
              MQTT_LOG_WARN("Replay subscription refused for '" + failed_topic + "' (reason=0x"
                            + StringFormat("%02X", reason_codes[rc]) + "). Removing.");
              //--- Remove from compact index and trie
              _SubIndexRemove(failed_topic);
              m_topic_matcher.RemoveFilter(failed_topic);
              uint last_idx = m_sub_count - 1;
              if (s < last_idx) {
                //--- The last entry will be swapped into position s; update its
                //--- compact index entry and trie to point to the new index.
                _SubIndexSet(m_sub_topic[last_idx], s);
                m_topic_matcher.RemoveSubIndex(last_idx);
                m_topic_matcher.AddFilter(m_sub_topic[last_idx], s);
              }
              m_sub_topic[s]    = m_sub_topic[last_idx];
              m_sub_qos[s]      = m_sub_qos[last_idx];
              m_sub_cb[s]       = m_sub_cb[last_idx];
              m_sub_id[s]       = m_sub_id[last_idx];
              m_sub_no_local[s] = m_sub_no_local[last_idx];
              m_sub_rap[s]      = m_sub_rap[last_idx];
              m_sub_rh[s]       = m_sub_rh[last_idx];
              m_sub_utf8_len[s] = m_sub_utf8_len[last_idx];
              //--- Swap-with-last: if the removed position falls within the already-
              //--- replayed range, the swapped-in entry would be skipped. Adjust.
              if (m_replay_in_progress && s < m_replay_next_index) {
                m_replay_next_index--;
              }
              m_sub_count--;
              ArrayResize(m_sub_topic, m_sub_count);
              ArrayResize(m_sub_qos, m_sub_count);
              ArrayResize(m_sub_cb, m_sub_count);
              ArrayResize(m_sub_id, m_sub_count);
              ArrayResize(m_sub_no_local, m_sub_count);
              ArrayResize(m_sub_rap, m_sub_count);
              ArrayResize(m_sub_rh, m_sub_count);
              ArrayResize(m_sub_utf8_len, m_sub_count);
              break;
            }
          }
        }
      }
      //--- Remove this PRS entry by swapping with last
      uint last_prs   = m_pending_replay_count - 1;
      m_prs_pkt_id[p] = m_prs_pkt_id[last_prs];
      m_prs_tcount[p] = m_prs_tcount[last_prs];
      m_prs_toff[p]   = m_prs_toff[last_prs];
      m_pending_replay_count--;
      ArrayResize(m_prs_pkt_id, m_pending_replay_count);
      ArrayResize(m_prs_tcount, m_pending_replay_count);
      ArrayResize(m_prs_toff, m_pending_replay_count);
      //--- Rebuild the flat topic array to reclaim orphaned strings left behind by
      //--- the swap-with-last removal. Without this, strings from removed entries
      //--- accumulate in m_prs_topics[] until the full replay cycle completes.
      if (m_pending_replay_count == 0) {
        ArrayResize(m_prs_topics, 0);
      } else {
        //--- Enumerate live entries, copy their topics to new_topics[], update offsets
        uint new_size = 0;
        for (uint q = 0; q < m_pending_replay_count; q++) {
          new_size += m_prs_tcount[q];
        }
        string new_topics[];
        ArrayResize(new_topics, new_size);
        uint off = 0;
        for (uint q = 0; q < m_pending_replay_count; q++) {
          for (uint t = 0; t < m_prs_tcount[q]; t++) {
            new_topics[off + t] = m_prs_topics[m_prs_toff[q] + t];
          }
          m_prs_toff[q]  = off;
          off           += m_prs_tcount[q];
        }
        ArrayResize(m_prs_topics, new_size);
        for (uint q = 0; q < new_size; q++) {
          m_prs_topics[q] = new_topics[q];
        }
      }
      break;
    }
  }

  //--- Release the packet ID used by SUBSCRIBE back to the pool
  m_context.session_db.ReleasePacketId(packet_id);
  //--- Notify packet ID pool monitoring callback if active
  if (m_on_packetid_low != NULL && m_packetid_low_threshold > 0) {
    uint avail = m_context.session_db.GetAvailablePacketIdCount();
    if (avail <= m_packetid_low_threshold) {
      m_on_packetid_low(avail, m_packetid_low_threshold);
      _SyncLogger();  // Restore this instance's log config after callback
    }
  }

  //--- Fire user callback
  if (m_on_suback != NULL) {
    m_on_suback(packet_id, reason_codes, count, m_last_suback_reason_string, m_last_suback_user_prop_keys,
                m_last_suback_user_prop_vals, (int)m_last_suback_user_prop_count);
    _SyncLogger();
  }
}
//+------------------------------------------------------------------+
void CMqttClient::_OnUnsubackReceived(uchar& pkt[]) {
  CUnsuback unsuback;
  int       err = unsuback.Read(pkt);
  if (err != MQTT_OK) {
    uchar reason_code = (err == MQTT_ERROR_PROTOCOL_VIOLATION || err == MQTT_ERROR_INVALID_REASON_CODE) ?
                          MQTT_REASON_CODE_PROTOCOL_ERROR :
                          MQTT_REASON_CODE_MALFORMED_PACKET;
    _ProtocolDisconnect(reason_code, "UNSUBACK parse error");
    return;
  }

  ushort packet_id                = unsuback.GetPacketIdentifier();
  m_last_unsuback_packet_id       = packet_id;
  m_last_unsuback_reason_string   = unsuback.GetReasonString();
  m_last_unsuback_user_prop_count = unsuback.GetUserPropertyCount();
  ArrayResize(m_last_unsuback_user_prop_keys, m_last_unsuback_user_prop_count);
  ArrayResize(m_last_unsuback_user_prop_vals, m_last_unsuback_user_prop_count);
  for (uint i = 0; i < m_last_unsuback_user_prop_count; i++) {
    m_last_unsuback_user_prop_keys[i] = unsuback.GetUserPropertyKey(i);
    m_last_unsuback_user_prop_vals[i] = unsuback.GetUserPropertyValue(i);
  }
  uchar reason_codes[];
  unsuback.GetReasonCodes(reason_codes);
  int count = ArraySize(reason_codes);

  //--- Release the packet ID used by UNSUBSCRIBE back to the pool
  m_context.session_db.ReleasePacketId(packet_id);
  //--- Notify packet ID pool monitoring callback if active
  if (m_on_packetid_low != NULL && m_packetid_low_threshold > 0) {
    uint avail = m_context.session_db.GetAvailablePacketIdCount();
    if (avail <= m_packetid_low_threshold) {
      m_on_packetid_low(avail, m_packetid_low_threshold);
      _SyncLogger();  // Restore this instance's log config after callback
    }
  }

  if (m_on_unsuback != NULL) {
    m_on_unsuback(packet_id, reason_codes, count, m_last_unsuback_reason_string, m_last_unsuback_user_prop_keys,
                  m_last_unsuback_user_prop_vals, (int)m_last_unsuback_user_prop_count);
    _SyncLogger();
  }

  for (uint i = 0; i < m_pending_unsub_count; i++) {
    if (m_punsub_pkt_id[i] != packet_id) {
      continue;
    }

    if (count == 0) {
      //--- UNSUBACK with no reason codes violates §3.11.3 (one required per topic filter).
      //--- Preserve the local subscription rather than silently removing it without broker confirmation.
      MQTT_LOG_WARN("UNSUBACK for packet ID " + (string)packet_id
                    + " has no reason codes — local subscription preserved.");
      break;
    }

    bool remove_local = true;
    for (int r = 0; r < count; r++) {
      uchar rc = reason_codes[r];
      if (rc >= 0x80) {
        remove_local = false;
        MQTT_LOG_WARN("UNSUBACK rejected local removal for " + m_punsub_topic[i] + " — broker reason code 0x"
                      + StringFormat("%02X", rc) + ".");
        break;
      }
    }

    if (remove_local) {
      _RemoveSubscriptionLocal(m_punsub_topic[i]);
    }

    uint last = m_pending_unsub_count - 1;
    if (i != last) {
      m_punsub_pkt_id[i] = m_punsub_pkt_id[last];
      m_punsub_topic[i]  = m_punsub_topic[last];
    }
    m_pending_unsub_count--;
    ArrayResize(m_punsub_pkt_id, m_pending_unsub_count);
    ArrayResize(m_punsub_topic, m_pending_unsub_count);
    break;
  }
}
//+------------------------------------------------------------------+
//| _OnTransportError                                                |
//| Enter reconnection mode after a fatal transport error.           |
//+------------------------------------------------------------------+
void CMqttClient::_OnTransportError(int err_code, string err_desc) {
  //--- Capture previous state before any mutation for disconnect callback guard
  ENUM_MQTT_CLIENT_STATE prev_state = m_state;
  m_abort_current_poll              = true;
  m_active_auth_method              = "";
  m_replay_in_progress              = false;
  m_replay_next_index               = 0;
  m_transport.Disconnect();
  _ClearDeferredTransportPackets();
  _ClearMessageCallbacks();
  //--- Reset to the correct internal transport based on m_transport_type
  //--- so subsequent Connect() / auto-reconnect uses the right transport object.
  if (m_transport_type == TRANSPORT_WS) {
    m_transport = GetPointer(m_ws_transport);
  } else {
    m_transport = GetPointer(m_tcp_transport);
  }
  _HandleConnectionClosed();
  m_context.OnDisconnect();
  m_connect_deadline_ms = 0;
  m_connack_deadline_ms = 0;

  //--- Set state to DISCONNECTED BEFORE firing callbacks.
  //--- This prevents reentrancy issues if a callback calls Connect().
  _SetState(MQTT_CLIENT_DISCONNECTED);

  _FireError(err_code, err_desc != "" ? err_desc : "Transport error");

  if (m_on_disconnect != NULL && (prev_state == MQTT_CLIENT_CONNECTED || prev_state == MQTT_CLIENT_WAITING_CONNACK)) {
    //--- Fire on_disconnect for CONNECTED and WAITING_CONNACK states so EAs
    //--- are notified of TLS/CONNACK-timeout failures, not only clean disconnects.
    string no_user_prop_keys[];
    string no_user_prop_vals[];
    m_on_disconnect(err_code, err_desc, "", no_user_prop_keys, no_user_prop_vals, 0);
    _SyncLogger();
  }

  if (m_reconnect_policy.IsEnabled()) {
    m_reconnect_policy.StartLoopIfNeeded();
    //--- If already reconnecting, preserve existing backoff state
    MQTT_LOG_DEBUG("Entering auto-reconnect mode (backoff=" + (string)m_reconnect_policy.GetCurrentBackoff() + "ms).");
  } else {
    MQTT_LOG_WARN("Transport error — auto-reconnect disabled.");
  }
}

//+------------------------------------------------------------------+
//| _ReplaySubscriptions                                             |
//| Send SUBSCRIBE for all registered persistent topics.             |
//| Splits into multiple SUBSCRIBE packets when the accumulated      |
//| topic filters would exceed the server's Maximum Packet Size      |
//| per §3.2.2.3.5.                                                  |
//+------------------------------------------------------------------+
void CMqttClient::_ReplaySubscriptions() {
  if (m_sub_count == 0) {
    m_replay_in_progress   = false;
    m_replay_next_index    = 0;
    m_pending_replay_count = 0;
    ArrayFree(m_prs_pkt_id);
    ArrayFree(m_prs_tcount);
    ArrayFree(m_prs_toff);
    ArrayFree(m_prs_topics);
    return;
  }

  //--- Start a fresh replay window on the first call after CONNACK.
  if (!m_replay_in_progress) {
    m_pending_replay_count = 0;
    ArrayFree(m_prs_pkt_id);
    ArrayFree(m_prs_tcount);
    ArrayFree(m_prs_toff);
    ArrayFree(m_prs_topics);
    m_replay_in_progress = true;
    m_replay_next_index  = 0;
  }

  if (m_replay_next_index >= m_sub_count) {
    m_replay_in_progress = false;
    return;
  }

  uint max_pkt = m_context.flow_control.GetMaximumPacketSize();
  if (max_pkt == 0) {
    max_pkt = 268435455;  // No server limit
  }

  //--- Build replay work in bounded batches. Each loop sends one SUBSCRIBE packet
  //--- that fits both the broker packet-size limit and a single subscription-id
  //--- group, then leaves any remainder in m_replay_next_index for the next Poll().
  while (m_replay_next_index < m_sub_count) {
    CSubscribe sub;
    uint       estimated_size = MQTT_SUBSCRIBE_FIXED_OVERHEAD;
    uint       batch_count    = 0;
    string     batch_topics[];
    uint       current_batch_sub_id = 0;
    uint       i                    = m_replay_next_index;

    for (; i < m_sub_count; i++) {
      bool use_sub_id = true;
      if (!_ValidateSubscribeRequest(m_sub_topic[i], use_sub_id)) {
        continue;
      }

      uint cached_utf8 = (i < (uint)ArraySize(m_sub_utf8_len)) ? m_sub_utf8_len[i] : 0;
      uint topic_bytes =
        (cached_utf8 > 0 ? cached_utf8 : (uint)StringLen(m_sub_topic[i])) + 3;  // +2 length prefix +1 opts
      uint topic_sub_id   = (use_sub_id && i < (uint)ArraySize(m_sub_id)) ? m_sub_id[i] : 0;
      bool sub_id_changed = (batch_count > 0 && topic_sub_id != current_batch_sub_id);
      if ((estimated_size + topic_bytes > max_pkt && batch_count > 0) || sub_id_changed) {
        break;
      }

      if (batch_count == 0) {
        current_batch_sub_id = topic_sub_id;
        if (current_batch_sub_id > 0) {
          sub.SetSubscriptionIdentifier(current_batch_sub_id);
        }
      }

      uchar replay_opts = (uchar)(m_sub_qos[i] & 0x03)
                        | ((i < (uint)ArraySize(m_sub_no_local) && m_sub_no_local[i]) ? 0x04 : 0)
                        | ((i < (uint)ArraySize(m_sub_rap) && m_sub_rap[i]) ? 0x08 : 0)
                        | (((i < (uint)ArraySize(m_sub_rh)) ? (m_sub_rh[i] & 0x03) : 0) << 4);
      sub.SetTopicFilter(m_sub_topic[i], replay_opts);
      estimated_size += topic_bytes;
      ArrayResize(batch_topics, (int)(batch_count + 1));
      batch_topics[batch_count] = m_sub_topic[i];
      batch_count++;
    }

    m_replay_next_index = i;
    if (batch_count == 0) {
      continue;
    }

    uchar pkt[];
    sub.Build(pkt, &m_context.session_db);

    if (ArraySize(pkt) == 0) {
      ushort leaked_id = sub.GetPacketId();
      if (leaked_id != 0) {
        m_context.session_db.ReleasePacketId(leaked_id);
      }
      _FireError(MQTT_REASON_CODE_MALFORMED_PACKET, "Failed to build SUBSCRIBE replay batch");
      m_replay_in_progress = false;
      return;
    }

    if (!m_context.flow_control.ValidateOutgoingPacketSize((uint)ArraySize(pkt))) {
      ushort leaked_id = sub.GetPacketId();
      if (leaked_id != 0) {
        m_context.session_db.ReleasePacketId(leaked_id);
      }
      _FireError(MQTT_REASON_CODE_PACKET_TOO_LARGE, "SUBSCRIBE replay batch exceeds server Maximum Packet Size");
      m_replay_in_progress = false;
      return;
    }

    ENUM_TRANSPORT_ERROR err = m_transport.Send(pkt);
    if (err != TRANSPORT_OK) {
      ushort leaked_id = sub.GetPacketId();
      if (leaked_id != 0) {
        m_context.session_db.ReleasePacketId(leaked_id);
      }
      _FireError((int)err, "Failed to replay subscriptions after reconnect");
      m_replay_in_progress = false;
      return;
    }

    uint idx   = m_pending_replay_count++;
    uint tbase = ArraySize(m_prs_topics);
    ArrayResize(m_prs_pkt_id, m_pending_replay_count, 4);
    ArrayResize(m_prs_tcount, m_pending_replay_count, 4);
    ArrayResize(m_prs_toff, m_pending_replay_count, 4);
    ArrayResize(m_prs_topics, (int)(tbase + batch_count), 8);
    m_prs_pkt_id[idx] = sub.GetPacketId();
    m_prs_tcount[idx] = batch_count;
    m_prs_toff[idx]   = tbase;
    for (uint t = 0; t < batch_count; t++) {
      m_prs_topics[tbase + t] = batch_topics[t];
    }
  }

  if (m_replay_next_index >= m_sub_count) {
    m_replay_in_progress = false;
    MQTT_LOG_DEBUG("Re-subscribed to " + (string)m_sub_count + " topic(s) in " + (string)m_pending_replay_count
                   + " batch(es) across one or more Poll() cycles.");
  }
}

//+------------------------------------------------------------------+
//| _RunRetransmissions                                              |
//| Transmit packets that were queued but not yet acknowledged.      |
//+------------------------------------------------------------------+
void CMqttClient::_RunRetransmissions(uint timeout_seconds) {
  if (timeout_seconds == 0) {
    timeout_seconds = m_retransmit_timeout_s;
  }
  PacketBuffer                           retrans[];
  ushort                                 retrans_ids[];
  CRetransmissionManager::DroppedMessage dropped[];
  uint                                   dropped_count = 0;
  uint count         = CRetransmissionManager::ProcessRetransmissions(m_context, retrans, retrans_ids, timeout_seconds,
                                                                      m_max_retransmit_count, dropped, dropped_count,
                                                                      m_pubrel_retry_timeout_s);

  uint retransmitted = 0;
  if (m_transport_type == TRANSPORT_TCP) {
    const uint batch_limit = 65536;
    uint       batch_size  = 0;
    uint       batch_count = 0;
    ushort     batch_ids[];  // packet IDs of messages in the current send batch

    ArrayResize(m_retransmit_batch_buf, 0);
    for (uint i = 0; i < count; i++) {
      int pkt_len = ArraySize(retrans[i].data);
      if (pkt_len <= 0) {
        continue;
      }

      if (batch_size > 0 && batch_size + (uint)pkt_len > batch_limit) {
        ENUM_TRANSPORT_ERROR err = m_transport.Send(m_retransmit_batch_buf, (int)batch_size);
        if (err != TRANSPORT_OK) {
          MQTT_LOG_ERROR("Retransmit batch send failed (err=" + (string)(int)err + ").");
          batch_size  = 0;
          batch_count = 0;
          ArrayResize(batch_ids, 0);
          break;
        }
        //--- Touch only the messages whose packets were successfully sent to the transport
        uint batch_ids_size = (uint)ArraySize(batch_ids);
        for (uint bi = 0; bi < batch_ids_size; bi++) {
          m_context.session_db.TouchMessage(batch_ids[bi]);
        }
        retransmitted += batch_count;
        batch_size     = 0;
        batch_count    = 0;
        ArrayResize(batch_ids, 0);
        ArrayResize(m_retransmit_batch_buf, 0);
      }

      uint write_off  = batch_size;
      batch_size     += (uint)pkt_len;
      ArrayResize(m_retransmit_batch_buf, (int)batch_size, (int)(batch_limit / 2));
      ArrayCopy(m_retransmit_batch_buf, retrans[i].data, (int)write_off, 0, pkt_len);
      ArrayResize(batch_ids, batch_count + 1);
      batch_ids[batch_count] = retrans_ids[i];
      batch_count++;
    }

    if (batch_count > 0 && batch_size > 0) {
      ENUM_TRANSPORT_ERROR err = m_transport.Send(m_retransmit_batch_buf, (int)batch_size);
      if (err != TRANSPORT_OK) {
        MQTT_LOG_ERROR("Retransmit batch send failed (err=" + (string)(int)err + ").");
      } else {
        uint batch_ids_size = (uint)ArraySize(batch_ids);
        for (uint bi = 0; bi < batch_ids_size; bi++) {
          m_context.session_db.TouchMessage(batch_ids[bi]);
        }
        retransmitted += batch_count;
      }
    }
  } else {
    for (uint i = 0; i < count; i++) {
      ENUM_TRANSPORT_ERROR err = m_transport.Send(retrans[i].data);
      if (err != TRANSPORT_OK) {
        MQTT_LOG_ERROR("Retransmit send failed (err=" + (string)(int)err + ").");
        break;
      }
      //--- Touch only after the send is confirmed to prevent consuming retransmit budget on failures
      m_context.session_db.TouchMessage(retrans_ids[i]);
      retransmitted++;
    }
  }
  if (retransmitted > 0) {
    MQTT_LOG_DEBUG("Retransmitted " + (string)retransmitted + " pending message(s).");
  }
  //--- Fire QoS drop callback for each dropped message
  if (m_on_qos_drop != NULL) {
    for (uint d = 0; d < dropped_count; d++) {
      m_on_qos_drop(dropped[d].packet_id, dropped[d].qos_level, dropped[d].topic, dropped[d].retransmit_count);
    }
    _SyncLogger();  // Restore this instance's log config after callback
  }
}

//+------------------------------------------------------------------+
//| _DrainPublishQueue                                               |
//| Send queued messages after reconnection.                         |
//| Uses CMqttPublishQueue's drain cursor to avoid O(N²) per-drain   |
//| compaction. Compaction occurs only once per drain cycle when the |
//| helper crosses the midpoint or the queue is fully cleared.       |
//+------------------------------------------------------------------+
void CMqttClient::_DrainPublishQueue() {
  uint available = m_publish_queue.GetQueuedMessageCount();
  uint sent      = 0;
  uint dropped   = 0;
  if (!m_publish_queue_coordinator.DrainQueueIfPending(m_context.session_db, m_publish_queue, *this, m_draining_queue,
                                                       sent, dropped)) {
    return;
  }

  if (sent > 0 || dropped > 0) {
    MQTT_LOG_DEBUG("Drained " + (string)sent + "/" + (string)available + " queued message(s)"
                   + (dropped > 0 ? ", " + (string)dropped + " expired/dropped." : "."));
  }
}

//+------------------------------------------------------------------+
//| _PurgeExpiredQueuedPublishes                                     |
//| Drop expired queued messages even while disconnected so they do  |
//| not consume queue capacity or heap indefinitely.                 |
//+------------------------------------------------------------------+
void CMqttClient::_PurgeExpiredQueuedPublishes() {
  uint dropped = m_publish_queue_coordinator.PurgeExpiredQueue(m_context.session_db, m_publish_queue, *this);

  if (dropped > 0) {
    MQTT_LOG_WARN("Dropped " + (string)dropped + " expired queued publish(es) while offline.");
  }
}

//+------------------------------------------------------------------+
//| PublishQueuedEntry                                               |
//| Adapter for helper-owned queue draining.                         |
//+------------------------------------------------------------------+
int CMqttClient::PublishQueuedEntry(const string topic, const uchar& payload_buffer[], uint payload_offset,
                                    uint payload_length, uchar qos, bool retain, const uchar& encoded_props_buffer[],
                                    uint prop_offset, uint prop_length, uint remaining_expiry,
                                    bool allow_outgoing_sub_id) {
  return (int)_PublishPreparedRange(topic, payload_buffer, (int)payload_offset, (int)payload_length, qos, retain,
                                    encoded_props_buffer, (int)prop_offset, (int)prop_length, remaining_expiry,
                                    allow_outgoing_sub_id);
}

//+------------------------------------------------------------------+
//| ReportQueueError                                                 |
//| Adapter for helper-owned queue draining.                         |
//+------------------------------------------------------------------+
void CMqttClient::ReportQueueError(int code, const string description) { _FireError(code, description); }

//+------------------------------------------------------------------+
//| _QueueMessageCallback                                            |
//| Purpose: Defer message delivery callbacks until protocol work    |
//|          for the current Poll() cycle has completed              |
//+------------------------------------------------------------------+
void CMqttClient::_QueueMessageCallback(MqttOnMessageCallback cb, const string topic, const uchar& payload[],
                                        int payload_len, uchar qos, bool retain_f, ushort packet_id,
                                        uint matched_sub_id, const uchar& publish_properties[]) {
  if (cb == NULL || m_abort_current_poll) {
    return;
  }

  //--- Copy payload and properties now because the transport packet buffer is reused
  //--- as soon as dispatch continues. Delivery can also spill into later Poll() calls
  //--- when the per-call budget limits how many callbacks can run immediately.
  uint payload_bytes        = (payload_len > 0) ? (uint)payload_len : 0;
  uint payload_off          = (uint)ArraySize(m_msg_evt_pbuf);
  int  new_payload_buf_size = 0;
  int  new_count_int        = 0;
  if (!_TryComputeArrayAppendSize(m_msg_evt_count, 1, new_count_int, "Deferred callback event count")) {
    _HandleBacklogOverflow("Deferred callback backlog exceeded local event capacity");
    return;
  }
  uint new_count = (uint)new_count_int;
  if (m_max_deferred_callback_events > 0 && new_count > m_max_deferred_callback_events) {
    _HandleBacklogOverflow("Deferred callback backlog count limit reached while dispatch was deferred");
    return;
  }
  if (payload_bytes > 0) {
    if (!_TryComputeArrayAppendSize(payload_off, payload_bytes, new_payload_buf_size,
                                    "Deferred callback payload buffer")) {
      _HandleBacklogOverflow("Deferred callback payload backlog exceeded local byte capacity");
      return;
    }
    if (m_max_deferred_callback_payload_bytes > 0 &&
        (uint)new_payload_buf_size > m_max_deferred_callback_payload_bytes) {
      _HandleBacklogOverflow("Deferred callback payload backlog limit reached while dispatch was deferred");
      return;
    }
  }

  uint prop_bytes        = (uint)ArraySize(publish_properties);
  uint prop_off          = (uint)ArraySize(m_msg_evt_prop_buf);
  int  new_prop_buf_size = 0;
  if (prop_bytes > 0) {
    if (!_TryComputeArrayAppendSize(prop_off, prop_bytes, new_prop_buf_size, "Deferred callback property buffer")) {
      _HandleBacklogOverflow("Deferred callback property backlog exceeded local byte capacity");
      return;
    }
    if (m_max_deferred_callback_property_bytes > 0 &&
        (uint)new_prop_buf_size > m_max_deferred_callback_property_bytes) {
      _HandleBacklogOverflow("Deferred callback property backlog limit reached while dispatch was deferred");
      return;
    }
  }

  ArrayResize(m_msg_evt_cb, new_count, 8);
  ArrayResize(m_msg_evt_topic, new_count, 8);
  ArrayResize(m_msg_evt_qos, new_count, 8);
  ArrayResize(m_msg_evt_retain, new_count, 8);
  ArrayResize(m_msg_evt_pktid, new_count, 8);
  ArrayResize(m_msg_evt_subid, new_count, 8);
  ArrayResize(m_msg_evt_poff, new_count, 8);
  ArrayResize(m_msg_evt_plen, new_count, 8);
  ArrayResize(m_msg_evt_prop_off, new_count, 8);
  ArrayResize(m_msg_evt_prop_len, new_count, 8);
  if (payload_bytes > 0) {
    ArrayResize(m_msg_evt_pbuf, new_payload_buf_size, 256);
    ArrayCopy(m_msg_evt_pbuf, payload, (int)payload_off, 0, (int)payload_bytes);
  }
  if (prop_bytes > 0) {
    ArrayResize(m_msg_evt_prop_buf, new_prop_buf_size, 128);
    ArrayCopy(m_msg_evt_prop_buf, publish_properties, (int)prop_off, 0, (int)prop_bytes);
  }

  uint slot                = m_msg_evt_count;
  m_msg_evt_cb[slot]       = cb;
  m_msg_evt_topic[slot]    = topic;
  m_msg_evt_qos[slot]      = qos;
  m_msg_evt_retain[slot]   = retain_f;
  m_msg_evt_pktid[slot]    = packet_id;
  m_msg_evt_subid[slot]    = matched_sub_id;
  m_msg_evt_poff[slot]     = payload_off;
  m_msg_evt_plen[slot]     = payload_bytes;
  m_msg_evt_prop_off[slot] = prop_off;
  m_msg_evt_prop_len[slot] = prop_bytes;
  m_msg_evt_count          = new_count;
}

//+------------------------------------------------------------------+
//| _ClearMessageCallbacks                                           |
//+------------------------------------------------------------------+
void CMqttClient::_ClearMessageCallbacks() {
  m_msg_evt_count = 0;
  ArrayResize(m_msg_evt_cb, 0);
  ArrayResize(m_msg_evt_topic, 0);
  ArrayResize(m_msg_evt_qos, 0);
  ArrayResize(m_msg_evt_retain, 0);
  ArrayResize(m_msg_evt_pktid, 0);
  ArrayResize(m_msg_evt_subid, 0);
  ArrayResize(m_msg_evt_poff, 0);
  ArrayResize(m_msg_evt_plen, 0);
  ArrayResize(m_msg_evt_pbuf, 0);
  ArrayResize(m_msg_evt_prop_off, 0);
  ArrayResize(m_msg_evt_prop_len, 0);
  ArrayResize(m_msg_evt_prop_buf, 0);
}

//+------------------------------------------------------------------+
//| _ResetIncomingMessageMetadata                                    |
//+------------------------------------------------------------------+
void CMqttClient::_ResetIncomingMessageMetadata(MqttIncomingMessageMetadata& metadata) {
  metadata.has_payload_format              = false;
  metadata.payload_format                  = 0;
  metadata.has_message_expiry              = false;
  metadata.message_expiry_interval         = 0;
  metadata.has_topic_alias                 = false;
  metadata.topic_alias                     = 0;
  metadata.response_topic                  = "";
  metadata.content_type                    = "";
  metadata.broker_subscription_id_count    = 0;
  metadata.user_property_count             = 0;
  metadata.matched_subscription_identifier = 0;
  ArrayResize(metadata.correlation_data, 0);
  ArrayResize(metadata.broker_subscription_ids, 0);
  ArrayResize(metadata.user_property_keys, 0);
  ArrayResize(metadata.user_property_vals, 0);
}

//+------------------------------------------------------------------+
//| _DecodeIncomingPublishMetadata                                   |
//| Purpose: Decode raw PUBLISH properties into facade metadata      |
//+------------------------------------------------------------------+
bool CMqttClient::_DecodeIncomingPublishMetadata(uchar& publish_properties[], uint matched_sub_id,
                                                 MqttIncomingMessageMetadata& metadata) {
  _ResetIncomingMessageMetadata(metadata);
  metadata.matched_subscription_identifier = matched_sub_id;

  uint idx                                 = 0;
  uint props_len                           = (uint)ArraySize(publish_properties);
  bool seen_props[256];
  ArrayInitialize(seen_props, false);

  while (idx < props_len) {
    uchar prop_id = publish_properties[idx++];

    if (prop_id != MQTT_PROP_IDENTIFIER_USER_PROPERTY && prop_id != MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER) {
      if (seen_props[prop_id]) {
        MQTT_LOG_WARN("Duplicate non-repeatable incoming PUBLISH property in queued metadata");
        return false;
      }
      seen_props[prop_id] = true;
    }

    switch (prop_id) {
      case MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR:
        if (idx >= props_len) {
          return false;
        }
        metadata.has_payload_format = true;
        metadata.payload_format     = publish_properties[idx++];
        break;

      case MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL:
        if (idx + 3 >= props_len) {
          return false;
        }
        metadata.has_message_expiry      = true;
        metadata.message_expiry_interval = ((uint)publish_properties[idx] << 24)
                                         | ((uint)publish_properties[idx + 1] << 16)
                                         | ((uint)publish_properties[idx + 2] << 8) | (uint)publish_properties[idx + 3];
        idx += 4;
        break;

      case MQTT_PROP_IDENTIFIER_CONTENT_TYPE:
        if (TryReadUtf8String(publish_properties, idx, metadata.content_type) != MQTT_OK) {
          return false;
        }
        break;

      case MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC:
        if (TryReadUtf8String(publish_properties, idx, metadata.response_topic) != MQTT_OK) {
          return false;
        }
        break;

      case MQTT_PROP_IDENTIFIER_CORRELATION_DATA:
        if (TryReadBinaryData(publish_properties, idx, metadata.correlation_data) != MQTT_OK) {
          return false;
        }
        break;

      case MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER: {
        uint sub_id = DecodeVariableByteInteger(publish_properties, idx);
        if (sub_id == UINT_MAX || sub_id == 0) {
          return false;
        }
        uint new_count = metadata.broker_subscription_id_count + 1;
        ArrayResize(metadata.broker_subscription_ids, (int)new_count);
        metadata.broker_subscription_ids[metadata.broker_subscription_id_count] = sub_id;
        metadata.broker_subscription_id_count                                   = new_count;
      } break;

      case MQTT_PROP_IDENTIFIER_TOPIC_ALIAS:
        if (idx + 1 >= props_len) {
          return false;
        }
        metadata.has_topic_alias  = true;
        metadata.topic_alias      = (ushort)(((uint)publish_properties[idx] << 8) | (uint)publish_properties[idx + 1]);
        idx                      += 2;
        break;

      case MQTT_PROP_IDENTIFIER_USER_PROPERTY: {
        string userprop_pair[2];
        if (TryReadUserProperty(publish_properties, idx, userprop_pair) != MQTT_OK) {
          return false;
        }
        uint new_count = metadata.user_property_count + 1;
        ArrayResize(metadata.user_property_keys, (int)new_count);
        ArrayResize(metadata.user_property_vals, (int)new_count);
        metadata.user_property_keys[metadata.user_property_count] = userprop_pair[0];
        metadata.user_property_vals[metadata.user_property_count] = userprop_pair[1];
        metadata.user_property_count                              = new_count;
      } break;

      default:
        MQTT_LOG_WARN("Unsupported queued incoming PUBLISH property 0x" + StringFormat("%02X", prop_id));
        return false;
    }
  }

  return idx == props_len;
}

//+------------------------------------------------------------------+
//| _DrainMessageCallbacks                                           |
//| Purpose: Deliver deferred message callbacks after protocol work  |
//+------------------------------------------------------------------+
void CMqttClient::_DrainMessageCallbacks() {
  if (m_msg_evt_count == 0) {
    return;
  }

  //--- Share the same budget concept as packet dispatch so a burst of user callbacks
  //--- cannot monopolize the chart thread and starve keep-alive, ACK, or reconnect work.
  uint budget = m_msg_evt_count;
  if (m_max_packets_per_poll > 0 && budget > m_max_packets_per_poll) {
    budget = m_max_packets_per_poll;
  }

  for (uint i = 0; i < budget; i++) {
    uchar payload[];
    uint  payload_len = m_msg_evt_plen[i];
    if (payload_len > 0) {
      ArrayResize(payload, (int)payload_len);
      ArrayCopy(payload, m_msg_evt_pbuf, 0, (int)m_msg_evt_poff[i], (int)payload_len);
    }
    if (m_msg_evt_cb[i] != NULL) {
      uchar props[];
      uint  props_len = m_msg_evt_prop_len[i];
      if (props_len > 0) {
        ArrayResize(props, (int)props_len);
        ArrayCopy(props, m_msg_evt_prop_buf, 0, (int)m_msg_evt_prop_off[i], (int)props_len);
      }
      MqttIncomingMessageMetadata metadata;
      if (!_DecodeIncomingPublishMetadata(props, m_msg_evt_subid[i], metadata)) {
        _ResetIncomingMessageMetadata(metadata);
        metadata.matched_subscription_identifier = m_msg_evt_subid[i];
      }
      m_msg_evt_cb[i](m_msg_evt_topic[i], payload, (int)payload_len, m_msg_evt_qos[i], m_msg_evt_retain[i],
                      m_msg_evt_pktid[i], metadata);
    }
    _SyncLogger();
  }

  if (budget >= m_msg_evt_count) {
    _ClearMessageCallbacks();
    return;
  }

  uint  remain = m_msg_evt_count - budget;
  uchar new_pbuf[];
  uchar new_prop_buf[];
  uint  new_pbuf_size     = 0;
  uint  new_prop_buf_size = 0;
  for (uint i = 0; i < remain; i++) {
    new_pbuf_size     += m_msg_evt_plen[budget + i];
    new_prop_buf_size += m_msg_evt_prop_len[budget + i];
  }
  ArrayResize(new_pbuf, (int)new_pbuf_size);
  ArrayResize(new_prop_buf, (int)new_prop_buf_size);

  uint off      = 0;
  uint prop_off = 0;
  for (uint i = 0; i < remain; i++) {
    uint src              = budget + i;
    m_msg_evt_cb[i]       = m_msg_evt_cb[src];
    m_msg_evt_topic[i]    = m_msg_evt_topic[src];
    m_msg_evt_qos[i]      = m_msg_evt_qos[src];
    m_msg_evt_retain[i]   = m_msg_evt_retain[src];
    m_msg_evt_pktid[i]    = m_msg_evt_pktid[src];
    m_msg_evt_subid[i]    = m_msg_evt_subid[src];
    m_msg_evt_poff[i]     = off;
    m_msg_evt_plen[i]     = m_msg_evt_plen[src];
    m_msg_evt_prop_off[i] = prop_off;
    m_msg_evt_prop_len[i] = m_msg_evt_prop_len[src];
    if (m_msg_evt_plen[src] > 0) {
      ArrayCopy(new_pbuf, m_msg_evt_pbuf, (int)off, (int)m_msg_evt_poff[src], (int)m_msg_evt_plen[src]);
      off += m_msg_evt_plen[src];
    }
    if (m_msg_evt_prop_len[src] > 0) {
      ArrayCopy(new_prop_buf, m_msg_evt_prop_buf, (int)prop_off, (int)m_msg_evt_prop_off[src],
                (int)m_msg_evt_prop_len[src]);
      prop_off += m_msg_evt_prop_len[src];
    }
  }

  ArrayResize(m_msg_evt_cb, (int)remain);
  ArrayResize(m_msg_evt_topic, (int)remain);
  ArrayResize(m_msg_evt_qos, (int)remain);
  ArrayResize(m_msg_evt_retain, (int)remain);
  ArrayResize(m_msg_evt_pktid, (int)remain);
  ArrayResize(m_msg_evt_subid, (int)remain);
  ArrayResize(m_msg_evt_poff, (int)remain);
  ArrayResize(m_msg_evt_plen, (int)remain);
  ArrayResize(m_msg_evt_prop_off, (int)remain);
  ArrayResize(m_msg_evt_prop_len, (int)remain);
  ArrayCopy(m_msg_evt_pbuf, new_pbuf, 0, 0, (int)new_pbuf_size);
  ArrayResize(m_msg_evt_pbuf, (int)new_pbuf_size);
  ArrayCopy(m_msg_evt_prop_buf, new_prop_buf, 0, 0, (int)new_prop_buf_size);
  ArrayResize(m_msg_evt_prop_buf, (int)new_prop_buf_size);
  m_msg_evt_count = remain;
}

//+------------------------------------------------------------------+
//| Poll                                                             |
//| Drive the client state machine; dispatch callbacks.              |
//+------------------------------------------------------------------+
void CMqttClient::Poll() {
  if (m_in_poll) {
    MQTT_LOG_WARN("Poll re-entry ignored while a prior Poll() call is still active");
    return;
  }

  m_in_poll = true;
  _PollInternal();
  m_in_poll = false;
}

//+------------------------------------------------------------------+
//| Poll Internal                                                    |
//+------------------------------------------------------------------+
void CMqttClient::_PollInternal() {
  _SyncLogger();  // Ensure this instance's log config is active
  m_abort_current_poll = false;
  m_context.session_db.FlushIfDirty(2);

  //--- One Poll() cycle runs in a fixed order: timeout/reconnect checks, bounded
  //--- transport dispatch, deferred user callbacks, offline queue drain, timed
  //--- retransmissions, then deferred redirects. Protocol work stays ahead of EA
  //--- callbacks so ACKs and state transitions settle before user code observes them.

  //--- DISCONNECTED: if auto-reconnect enabled, check if it's time to reconnect
  if (m_state == MQTT_CLIENT_DISCONNECTED) {
    _PurgeExpiredQueuedPublishes();
    //--- Offline Poll() stays intentionally cheap: expire stale queued publishes,
    //--- maybe schedule the next reconnect attempt, then return.
    if (m_reconnect_policy.ShouldReconnectNow()) {
      //--- Circuit breaker: stop if max consecutive attempt limit reached
      if (m_reconnect_policy.IsCircuitBreakerOpen()) {
        MQTT_LOG_WARN("Circuit breaker open — reached max reconnect attempts ("
                      + (string)m_reconnect_policy.GetMaxAttempts() + "). Stopping auto-reconnect.");
        m_reconnect_policy.Stop();
        _FireError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, "Circuit breaker: max reconnect attempts ("
                                                                     + (string)m_reconnect_policy.GetMaxAttempts()
                                                                     + ") reached");
        return;
      }
      m_reconnect_policy.RegisterReconnectAttempt();
      m_reconnect_count++;
      //--- Persist count so an EA restart during the outage honours
      //--- previously-accumulated attempts in the circuit breaker.
      m_context.session_db.SetReconnectFailureCount(m_reconnect_policy.GetCurrentAttemptCount());
      MQTT_LOG_INFO("Auto-reconnect attempt #" + (string)m_reconnect_count
                    + " (consecutive: " + (string)m_reconnect_policy.GetCurrentAttemptCount()
                    + (m_reconnect_policy.GetMaxAttempts() > 0 ? "/" + (string)m_reconnect_policy.GetMaxAttempts() : "")
                    + ", backoff=" + (string)m_reconnect_policy.GetCurrentBackoff() + "ms).");
      Connect();
    }
    return;
  }

  //--- Transport/TLS setup timeout check
  if ((m_state == MQTT_CLIENT_CONNECTING || m_state == MQTT_CLIENT_TLS_HANDSHAKING) && m_connect_deadline_ms > 0) {
    ulong now_ms = GetMicrosecondCount() / 1000;
    if (now_ms >= m_connect_deadline_ms) {
      uint active_timeout_ms = (m_active_connect_timeout_ms > 0) ? m_active_connect_timeout_ms : m_connect_timeout_ms;
      MQTT_LOG_ERROR("Transport setup timeout after " + (string)active_timeout_ms + "ms.");
      _OnTransportError(TRANSPORT_ERROR_TIMEOUT, _DescribeConnectTimeout());
      return;
    }
  }

  //--- CONNACK timeout check
  if (m_state == MQTT_CLIENT_WAITING_CONNACK && m_connack_deadline_ms > 0) {
    ulong now_ms = GetMicrosecondCount() / 1000;
    if (now_ms >= m_connack_deadline_ms) {
      MQTT_LOG_ERROR("CONNACK timeout after " + (string)m_connack_timeout_ms + "ms.");
      _OnTransportError(TRANSPORT_ERROR_TIMEOUT,
                        "MQTT_FAILURE_BROKER: CONNACK timeout - broker did not acknowledge CONNECT in time");
      return;
    }
  }

  //--- Spread post-reconnect subscription replay across Poll() cycles.
  //--- _OnConnackReceived() sends the first batch; subsequent Poll() calls send one batch each.
  if (m_state == MQTT_CLIENT_CONNECTED && m_replay_in_progress) {
    _ReplaySubscriptions();
    if (m_abort_current_poll) {
      return;
    }
  }

  //--- CONNECTING / WAITING_CONNACK / CONNECTED: poll transport for events
  PacketBuffer transport_pkts[];
  uint         transport_count = 0;

  PacketBuffer dispatch_pkts[];
  uint         dispatch_count = 0;
  uint         packet_budget  = m_max_packets_per_poll;

  //--- Always dispatch leftovers from the previous timer tick before reading more
  //--- packets from the transport so packet order stays deterministic under load.
  if (m_deferred_transport_count > 0) {
    _TakeDeferredPackets(dispatch_pkts, packet_budget, dispatch_count);
  }

  ENUM_TRANSPORT_ERROR transport_err = TRANSPORT_OK;
  if (packet_budget == 0 || dispatch_count < packet_budget) {
    transport_err            = m_transport.Poll(transport_pkts, transport_count);

    uint take_from_transport = transport_count;
    if (packet_budget > 0) {
      uint remaining_budget = packet_budget - dispatch_count;
      if (take_from_transport > remaining_budget) {
        take_from_transport = remaining_budget;
      }
    }

    for (uint tp = 0; tp < take_from_transport; tp++) {
      _AppendPacketCopy(dispatch_pkts, dispatch_count, transport_pkts[tp].data);
    }
    if (take_from_transport < transport_count) {
      _AppendDeferredPackets(transport_pkts, take_from_transport, transport_count - take_from_transport);
    }
  }

  if (m_abort_current_poll) {
    return;
  }

  //--- Fatal transport errors
  if (transport_err != TRANSPORT_OK && transport_err != TRANSPORT_CONNECTING) {
    MQTT_LOG_ERROR("Transport error " + (string)(int)transport_err + " in state " + (string)(int)m_state);
    _OnTransportError((int)transport_err, "Transport poll error");
    return;
  }

  if ((m_state == MQTT_CLIENT_CONNECTING || m_state == MQTT_CLIENT_TLS_HANDSHAKING) && !m_transport.IsConnected()) {
    ENUM_TRANSPORT_CONNECT_PHASE phase = m_transport.GetConnectPhase();
    if (phase == TRANSPORT_PHASE_TLS_HANDSHAKING || phase == TRANSPORT_PHASE_WS_SENDING_REQUEST
        || phase == TRANSPORT_PHASE_WS_WAITING_HEADERS) {
      if (m_state != MQTT_CLIENT_TLS_HANDSHAKING) {
        _SetState(MQTT_CLIENT_TLS_HANDSHAKING);
      }
    } else if (phase == TRANSPORT_PHASE_TCP_CONNECTING && m_state != MQTT_CLIENT_CONNECTING) {
      _SetState(MQTT_CLIENT_CONNECTING);
    }
  } else if (m_state == MQTT_CLIENT_TLS_HANDSHAKING && m_transport.IsConnected()) {
    _SetState(MQTT_CLIENT_CONNECTING);
  }

  if ((m_state == MQTT_CLIENT_CONNECTING || m_state == MQTT_CLIENT_TLS_HANDSHAKING)
      && _EnforceBlockingTransportHardLimit()) {
    return;
  }

  //--- CONNECTING → WAITING_CONNACK transition
  if (m_state == MQTT_CLIENT_CONNECTING && m_transport.IsConnected() && transport_err == TRANSPORT_OK) {
    _SendConnect();
    if (m_abort_current_poll) {
      return;
    }
  }

  //--- Dispatch received transport packets
  for (uint i = 0; i < dispatch_count; i++) {
    int pkt_size = ArraySize(dispatch_pkts[i].data);

    if (pkt_size < 1) {
      continue;
    }
    if (!m_context.flow_control.ValidateIncomingPacketSize((uint)pkt_size)) {
      CDisconnect disc;
      disc.SetReasonCode(MQTT_REASON_CODE_PACKET_TOO_LARGE);
      uchar d[];
      disc.Build(d);
      m_transport.Send(d);
      _OnTransportError(MQTT_REASON_CODE_PACKET_TOO_LARGE, "Incoming packet exceeds client maximum packet size");
      return;
    }
    uchar pkt_type = (dispatch_pkts[i].data[0] >> 4) & 0x0F;

    switch (pkt_type) {
      case CONNACK:
        if (m_state == MQTT_CLIENT_WAITING_CONNACK) {
          _OnConnackReceived(dispatch_pkts[i].data);
        } else {
          //--- Per §3.1.4 / §4.2: a second CONNACK while already connected is a Protocol
          //--- Error. The client MUST close the connection with reason code 0x82.
          MQTT_LOG_ERROR("Unexpected CONNACK while already connected — Protocol Error per §3.2 / §4.2.");
          CDisconnect _prot_disc;
          _prot_disc.SetReasonCode(MQTT_REASON_CODE_PROTOCOL_ERROR);
          uchar _prot_disc_buf[];
          _prot_disc.Build(_prot_disc_buf);
          m_transport.Send(_prot_disc_buf);
          _OnTransportError(MQTT_REASON_CODE_PROTOCOL_ERROR,
                            "Protocol Error: CONNACK received while already connected (§3.2 / §4.2)");
          return;
        }
        break;

      case PUBLISH:
        _OnPublishReceived(dispatch_pkts[i].data);
        break;

      case PUBACK: {
        //--- Per §3.4.1: reserved flags in fixed header MUST be 0x00
        if ((dispatch_pkts[i].data[0] & 0x0F) != 0x00) {
          _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "PUBACK reserved flags invalid per §3.4.1");
          return;
        }
        {
          ushort pktid;
          uchar  reason;
          string ack_reason_string;
          string ack_user_prop_keys[];
          string ack_user_prop_vals[];
          uint   ack_user_prop_count = 0;
          if (!_ParseSimpleAckPacket(dispatch_pkts[i].data, pkt_size, pktid, reason)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBACK packet");
            return;
          }
          if (!_ReadSimpleAckDiagnostics(dispatch_pkts[i].data, "PUBACK", ack_reason_string, ack_user_prop_keys,
                                         ack_user_prop_vals, ack_user_prop_count)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBACK properties");
            return;
          }
          _DispatchAckDiagnostics(PUBACK, pktid, reason, ack_reason_string, ack_user_prop_keys, ack_user_prop_vals,
                                  ack_user_prop_count);
          SessionMessage ack_msg;
          if (!_TryGetOutgoingAckMessage("PUBACK", pktid, QoS_1, false, QOS2_STATE_NONE, ack_msg)) {
            break;
          }
          m_context.flow_control.OnPubackReceived(pktid);
          m_context.session_db.RemoveMessage(pktid);
          //--- Notify packet ID pool if monitoring is active
          if (m_on_packetid_low != NULL && m_packetid_low_threshold > 0) {
            uint avail = m_context.session_db.GetAvailablePacketIdCount();
            if (avail <= m_packetid_low_threshold) {
              m_on_packetid_low(avail, m_packetid_low_threshold);
              _SyncLogger();  // Restore this instance's log config after callback
            }
          }
          //--- Fire publish result callback for all reason codes
          if (m_on_publish_result != NULL) {
            m_on_publish_result(pktid, reason, false);
            _SyncLogger();  // Restore this instance's log config after callback
          }
          //--- Fire error callback for error reason codes (§3.4.2.1)
          if (reason >= 0x80) {
            _FireError((int)reason,
                       "PUBACK error for packet ID " + (string)pktid + " — reason 0x" + StringFormat("%02X", reason));
          }
        }
        break;
      }

      case PUBREC: {
        //--- Per §3.5.1: reserved flags in fixed header MUST be 0x00
        if ((dispatch_pkts[i].data[0] & 0x0F) != 0x00) {
          _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "PUBREC reserved flags invalid per §3.5.1");
          return;
        }
        {
          ushort pktid;
          uchar  reason;
          string ack_reason_string;
          string ack_user_prop_keys[];
          string ack_user_prop_vals[];
          uint   ack_user_prop_count = 0;
          if (!_ParseSimpleAckPacket(dispatch_pkts[i].data, pkt_size, pktid, reason)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBREC packet");
            return;
          }
          if (!_ReadSimpleAckDiagnostics(dispatch_pkts[i].data, "PUBREC", ack_reason_string, ack_user_prop_keys,
                                         ack_user_prop_vals, ack_user_prop_count)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBREC properties");
            return;
          }
          _DispatchAckDiagnostics(PUBREC, pktid, reason, ack_reason_string, ack_user_prop_keys, ack_user_prop_vals,
                                  ack_user_prop_count);
          SessionMessage rec_msg;
          if (!_TryGetOutgoingAckMessage("PUBREC", pktid, QoS_2, true, QOS2_STATE_PUBLISH_SENT, rec_msg)) {
            break;
          }
          //--- Per §4.3.3: if PUBREC carries an error reason code (≥0x80),
          //--- the QoS 2 exchange is immediately ended; do NOT send PUBREL.
          if (reason >= 0x80) {
            m_context.flow_control.ReleaseQoS(pktid);
            m_context.session_db.RemoveMessage(pktid);
            if (m_on_publish_result != NULL) {
              m_on_publish_result(pktid, reason, false);
              _SyncLogger();  // Restore this instance's log config after callback
            }
            _FireError((int)reason,
                       "PUBREC error for packet ID " + (string)pktid + " — reason 0x" + StringFormat("%02X", reason));
            break;
          }
          m_context.flow_control.OnPubrecReceived(pktid);
          if (!m_context.session_db.UpdateQoS2State(pktid, QOS2_STATE_PUBREC_RECEIVED)) {
            MQTT_LOG_WARN("PUBREC state update failed for packet ID " + (string)pktid + ".");
            break;
          }
          ENUM_TRANSPORT_ERROR rel_err = _SendPubrelPacket(pktid);
          if (rel_err != TRANSPORT_OK) {
            _OnTransportError((int)rel_err, "Failed to send PUBREL");
            return;
          }
        }
        break;
      }

      case PUBREL: {
        //--- Per §3.6.1: reserved flags in fixed header MUST be 0x02 (bits 0010)
        if ((dispatch_pkts[i].data[0] & 0x0F) != 0x02) {
          _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "PUBREL reserved flags invalid per §3.6.1");
          return;
        }
        {
          ushort pktid;
          uchar  reason;
          string ack_reason_string;
          string ack_user_prop_keys[];
          string ack_user_prop_vals[];
          uint   ack_user_prop_count = 0;
          if (!_ParseSimpleAckPacket(dispatch_pkts[i].data, pkt_size, pktid, reason)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBREL packet");
            return;
          }
          if (!_ReadSimpleAckDiagnostics(dispatch_pkts[i].data, "PUBREL", ack_reason_string, ack_user_prop_keys,
                                         ack_user_prop_vals, ack_user_prop_count)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBREL properties");
            return;
          }
          _DispatchAckDiagnostics(PUBREL, pktid, reason, ack_reason_string, ack_user_prop_keys, ack_user_prop_vals,
                                  ack_user_prop_count);
          if (reason >= 0x80) {
            MQTT_LOG_WARN("PUBREL for packet ID " + (string)pktid + " carried reason 0x" + StringFormat("%02X", reason)
                          + ". Inspect GetLastAck*() for broker diagnostics.");
          }
          m_context.flow_control.OnPubrelReceived(pktid);

          SessionMessage incoming;
          bool           has_incoming = m_context.session_db.GetMessage(pktid, incoming, false);
          if (!has_incoming) {
            MQTT_LOG_WARN("Received PUBREL for packet ID " + (string)pktid
                          + " but no stored incoming QoS2 state was found.");
          }
          if (has_incoming) {
            MQTT_LOG_INFO("Delivering stored incoming QoS2 message for packet ID " + (string)pktid + " on topic '"
                          + incoming.topic + "'.");
            //--- Increment received counter for QoS 2 messages
            m_messages_received++;

            int   payload_len = (int)ArraySize(incoming.payload);
            //--- Preserve original retain flag from PUBLISH packet.
            //--- The retain flag has meaning for the receiver per §3.3.1.3.
            bool  retain_flag = incoming.retain;
            uchar incoming_props[];
            ArrayResize(incoming_props, ArraySize(incoming.publish_properties));
            if (ArraySize(incoming.publish_properties) > 0) {
              ArrayCopy(incoming_props, incoming.publish_properties);
            }

            //--- Route QoS 2 delivery through trie-based per-topic callbacks (O(topic-depth))
            //--- Global handler fires once for NULL-callback matches.
            bool qos2_dispatched   = false;
            bool qos2_global_fired = false;
            //--- Reuse m_match_scratch to avoid per-PUBCOMP GC allocations
            uint qos2_match_count  = 0;
            m_topic_matcher.Match(incoming.topic, m_match_scratch, qos2_match_count);
            for (uint mi = 0; mi < qos2_match_count; mi++) {
              uint si = m_match_scratch[mi];
              if (si < m_sub_count) {
                uint q2_sub_id = (si < (uint)ArraySize(m_sub_id)) ? m_sub_id[si] : 0;
                if (m_sub_cb[si] != NULL) {
                  _QueueMessageCallback(m_sub_cb[si], incoming.topic, incoming.payload, payload_len, incoming.qos_level,
                                        retain_flag, pktid, q2_sub_id, incoming_props);
                  qos2_dispatched = true;
                } else if (!qos2_global_fired && m_on_message != NULL) {
                  //--- Subscription uses global handler — fire it exactly once per message
                  _QueueMessageCallback(m_on_message, incoming.topic, incoming.payload, payload_len, incoming.qos_level,
                                        retain_flag, pktid, q2_sub_id, incoming_props);
                  qos2_dispatched   = true;
                  qos2_global_fired = true;
                }
              }
            }
            //--- Fallback: no trie match at all — fire global handler if configured
            if (!qos2_dispatched && m_on_message != NULL) {
              _QueueMessageCallback(m_on_message, incoming.topic, incoming.payload, payload_len, incoming.qos_level,
                                    retain_flag, pktid, 0, incoming_props);
            }
          }

          //--- Per §4.3.3, if no state for this packet ID, send PUBCOMP with 0x92
          uchar                comp_reason = has_incoming ? (uchar)0x00 : (uchar)0x92;
          ENUM_TRANSPORT_ERROR comp_err    = _SendPubcompPacket(pktid, comp_reason);
          if (comp_err != TRANSPORT_OK) {
            _OnTransportError((int)comp_err, "Failed to send PUBCOMP");
            return;
          }
          //--- Only release incoming flow control slot when we actually had an
          //--- incoming message. Prevents double-decrement on PUBREL retransmission.
          if (has_incoming) {
            m_context.flow_control.ReleaseIncomingQoS(pktid);
            m_context.session_db.RemoveMessage(pktid, false);
            //--- Drain QoS 2 delivery callbacks immediately after PUBCOMP is
            //--- confirmed sent and the DB entry is removed. If a subsequent packet in
            //--- this same dispatch batch triggers _OnTransportError, the delivery would
            //--- be lost because _ClearMessageCallbacks() would discard these queued
            //--- callbacks and the DB entry is already gone (broker won't retransmit).
            _DrainMessageCallbacks();
          }
        }
        break;
      }

      case PUBCOMP: {
        //--- Per §3.7.1: reserved flags in fixed header MUST be 0x00
        if ((dispatch_pkts[i].data[0] & 0x0F) != 0x00) {
          _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "PUBCOMP reserved flags invalid per §3.7.1");
          return;
        }
        {
          ushort pktid;
          uchar  reason;
          string ack_reason_string;
          string ack_user_prop_keys[];
          string ack_user_prop_vals[];
          uint   ack_user_prop_count = 0;
          if (!_ParseSimpleAckPacket(dispatch_pkts[i].data, pkt_size, pktid, reason)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBCOMP packet");
            return;
          }
          if (!_ReadSimpleAckDiagnostics(dispatch_pkts[i].data, "PUBCOMP", ack_reason_string, ack_user_prop_keys,
                                         ack_user_prop_vals, ack_user_prop_count)) {
            _ProtocolDisconnect(MQTT_REASON_CODE_MALFORMED_PACKET, "Malformed PUBCOMP properties");
            return;
          }
          _DispatchAckDiagnostics(PUBCOMP, pktid, reason, ack_reason_string, ack_user_prop_keys, ack_user_prop_vals,
                                  ack_user_prop_count);
          SessionMessage comp_msg;
          if (!_TryGetOutgoingAckMessage("PUBCOMP", pktid, QoS_2, true, QOS2_STATE_PUBREC_RECEIVED, comp_msg)) {
            break;
          }
          m_context.flow_control.OnPubcompReceived(pktid);
          m_context.session_db.RemoveMessage(pktid);
          //--- Notify packet ID pool if monitoring is active
          if (m_on_packetid_low != NULL && m_packetid_low_threshold > 0) {
            uint avail = m_context.session_db.GetAvailablePacketIdCount();
            if (avail <= m_packetid_low_threshold) {
              m_on_packetid_low(avail, m_packetid_low_threshold);
              _SyncLogger();  // Restore this instance's log config after callback
            }
          }
          //--- Fire publish result callback
          if (m_on_publish_result != NULL) {
            m_on_publish_result(pktid, reason, true);
            _SyncLogger();  // Restore this instance's log config after callback
          }
          //--- Fire error callback for error reason codes (§3.7.2.1)
          if (reason >= 0x80 && reason != MQTT_REASON_CODE_PACKET_IDENTIFIER_NOT_FOUND) {
            _FireError((int)reason,
                       "PUBCOMP error for packet ID " + (string)pktid + " — reason 0x" + StringFormat("%02X", reason));
          }
        }
        break;
      }

      case SUBACK:
        _OnSubackReceived(dispatch_pkts[i].data);
        break;

      case UNSUBACK:
        _OnUnsubackReceived(dispatch_pkts[i].data);
        break;

      case PINGRESP:
        //--- PINGRESP is consumed by the transport's CKeepAlive layer
        //--- for deadline cancellation. Update client-level RTT metric here.
        m_last_ping_rtt_us = m_transport.GetLastPingRTT_us();
        //--- Fire RTT threshold callback if configured and exceeded
        if (m_on_rtt_threshold != NULL && m_rtt_threshold_us > 0 && m_last_ping_rtt_us > m_rtt_threshold_us) {
          m_on_rtt_threshold(m_last_ping_rtt_us, m_rtt_threshold_us);
          _SyncLogger();  // Restore this instance's log config after callback
        }
        break;

      case DISCONNECT:
        _OnDisconnectReceived(dispatch_pkts[i].data);
        return;  // Connection is gone — exit Poll

      case AUTH: {
        CAuth auth;
        int   aerr = auth.Read(dispatch_pkts[i].data);
        if (aerr != MQTT_OK) {
          uchar reason_code = (aerr == MQTT_ERROR_PROTOCOL_VIOLATION || aerr == MQTT_ERROR_INVALID_REASON_CODE) ?
                                MQTT_REASON_CODE_PROTOCOL_ERROR :
                                MQTT_REASON_CODE_MALFORMED_PACKET;
          _ProtocolDisconnect(reason_code, "AUTH parse error");
          return;
        }
        if (m_active_auth_method == "") {
          _ProtocolDisconnect(MQTT_REASON_CODE_PROTOCOL_ERROR,
                              "Protocol Error: received AUTH without CONNECT Authentication Method");
          return;
        }
        if (auth.GetAuthMethod() != m_active_auth_method) {
          _ProtocolDisconnect(MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD,
                              "Bad Authentication Method: AUTH method does not match CONNECT Authentication Method");
          return;
        }
        m_last_auth_reason_code   = auth.GetReasonCode();
        m_last_auth_reason_string = auth.GetReasonString();
        m_last_auth_method        = auth.GetAuthMethod();
        auth.GetAuthData(m_last_auth_data);
        m_last_auth_user_prop_count = auth.GetUserPropertyCount();
        ArrayResize(m_last_auth_user_prop_keys, m_last_auth_user_prop_count);
        ArrayResize(m_last_auth_user_prop_vals, m_last_auth_user_prop_count);
        for (uint auth_upi = 0; auth_upi < m_last_auth_user_prop_count; auth_upi++) {
          m_last_auth_user_prop_keys[auth_upi] = auth.GetUserPropertyKey(auth_upi);
          m_last_auth_user_prop_vals[auth_upi] = auth.GetUserPropertyValue(auth_upi);
        }
        //--- Fire auth callback if registered, enabling multi-step auth exchange (§4.12)
        if (m_on_auth != NULL) {
          uchar auth_data[];
          auth.GetAuthData(auth_data);
          m_on_auth(auth.GetReasonCode(), auth.GetAuthMethod(), auth_data, ArraySize(auth_data));
          _SyncLogger();  // Restore this instance's log config after callback
        }
        if (m_on_auth_ex != NULL) {
          uchar auth_data[];
          auth.GetAuthData(auth_data);
          m_on_auth_ex(auth.GetReasonCode(), auth.GetAuthMethod(), auth_data, ArraySize(auth_data),
                       auth.GetReasonString(), m_last_auth_user_prop_keys, m_last_auth_user_prop_vals,
                       (int)m_last_auth_user_prop_count);
          _SyncLogger();
        }
        if (m_on_auth == NULL && m_on_auth_ex == NULL) {
          _FireError(MQTT_REASON_CODE_CONTINUE_AUTHENTICATION,
                     "Broker requested AUTH exchange; register auth handler via SetOnAuth()");
        }
        break;
      }

      default:
        //--- AUTH and other packets — log but don't crash
        MQTT_LOG_WARN("Unhandled packet type " + (string)pkt_type);
        break;
    }

    if (m_abort_current_poll) {
      return;
    }
  }

  //--- Only after protocol-side packet handling for this batch is complete do we
  //--- enter arbitrary EA callbacks queued by _OnPublishReceived().
  _DrainMessageCallbacks();
  if (m_abort_current_poll) {
    return;
  }

  //--- Resume draining the offline publish queue when flow-control slots free up after ACKs.
  //--- _OnConnackReceived() starts the initial drain; remaining entries are picked up here
  //--- on each Poll() call until the queue is fully sent.
  if (m_state == MQTT_CLIENT_CONNECTED) {
    _DrainPublishQueue();
    if (m_abort_current_poll) {
      return;
    }
  }

  //--- Periodic retransmission in CONNECTED state
  //--- Check at 25% of timeout interval to bound worst-case retransmit latency
  //--- to ~1.25× timeout instead of ~2×.
  if (m_state == MQTT_CLIENT_CONNECTED) {
    ulong now_ms            = GetMicrosecondCount() / 1000;
    ulong check_interval_ms = (ulong)m_retransmit_timeout_s * MQTT_RETRANSMIT_CHECK_FRACTION_MS;  // 25% of timeout
    if (check_interval_ms < 1000) {
      check_interval_ms = 1000;                                                                   // Minimum 1s
    }
    if (now_ms - m_last_retransmit_check_ms >= check_interval_ms) {
      m_last_retransmit_check_ms = now_ms;
      _RunRetransmissions();
      if (m_abort_current_poll) {
        return;
      }
    }
  }

  //--- Deferred server redirection: process Connect() here, outside all packet handlers,
  //--- to avoid re-entrant state mutation during CONNACK/DISCONNECT dispatch.
  if (m_redirect_pending) {
    m_redirect_pending = false;
    m_transport.Disconnect();
    _ClearDeferredTransportPackets();
    m_reconnect_policy.Stop();
    Connect();
  }
}

//+------------------------------------------------------------------+
//| GetConnectionInfo — snapshot of client health metrics            |
//+------------------------------------------------------------------+
void CMqttClient::GetConnectionInfo(MqttConnectionInfo& info) const {
  if (m_state == MQTT_CLIENT_CONNECTED && m_connected_since_ms > 0) {
    info.connection_duration_ms = (GetMicrosecondCount() / 1000) - m_connected_since_ms;
  } else {
    info.connection_duration_ms = 0;
  }
  info.messages_sent     = m_messages_sent;
  info.messages_received = m_messages_received;
  info.in_flight_count   = m_context.flow_control.GetInFlightCount();
  info.reconnect_count   = m_reconnect_count;
}

//+------------------------------------------------------------------+
//| GetOldestQueuedMessageAgeMs                                      |
//| Purpose: Report age of the oldest live queued publish in memory  |
//+------------------------------------------------------------------+
ulong CMqttClient::GetOldestQueuedMessageAgeMs() const {
  return m_publish_queue.GetOldestQueuedMessageAgeMs(GetMicrosecondCount(), TimeLocal());
}

#undef MQTT_LOG_ERROR
#undef MQTT_LOG_WARN
#undef MQTT_LOG_INFO
#undef MQTT_LOG_DEBUG
#define MQTT_LOG_ERROR(msg)                                                                                           \
  do {                                                                                                                \
    if (MQTT_LEVEL_ERROR <= _MqttGetActiveLogLevel())                                                                 \
      _MqttLogWithLogger(_MqttGetActiveLogger(), MQTT_LEVEL_ERROR, "ERROR", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)
#define MQTT_LOG_WARN(msg)                                                                                          \
  do {                                                                                                              \
    if (MQTT_LEVEL_WARN <= _MqttGetActiveLogLevel())                                                                \
      _MqttLogWithLogger(_MqttGetActiveLogger(), MQTT_LEVEL_WARN, "WARN", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)
#define MQTT_LOG_INFO(msg)                                                                                          \
  do {                                                                                                              \
    if (MQTT_LEVEL_INFO <= _MqttGetActiveLogLevel())                                                                \
      _MqttLogWithLogger(_MqttGetActiveLogger(), MQTT_LEVEL_INFO, "INFO", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)
#define MQTT_LOG_DEBUG(msg)                                                                                           \
  do {                                                                                                                \
    if (MQTT_LEVEL_DEBUG <= _MqttGetActiveLogLevel())                                                                 \
      _MqttLogWithLogger(_MqttGetActiveLogger(), MQTT_LEVEL_DEBUG, "DEBUG", __FILE__, __FUNCTION__, __LINE__, (msg)); \
  } while (0)

#endif  // MQTT_INTERNAL_CLIENT_MQH
