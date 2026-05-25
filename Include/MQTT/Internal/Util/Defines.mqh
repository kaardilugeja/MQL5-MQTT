//+------------------------------------------------------------------+
//|                                                      Defines.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 protocol constants, property identifiers, reason codes, |
//| and variable byte integer limits as defined in the MQTT 5.0 spec |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_UTIL_DEFINES_MQH
#define MQTT_INTERNAL_UTIL_DEFINES_MQH

#include "Logger.mqh"

//+------------------------------------------------------------------+
//| Properties                                                       |
//+------------------------------------------------------------------+
/*
The last field in the Variable Header of the CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC,
PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, and
AUTH packet is a set of Properties. In the CONNECT packet there is also an optional set of Properties in
the Will Properties field with the Payload
*/

//--- Property identifiers with their data types
#define MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR           0x01  // (1) Byte
#define MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL            0x02  // (2) Four Byte Integer
#define MQTT_PROP_IDENTIFIER_CONTENT_TYPE                       0x03  // (3) UTF-8 Encoded String
#define MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC                     0x08  // (8) UTF-8 Encoded String
#define MQTT_PROP_IDENTIFIER_CORRELATION_DATA                   0x09  // (9) Binary Data
#define MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER            0x0B  // (11) Variable Byte Integer
#define MQTT_PROP_IDENTIFIER_SESSION_EXPIRY_INTERVAL            0x11  // (17) Four Byte Integer
#define MQTT_PROP_IDENTIFIER_ASSIGNED_CLIENT_IDENTIFIER         0x12  // (18) UTF-8 Encoded String
#define MQTT_PROP_IDENTIFIER_SERVER_KEEP_ALIVE                  0x13  // (19) Two Byte Integer
#define MQTT_PROP_IDENTIFIER_AUTHENTICATION_METHOD              0x15  // (21) UTF-8 Encoded String
#define MQTT_PROP_IDENTIFIER_AUTHENTICATION_DATA                0x16  // (22) Binary Data
#define MQTT_PROP_IDENTIFIER_REQUEST_PROBLEM_INFORMATION        0x17  // (23) Byte
#define MQTT_PROP_IDENTIFIER_WILL_DELAY_INTERVAL                0x18  // (24) Four Byte Integer
#define MQTT_PROP_IDENTIFIER_REQUEST_RESPONSE_INFORMATION       0x19  // (25) Byte
#define MQTT_PROP_IDENTIFIER_RESPONSE_INFORMATION               0x1A  // (26) UTF-8 Encoded String
#define MQTT_PROP_IDENTIFIER_SERVER_REFERENCE                   0x1C  // (28) UTF-8 Encoded String
#define MQTT_PROP_IDENTIFIER_REASON_STRING                      0x1F  // (31) UTF-8 Encoded String
#define MQTT_PROP_IDENTIFIER_RECEIVE_MAXIMUM                    0x21  // (33) Two Byte Integer
#define MQTT_PROP_IDENTIFIER_TOPIC_ALIAS_MAXIMUM                0x22  // (34) Two Byte Integer
#define MQTT_PROP_IDENTIFIER_TOPIC_ALIAS                        0x23  // (35) Two Byte Integer
#define MQTT_PROP_IDENTIFIER_MAXIMUM_QOS                        0x24  // (36) Byte
#define MQTT_PROP_IDENTIFIER_RETAIN_AVAILABLE                   0x25  // (37) Byte
#define MQTT_PROP_IDENTIFIER_USER_PROPERTY                      0x26  // (38) UTF-8 String Pair
#define MQTT_PROP_IDENTIFIER_MAXIMUM_PACKET_SIZE                0x27  // (39) Four Byte Integer
#define MQTT_PROP_IDENTIFIER_WILDCARD_SUBSCRIPTION_AVAILABLE    0x28  // (40) Byte
#define MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER_AVAILABLE  0x29  // (41) Byte
#define MQTT_PROP_IDENTIFIER_SHARED_SUBSCRIPTION_AVAILABLE      0x2A  // (42) Byte

//+------------------------------------------------------------------+
//| Reason Codes                                                     |
//+------------------------------------------------------------------+
/*
A Reason Code is a one byte unsigned value that indicates the result of an operation. Reason Codes less
than 0x80 indicate successful completion of an operation. The normal Reason Code for success is 0.
Reason Code values of 0x80 or greater indicate failure.

The CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, DISCONNECT and AUTH Control Packets
have a single Reason Code as part of the Variable Header. The SUBACK and UNSUBACK packets
contain a list of one or more Reason Codes in the Payload.
*/

//--- Success and normal operation codes (0x00 - 0x7F)
#define MQTT_REASON_CODE_SUCCESS                                0x00  // (0) Normal success
#define MQTT_REASON_CODE_NORMAL_DISCONNECTION                   0x00  // (0) Normal disconnect
#define MQTT_REASON_CODE_GRANTED_QOS_0                          0x00  // (0) QoS 0 granted
#define MQTT_REASON_CODE_GRANTED_QOS_1                          0x01  // (1) QoS 1 granted
#define MQTT_REASON_CODE_GRANTED_QOS_2                          0x02  // (2) QoS 2 granted
#define MQTT_REASON_CODE_DISCONNECT_WITH_WILL_MESSAGE           0x04  // (4) Disconnect with will
#define MQTT_REASON_CODE_NO_MATCHING_SUBSCRIBERS                0x10  // (16) No matching subscribers
#define MQTT_REASON_CODE_NO_SUBSCRIPTION_EXISTED                0x11  // (17) No subscription existed
#define MQTT_REASON_CODE_CONTINUE_AUTHENTICATION                0x18  // (24) Continue authentication
#define MQTT_REASON_CODE_RE_AUTHENTICATE                        0x19  // (25) Re-authenticate

//--- Error codes (0x80 - 0xFF)
#define MQTT_REASON_CODE_UNSPECIFIED_ERROR                      0x80  // (128) Unspecified error
#define MQTT_REASON_CODE_MALFORMED_PACKET                       0x81  // (129) Malformed packet
#define MQTT_REASON_CODE_PROTOCOL_ERROR                         0x82  // (130) Protocol error
#define MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR          0x83  // (131) Implementation error
#define MQTT_REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION           0x84  // (132) Unsupported version
#define MQTT_REASON_CODE_CLIENT_IDENTIFIER_NOT_VALID            0x85  // (133) Invalid client ID
#define MQTT_REASON_CODE_BAD_USER_NAME_OR_PASSWORD              0x86  // (134) Bad credentials
#define MQTT_REASON_CODE_NOT_AUTHORIZED                         0x87  // (135) Not authorized
#define MQTT_REASON_CODE_SERVER_UNAVAILABLE                     0x88  // (136) Server unavailable
#define MQTT_REASON_CODE_SERVER_BUSY                            0x89  // (137) Server busy
#define MQTT_REASON_CODE_BANNED                                 0x8A  // (138) Client banned
#define MQTT_REASON_CODE_SERVER_SHUTTING_DOWN                   0x8B  // (139) Server shutting down
#define MQTT_REASON_CODE_BAD_AUTHENTICATION_METHOD              0x8C  // (140) Bad auth method
#define MQTT_REASON_CODE_KEEP_ALIVE_TIMEOUT                     0x8D  // (141) Keep alive timeout
#define MQTT_REASON_CODE_SESSION_TAKEN_OVER                     0x8E  // (142) Session taken over
#define MQTT_REASON_CODE_TOPIC_FILTER_INVALID                   0x8F  // (143) Invalid topic filter
#define MQTT_REASON_CODE_TOPIC_NAME_INVALID                     0x90  // (144) Invalid topic name
#define MQTT_REASON_CODE_PACKET_IDENTIFIER_IN_USE               0x91  // (145) Packet ID in use
#define MQTT_REASON_CODE_PACKET_IDENTIFIER_NOT_FOUND            0x92  // (146) Packet ID not found
#define MQTT_REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED               0x93  // (147) Receive max exceeded
#define MQTT_REASON_CODE_TOPIC_ALIAS_INVALID                    0x94  // (148) Invalid topic alias
#define MQTT_REASON_CODE_PACKET_TOO_LARGE                       0x95  // (149) Packet too large
#define MQTT_REASON_CODE_MESSAGE_RATE_TOO_HIGH                  0x96  // (150) Message rate too high
#define MQTT_REASON_CODE_QUOTA_EXCEEDED                         0x97  // (151) Quota exceeded
#define MQTT_REASON_CODE_ADMINISTRATIVE_ACTION                  0x98  // (152) Administrative action
#define MQTT_REASON_CODE_PAYLOAD_FORMAT_INVALID                 0x99  // (153) Invalid payload format
#define MQTT_REASON_CODE_RETAIN_NOT_SUPPORTED                   0x9A  // (154) Retain not supported
#define MQTT_REASON_CODE_QOS_NOT_SUPPORTED                      0x9B  // (155) QoS not supported
#define MQTT_REASON_CODE_USE_ANOTHER_SERVER                     0x9C  // (156) Use another server
#define MQTT_REASON_CODE_SERVER_MOVED                           0x9D  // (157) Server moved
#define MQTT_REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED     0x9E  // (158) Shared subs not supported
#define MQTT_REASON_CODE_CONNECTION_RATE_EXCEEDED               0x9F  // (159) Connection rate exceeded
#define MQTT_REASON_CODE_MAXIMUM_CONNECT_TIME                   0xA0  // (160) Max connect time
#define MQTT_REASON_CODE_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED 0xA1  // (161) Sub IDs not supported
#define MQTT_REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED   0xA2  // (162) Wildcards not supported

//+------------------------------------------------------------------+
//| Variable Byte Integer Limits                                     |
//+------------------------------------------------------------------+
/*
The maximum number of bytes in the Variable Byte Integer field is four.
The encoded value MUST use the minimum number of bytes necessary to represent the value
Size of Variable Byte Integer
Digits  From                               To
1       0 (0x00)                           127 (0x7F)
2       128 (0x80, 0x01)                   16,383 (0xFF, 0x7F) => (255,127)
3       16,384 (0x80, 0x80, 0x01)          2,097,151 (0xFF, 0xFF, 0x7F)
4       2,097,152 (0x80, 0x80, 0x80, 0x01) 268,435,455 (0xFF, 0xFF, 0xFF, 0x7F)
*/

#define VARINT_MIN_ONE_BYTE                                     0x00       // (0)
#define VARINT_MAX_ONE_BYTE                                     0x7F       // (127)
#define VARINT_MIN_TWO_BYTES                                    0x80       // (128)
#define VARINT_MAX_TWO_BYTES                                    0x3FFF     // (16,383)
#define VARINT_MIN_THREE_BYTES                                  0x4000     // (16,384)
#define VARINT_MAX_THREE_BYTES                                  0x1FFFFF   // (2,097,151)
#define VARINT_MIN_FOUR_BYTES                                   0x200000   // (2,097,152)
#define VARINT_MAX_FOUR_BYTES                                   0xFFFFFFF  // (268,435,455)

//+------------------------------------------------------------------+
//| QoS Levels                                                       |
//+------------------------------------------------------------------+
#define QoS_0                                                   0  // QoS 0 - At most once
#define QoS_1                                                   1  // QoS 1 - At least once
#define QoS_2                                                   2  // QoS 2 - Exactly once

//+------------------------------------------------------------------+
//| ENUM_MQTT_CLIENT_STATE                                           |
//| Lifecycle states of CMqttClient                                  |
//+------------------------------------------------------------------+
enum ENUM_MQTT_CLIENT_STATE {
  MQTT_CLIENT_DISCONNECTED    = 0,  // Idle; no connection attempt in progress
  MQTT_CLIENT_CONNECTING      = 1,  // TCP connect in progress (async; pre-TLS handshake)
  MQTT_CLIENT_WAITING_CONNACK = 2,  // Transport connected; CONNECT sent; awaiting CONNACK
  MQTT_CLIENT_CONNECTED       = 3,  // Fully operational
  MQTT_CLIENT_TLS_HANDSHAKING = 4,  // TCP connected; TLS/WS upgrade in progress
                                    // NOTE: MQL5 SocketTlsHandshake is blocking — the EA
                                    // timer tick that triggers TLS will freeze briefly.
                                    // EA authors should gate tick-driven publish/trading
                                    // logic with CMqttClient::IsSafeToPublish() during
                                    // this state.
};

//+------------------------------------------------------------------+
//| Callback typedefs for CMqttClient event system                   |
//|                                                                  |
//| Function pointer callbacks fire synchronously inside Poll().     |
//| When no callback is registered (null), the event is silently     |
//| discarded. There is no reentrancy risk since MQL5 is single-     |
//| threaded per chart.                                              |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| ENUM_TRANSPORT_CONNECT_PHASE                                     |
//| Fine-grained transport connect phases exposed for diagnostics    |
//| and client-state reporting during cooperative async connect.     |
//+------------------------------------------------------------------+
enum ENUM_TRANSPORT_CONNECT_PHASE {
  TRANSPORT_PHASE_IDLE               = 0,
  TRANSPORT_PHASE_TCP_CONNECTING     = 1,
  TRANSPORT_PHASE_TLS_HANDSHAKING    = 2,
  TRANSPORT_PHASE_WS_SENDING_REQUEST = 3,
  TRANSPORT_PHASE_WS_WAITING_HEADERS = 4,
  TRANSPORT_PHASE_CONNECTED          = 5,
};

//--- Fires after CONNACK is successfully processed.
//--- session_present: true if the broker has an existing session for this client.
typedef void (*MqttOnConnectCallback)(bool session_present);

//--- Facade-level outgoing PUBLISH properties exposed by CMqttClient.
//--- Mirrors MQTT 5.0 §3.3.2.3 without forcing callers to construct CPublish directly.
struct MqttPublishProperties {
  bool   has_payload_format;
  uchar  payload_format;
  bool   has_message_expiry;
  uint   message_expiry_interval;
  bool   has_topic_alias;
  ushort topic_alias;
  string response_topic;
  uchar  correlation_data[];
  string content_type;
  bool   has_subscription_identifier;
  uint   subscription_identifier;
  bool   allow_outgoing_subscription_identifier;
  string user_property_keys[];
  string user_property_vals[];
};

//--- Outgoing MQTT 5 diagnostics for simple ACK packets.
//--- Used by CMqttClient when it auto-generates PUBACK/PUBREC/PUBREL/PUBCOMP.
struct MqttAckProperties {
  bool   has_reason_string;
  string reason_string;
  string user_property_keys[];
  string user_property_vals[];
};

//--- Facade-level incoming PUBLISH metadata exposed by CMqttClient.
//--- Includes broker-sent MQTT 5 properties plus the local matched subscription identifier.
struct MqttIncomingMessageMetadata {
  bool   has_payload_format;
  uchar  payload_format;
  bool   has_message_expiry;
  uint   message_expiry_interval;
  bool   has_topic_alias;
  ushort topic_alias;
  string response_topic;
  uchar  correlation_data[];
  string content_type;
  uint   broker_subscription_ids[];
  uint   broker_subscription_id_count;
  string user_property_keys[];
  string user_property_vals[];
  uint   user_property_count;
  uint   matched_subscription_identifier;
};

//--- Fires when the connection is lost or the broker sends DISCONNECT.
//--- reason_code:   MQTT 5.0 reason code (0x00 = normal, 0x80+ = error).
//--- reason_string: human-readable description (may be empty).
//--- server_reference: broker-provided redirect hint, if any.
//--- user_prop_keys/user_prop_vals: broker-provided DISCONNECT User Properties.
typedef void (*MqttOnDisconnectCallback)(int reason_code, const string reason_string, const string server_reference,
                                         const string &user_prop_keys[], const string &user_prop_vals[],
                                         int user_prop_count);

//--- Fires for every incoming PUBLISH packet (after QoS ack is auto-dispatched)
//--- with full MQTT 5 metadata.
typedef void (*MqttOnMessageCallback)(const string topic, const uchar &payload[], int payload_len, uchar qos,
                                      bool retain, ushort packet_id, const MqttIncomingMessageMetadata &metadata);

//--- Fires when a PUBACK or PUBCOMP is received with a reason code (fires for all reason codes).
//--- packet_id:   packet identifier of the published message.
//--- reason_code: PUBACK/PUBCOMP reason code (0x00 = success; \u22650x80 = error).
//--- is_pubcomp:  true if this is a PUBCOMP (QoS 2 final), false if PUBACK (QoS 1).
typedef void (*MqttOnPublishResultCallback)(ushort packet_id, uchar reason_code, bool is_pubcomp);

//--- Fires for all simple ACK packets (PUBACK/PUBREC/PUBREL/PUBCOMP)
//--- after MQTT 5 diagnostics have been parsed.
typedef void (*MqttOnAckCallback)(uchar packet_type, ushort packet_id, uchar reason_code, const string reason_string,
                                  const string &user_prop_keys[], const string &user_prop_vals[], int user_prop_count);

//--- Fires when the available packet ID pool drops below a configured threshold.
//--- available_ids:   number of packet IDs currently available.
//--- threshold:       the configured low-water-mark threshold.
typedef void (*MqttOnPacketIdLowCallback)(uint available_ids, uint threshold);

//--- Fires when a SUBACK is received from the broker with MQTT 5 diagnostics.
typedef void (*MqttOnSubscribeAckCallback)(ushort packet_id, const uchar &reason_codes[], int count,
                                           const string reason_string, const string &user_prop_keys[],
                                           const string &user_prop_vals[], int user_prop_count);

//--- Fires when an UNSUBACK is received from the broker with MQTT 5 diagnostics.
typedef void (*MqttOnUnsubscribeAckCallback)(ushort packet_id, const uchar &reason_codes[], int count,
                                             const string reason_string, const string &user_prop_keys[],
                                             const string &user_prop_vals[], int user_prop_count);

//--- Fires on transport errors, protocol violations, and retransmission exhaustion.
//--- error_code:  internal error code (ENUM_TRANSPORT_ERROR or ENUM_MQTT_ERROR).
//--- description: human-readable description of the error.
typedef void (*MqttOnErrorCallback)(int error_code, const string description);

//+------------------------------------------------------------------+
//| MqttErrorContext                                                 |
//| Structured error information for diagnostics                     |
//+------------------------------------------------------------------+
struct MqttErrorContext {
  int    error_code;     // ENUM_TRANSPORT_ERROR or ENUM_MQTT_ERROR
  string description;    // Human-readable description
  string source_file;    // Source file where error originated (__FILE__)
  int    source_line;    // Source line number (__LINE__)
  string function_name;  // Function name where error was raised
};

//+------------------------------------------------------------------+
//| Extended error callback with structured context                  |
//| Provides file, line, and function context in addition to code.   |
//+------------------------------------------------------------------+
typedef void (*MqttOnErrorExCallback)(const MqttErrorContext &context);

//--- Fires on every client state transition.
//--- old_state: state the client was in before the transition.
//--- new_state: state the client is entering.
typedef void (*MqttOnStateChangeCallback)(ENUM_MQTT_CLIENT_STATE old_state, ENUM_MQTT_CLIENT_STATE new_state);

//--- Fires when the broker sends an AUTH packet for multi-step authentication (§4.12).
//--- reason_code: 0x18 (Continue Authentication) or 0x19 (Re-authenticate).
//--- method:      Authentication Method string.
//--- data:        Authentication Data bytes (may be empty).
//--- data_len:    number of valid bytes in data[].
typedef void (*MqttOnAuthCallback)(uchar reason_code, const string method, const uchar &data[], int data_len);

//--- Extended AUTH callback with MQTT 5 diagnostics.
//--- reason_string: AUTH Reason String, if present.
//--- user_prop_keys/user_prop_vals: AUTH User Properties.
typedef void (*MqttOnAuthExCallback)(uchar reason_code, const string method, const uchar &data[], int data_len,
                                     const string reason_string, const string &user_prop_keys[],
                                     const string &user_prop_vals[], int user_prop_count);

//+------------------------------------------------------------------+
//| Credential Logging Guard                                         |
//+------------------------------------------------------------------+
//| By default the library NEVER prints credentials (passwords,      |
//| usernames, authentication method/data) to the MetaTrader log.    |
//|                                                                  |
//| To enable credential-containing debug output, define this symbol |
//| BEFORE including any mql5-mqtt-cli header:                       |
//|                                                                  |
//|   #define MQTT_LOG_CREDENTIALS                                   |
//|   #include <MQTT/MQTT.mqh>                                       |
//|                                                                  |
//| WARNING: Only enable MQTT_LOG_CREDENTIALS in development/debug   |
//| environments. Credentials written to the log may be persisted    |
//| in plaintext log files and visible in the MetaTrader journal.    |
//+------------------------------------------------------------------+
// #define MQTT_LOG_CREDENTIALS   // Uncomment ONLY for debug builds

#ifdef MQTT_LOG_CREDENTIALS
#define MQTT_CREDENTIAL_PRINT(msg) MQTT_LOG_DEBUG(msg)
#else
#define MQTT_CREDENTIAL_PRINT(msg)  // redacted — define MQTT_LOG_CREDENTIALS to enable
#endif

//+------------------------------------------------------------------+
//| Subscription Options                                             |
//+------------------------------------------------------------------+
#define MQTT_SUB_OPTS_QoS_0               0x00  // QoS 0 subscription
#define MQTT_SUB_OPTS_QoS_1               0x01  // QoS 1 subscription
#define MQTT_SUB_OPTS_QoS_2               0x02  // QoS 2 subscription
#define MQTT_SUB_OPTS_NON_LOCAL           0x04  // Non-local publication
#define MQTT_SUB_OPTS_RETAIN_AS_PUBLISHED 0x08  // Retain as published
#define MQTT_SUB_OPTS_RETAIN_HANDLING_0   0x00  // Send retained at subscribe
#define MQTT_SUB_OPTS_RETAIN_HANDLING_1   0x10  // Send retained only if new
#define MQTT_SUB_OPTS_RETAIN_HANDLING_2   0x20  // Do not send retained

//+------------------------------------------------------------------+
//| Struct MqttKeepAlive                                             |
//| Purpose: Stores the keep alive interval in seconds (2 bytes)     |
//+------------------------------------------------------------------+
struct MqttKeepAlive {
  uchar msb;  // Most Significant Byte of keep alive value
  uchar lsb;  // Least Significant Byte of keep alive value
};

//+------------------------------------------------------------------+
//| Enum ENUM_TRANSPORT_ERROR                                        |
//| Purpose: Define return codes for the transport layer, covering   |
//|          socket, TLS, and MQTT framing level errors.             |
//+------------------------------------------------------------------+
enum ENUM_TRANSPORT_ERROR {
  TRANSPORT_OK                = 0,   // Success / connected
  TRANSPORT_CONNECTING        = 1,   // Non-blocking connect is in progress (non-fatal, poll again)
  TRANSPORT_ERROR_SOCKET      = -1,  // Socket creation or TCP connection failure
  TRANSPORT_ERROR_TLS         = -2,  // TLS handshake failure or certificate error
  TRANSPORT_ERROR_SEND        = -3,  // Error during raw socket transmission
  TRANSPORT_ERROR_RECV        = -4,  // Error during raw socket reception or timeout
  TRANSPORT_ERROR_CLOSED      = -5,  // Connection closed by the remote broker
  TRANSPORT_ERROR_BAD_FRAME   = -6,  // Malformed MQTT fixed header or invalid variable-byte integer
  TRANSPORT_ERROR_PKT_TOO_BIG = -7,  // Incoming packet size exceeds MaxPacketSize configuration
  TRANSPORT_ERROR_TIMEOUT     = -8,  // Overall async-connect deadline exceeded with no success
};

//+------------------------------------------------------------------+
//| Struct PacketBuffer                                              |
//| Purpose: Container for raw byte array (for collections)          |
//+------------------------------------------------------------------+
struct PacketBuffer {
  uchar data[];
};

//+------------------------------------------------------------------+
//| Method Return Codes                                              |
//+------------------------------------------------------------------+
//| Standardized return codes for packet Read()/Build() methods.     |
//| Positive values indicate success, negative values indicate error.|
//+------------------------------------------------------------------+
enum ENUM_MQTT_ERROR {
  MQTT_OK                        = 0,    // Operation completed successfully
  MQTT_ERROR_PACKET_TOO_SHORT    = -1,   // Packet buffer is too short
  MQTT_ERROR_MALFORMED_HEADER    = -2,   // Fixed header is malformed
  MQTT_ERROR_INVALID_PACKET_TYPE = -3,   // Unexpected or wrong packet type
  MQTT_ERROR_MALFORMED_VARINT    = -4,   // Variable Byte Integer encoding error
  MQTT_ERROR_INVALID_REASON_CODE = -5,   // Reason code not valid for this packet type
  MQTT_ERROR_INVALID_PROPS_LEN   = -6,   // Properties length exceeds limits
  MQTT_ERROR_BUFFER_OVERFLOW     = -7,   // Read would exceed buffer bounds
  MQTT_ERROR_MISSING_REQUIRED    = -8,   // Required field (e.g. Auth Method) is missing
  MQTT_ERROR_PROTOCOL_VIOLATION  = -9,   // Generic protocol-level violation
  MQTT_ERROR_PACKET_TOO_LARGE    = -10,  // Packet exceeds Maximum Packet Size
  MQTT_ERROR_MALFORMED_PACKET    = -11,  // Packet payload or properties are malformed
};

//+------------------------------------------------------------------+
//| MqttErrorToString                                                |
//| Purpose: Get human-readable description for MQTT error code      |
//| Parameters: error - error code from ENUM_MQTT_ERROR              |
//| Return: Descriptive string                                       |
//+------------------------------------------------------------------+
string MqttErrorToString(ENUM_MQTT_ERROR error) {
  switch (error) {
    case MQTT_OK:
      return "Success";
    case MQTT_ERROR_PACKET_TOO_SHORT:
      return "Packet too short";
    case MQTT_ERROR_MALFORMED_HEADER:
      return "Malformed header";
    case MQTT_ERROR_INVALID_PACKET_TYPE:
      return "Invalid packet type";
    case MQTT_ERROR_MALFORMED_VARINT:
      return "Malformed variable byte integer";
    case MQTT_ERROR_INVALID_REASON_CODE:
      return "Invalid reason code";
    case MQTT_ERROR_INVALID_PROPS_LEN:
      return "Invalid properties length";
    case MQTT_ERROR_BUFFER_OVERFLOW:
      return "Buffer overflow";
    case MQTT_ERROR_MISSING_REQUIRED:
      return "Missing required field";
    case MQTT_ERROR_PROTOCOL_VIOLATION:
      return "Protocol violation";
    case MQTT_ERROR_PACKET_TOO_LARGE:
      return "Packet too large";
    case MQTT_ERROR_MALFORMED_PACKET:
      return "Malformed packet";
    default:
      return "Unknown error (" + (string)error + ")";
  }
}

#endif  // MQTT_DEFINES_MQH
