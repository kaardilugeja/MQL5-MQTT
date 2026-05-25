//+------------------------------------------------------------------+
//|                                                       Logger.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Multi-level structured logging for the mql5-mqtt-cli library.    |
//|                                                                  |
//| Usage (in library code):                                         |
//|   MQTT_LOG_ERROR("SocketCreate() failed — error " +              |
//|   (string)GetLastError())                                        |
//|   MQTT_LOG_WARN("credentials sent over plain connection")        |
//|   MQTT_LOG_INFO("CONNACK success — session_present=" +           |
//|   (string)sp)                                                    |
//|   MQTT_LOG_DEBUG("PINGREQ sent")                                 |
//|                                                                  |
//| Usage (in EA / calling code):                                    |
//|   #include <MQTT/MQTT.mqh>                                       |
//|   mqtt.SetLogLevel(MQTT_LEVEL_DEBUG);  // enable DEBUG output    |
//|   mqtt.SetLogSink(&MyLogHandler);      // optional custom sink   |
//|   mqtt.SetLogLevel(MQTT_LEVEL_NONE);   // silence all output     |
//|   void OnDeinit(const int reason) { MqttReleaseChartLogger(); }  |
//|                                                                  |
//| Compile-time default level override:                             |
//|   #define MQTT_DEFAULT_LOG_LEVEL MQTT_LEVEL_DEBUG                |
//|   #include <MQTT/MQTT.mqh>                                       |
//|                                                                  |
//| Output format:                                                   |
//|   [LEVEL] filename.mqh:line in Function::Name — message text     |
//|   Example: [ERROR] Transport.mqh:786 in CMqttTransport::Connect  |
//|             — SocketConnect() failed — error 5273                |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_UTIL_LOGGER_MQH
#define MQTT_INTERNAL_UTIL_LOGGER_MQH

#include <Generic\HashMap.mqh>

//+------------------------------------------------------------------+
//| ENUM_MQTT_LOG_LEVEL                                              |
//| Ordered severity levels for log filtering.                       |
//| NONE silences all output; DEBUG allows everything through.       |
//+------------------------------------------------------------------+
enum ENUM_MQTT_LOG_LEVEL {
  MQTT_LEVEL_NONE  = 0,  // No output — completely silent
  MQTT_LEVEL_ERROR = 1,  // Fatal / unrecoverable errors only
  MQTT_LEVEL_WARN  = 2,  // Recoverable issues and degraded conditions
  MQTT_LEVEL_INFO  = 3,  // Connection lifecycle events (default)
  MQTT_LEVEL_DEBUG = 4,  // Low-frequency internals and packet details
};

//+------------------------------------------------------------------+
//| MqttLogSinkCallback                                              |
//| Optional custom output handler.  When non-NULL, receives every   |
//| log line instead of Print().                                     |
//|   level   — ENUM_MQTT_LOG_LEVEL value (cast to int for callback) |
//|   message — fully-formatted log line including [TAG] prefix      |
//+------------------------------------------------------------------+
typedef void (*MqttLogSinkCallback)(int level, const string message);

//+------------------------------------------------------------------+
//| Global runtime log level                                         |
//| Compile-time override:                                           |
//|   #define MQTT_DEFAULT_LOG_LEVEL MQTT_LEVEL_X                    |
//| before including any mql5-mqtt-cli header.                       |
//+------------------------------------------------------------------+
#ifndef MQTT_DEFAULT_LOG_LEVEL
#define MQTT_DEFAULT_LOG_LEVEL MQTT_LEVEL_INFO
#endif

//+------------------------------------------------------------------+
//| CLogger                                                          |
//| Per-instance log configuration.  Owned by CMqttContext so that   |
//| log routing is tied to the connection session rather than to bare|
//| global variables.                                                |
//|                                                                  |
//| CMqttClient copies its m_context.logger into a chart-scoped      |
//| logger registry (via _SyncLogger) before every logged operation, |
//| ensuring the correct sink and level are active for that chart's  |
//| execution thread.                                                |
//|                                                                  |
//| g_mqtt_logger remains only as the library-wide default logger    |
//| for code paths that execute before any chart-specific logger     |
//| has been registered. Active runtime routing uses the chart slot. |
//+------------------------------------------------------------------+
struct CLogger {
  int                 m_log_level;  // Active severity filter
  MqttLogSinkCallback m_log_sink;   // NULL = Print() fallback

  CLogger()
      : m_log_level(MQTT_DEFAULT_LOG_LEVEL)
      , m_log_sink(NULL) {}
};

//+------------------------------------------------------------------+
//| Fallback logger used before chart-specific routing is available  |
//| and as a default for contexts that do not resolve a chart slot.  |
//+------------------------------------------------------------------+
CLogger             g_mqtt_logger;
long                g_mqtt_logger_chart_ids[];
CLogger             g_mqtt_chart_loggers[];
CHashMap<long, int> g_mqtt_logger_chart_slot_index;

//+------------------------------------------------------------------+
//| _MqttFindLoggerSlot - Locate chart-scoped logger entry           |
//| Uses a HashMap chart_id -> slot index for O(1) amortised lookup. |
//+------------------------------------------------------------------+
int                 _MqttFindLoggerSlot(long chart_id) {
  int idx = -1;
  if (!g_mqtt_logger_chart_slot_index.TryGetValue(chart_id, idx)) {
    return -1;
  }
  if (idx < 0 || idx >= ArraySize(g_mqtt_logger_chart_ids) || g_mqtt_logger_chart_ids[idx] != chart_id) {
    g_mqtt_logger_chart_slot_index.Remove(chart_id);
    return -1;
  }
  return idx;
}

//+------------------------------------------------------------------+
//| _MqttAddLoggerSlot - Create a chart-scoped logger slot           |
//+------------------------------------------------------------------+
int _MqttAddLoggerSlot(long chart_id) {
  int idx = ArraySize(g_mqtt_logger_chart_ids);
  ArrayResize(g_mqtt_logger_chart_ids, idx + 1);
  ArrayResize(g_mqtt_chart_loggers, idx + 1);
  g_mqtt_logger_chart_ids[idx] = chart_id;
  g_mqtt_logger_chart_slot_index.Add(chart_id, idx);
  return idx;
}

//+------------------------------------------------------------------+
//| MqttReleaseChartLogger - Release the current chart logger slot   |
//| Call from EA/Script OnDeinit() after the last MQTT operation.    |
//+------------------------------------------------------------------+
void MqttReleaseChartLogger() {
  long chart_id = (long)ChartID();
  int  idx      = _MqttFindLoggerSlot(chart_id);
  if (idx < 0) {
    return;
  }

  g_mqtt_logger_chart_slot_index.Remove(chart_id);

  const int last_idx = ArraySize(g_mqtt_logger_chart_ids) - 1;
  if (idx < last_idx) {
    long moved_chart_id          = g_mqtt_logger_chart_ids[last_idx];
    g_mqtt_logger_chart_ids[idx] = moved_chart_id;
    g_mqtt_chart_loggers[idx]    = g_mqtt_chart_loggers[last_idx];
    g_mqtt_logger_chart_slot_index.Remove(moved_chart_id);
    g_mqtt_logger_chart_slot_index.Add(moved_chart_id, idx);
  }

  ArrayResize(g_mqtt_logger_chart_ids, last_idx);
  ArrayResize(g_mqtt_chart_loggers, last_idx);
}

//+------------------------------------------------------------------+
//| _MqttSetActiveLogger - Bind logger to the current chart thread   |
//+------------------------------------------------------------------+
void _MqttSetActiveLogger(const CLogger& logger) {
  long chart_id = (long)ChartID();
  int  idx      = _MqttFindLoggerSlot(chart_id);
  if (idx < 0) {
    idx = _MqttAddLoggerSlot(chart_id);
  }

  g_mqtt_chart_loggers[idx] = logger;
}

//+------------------------------------------------------------------+
//| _MqttGetActiveLogger - Resolve logger for the current chart      |
//+------------------------------------------------------------------+
CLogger _MqttGetActiveLogger() {
  long chart_id = (long)ChartID();
  int  idx      = _MqttFindLoggerSlot(chart_id);
  if (idx >= 0) {
    return g_mqtt_chart_loggers[idx];
  }
  return g_mqtt_logger;
}

//+------------------------------------------------------------------+
//| _MqttGetActiveLogLevel - Fast level lookup for macros            |
//+------------------------------------------------------------------+
int _MqttGetActiveLogLevel() {
  CLogger logger = _MqttGetActiveLogger();
  return logger.m_log_level;
}

//+------------------------------------------------------------------+
//| _MqttLog — internal log dispatcher                               |
//| Do NOT call directly; use the MQTT_LOG_* macros below so that    |
//| __FILE__, __FUNCTION__, __LINE__ are expanded at the call site.  |
//|                                                                  |
//| Parameters:                                                      |
//|   level — severity of this message                               |
//|   tag   — text label inserted into [TAG] prefix ("ERROR" etc.)   |
//|   file  — source filename (__FILE__ from macro)                  |
//|   func  — function name  (__FUNCTION__ from macro)               |
//|   line  — source line    (__LINE__ from macro)                   |
//|   msg   — application message text                               |
//+------------------------------------------------------------------+
void _MqttLog(int level, const string tag, const string file, const string func, int line, const string msg) {
  CLogger logger = _MqttGetActiveLogger();

  //--- Filter: suppress messages below the configured threshold
  if (level > logger.m_log_level) {
    return;
  }

  //--- Format: [TAG] file:line in func — msg
  string out = StringFormat("[%s] %s:%d in %s — %s", tag, file, line, func, msg);

  //--- Dispatch to custom sink or fallback Print()
  if (logger.m_log_sink != NULL) {
    logger.m_log_sink(level, out);
  } else {
    Print(out);
  }
}

//+------------------------------------------------------------------+
//| _MqttLogWithLogger — explicit logger dispatcher                  |
//| Accepts a resolved CLogger value. The public MQTT_LOG_* macros   |
//| obtain that logger from the chart-scoped registry so packet and  |
//| transport layers follow the active CMqttClient on each chart.    |
//+------------------------------------------------------------------+
void _MqttLogWithLogger(const CLogger& logger, int level, const string tag, const string file, const string func,
                        int line, const string msg) {
  if (level > logger.m_log_level) {
    return;
  }
  string out = StringFormat("[%s] %s:%d in %s — %s", tag, file, line, func, msg);
  if (logger.m_log_sink != NULL) {
    logger.m_log_sink(level, out);
  } else {
    Print(out);
  }
}

//+------------------------------------------------------------------+
//| Public logging macros                                            |
//| MQL5 predefined constants __FILE__, __FUNCTION__, __LINE__ are   |
//| expanded at the CALL SITE (inside the macro), so the logged      |
//| location always reflects where the macro was invoked, not        |
//| inside _MqttLog.                                                 |
//|                                                                  |
//| Level guard prevents string-argument evaluation                  |
//| (and associated heap allocations) when the message would be      |
//| suppressed anyway.  Enum values renamed MQTT_LEVEL_* to avoid    |
//| the C function-like macro name collision with MQTT_LOG_*(msg).   |
//+------------------------------------------------------------------+
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

#endif  // MQTT_LOGGER_MQH
