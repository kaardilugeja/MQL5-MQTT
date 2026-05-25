//+------------------------------------------------------------------+
//|                                                      Context.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT Session Context wrapper.                                    |
//| Holds the state for a single MQTT session, allowing multiple     |
//| simultaneous sessions by decoupling from global state.           |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_SESSION_CONTEXT_MQH
#define MQTT_INTERNAL_SESSION_CONTEXT_MQH

#include "..\\Storage\\SessionDatabase.mqh"
#include "..\\Connection\\FlowControl.mqh"
#include "..\\Util\\Logger.mqh"
#include "TopicAliasManager.mqh"

//+------------------------------------------------------------------+
//| Class CMqttContext                                               |
//| Purpose: Encapsulates all session-specific state, including the  |
//|          database, flow control, and topic alias management.     |
//+------------------------------------------------------------------+
class CMqttContext {
 public:
  //--- Logger is owned here so it is scoped per-session.
  //--- CMqttClient calls _SyncLogger() to refresh the active chart logger
  //--- before any operation that may emit log output.
  CLogger            logger;
  CSessionDatabase   session_db;
  CFlowControl       flow_control;
  CTopicAliasManager topic_alias_manager;

  CMqttContext() {}
  ~CMqttContext() {}

  //+------------------------------------------------------------------+
  //| Reset                                                            |
  //| Purpose: Clear all session state for a fresh start               |
  //+------------------------------------------------------------------+
  void Reset() {
    session_db.ResetSession();
    flow_control.ResetAll();
    topic_alias_manager.ClearAll();
  }

  //--- Must be called whenever the Network Connection is lost or closed.
  //+------------------------------------------------------------------+
  //| OnDisconnect                                                     |
  //| Purpose: Handle network disconnection (clear aliases & flow win) |
  //+------------------------------------------------------------------+
  //--- Per MQTT 5.0 §3.3.2.3.4, Topic Alias mappings "last only for the
  //--- lifetime of that Network Connection" — they MUST be cleared before
  //--- any reconnect attempt so stale aliases are never used on the new
  //--- connection.  Flow control in-flight state is also reset because the
  //--- new connection starts with an empty send window.
  void OnDisconnect() {
    //--- Clear transient per-connection state BEFORE flushing to disk.
    //--- If the flush is called first and fails (full disk, I/O error),
    //--- stale alias mappings or in-flight counters could survive on disk
    //--- and be loaded by the next reconnect, causing protocol errors.
    topic_alias_manager.ClearAll();      // Invalidate all alias→topic mappings
    flow_control.ResetTransientState();  // Clear transient window, keep negotiated limits
    session_db.FlushIfDirty(0);          // Flush only after state is clean
  }
};

#endif  // MQTT_INTERNAL_SESSION_CONTEXT_MQH
