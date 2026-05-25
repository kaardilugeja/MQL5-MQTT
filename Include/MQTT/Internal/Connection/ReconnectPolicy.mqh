//+------------------------------------------------------------------+
//|                                              ReconnectPolicy.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_CONNECTION_RECONNECTPOLICY_MQH
#define MQTT_INTERNAL_CONNECTION_RECONNECTPOLICY_MQH

#include "AutoReconnect.mqh"

class CMqttReconnectPolicy {
 private:
  CAutoReconnect m_backoff;
  bool           m_enabled;
  uint           m_min_backoff_ms;
  uint           m_max_backoff_ms;
  uint           m_max_attempts;
  uint           m_current_attempts;

 public:
  CMqttReconnectPolicy()
      : m_backoff(1000, 60000)
      , m_enabled(true)
      , m_min_backoff_ms(1000)
      , m_max_backoff_ms(60000)
      , m_max_attempts(0)
      , m_current_attempts(0) {}

  void Configure(bool enable, uint min_backoff_ms, uint max_backoff_ms) {
    m_enabled        = enable;
    m_min_backoff_ms = min_backoff_ms;
    m_max_backoff_ms = max_backoff_ms;
  }

  bool IsEnabled() const { return m_enabled; }

  void SetMaxAttempts(uint count) { m_max_attempts = count; }
  uint GetMaxAttempts() const { return m_max_attempts; }

  uint GetCurrentAttemptCount() const { return m_current_attempts; }
  void SetCurrentAttemptCount(uint count) { m_current_attempts = count; }

  void RestorePersistedAttemptCount(uint count) {
    if (count > m_current_attempts) {
      m_current_attempts = count;
    }
  }

  void StartLoopIfNeeded() {
    if (!m_enabled || m_backoff.IsReconnecting()) {
      return;
    }
    m_backoff = CAutoReconnect(m_min_backoff_ms, m_max_backoff_ms);
    m_backoff.Start();
  }

  void Stop() { m_backoff.Stop(); }

  void Disable() { m_enabled = false; }

  bool IsReconnecting() const { return m_backoff.IsReconnecting(); }
  bool IsReconnectInProgress() const { return m_enabled && m_backoff.IsReconnecting(); }
  bool ShouldReconnectNow() { return m_enabled && m_backoff.IsReconnecting() && m_backoff.ShouldReconnect(); }
  bool IsCircuitBreakerOpen() const { return m_max_attempts > 0 && m_current_attempts >= m_max_attempts; }
  uint GetCurrentBackoff() const { return m_backoff.GetCurrentBackoff(); }

  void RegisterReconnectAttempt() { m_current_attempts++; }

  void OnManualConnect() {
    m_backoff.Stop();
    m_current_attempts = 0;
  }

  void OnSuccessfulConnect() {
    m_current_attempts = 0;
    m_backoff.Stop();
  }
};

#endif
