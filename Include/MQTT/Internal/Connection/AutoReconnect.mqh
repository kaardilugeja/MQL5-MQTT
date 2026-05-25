//+------------------------------------------------------------------+
//|                                                AutoReconnect.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Automatic reconnection helper with exponential backoff strategy. |
//|                                                                  |
//| Implements a library-level reconnect policy with configurable    |
//| backoff parameters and symmetric jitter to spread client retry   |
//| attempts after shared outages.                                   |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_CONNECTION_AUTORECONNECT_MQH
#define MQTT_INTERNAL_CONNECTION_AUTORECONNECT_MQH

//+------------------------------------------------------------------+
//| Class CAutoReconnect                                             |
//| Purpose: Manages automatic reconnection with exponential backoff |
//|          strategy per MQTT 5.0 compliance recommendations        |
//| Usage:   Call Start() when connection is lost, then check        |
//|          ShouldReconnect() periodically to determine when to     |
//|          attempt reconnection. Call Stop() when connected.       |
//| Note:    Exponential backoff prevents connection flooding and    |
//|          allows the broker time to recover. A symmetric ±25%     |
//|          jitter is applied to the scheduled delay, including at  |
//|          the configured maximum base backoff.                    |
//+------------------------------------------------------------------+
class CAutoReconnect {
 private:
  //--- Backoff configuration (milliseconds)
  uint  m_min_backoff_ms;      // Lower bound for reconnection delay
  uint  m_max_backoff_ms;      // Maximum base backoff before jitter is applied
  uint  m_current_backoff_ms;  // Next scheduled interval after exponential growth and jitter

  //--- State tracking
  ulong m_last_attempt_ms;  // Timestamp of the start of the last reconnection attempt
  bool  m_is_reconnecting;  // True when the helper is actively monitoring for retries
  bool  m_first_attempt;    // True until the very first ShouldReconnect() fires (immediate trigger)
  uint  m_rng_state;        // Local PRNG state so Start() never reseeds MathRand()

  uint  _NextRandom32();

 public:
  //--- Constructor and Destructor
  CAutoReconnect(uint min_ms = 1000, uint max_ms = 60000);
  ~CAutoReconnect() {}

  //--- Control methods
  void Reset();  // Reset state to initial min_backoff (e.g., after success)
  void Start();  // Enter reconnection mode (e.g., after connection loss)
  void Stop();   // Exit reconnection mode (e.g., after successful connect)

  //--- Query methods
  bool ShouldReconnect();          // Returns true if backoff interval has elapsed
  bool IsReconnecting() const;     // Returns true if helper is in reconnection mode
  uint GetCurrentBackoff() const;  // Returns current delay in milliseconds
};

//+------------------------------------------------------------------+
//| Constructor - Initialize with configurable backoff parameters    |
//| Parameters: min_ms - minimum backoff interval (default 1000ms)   |
//|             max_ms - maximum base backoff (default 60000ms)      |
//| Note: Default values provide 1s initial delay and a 60s base     |
//|       ceiling before the helper applies its symmetric ±25%       |
//|       reconnect jitter.                                          |
//+------------------------------------------------------------------+
CAutoReconnect::CAutoReconnect(uint min_ms, uint max_ms) {
  m_min_backoff_ms =
    (min_ms > 0) ? min_ms : 100;  // Clamp to 100 ms minimum — zero would create an infinite tight reconnect loop
  m_max_backoff_ms = (max_ms >= m_min_backoff_ms) ? max_ms : m_min_backoff_ms;
  Reset();
}

//+------------------------------------------------------------------+
//| Reset - Clear all state and return to initial backoff            |
//| Purpose: Reset backoff timer to minimum value, typically called  |
//|          after a successful connection or when manually retrying |
//+------------------------------------------------------------------+
void CAutoReconnect::Reset() {
  m_current_backoff_ms = m_min_backoff_ms;
  m_last_attempt_ms    = 0;
  m_is_reconnecting    = false;
  m_first_attempt      = false;
}

//+------------------------------------------------------------------+
//| Start - Begin reconnection mode                                  |
//| Purpose: Enable reconnection attempts, reset backoff to minimum  |
//| Note: Sets m_last_attempt_ms to 0 to trigger immediate reconnect |
//|       on first ShouldReconnect() call                            |
//+------------------------------------------------------------------+
void CAutoReconnect::Start() {
  m_is_reconnecting    = true;
  m_current_backoff_ms = m_min_backoff_ms;
  m_last_attempt_ms    = 0;
  m_first_attempt      = true;  // Trigger immediate reconnect on first ShouldReconnect()

  //--- Seed a local LCG so reconnect jitter stays instance-local and does not
  //--- perturb any EA strategy code that also relies on MathRand().
  ulong seed =
    GetMicrosecondCount() ^ ((ulong)ChartID() << 17) ^ ((ulong)m_min_backoff_ms << 1) ^ ((ulong)m_max_backoff_ms << 33);
  seed        ^= (seed >> 33);
  m_rng_state  = (uint)(seed & 0xFFFFFFFF);
  if (m_rng_state == 0) {
    m_rng_state = 0x6D2B79F5;
  }
}

uint CAutoReconnect::_NextRandom32() {
  m_rng_state = 1664525u * m_rng_state + 1013904223u;
  return m_rng_state;
}

//+------------------------------------------------------------------+
//| ShouldReconnect - Check if it's time to attempt reconnection     |
//| Purpose: Logic to determine if a new attempt should be made      |
//| Note: Implements exponential backoff strategy for reconnects.    |
//|       Each failed attempt doubles the base delay until it        |
//|       reaches m_max_backoff_ms, then applies symmetric ±25%      |
//|       jitter to the scheduled delay, including at that ceiling.  |
//| Return: true if the backoff time has passed, false otherwise     |
//+------------------------------------------------------------------+
bool CAutoReconnect::ShouldReconnect() {
  //--- Do nothing if not in reconnection mode
  if (!m_is_reconnecting) {
    return false;
  }

  ulong now          = GetMicrosecondCount() / 1000;

  //--- Check if enough time has elapsed since last attempt
  //--- m_first_attempt flags the very first call so it fires immediately,
  //--- avoiding the m_last_attempt_ms==0 sentinel that collides with the
  //--- real timestamp value 0 returned at script startup.
  bool  is_immediate = m_first_attempt;
  if (is_immediate || (now - m_last_attempt_ms >= m_current_backoff_ms)) {
    m_first_attempt   = false;
    m_last_attempt_ms = now;

    //--- Apply exponential backoff for the NEXT attempt.
    //--- max_backoff_ms is the base ceiling before jitter, not a strict
    //--- upper bound on the scheduled delay returned by GetCurrentBackoff().
    //--- Do not double or jitter the initial 0ms immediate attempt.
    if (!is_immediate) {
      uint next_base_backoff_ms = m_current_backoff_ms;
      if (next_base_backoff_ms < m_max_backoff_ms) {
        next_base_backoff_ms *= 2;
        if (next_base_backoff_ms > m_max_backoff_ms) {
          next_base_backoff_ms = m_max_backoff_ms;
        }
      } else {
        next_base_backoff_ms = m_max_backoff_ms;
      }

      uint jitter_range    = next_base_backoff_ms / 4;
      m_current_backoff_ms = next_base_backoff_ms;
      if (jitter_range > 0) {
        uint jitter_span     = jitter_range * 2u + 1u;
        int  jitter_offset   = (int)(_NextRandom32() % jitter_span) - (int)jitter_range;
        m_current_backoff_ms = (uint)MathMax(1, (int)next_base_backoff_ms + jitter_offset);
      }
    }
    return true;  // Re-attempt should be performed now
  }

  //--- Still waiting for backoff interval to expire
  return false;
}

//+------------------------------------------------------------------+
//| Stop - End reconnection mode                                     |
//| Purpose: Called when connection is successfully established      |
//+------------------------------------------------------------------+
void CAutoReconnect::Stop() { m_is_reconnecting = false; }

//+------------------------------------------------------------------+
//| IsReconnecting - Check if in reconnection mode                   |
//| Return: true if Start() was called but Stop() not yet called     |
//+------------------------------------------------------------------+
bool CAutoReconnect::IsReconnecting() const { return m_is_reconnecting; }

//+------------------------------------------------------------------+
//| GetCurrentBackoff - Get current backoff interval                 |
//| Return: Current backoff in milliseconds                          |
//| Note: Useful for logging or displaying reconnection status       |
//+------------------------------------------------------------------+
uint CAutoReconnect::GetCurrentBackoff() const { return m_current_backoff_ms; }

#endif  // MQTT_AUTORECONNECT_MQH
