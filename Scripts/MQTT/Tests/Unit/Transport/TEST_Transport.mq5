//+------------------------------------------------------------------+
//|                                               TEST_Transport.mq5 |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| CPacketFramer — TCP stream reassembly (partial reads).           |
//|                                                                  |
//| Rationale: TCP is a byte-stream protocol. A single socket read   |
//| may return fewer bytes than a complete MQTT packet (partial      |
//| read), or multiple packets concatenated (coalesced read). The    |
//| CPacketFramer::Feed() / NextPacket() pair must handle all cases. |
//| These tests cover the scenarios listed in AUDIT §10.3, item 1:   |
//|   • Send() retry loop / partial-read framing                     |
//|   • Buffer compaction (m_head >= m_tail/2)                       |
//|                                                                  |
//| All tests are fully offline — no broker required.                |
//+------------------------------------------------------------------+
#define MQTT_UNIT_TESTS
#include "..\..\TestUtil.mqh"
#include "..\..\..\..\..\Include\MQTT\Internal\Transport\Transport.mqh"
#include "..\..\..\..\..\Include\MQTT\Internal\Transport\WebSocketTransport.mqh"

//+------------------------------------------------------------------+
//| Helper: build a minimal MQTT 5.0 QoS-0 PUBLISH packet            |
//| Topic: "t" (1 byte), Payload: "X" (1 byte)                       |
//| Wire:  [0x30, 0x05, 0x00, 0x01, 't', 0x00, 'X']  (7 bytes)       |
//+------------------------------------------------------------------+
void BuildMinimalPublish(uchar &pkt[]) {
  ArrayResize(pkt, 7);
  pkt[0] = 0x30;  // PUBLISH, QoS-0, no DUP, no RETAIN
  pkt[1] = 0x05;  // Remaining Length = 5
  pkt[2] = 0x00;  // Topic Length MSB
  pkt[3] = 0x01;  // Topic Length LSB = 1
  pkt[4] = 0x74;  // 't'
  pkt[5] = 0x00;  // Properties Length = 0
  pkt[6] = 0x58;  // 'X'
}

//+------------------------------------------------------------------+
//| Helper: build a larger PUBLISH (topic="data", payload=20 bytes)  |
//| Used to produce distinct packet content for multi-packet tests.  |
//+------------------------------------------------------------------+
void BuildLargerPublish(uchar &pkt[]) {
  //--- Topic "data" = 4 bytes → topic field = 6 bytes (2 len + 4 chars)
  //--- Properties = 1 byte (0x00)
  //--- Payload = 20 bytes
  //--- Remaining length = 6 + 1 + 20 = 27
  ArrayResize(pkt, 29);
  pkt[0] = 0x30;  // PUBLISH QoS-0
  pkt[1] = 0x1B;  // Remaining Length = 27
  pkt[2] = 0x00;
  pkt[3] = 0x04;  // Topic Length = 4
  pkt[4] = 0x64;
  pkt[5] = 0x61;
  pkt[6] = 0x74;
  pkt[7] = 0x61;  // "data"
  pkt[8] = 0x00;  // Properties Length = 0
  //--- 20 byte payload: 0x00-0x13
  for (int i = 0; i < 20; i++) {
    pkt[9 + i] = (uchar)i;
  }
}

//+------------------------------------------------------------------+
//| Helper: build a small WebSocket frame for parser unit tests      |
//| Uses a deterministic mask key so compatibility-mode assertions   |
//| can verify the unmasked bytes exactly.                           |
//+------------------------------------------------------------------+
void BuildWsFrame(const uchar opcode, const uchar &payload[], const bool masked, uchar &frame[]) {
  uint  payload_len = (uint)ArraySize(payload);
  uchar mask_key[]  = {0x11, 0x22, 0x33, 0x44};
  uint  frame_len   = 2 + (masked ? 4 : 0) + payload_len;

  ArrayResize(frame, (int)frame_len);
  frame[0] = (uchar)(0x80 | (opcode & 0x0F));
  frame[1] = (uchar)((masked ? 0x80 : 0x00) | payload_len);

  uint payload_offset = 2;
  if (masked) {
    frame[2]      = mask_key[0];
    frame[3]      = mask_key[1];
    frame[4]      = mask_key[2];
    frame[5]      = mask_key[3];
    payload_offset = 6;
  }

  for (uint i = 0; i < payload_len; i++) {
    frame[payload_offset + i] = masked ? (payload[i] ^ mask_key[i % 4]) : payload[i];
  }
}

//+------------------------------------------------------------------+
//| TEST_Framer_CompletePacket                                       |
//| Feed a fully-formed packet in a single call — must extract       |
//| immediately and report no further packets available.             |
//+------------------------------------------------------------------+
bool TEST_Framer_CompletePacket() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt[];
  BuildMinimalPublish(pkt);

  framer.Feed(pkt, 7);

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));
  ASSERT_EQ(0x30, (int)out[0]);  // Fixed header preserved

  //--- Second call: buffer empty → false
  ASSERT_FALSE(framer.NextPacket(out, err));
  ASSERT_EQ((int)TRANSPORT_OK, (int)err);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_ByteByByteFeed                                       |
//| Simulate maximum fragmentation: one byte per Feed() call.        |
//| Models the worst-case TCP partial-read scenario from AUDIT §10.3 |
//| item 1 — the Send() retry loop mirrors this on the write side.   |
//+------------------------------------------------------------------+
bool TEST_Framer_ByteByByteFeed() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt[];
  BuildMinimalPublish(pkt);  // 7 bytes total

  uchar                single[1];
  uchar                out[];
  ENUM_TRANSPORT_ERROR err;

  //--- Feed bytes 0-5: packet is still incomplete each time
  for (int i = 0; i < 6; i++) {
    single[0] = pkt[i];
    framer.Feed(single, 1);
    ASSERT_FALSE(framer.NextPacket(out, err));
    ASSERT_EQ((int)TRANSPORT_OK, (int)err);
  }

  //--- Feed the 7th and final byte
  single[0] = pkt[6];
  framer.Feed(single, 1);

  bool ok = framer.NextPacket(out, err);
  ASSERT_TRUE(ok);
  ASSERT_EQ(7, ArraySize(out));
  ASSERT_EQ(0x58, (int)out[6]);  // Payload byte 'X'

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_TwoPacketsCoalesced                                  |
//| Feed two packets in one chunk — both must be individually        |
//| extracted via successive NextPacket() calls.                     |
//+------------------------------------------------------------------+
bool TEST_Framer_TwoPacketsCoalesced() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt1[], pkt2[], combined[];
  BuildMinimalPublish(pkt1);  // 7 bytes, payload 'X' (0x58)
  BuildLargerPublish(pkt2);   // 29 bytes

  ArrayResize(combined, 36);
  ArrayCopy(combined, pkt1, 0, 0, 7);
  ArrayCopy(combined, pkt2, 7, 0, 29);

  framer.Feed(combined, 36);

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;

  //--- First packet
  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));
  ASSERT_EQ(0x58, (int)out[6]);  // 'X'

  //--- Second packet
  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(29, ArraySize(out));
  ASSERT_EQ(0x30, (int)out[0]);

  //--- No third packet
  ASSERT_FALSE(framer.NextPacket(out, err));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_SplitAtFixedHeaderBoundary                           |
//| Feed only the first byte (Type byte), then the rest.             |
//| Exercises the guard that requires ≥2 bytes before decoding       |
//| the Remaining Length varint.                                     |
//+------------------------------------------------------------------+
bool TEST_Framer_SplitAtFixedHeaderBoundary() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt[];
  BuildMinimalPublish(pkt);

  //--- Feed just the fixed-header type byte
  uchar hdr[1];
  hdr[0] = pkt[0];
  framer.Feed(hdr, 1);

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  ASSERT_FALSE(framer.NextPacket(out, err));  // Needs ≥2 bytes
  ASSERT_EQ((int)TRANSPORT_OK, (int)err);

  //--- Feed remaining 6 bytes
  uchar rest[];
  ArrayResize(rest, 6);
  ArrayCopy(rest, pkt, 0, 1, 6);
  framer.Feed(rest, 6);

  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_SplitAtPayloadBoundary                               |
//| Feed the fixed header + remlen varint, then feed the payload.    |
//| Exercises the "partial packet — not all bytes arrived" path.     |
//+------------------------------------------------------------------+
bool TEST_Framer_SplitAtPayloadBoundary() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt[];
  BuildMinimalPublish(pkt);  // 2-byte header, 5-byte body

  //--- Feed header only (2 bytes: type + remlen)
  uchar header[2];
  header[0] = pkt[0];
  header[1] = pkt[1];
  framer.Feed(header, 2);

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  ASSERT_FALSE(framer.NextPacket(out, err));  // Body not yet received
  ASSERT_EQ((int)TRANSPORT_OK, (int)err);

  //--- Feed remaining 5 bytes (body)
  uchar body[];
  ArrayResize(body, 5);
  ArrayCopy(body, pkt, 0, 2, 5);
  framer.Feed(body, 5);

  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_Reset                                                |
//| Verify that Reset() discards partial data and the framer         |
//| accepts new packets cleanly afterwards.                          |
//+------------------------------------------------------------------+
bool TEST_Framer_Reset() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt[];
  BuildMinimalPublish(pkt);

  //--- Feed only 3 bytes (mid-stream partial)
  framer.Feed(pkt, 3);
  ASSERT_EQ(3, (int)framer.Available());

  //--- Reset: all buffered data discarded
  framer.Reset();
  ASSERT_EQ(0, (int)framer.Available());

  //--- Feed complete packet after reset: must work correctly
  framer.Feed(pkt, 7);
  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_BufferCompaction                                     |
//| After consuming a packet, m_head advances. On the next Feed(),   |
//| the compaction guard (m_head >= m_tail/2) triggers a memmove-    |
//| style copy that collapses the buffer to offset 0.                |
//| Verifies that compaction is transparent: packets fed and         |
//| consumed in a loop produce the expected results throughout.      |
//+------------------------------------------------------------------+
bool TEST_Framer_BufferCompaction() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt[];
  BuildMinimalPublish(pkt);

  //--- Pump 20 packets through the framer.
  //--- After each NextPacket() m_head == m_tail (buffer fully consumed).
  //--- The NEXT Feed() triggers compaction (m_head=7 >= m_tail/2=3).
  const int ROUNDS = 20;
  for (int r = 0; r < ROUNDS; r++) {
    framer.Feed(pkt, 7);

    uchar                out[];
    ENUM_TRANSPORT_ERROR err;
    bool                 ok = framer.NextPacket(out, err);
    ASSERT_TRUE(ok);
    ASSERT_EQ(7, ArraySize(out));
    ASSERT_EQ(0x30, (int)out[0]);
    ASSERT_EQ(0x58, (int)out[6]);  // Payload 'X' intact after compaction
    ASSERT_EQ((int)TRANSPORT_OK, (int)err);
  }

  //--- After all rounds, buffer should be empty
  ASSERT_EQ(0, (int)framer.Available());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_MaxPacketSize_Reject                                 |
//| Packets exceeding SetMaxPacketSize() must be dropped and         |
//| TRANSPORT_ERROR_PKT_TOO_BIG returned.                            |
//+------------------------------------------------------------------+
bool TEST_Framer_MaxPacketSize_Reject() {
  TEST_CASE_START();

  CPacketFramer framer;
  //--- Our packet is 7 bytes total (remlen=5). Limit to 5 total bytes.
  framer.SetMaxPacketSize(5);

  uchar pkt[];
  BuildMinimalPublish(pkt);  // total_len = 7 > limit 5 → rejected
  framer.Feed(pkt, 7);

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  bool                 ok = framer.NextPacket(out, err);

  ASSERT_FALSE(ok);
  ASSERT_EQ((int)TRANSPORT_ERROR_PKT_TOO_BIG, (int)err);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_MaxPacketSize_Accept                                 |
//| Packets ≤ SetMaxPacketSize() must be accepted normally.          |
//+------------------------------------------------------------------+
bool TEST_Framer_MaxPacketSize_Accept() {
  TEST_CASE_START();

  CPacketFramer framer;
  framer.SetMaxPacketSize(7);  // Exactly our 7-byte packet → accept

  uchar pkt[];
  BuildMinimalPublish(pkt);
  framer.Feed(pkt, 7);

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_MaxBufferSize_Overflow                               |
//| When SetMaxBufferSize() is hit, data is dropped and              |
//| NextPacket() returns TRANSPORT_ERROR_PKT_TOO_BIG via m_overflow. |
//+------------------------------------------------------------------+
bool TEST_Framer_MaxBufferSize_Overflow() {
  TEST_CASE_START();

  CPacketFramer framer;
  framer.SetMaxBufferSize(4);  // Force overflow on our 7-byte feed

  uchar pkt[];
  BuildMinimalPublish(pkt);
  framer.Feed(pkt, 7);  // triggers m_overflow

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  bool                 ok = framer.NextPacket(out, err);

  ASSERT_FALSE(ok);
  ASSERT_EQ((int)TRANSPORT_ERROR_PKT_TOO_BIG, (int)err);

  //--- The configured limit must remain active after the reset.
  framer.Feed(pkt, 7);
  ASSERT_FALSE(framer.NextPacket(out, err));
  ASSERT_EQ((int)TRANSPORT_ERROR_PKT_TOO_BIG, (int)err);

  //--- Raising the limit should allow the same packet through.
  framer.SetMaxBufferSize(8);
  framer.Feed(pkt, 7);
  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_OverflowPreservesEarlierPackets                      |
//| A buffer overflow must not discard packets fully buffered before |
//| the truncated boundary is reached.                               |
//+------------------------------------------------------------------+
bool TEST_Framer_OverflowPreservesEarlierPackets() {
  TEST_CASE_START();

  CPacketFramer framer;
  framer.SetMaxBufferSize(12);  // Fits pkt1 (7 bytes) plus 5 bytes of pkt2

  uchar pkt1[], pkt2[], combined[];
  BuildMinimalPublish(pkt1);
  BuildMinimalPublish(pkt2);
  ArrayResize(combined, 14);
  ArrayCopy(combined, pkt1, 0, 0, 7);
  ArrayCopy(combined, pkt2, 7, 0, 7);

  framer.Feed(combined, 14);  // pkt1 preserved, pkt2 truncated by overflow

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;

  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(7, ArraySize(out));
  ASSERT_EQ(0x30, (int)out[0]);
  ASSERT_EQ((int)TRANSPORT_OK, (int)err);

  ASSERT_FALSE(framer.NextPacket(out, err));
  ASSERT_EQ((int)TRANSPORT_ERROR_PKT_TOO_BIG, (int)err);
  ASSERT_EQ(0, (int)framer.Available());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_MalformedRemainingLengthSignalsBadFrame              |
//| A 5-byte Remaining Length varint must fail closed as BAD_FRAME.  |
//+------------------------------------------------------------------+
bool TEST_Framer_MalformedRemainingLengthSignalsBadFrame() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         bad_pkt[] = {0x30, 0x80, 0x80, 0x80, 0x80};

  framer.Feed(bad_pkt, ArraySize(bad_pkt));

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  ASSERT_FALSE(framer.NextPacket(out, err));
  ASSERT_EQ((int)TRANSPORT_ERROR_BAD_FRAME, (int)err);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_SkipOneByte                                          |
//| Insert a corrupt leading byte then call SkipOneByte() to resync, |
//| then feed a valid packet — verify it is extracted correctly.     |
//+------------------------------------------------------------------+
bool TEST_Framer_SkipOneByte() {
  TEST_CASE_START();

  CPacketFramer framer;

  //--- Feed 1 garbage byte + 1 valid packet
  uchar         garbage[1];
  garbage[0] = 0xFF;  // Invalid fixed header for a 7-type field
  framer.Feed(garbage, 1);

  uchar pkt[];
  BuildMinimalPublish(pkt);
  framer.Feed(pkt, 7);

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;

  //--- First NextPacket() reads 8 bytes starting at garbage.
  //--- remlen at offset 1 would be 0x30 = 48 bytes; then 48+2=50 byte
  //--- packet not yet in buffer → returns false (needs more data).
  //--- Skip the garbage byte to re-sync.
  bool                 ok = framer.NextPacket(out, err);
  if (!ok) {
    ASSERT_TRUE(framer.SkipOneByte());
  }

  //--- Now NextPacket() should find the valid packet at the new head
  ok = framer.NextPacket(out, err);
  ASSERT_TRUE(ok);
  ASSERT_EQ(7, ArraySize(out));
  ASSERT_EQ(0x30, (int)out[0]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_Available                                            |
//| Verify Available() tracks unprocessed bytes accurately.          |
//+------------------------------------------------------------------+
bool TEST_Framer_Available() {
  TEST_CASE_START();

  CPacketFramer framer;
  ASSERT_EQ(0, (int)framer.Available());

  uchar pkt[];
  BuildMinimalPublish(pkt);
  framer.Feed(pkt, 7);
  ASSERT_EQ(7, (int)framer.Available());

  uchar                out[];
  ENUM_TRANSPORT_ERROR err;
  framer.NextPacket(out, err);
  ASSERT_EQ(0, (int)framer.Available());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Framer_LargePacketByteByByte                                |
//| Feed a 29-byte packet one byte at a time; verify no false        |
//| positives and exact reassembly, including multi-byte payloads.   |
//+------------------------------------------------------------------+
bool TEST_Framer_LargePacketByteByByte() {
  TEST_CASE_START();

  CPacketFramer framer;
  uchar         pkt[];
  BuildLargerPublish(pkt);  // 29 bytes

  uchar                single[1];
  uchar                out[];
  ENUM_TRANSPORT_ERROR err;

  for (int i = 0; i < 28; i++) {
    single[0] = pkt[i];
    framer.Feed(single, 1);
    ASSERT_FALSE(framer.NextPacket(out, err));
  }

  single[0] = pkt[28];
  framer.Feed(single, 1);

  ASSERT_TRUE(framer.NextPacket(out, err));
  ASSERT_EQ(29, ArraySize(out));
  ASSERT_EQ(0x30, (int)out[0]);
  ASSERT_EQ(0x1B, (int)out[1]);  // Remaining Length = 27

  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_DeferredTlsWriteWaitsForProbeWindow               |
//| After TLS falls back to application-write probing, Poll()        |
//| should honor the next scheduled probe time instead of            |
//| hammering CONNECT on every timer tick.                           |
//+------------------------------------------------------------------+
bool TEST_Transport_DeferredTlsWriteWaitsForProbeWindow() {
  TEST_CASE_START();

  CMqttTransport       tx;
  PacketBuffer         out_packets[];
  uint                 out_count = 0;
  ENUM_TRANSPORT_ERROR err;

  tx.TestSetSocket(1);
  tx.TestSetConnected(true);
  tx.TestSetConnectPhase(TRANSPORT_PHASE_CONNECTED);
  tx.TestSetTls(true);
  tx.TestSetTlsHandshakeComplete(false);
  tx.TestSetTlsAppdataReady(false);
  tx.TestSetTlsPhaseStartedUs(GetMicrosecondCount() - 6000000ULL);
  tx.TestSetTlsNextWriteProbeUs(GetMicrosecondCount() + 1000000ULL);
  tx.TestSetWriteReady(false);

  err = tx.Poll(out_packets, out_count);

  ASSERT_EQ((int)TRANSPORT_CONNECTING, (int)err);
  ASSERT_EQ(0, (int)out_count);
  tx.TestClearWriteReadyStub();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_DeferredTlsWriteProbesAtProbeWindow               |
//| Once the scheduled deferred-write probe window is reached,       |
//| Poll() must let the caller attempt CONNECT even if MT5 still     |
//| does not report the TLS socket writable.                         |
//+------------------------------------------------------------------+
bool TEST_Transport_DeferredTlsWriteProbesAtProbeWindow() {
  TEST_CASE_START();

  CMqttTransport       tx;
  PacketBuffer         out_packets[];
  uint                 out_count = 0;
  ENUM_TRANSPORT_ERROR err;

  tx.TestSetSocket(1);
  tx.TestSetConnected(true);
  tx.TestSetConnectPhase(TRANSPORT_PHASE_CONNECTED);
  tx.TestSetTls(true);
  tx.TestSetTlsHandshakeComplete(false);
  tx.TestSetTlsAppdataReady(false);
  tx.TestSetTlsPhaseStartedUs(GetMicrosecondCount() - 6000000ULL);
  tx.TestSetTlsNextWriteProbeUs(GetMicrosecondCount() - 1000ULL);
  tx.TestSetWriteReady(false);

  err = tx.Poll(out_packets, out_count);

  ASSERT_EQ((int)TRANSPORT_OK, (int)err);
  ASSERT_EQ(0, (int)out_count);
  tx.TestClearWriteReadyStub();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_ExplicitTlsWriteWaitsForProbeWindow               |
//| Even after explicit TLS completes, Poll() should keep CONNECT    |
//| deferred until the socket is writable or the paced probe window  |
//| expires.                                                         |
//+------------------------------------------------------------------+
bool TEST_Transport_ExplicitTlsWriteWaitsForProbeWindow() {
  TEST_CASE_START();

  CMqttTransport       tx;
  PacketBuffer         out_packets[];
  uint                 out_count = 0;
  ENUM_TRANSPORT_ERROR err;

  tx.TestSetSocket(1);
  tx.TestSetConnected(true);
  tx.TestSetConnectPhase(TRANSPORT_PHASE_CONNECTED);
  tx.TestSetTls(true);
  tx.TestSetTlsHandshakeComplete(true);
  tx.TestSetTlsAppdataReady(false);
  tx.TestSetTlsNextWriteProbeUs(GetMicrosecondCount() + 250000ULL);
  tx.TestSetWriteReady(false);

  err = tx.Poll(out_packets, out_count);

  ASSERT_EQ((int)TRANSPORT_CONNECTING, (int)err);
  ASSERT_EQ(0, (int)out_count);
  tx.TestClearWriteReadyStub();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_ExplicitTlsWriteProbesAfterWindow                 |
//| Once the paced probe window expires, the caller may attempt the  |
//| first post-handshake CONNECT even if MT5 still reports the       |
//| socket non-writable.                                             |
//+------------------------------------------------------------------+
bool TEST_Transport_ExplicitTlsWriteProbesAfterWindow() {
  TEST_CASE_START();

  CMqttTransport       tx;
  PacketBuffer         out_packets[];
  uint                 out_count = 0;
  ENUM_TRANSPORT_ERROR err;

  tx.TestSetSocket(1);
  tx.TestSetConnected(true);
  tx.TestSetConnectPhase(TRANSPORT_PHASE_CONNECTED);
  tx.TestSetTls(true);
  tx.TestSetTlsHandshakeComplete(true);
  tx.TestSetTlsAppdataReady(false);
  tx.TestSetTlsNextWriteProbeUs(GetMicrosecondCount() - 1000ULL);
  tx.TestSetWriteReady(false);

  err = tx.Poll(out_packets, out_count);

  ASSERT_EQ((int)TRANSPORT_OK, (int)err);
  ASSERT_EQ(0, (int)out_count);
  tx.TestClearWriteReadyStub();
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_RestartedPendingTlsFallsBackOnLowBudget           |
//| A restarted non-443 TLS handshake should switch to deferred      |
//| first-write mode once the remaining overall connect budget is    |
//| too small to justify more 5274 polling.                          |
//+------------------------------------------------------------------+
bool TEST_Transport_RestartedPendingTlsFallsBackOnLowBudget() {
  TEST_CASE_START();

  CMqttTransport tx;

  tx.TestSetTlsHandshakeRestartCount(1);

  ASSERT_TRUE(tx.TestShouldDeferPendingTlsHandshake(3000000ULL, 5000u, 500u));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_RestartedPendingTlsKeepsRetryingWithBudget        |
//| A restarted non-443 TLS socket should not fall back to deferred  |
//| first-write mode while there is still enough connect budget for  |
//| another bounded explicit-handshake retry.                        |
//+------------------------------------------------------------------+
bool TEST_Transport_RestartedPendingTlsKeepsRetryingWithBudget() {
  TEST_CASE_START();

  CMqttTransport tx;

  tx.TestSetTlsHandshakeRestartCount(1);

  ASSERT_FALSE(tx.TestShouldDeferPendingTlsHandshake(25000000ULL, 45000u, 500u));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_ThirdPendingTlsFallsBackAfterTwoRestarts          |
//| After two explicit-handshake restarts, the transport may switch  |
//| to deferred first-write mode if another socket still hangs.      |
//+------------------------------------------------------------------+
bool TEST_Transport_ThirdPendingTlsFallsBackAfterTwoRestarts() {
  TEST_CASE_START();

  CMqttTransport tx;

  tx.TestSetTlsHandshakeRestartCount(2);

  ASSERT_TRUE(tx.TestShouldDeferPendingTlsHandshake(25000000ULL, 45000u, 500u));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_Transport_FirstPendingTlsKeepsWaitingWithSameBudget         |
//| The first pending explicit TLS socket should still use the       |
//| bounded restart path instead of falling back solely because the  |
//| remaining budget is low.                                         |
//+------------------------------------------------------------------+
bool TEST_Transport_FirstPendingTlsKeepsWaitingWithSameBudget() {
  TEST_CASE_START();

  CMqttTransport tx;

  tx.TestSetTlsHandshakeRestartCount(0);

  ASSERT_FALSE(tx.TestShouldDeferPendingTlsHandshake(3000000ULL, 5000u, 500u));
  return true;
}

//+------------------------------------------------------------------+
//| TEST_WebSocket_MaskedServerFrameRejectedByDefault                |
//| Strict mode must fail closed on a masked server-to-client frame. |
//+------------------------------------------------------------------+
bool TEST_WebSocket_MaskedServerFrameRejectedByDefault() {
  TEST_CASE_START();

  CWebSocketTransport ws;
  uchar               payload[] = {0x30, 0x00};
  uchar               frame[];
  uchar               out[];

  BuildWsFrame(0x2, payload, true, frame);
  ws.TestSetConnected(true);
  ws.TestInjectWsBytes(frame, (uint)ArraySize(frame));

  ASSERT_FALSE(ws.TestNextWsFramePayload(out));
  ASSERT_EQ(0, ArraySize(out));
  ASSERT_FALSE(ws.TestIsConnected());

  return true;
}

//+------------------------------------------------------------------+
//| TEST_WebSocket_MaskedServerFrameAllowedInCompatibilityMode       |
//| Compatibility mode must still unmask and deliver the payload.    |
//+------------------------------------------------------------------+
bool TEST_WebSocket_MaskedServerFrameAllowedInCompatibilityMode() {
  TEST_CASE_START();

  CWebSocketTransport ws;
  uchar               payload[] = {0x30, 0x02, 0x00};
  uchar               frame[];
  uchar               out[];

  BuildWsFrame(0x2, payload, true, frame);
  ws.SetAllowMaskedServerFrames(true);
  ws.TestInjectWsBytes(frame, (uint)ArraySize(frame));

  ASSERT_TRUE(ws.TestNextWsFramePayload(out));
  ASSERT_EQ(ArraySize(payload), ArraySize(out));
  ASSERT_EQ((int)payload[0], (int)out[0]);
  ASSERT_EQ((int)payload[1], (int)out[1]);
  ASSERT_EQ((int)payload[2], (int)out[2]);

  return true;
}

//+------------------------------------------------------------------+
//| TEST_WebSocket_PingFrameDoesNotSurfaceAsMqttPayload             |
//| Ping frames must be consumed internally and not reach MQTT.      |
//+------------------------------------------------------------------+
bool TEST_WebSocket_PingFrameDoesNotSurfaceAsMqttPayload() {
  TEST_CASE_START();

  CWebSocketTransport ws;
  uchar               payload[] = {0xAA, 0xBB};
  uchar               frame[];
  uchar               out[];

  BuildWsFrame(0x9, payload, false, frame);
  ws.TestInjectWsBytes(frame, (uint)ArraySize(frame));

  ASSERT_FALSE(ws.TestNextWsFramePayload(out));
  ASSERT_EQ(0, ArraySize(out));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_WebSocket_PongFrameDoesNotSurfaceAsMqttPayload             |
//| Pong frames must be ignored by the MQTT framer path.             |
//+------------------------------------------------------------------+
bool TEST_WebSocket_PongFrameDoesNotSurfaceAsMqttPayload() {
  TEST_CASE_START();

  CWebSocketTransport ws;
  uchar               payload[] = {0x01};
  uchar               frame[];
  uchar               out[];

  BuildWsFrame(0xA, payload, false, frame);
  ws.TestInjectWsBytes(frame, (uint)ArraySize(frame));

  ASSERT_FALSE(ws.TestNextWsFramePayload(out));
  ASSERT_EQ(0, ArraySize(out));

  return true;
}

//+------------------------------------------------------------------+
//| TEST_WebSocket_CloseFrameDisconnectsAndStaysOutOfMqtt           |
//| Close frames must not be surfaced as MQTT payload and must       |
//| transition the transport to disconnected.                        |
//+------------------------------------------------------------------+
bool TEST_WebSocket_CloseFrameDisconnectsAndStaysOutOfMqtt() {
  TEST_CASE_START();

  CWebSocketTransport ws;
  uchar               payload[] = {0x03, 0xE8};
  uchar               frame[];
  uchar               out[];

  BuildWsFrame(0x8, payload, false, frame);
  ws.TestSetConnected(true);
  ws.TestInjectWsBytes(frame, (uint)ArraySize(frame));

  ASSERT_FALSE(ws.TestNextWsFramePayload(out));
  ASSERT_EQ(0, ArraySize(out));
  ASSERT_FALSE(ws.TestIsConnected());

  return true;
}

//+------------------------------------------------------------------+
//| OnStart — test runner                                            |
//+------------------------------------------------------------------+
void OnStart() {
  const string suite_name = "TEST_Transport";
  TestUtilRecordSuiteStart(suite_name);

  int    total            = 0;
  int    passed           = 0;
  int    total_assertions = 0;

  string names[];
  ArrayResize(names, 27);
  names[0]  = "TEST_Framer_CompletePacket";
  names[1]  = "TEST_Framer_ByteByByteFeed";
  names[2]  = "TEST_Framer_TwoPacketsCoalesced";
  names[3]  = "TEST_Framer_SplitAtFixedHeaderBoundary";
  names[4]  = "TEST_Framer_SplitAtPayloadBoundary";
  names[5]  = "TEST_Framer_Reset";
  names[6]  = "TEST_Framer_BufferCompaction";
  names[7]  = "TEST_Framer_MaxPacketSize_Reject";
  names[8]  = "TEST_Framer_MaxPacketSize_Accept";
  names[9]  = "TEST_Framer_MaxBufferSize_Overflow";
  names[10] = "TEST_Framer_OverflowPreservesEarlierPackets";
  names[11] = "TEST_Framer_MalformedRemainingLengthSignalsBadFrame";
  names[12] = "TEST_Framer_SkipOneByte";
  names[13] = "TEST_Framer_Available";
  names[14] = "TEST_Transport_DeferredTlsWriteWaitsForProbeWindow";
  names[15] = "TEST_Transport_DeferredTlsWriteProbesAtProbeWindow";
  names[16] = "TEST_Transport_ExplicitTlsWriteWaitsForProbeWindow";
  names[17] = "TEST_Transport_ExplicitTlsWriteProbesAfterWindow";
  names[18] = "TEST_Transport_RestartedPendingTlsFallsBackOnLowBudget";
  names[19] = "TEST_Transport_RestartedPendingTlsKeepsRetryingWithBudget";
  names[20] = "TEST_Transport_ThirdPendingTlsFallsBackAfterTwoRestarts";
  names[21] = "TEST_Transport_FirstPendingTlsKeepsWaitingWithSameBudget";
  names[22] = "TEST_WebSocket_MaskedServerFrameRejectedByDefault";
  names[23] = "TEST_WebSocket_MaskedServerFrameAllowedInCompatibilityMode";
  names[24] = "TEST_WebSocket_PingFrameDoesNotSurfaceAsMqttPayload";
  names[25] = "TEST_WebSocket_PongFrameDoesNotSurfaceAsMqttPayload";
  names[26] = "TEST_WebSocket_CloseFrameDisconnectsAndStaysOutOfMqtt";

  for (int i = 0; i < ArraySize(names); i++) {
    total++;
    g_tests_passed = 0;
    g_tests_failed = 0;

    //--- Clear any staged comparator failure details before the next case runs.
    TestUtilClearPendingFailure();
    bool result = false;

    if (names[i] == "TEST_Framer_CompletePacket") {
      result = TEST_Framer_CompletePacket();
    } else if (names[i] == "TEST_Framer_ByteByByteFeed") {
      result = TEST_Framer_ByteByByteFeed();
    } else if (names[i] == "TEST_Framer_TwoPacketsCoalesced") {
      result = TEST_Framer_TwoPacketsCoalesced();
    } else if (names[i] == "TEST_Framer_SplitAtFixedHeaderBoundary") {
      result = TEST_Framer_SplitAtFixedHeaderBoundary();
    } else if (names[i] == "TEST_Framer_SplitAtPayloadBoundary") {
      result = TEST_Framer_SplitAtPayloadBoundary();
    } else if (names[i] == "TEST_Framer_Reset") {
      result = TEST_Framer_Reset();
    } else if (names[i] == "TEST_Framer_BufferCompaction") {
      result = TEST_Framer_BufferCompaction();
    } else if (names[i] == "TEST_Framer_MaxPacketSize_Reject") {
      result = TEST_Framer_MaxPacketSize_Reject();
    } else if (names[i] == "TEST_Framer_MaxPacketSize_Accept") {
      result = TEST_Framer_MaxPacketSize_Accept();
    } else if (names[i] == "TEST_Framer_MaxBufferSize_Overflow") {
      result = TEST_Framer_MaxBufferSize_Overflow();
    } else if (names[i] == "TEST_Framer_OverflowPreservesEarlierPackets") {
      result = TEST_Framer_OverflowPreservesEarlierPackets();
    } else if (names[i] == "TEST_Framer_MalformedRemainingLengthSignalsBadFrame") {
      result = TEST_Framer_MalformedRemainingLengthSignalsBadFrame();
    } else if (names[i] == "TEST_Framer_SkipOneByte") {
      result = TEST_Framer_SkipOneByte();
    } else if (names[i] == "TEST_Framer_Available") {
      result = TEST_Framer_Available();
    } else if (names[i] == "TEST_Transport_DeferredTlsWriteWaitsForProbeWindow") {
      result = TEST_Transport_DeferredTlsWriteWaitsForProbeWindow();
    } else if (names[i] == "TEST_Transport_DeferredTlsWriteProbesAtProbeWindow") {
      result = TEST_Transport_DeferredTlsWriteProbesAtProbeWindow();
    } else if (names[i] == "TEST_Transport_ExplicitTlsWriteWaitsForProbeWindow") {
      result = TEST_Transport_ExplicitTlsWriteWaitsForProbeWindow();
    } else if (names[i] == "TEST_Transport_ExplicitTlsWriteProbesAfterWindow") {
      result = TEST_Transport_ExplicitTlsWriteProbesAfterWindow();
    } else if (names[i] == "TEST_Transport_RestartedPendingTlsFallsBackOnLowBudget") {
      result = TEST_Transport_RestartedPendingTlsFallsBackOnLowBudget();
    } else if (names[i] == "TEST_Transport_RestartedPendingTlsKeepsRetryingWithBudget") {
      result = TEST_Transport_RestartedPendingTlsKeepsRetryingWithBudget();
    } else if (names[i] == "TEST_Transport_ThirdPendingTlsFallsBackAfterTwoRestarts") {
      result = TEST_Transport_ThirdPendingTlsFallsBackAfterTwoRestarts();
    } else if (names[i] == "TEST_Transport_FirstPendingTlsKeepsWaitingWithSameBudget") {
      result = TEST_Transport_FirstPendingTlsKeepsWaitingWithSameBudget();
    } else if (names[i] == "TEST_WebSocket_MaskedServerFrameRejectedByDefault") {
      result = TEST_WebSocket_MaskedServerFrameRejectedByDefault();
    } else if (names[i] == "TEST_WebSocket_MaskedServerFrameAllowedInCompatibilityMode") {
      result = TEST_WebSocket_MaskedServerFrameAllowedInCompatibilityMode();
    } else if (names[i] == "TEST_WebSocket_PingFrameDoesNotSurfaceAsMqttPayload") {
      result = TEST_WebSocket_PingFrameDoesNotSurfaceAsMqttPayload();
    } else if (names[i] == "TEST_WebSocket_PongFrameDoesNotSurfaceAsMqttPayload") {
      result = TEST_WebSocket_PongFrameDoesNotSurfaceAsMqttPayload();
    } else if (names[i] == "TEST_WebSocket_CloseFrameDisconnectsAndStaysOutOfMqtt") {
      result = TEST_WebSocket_CloseFrameDisconnectsAndStaysOutOfMqtt();
    }

    total_assertions += g_tests_passed + g_tests_failed;
    if (result && g_tests_failed == 0) {
      passed++;
      TestUtilRecordCasePass(names[i], g_tests_passed);
    } else {
      TestUtilRecordCaseFail(names[i], g_tests_failed);
    }
  }

  TestUtilFinalizeSuite(suite_name, passed, total, total_assertions);
}
