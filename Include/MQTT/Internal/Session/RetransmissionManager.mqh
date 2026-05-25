//+------------------------------------------------------------------+
//|                                        RetransmissionManager.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| High-level manager for automatic MQTT packet retransmission.     |
//| Handles QoS 1 and QoS 2 flow resolution based on timeouts.       |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_SESSION_RETRANSMISSION_MANAGER_MQH
#define MQTT_INTERNAL_SESSION_RETRANSMISSION_MANAGER_MQH

#include "Context.mqh"
#include "..\\Storage\\SessionDatabase.mqh"
#include "..\\Protocol\\Publish.mqh"
#include "..\\Protocol\\PubRel.mqh"

//+------------------------------------------------------------------+
//| Class CRetransmissionManager                                     |
//| Purpose: High-level logic for handling retransmissions           |
//+------------------------------------------------------------------+
class CRetransmissionManager {
 public:
  //--- Info about a message dropped due to max retransmit limit
  struct DroppedMessage {
    ushort packet_id;
    uchar  qos_level;
    string topic;
    uint   retransmit_count;
  };

  //--- Process all stalled messages and generate retransmission packets.
  //--- Messages that have been retransmitted >= max_retransmit times are
  //--- discarded from the session database and NOT included in out_packets.
  //--- Dropped messages are reported in out_dropped for callback notification.
  //--- pubrel_timeout_seconds: independent timeout for QoS 2 step-2 (PUBREL) retransmissions.
  //--- When 0, uses timeout_seconds for all QoS levels.
  //--- Returns count of packets generated (i.e. not counting dropped messages).
  static uint ProcessRetransmissions(CMqttContext &ctx, PacketBuffer &out_packets[], ushort &out_pkt_ids[],
                                     const uint timeout_seconds, const uint max_retransmit,
                                     DroppedMessage &out_dropped[], uint &out_dropped_count,
                                     const uint pubrel_timeout_seconds = 0);
};

//+------------------------------------------------------------------+
//| Process stalled messages and build packets                       |
//| Parameters: ctx - MQTT Session Context                           |
//|             out_packets - [OUT] array of packet buffers to send  |
//|             timeout_seconds - threshold for retransmission       |
//|             out_dropped - [OUT] dropped messages info            |
//|             out_dropped_count - [OUT] count of dropped messages  |
//| Return: Number of packets ready for retransmission               |
//+------------------------------------------------------------------+
uint CRetransmissionManager::ProcessRetransmissions(CMqttContext &ctx, PacketBuffer &out_packets[],
                                                    ushort &out_pkt_ids[], const uint timeout_seconds,
                                                    const uint max_retransmit, DroppedMessage &out_dropped[],
                                                    uint &out_dropped_count, const uint pubrel_timeout_seconds) {
  //--- Effective per-phase timeouts
  const uint publish_timeout = timeout_seconds;
  const uint pubrel_timeout  = (pubrel_timeout_seconds > 0) ? pubrel_timeout_seconds : timeout_seconds;
  ArrayFree(out_packets);
  ArrayFree(out_pkt_ids);
  out_dropped_count = 0;
  ArrayFree(out_dropped);
  SessionMessage stalled[];
  //--- Use the minimum of the two timeouts so both phases are scanned correctly.
  //--- When pubrel_timeout < publish_timeout, PUBREL-phase messages need the smaller
  //--- threshold for GetStalledMessages to include them. The per-phase filter below
  //--- ensures each phase is retransmitted only at its own configured interval.
  const uint     scan_timeout = (pubrel_timeout < publish_timeout) ? pubrel_timeout : publish_timeout;
  uint           count        = ctx.session_db.GetStalledMessages(stalled, scan_timeout);

  if (count == 0) {
    return 0;
  }

  ArrayResize(out_packets, (int)count);
  ArrayResize(out_pkt_ids, (int)count);
  ArrayResize(out_dropped, (int)count);

  CPublish pub_builder;
  uint     generated = 0;
  for (uint i = 0; i < count; i++) {
    //--- Apply per-phase timeout filter:
    //---   PUBLISH phase (QoS1 / QoS2 step-1): use publish_timeout.
    //---   PUBREL  phase (QoS2 step-2)        : use pubrel_timeout.
    //--- GetStalledMessages uses min(publish_timeout, pubrel_timeout) so both phases
    //--- may appear in stalled[]; re-check each against its phase-specific threshold.
    bool is_pubrel_phase = (stalled[i].qos_level == QoS_2 && stalled[i].qos2_state == QOS2_STATE_PUBREC_RECEIVED);
    if (pubrel_timeout != publish_timeout) {
      ulong elapsed_us  = GetMicrosecondCount() - stalled[i].mono_timestamp_us;
      ulong phase_limit = is_pubrel_phase ? (ulong)pubrel_timeout * 1000000UL : (ulong)publish_timeout * 1000000UL;
      if (elapsed_us < phase_limit) {
        continue;
      }
    }
    //--- Drop messages whose Message Expiry Interval has passed per §3.3.2.3.3.
    //--- A stale trading signal MUST NOT be retransmitted to subscribers who are now in
    //--- a different market state — this could cause stale signal execution.
    if (stalled[i].expiry_time > 0 && TimeLocal() >= stalled[i].expiry_time) {
      MQTT_LOG_INFO("Dropping expired message for packet ID " + (string)stalled[i].packet_id
                    + " — Message Expiry Interval exceeded per §3.3.2.3.3. Topic: " + stalled[i].topic);
      ctx.flow_control.ReleaseQoS(stalled[i].packet_id);
      ctx.session_db.RemoveMessage(stalled[i].packet_id);
      continue;
    }

    //--- Drop messages that have exceeded the retransmission limit
    if (max_retransmit > 0 && stalled[i].retransmit_count >= max_retransmit) {
      MQTT_LOG_WARN("Dropping packet ID " + (string)stalled[i].packet_id + " after "
                    + (string)stalled[i].retransmit_count + " retransmit attempts (max=" + (string)max_retransmit
                    + "). Topic: " + stalled[i].topic);
      //--- Record dropped message info for callback
      out_dropped[out_dropped_count].packet_id        = stalled[i].packet_id;
      out_dropped[out_dropped_count].qos_level        = stalled[i].qos_level;
      out_dropped[out_dropped_count].topic            = stalled[i].topic;
      out_dropped[out_dropped_count].retransmit_count = stalled[i].retransmit_count;
      out_dropped_count++;
      ctx.flow_control.ReleaseQoS(stalled[i].packet_id);
      ctx.session_db.RemoveMessage(stalled[i].packet_id);
      continue;
    }

    uchar buf[];
    bool  ready = false;

    //--- Rule 1: QoS 1 or QoS 2 Step 1 (PUBLISH sent, no PUBREC/PUBACK)
    if (stalled[i].qos_level == QoS_1
        || (stalled[i].qos_level == QoS_2 && stalled[i].qos2_state == QOS2_STATE_PUBLISH_SENT)) {
      pub_builder.Reset();
      pub_builder.SetTopicNameFast(stalled[i].topic);
      pub_builder.SetPayload(stalled[i].payload);
      pub_builder.SetRetain(stalled[i].retain);
      pub_builder.SetEncodedProperties(stalled[i].publish_properties);
      if (stalled[i].expiry_time > 0) {
        datetime now              = TimeLocal();
        uint     remaining_expiry = 0;
        if (stalled[i].expiry_time > now) {
          datetime diff = stalled[i].expiry_time - now;
          //--- MQTT Message Expiry Interval is a 32-bit value (max ~136 years).
          //--- Guard against a corrupt DB entry with a far-future expiry timestamp
          //--- that would silently overflow the uint cast.
          if (diff <= (datetime)0xFFFFFFFF) {
            remaining_expiry = (uint)diff;
          }
        }
        pub_builder.SetMessageExpiryInterval(remaining_expiry);
      }
      if (stalled[i].qos_level == QoS_1) {
        pub_builder.SetQoS_1(true);
      } else if (stalled[i].qos_level == QoS_2) {
        pub_builder.SetQoS_2(true);
      }
      pub_builder.SetPacketId(stalled[i].packet_id);
      pub_builder.SetDup(true);  // MUST set DUP flag per spec
      //--- Always use NULL alias manager for retransmissions.
      //--- After reconnect, all topic alias mappings are cleared (§3.3.2.3.4).
      //--- Passing NULL guarantees the full topic name is always included,
      //--- eliminating any dependency on alias-manager internal state.
      pub_builder.Build(buf, NULL);
      ready = true;
    }
    //--- Rule 2: QoS 2 Step 2 (PUBREC received, PUBREL sent, no PUBCOMP)
    else if (stalled[i].qos_level == QoS_2 && stalled[i].qos2_state == QOS2_STATE_PUBREC_RECEIVED) {
      CPubrel rel;
      rel.SetPacketId(stalled[i].packet_id);
      rel.Build(buf);
      ready = true;
    }

    if (ready) {
      //--- Validate retransmitted packet size against server max.
      //--- If the server reduced Maximum Packet Size on reconnect, a previously
      //--- stored message may now exceed the new limit — drop it rather than
      //--- sending an oversized packet that would cause a Protocol Error.
      uint pkt_sz = (uint)ArraySize(buf);
      if (pkt_sz > 0 && !ctx.flow_control.ValidateOutgoingPacketSize(pkt_sz)) {
        MQTT_LOG_WARN("Retransmit packet ID " + (string)stalled[i].packet_id
                      + " exceeds server Maximum Packet Size after reconnect — dropping.");
        out_dropped[out_dropped_count].packet_id        = stalled[i].packet_id;
        out_dropped[out_dropped_count].qos_level        = stalled[i].qos_level;
        out_dropped[out_dropped_count].topic            = stalled[i].topic;
        out_dropped[out_dropped_count].retransmit_count = stalled[i].retransmit_count;
        out_dropped_count++;
        ctx.flow_control.ReleaseQoS(stalled[i].packet_id);
        ctx.session_db.RemoveMessage(stalled[i].packet_id);
        continue;
      }

      //--- For PUBLISH-phase retransmissions (QoS 1 and QoS 2 step-1),
      //--- check whether the server's Receive Maximum window has room before queuing.
      //--- After a reconnect the server may advertise a LOWER Receive Maximum; bursting
      //--- all pending retransmissions at once could violate §4.9 flow control and cause
      //--- the server to send DISCONNECT with 0x93 (Receive Maximum exceeded).
      //---
      //--- PUBREL-phase messages (QoS 2 step-2) are exempt: they complete an already-
      //--- registered exchange and do NOT consume an additional flow-control slot.
      if (!is_pubrel_phase && !ctx.flow_control.CanSendQoSMessage(stalled[i].qos_level)) {
        MQTT_LOG_DEBUG("Retransmit deferred for packet ID " + (string)stalled[i].packet_id
                       + " — server Receive Maximum window full, will retry next poll.");
        continue;  // Defer to the next ProcessRetransmissions() call
      }

      //--- Store in output array using temporary struct
      int buf_size = ArraySize(buf);
      ArrayResize(out_packets[generated].data, buf_size);
      ArrayCopy(out_packets[generated].data, buf);

      //--- Record the packet ID in a parallel array so the caller can call TouchMessage
      //--- only for packets that were actually transmitted over the transport, preventing
      //--- the retransmit budget from being consumed by send failures.
      out_pkt_ids[generated] = stalled[i].packet_id;
      generated++;

      //--- Store retransmit count BEFORE touching the message (to avoid potential use-after-free)
      uint retransmit_count = stalled[i].retransmit_count;

      MQTT_LOG_INFO("Generated retransmission for packet ID " + (string)stalled[i].packet_id + " (Attempt "
                    + (string)retransmit_count + ")");
    }
  }

  ArrayResize(out_packets, (int)generated);
  ArrayResize(out_pkt_ids, (int)generated);
  ArrayResize(out_dropped, (int)out_dropped_count);
  return generated;
}

#endif  // MQTT_INTERNAL_SESSION_RETRANSMISSION_MANAGER_MQH
