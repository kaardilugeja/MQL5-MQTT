//+------------------------------------------------------------------+
//|                                      PublishQueueCoordinator.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Policy and durability helper around CMqttPublishQueue.           |
//| Owns offline admission, restore, expiry purge, and drain result  |
//| classification so CMqttClient keeps transport/session behavior   |
//| separate from queue bookkeeping.                                 |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_QUEUE_PUBLISHQUEUECOORDINATOR_MQH
#define MQTT_INTERNAL_QUEUE_PUBLISHQUEUECOORDINATOR_MQH

#include "..\Util\Defines.mqh"
#include "PublishQueue.mqh"

enum ENUM_MQTT_QUEUED_DRAIN_ACTION {
  MQTT_QUEUED_DRAIN_ACTION_RETRY = 0,
  MQTT_QUEUED_DRAIN_ACTION_CONSUME,
  MQTT_QUEUED_DRAIN_ACTION_DROP
};

enum ENUM_MQTT_OFFLINE_QUEUE_RESULT {
  MQTT_OFFLINE_QUEUE_RESULT_QUEUED = 0,
  MQTT_OFFLINE_QUEUE_RESULT_NOT_CONNECTED,
  MQTT_OFFLINE_QUEUE_RESULT_RECONNECTING,
  MQTT_OFFLINE_QUEUE_RESULT_QUEUE_FULL,
  MQTT_OFFLINE_QUEUE_RESULT_SEND_FAILED
};

//+------------------------------------------------------------------+
//| CMqttPublishQueueCoordinator                                     |
//| Bridges three states of one publish: accepted while offline,     |
//| durably restored after restart, and eventually consumed or       |
//| dropped during reconnect-driven drain.                           |
//+------------------------------------------------------------------+
class CMqttPublishQueueCoordinator {
 private:
  //--- Lightweight property scanner used only for queue budgeting. It avoids a
  //--- full MQTT property decode when the coordinator only needs to know whether
  //--- an expiry property is already present in the stored publish metadata.
  bool HasMessageExpiryProperty(const uchar &publish_properties[]) const {
    uint idx       = 0;
    uint prop_size = (uint)ArraySize(publish_properties);

    while (idx < prop_size) {
      uchar prop_id = publish_properties[idx++];
      if (prop_id == MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL) {
        if (idx + 4 > prop_size) {
          return false;
        }
        return true;
      }

      switch (prop_id) {
        case MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR:
          idx += 1;
          break;
        case MQTT_PROP_IDENTIFIER_TOPIC_ALIAS:
          idx += 2;
          break;
        case MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC:
        case MQTT_PROP_IDENTIFIER_CORRELATION_DATA:
        case MQTT_PROP_IDENTIFIER_CONTENT_TYPE: {
          if (idx + 2 > prop_size) {
            return false;
          }
          uint str_len  = ((uint)publish_properties[idx] << 8) | (uint)publish_properties[idx + 1];
          idx          += 2 + str_len;
          break;
        }
        case MQTT_PROP_IDENTIFIER_USER_PROPERTY: {
          if (idx + 2 > prop_size) {
            return false;
          }
          uint key_len  = ((uint)publish_properties[idx] << 8) | (uint)publish_properties[idx + 1];
          idx          += 2 + key_len;
          if (idx + 2 > prop_size) {
            return false;
          }
          uint val_len  = ((uint)publish_properties[idx] << 8) | (uint)publish_properties[idx + 1];
          idx          += 2 + val_len;
          break;
        }
        case MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER: {
          int   bytes_read = 0;
          uchar digit      = 0;
          do {
            if (idx >= prop_size || bytes_read >= 4) {
              return false;
            }
            digit = publish_properties[idx++];
            bytes_read++;
          } while ((digit & 0x80) != 0);
          break;
        }
        default:
          return false;
      }

      if (idx > prop_size) {
        return false;
      }
    }

    return false;
  }

  //--- Queue admission budgets must reserve the eventual replay property bytes, not
  //--- just the currently encoded bytes. If expiry is tracked out-of-band, budget the
  //--- 5 extra bytes needed to synthesize Message Expiry on the outbound replay path.
  uint ComputePropertyBudgetLen(const uchar &publish_properties[], uint remaining_expiry) const {
    uint property_budget_len = (uint)ArraySize(publish_properties);
    if (remaining_expiry > 0 && !HasMessageExpiryProperty(publish_properties)) {
      property_budget_len += 5;  // Property id + 4-byte expiry value.
    }
    return property_budget_len;
  }

 public:
  string DescribeAdmissionFailure(ENUM_MQTT_QUEUE_ADMISSION decision, const string topic) const {
    switch (decision) {
      case MQTT_QUEUE_ADMIT_COUNT_LIMIT:
        return "Rejecting queued publish for " + topic + " - offline message-count budget exhausted";
      case MQTT_QUEUE_ADMIT_SINGLE_MESSAGE_LIMIT:
        return "Rejecting queued publish for " + topic
             + " - payload+property size exceeds offline single-message budget";
      case MQTT_QUEUE_ADMIT_PAYLOAD_BYTES_LIMIT:
        return "Rejecting queued publish for " + topic + " - offline payload-byte budget exhausted";
      case MQTT_QUEUE_ADMIT_PROPERTY_BYTES_LIMIT:
        return "Rejecting queued publish for " + topic + " - offline property-byte budget exhausted";
      default:
        break;
    }

    return "Rejecting queued publish for " + topic + " - offline queue admission failed";
  }

  ENUM_MQTT_QUEUED_DRAIN_ACTION ClassifyDrainResult(int publish_error) const {
    if (publish_error == 0) {
      return MQTT_QUEUED_DRAIN_ACTION_CONSUME;
    }
    if (publish_error == -10 || publish_error == -6 || publish_error == -5) {
      return MQTT_QUEUED_DRAIN_ACTION_DROP;
    }
    return MQTT_QUEUED_DRAIN_ACTION_RETRY;
  }

  bool RestorePersistedQueue(CSessionDatabase &session_db, CMqttPublishQueue &queue, ulong now_us, datetime now,
                             uint &restored_count, string &error_text) const {
    restored_count = 0;
    error_text     = "";

    OfflineQueuedMessage queued[];
    uint                 queued_count = session_db.GetOfflineQueuedMessages(queued);
    if (queued_count == 0) {
      return true;
    }

    for (uint i = 0; i < queued_count; i++) {
      ulong expiry_deadline_us = 0;
      uint  remaining_expiry   = queued[i].remaining_expiry_seconds;
      //--- Older durable rows may carry only an absolute expiry timestamp. Convert
      //--- that back into a monotonic deadline before appending into the in-memory queue.
      if (remaining_expiry == 0 && queued[i].expiry_time > 0) {
        if (queued[i].expiry_time <= now) {
          if (!FinalizeDurableMessage(session_db, queued[i].queued_id, error_text)) {
            return false;
          }
          continue;
        }
        remaining_expiry = (uint)(queued[i].expiry_time - now);
      }

      if (remaining_expiry > 0) {
        expiry_deadline_us = now_us + (ulong)remaining_expiry * 1000000ULL;
      }

      uint property_budget_len = ComputePropertyBudgetLen(queued[i].publish_properties, remaining_expiry);

      if (!queue.Append(queued[i].topic, queued[i].payload, queued[i].payload_size, queued[i].qos_level,
                        queued[i].retain, queued[i].publish_properties, expiry_deadline_us,
                        queued[i].allow_outgoing_subscription_identifier, queued[i].queued_id, property_budget_len)) {
        error_text = "Failed to restore durable offline publish queue into memory";
        return false;
      }
      uint restored_idx = queue.GetTotalCount() - 1;
      queue.SetEnqueuedAtUs(restored_idx, 0);
      queue.SetEnqueuedAtTime(restored_idx, queued[i].timestamp);
      restored_count++;
    }

    return true;
  }

  bool PersistAcceptedQueueTail(CSessionDatabase &session_db, CMqttPublishQueue &queue, uchar qos, const string topic,
                                const uchar &payload[], uint payload_size, bool retain, uint expiry_interval,
                                const uchar &publish_properties[], bool allow_outgoing_sub_id, uint property_budget_len,
                                string &error_text) const {
    error_text = "";
    if (!session_db.IsPersistent()) {
      return true;
    }

    if (queue.GetTotalCount() == 0) {
      error_text = "Accepted offline publish was not present in memory queue for durable sync";
      return false;
    }

    ulong durable_store_id = session_db.StoreOfflineQueuedMessage(
      qos, topic, payload, payload_size, retain, expiry_interval, publish_properties, allow_outgoing_sub_id);
    if (durable_store_id == 0) {
      queue.RollbackTail(payload_size, property_budget_len);
      error_text = "Failed to persist accepted offline QoS publish before returning MQTT_PUB_QUEUED";
      return false;
    }

    //--- Write-through semantics: do not report MQTT_PUB_QUEUED until the durable row
    //--- is both recorded and flushed, otherwise an EA restart could silently lose it.
    queue.SetStoreId(queue.GetTotalCount() - 1, durable_store_id);
    if (!session_db.FlushIfDirty(0)) {
      string cleanup_error = "";
      RemoveDurableMessage(session_db, durable_store_id, cleanup_error);
      queue.RollbackTail(payload_size, property_budget_len);
      error_text = "Failed to write through accepted offline QoS publish before returning MQTT_PUB_QUEUED";
      if (StringLen(cleanup_error) > 0) {
        error_text += "; " + cleanup_error;
      }
      return false;
    }

    return true;
  }

  ENUM_MQTT_OFFLINE_QUEUE_RESULT
  QueueWhileDisconnected(CSessionDatabase &session_db, CMqttPublishQueue &queue, IMqttPublishQueueDrainSink &sink,
                         bool is_draining, bool queue_qos0_while_disconnected, const string topic,
                         const uchar &payload[], uint payload_size, uchar qos, bool retain, uint expiry_interval,
                         const uchar &publish_properties[], bool allow_outgoing_sub_id, uint &purged_count,
                         string &warning_text, string &error_text) const {
    return QueueWhileDisconnected(session_db, queue, sink, is_draining, queue_qos0_while_disconnected, topic, payload,
                                  payload_size, qos, retain, expiry_interval, publish_properties,
                                  ComputePropertyBudgetLen(publish_properties, expiry_interval), allow_outgoing_sub_id,
                                  purged_count, warning_text, error_text);
  }

  ENUM_MQTT_OFFLINE_QUEUE_RESULT
  QueueWhileDisconnected(CSessionDatabase &session_db, CMqttPublishQueue &queue, IMqttPublishQueueDrainSink &sink,
                         bool is_draining, bool queue_qos0_while_disconnected, const string topic,
                         const uchar &payload[], uint payload_size, uchar qos, bool retain, uint expiry_interval,
                         const uchar &publish_properties[], uint property_budget_len, bool allow_outgoing_sub_id,
                         uint &purged_count, string &warning_text, string &error_text) const {
    purged_count = 0;
    warning_text = "";
    error_text   = "";

    if (is_draining) {
      return MQTT_OFFLINE_QUEUE_RESULT_RECONNECTING;
    }

    if (qos == QoS_0 && !queue_qos0_while_disconnected) {
      return MQTT_OFFLINE_QUEUE_RESULT_NOT_CONNECTED;
    }

    //--- Purge first so expired rows release both memory and durable budget before
    //--- the new publish is measured against queue admission limits.
    purged_count                        = PurgeExpiredQueue(session_db, queue, sink);

    ENUM_MQTT_QUEUE_ADMISSION admission = queue.EvaluateAdmission(payload_size, property_budget_len);
    if (admission == MQTT_QUEUE_ADMIT_COUNT_LIMIT) {
      return MQTT_OFFLINE_QUEUE_RESULT_QUEUE_FULL;
    }
    if (admission != MQTT_QUEUE_ADMIT_OK) {
      warning_text = DescribeAdmissionFailure(admission, topic);
      return MQTT_OFFLINE_QUEUE_RESULT_QUEUE_FULL;
    }

    if (queue.NeedsMidpointCompaction()) {
      queue.Compact();
    }

    ulong expiry_deadline_us =
      (expiry_interval > 0) ? (GetMicrosecondCount() + (ulong)expiry_interval * 1000000ULL) : 0;
    if (!queue.Append(topic, payload, payload_size, qos, retain, publish_properties, expiry_deadline_us,
                      allow_outgoing_sub_id, 0, property_budget_len)) {
      return MQTT_OFFLINE_QUEUE_RESULT_SEND_FAILED;
    }

    if (qos > 0 && session_db.IsPersistent()) {
      if (!PersistAcceptedQueueTail(session_db, queue, qos, topic, payload, payload_size, retain, expiry_interval,
                                    publish_properties, allow_outgoing_sub_id, property_budget_len, error_text)) {
        return MQTT_OFFLINE_QUEUE_RESULT_SEND_FAILED;
      }
    }

    return MQTT_OFFLINE_QUEUE_RESULT_QUEUED;
  }

  bool RemoveDurableMessage(CSessionDatabase &session_db, ulong durable_store_id, string &error_text) const {
    error_text = "";
    if (durable_store_id == 0 || !session_db.IsPersistent()) {
      return true;
    }

    if (!session_db.RemoveOfflineQueuedMessage(durable_store_id)) {
      error_text = "Failed to remove durable offline queued publish id=" + (string)durable_store_id;
      return false;
    }

    return true;
  }

  bool RemoveDurableMessages(CSessionDatabase &session_db, const ulong &durable_store_ids[], string &error_text) const {
    error_text = "";
    for (int i = 0; i < ArraySize(durable_store_ids); i++) {
      if (!RemoveDurableMessage(session_db, durable_store_ids[i], error_text)) {
        return false;
      }
    }
    return true;
  }

  bool FinalizeDurableMessage(CSessionDatabase &session_db, ulong durable_store_id, string &error_text) const {
    error_text = "";
    if (durable_store_id == 0 || !session_db.IsPersistent()) {
      return true;
    }

    if (!session_db.FinalizeOfflineQueuedMessage(durable_store_id)) {
      error_text = "Failed to finalize durable offline queued publish id=" + (string)durable_store_id;
      return false;
    }

    return true;
  }

  bool FinalizeDurableMessages(CSessionDatabase &session_db, const ulong &durable_store_ids[],
                               string &error_text) const {
    error_text = "";
    for (int i = 0; i < ArraySize(durable_store_ids); i++) {
      if (!FinalizeDurableMessage(session_db, durable_store_ids[i], error_text)) {
        return false;
      }
    }
    return true;
  }

  void DrainQueue(CSessionDatabase &session_db, CMqttPublishQueue &queue, IMqttPublishQueueDrainSink &sink,
                  uint &sent_count, uint &dropped_count) const {
    sent_count     = 0;
    dropped_count  = 0;

    uint available = queue.GetQueuedMessageCount();
    if (available == 0) {
      return;
    }

    uint drain_end = queue.GetTotalCount();
    for (uint i = queue.GetDrainHead(); i < drain_end; i++) {
      string topic = queue.GetTopic(i);
      if (topic == "") {
        sink.ReportQueueError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                              "Failed to locate queued publish entry during drain");
        break;
      }

      ulong now_us           = GetMicrosecondCount();
      ulong expiry_time_us   = queue.GetExpiryTimeUs(i);
      ulong durable_store_id = queue.GetStoreId(i);
      //--- Expired entries are dropped before they re-enter the live publish path.
      //--- That keeps reconnect drains from reviving publishes whose expiry elapsed
      //--- while the client was offline.
      if (expiry_time_us > 0 && now_us >= expiry_time_us) {
        sink.ReportQueueError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                              "Dropping expired queued publish: " + topic);
        string error_text = "";
        if (!RemoveDurableMessage(session_db, durable_store_id, error_text)) {
          sink.ReportQueueError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, error_text);
          break;
        }
        queue.AdvanceDrainHead();
        dropped_count++;
        continue;
      }

      uint remaining_expiry = 0;
      if (expiry_time_us > 0) {
        remaining_expiry = sink.RemainingExpirySecondsFromDeadlineUs(expiry_time_us, now_us);
      }

      int publish_error = 0;
      if (!queue.DrainEntryToSink(i, sink, remaining_expiry, publish_error)) {
        sink.ReportQueueError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR,
                              "Failed to stream queued publish entry during drain");
        break;
      }
      //--- The client reports whether the attempted publish already handed the entry
      //--- off durably into the in-flight QoS path. That lets the coordinator consume
      //--- the queue slot even if the immediate publish call returned a retry status.
      ENUM_MQTT_QUEUED_DRAIN_ACTION action = ClassifyDrainResult(publish_error);
      if (action == MQTT_QUEUED_DRAIN_ACTION_RETRY && sink.LastQueuedPublishDurablyHandedOff()) {
        action = MQTT_QUEUED_DRAIN_ACTION_CONSUME;
      }
      if (action == MQTT_QUEUED_DRAIN_ACTION_CONSUME) {
        string error_text = "";
        if (!FinalizeDurableMessage(session_db, durable_store_id, error_text)) {
          sink.ReportQueueError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, error_text);
          break;
        }
        queue.AdvanceDrainHead();
        sent_count++;
      } else if (action == MQTT_QUEUED_DRAIN_ACTION_DROP) {
        sink.ReportQueueError(publish_error, "Dropping undeliverable queued publish: " + topic);
        string error_text = "";
        if (!FinalizeDurableMessage(session_db, durable_store_id, error_text)) {
          sink.ReportQueueError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, error_text);
          break;
        }
        queue.AdvanceDrainHead();
        dropped_count++;
      } else {
        sink.ReportQueueError(publish_error, "Failed to drain queued publish for topic: " + topic);
        break;
      }
    }

    if (queue.ShouldCompactAfterDrain()) {
      queue.Compact();
    }
  }

  bool ShouldStartDrain(const CMqttPublishQueue &queue, bool is_draining) const {
    return queue.HasPendingDrain() && !is_draining;
  }

  bool DrainQueueIfPending(CSessionDatabase &session_db, CMqttPublishQueue &queue, IMqttPublishQueueDrainSink &sink,
                           bool &is_draining, uint &sent_count, uint &dropped_count) const {
    sent_count    = 0;
    dropped_count = 0;
    if (!ShouldStartDrain(queue, is_draining)) {
      return false;
    }

    is_draining = true;
    DrainQueue(session_db, queue, sink, sent_count, dropped_count);
    is_draining = false;
    return true;
  }

  uint PurgeExpiredQueue(CSessionDatabase &session_db, CMqttPublishQueue &queue,
                         IMqttPublishQueueDrainSink &sink) const {
    if (queue.GetTotalCount() == 0) {
      return 0;
    }

    //--- Queue-level purge returns the durable ids that became unreachable from the
    //--- in-memory queue so the coordinator can finalize those rows in one follow-up step.
    ulong  removed_store_ids[];
    uint   dropped    = queue.PurgeExpired(GetMicrosecondCount(), removed_store_ids);
    string error_text = "";
    if (!FinalizeDurableMessages(session_db, removed_store_ids, error_text)) {
      sink.ReportQueueError(MQTT_REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR, error_text);
    }

    return dropped;
  }
};

#endif
