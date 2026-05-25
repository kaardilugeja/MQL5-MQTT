//+------------------------------------------------------------------+
//|                                                      Connect.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 CONNECT packet implementation per spec §3.1.            |
//| Used to initiate a connection to an MQTT broker.                 |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_PROTOCOL_CONNECT_MQH
#define MQTT_INTERNAL_PROTOCOL_CONNECT_MQH

#include "..\\Connection\\FlowControl.mqh"
#include "..\\Util\\PropertyEncoder.mqh"

//+------------------------------------------------------------------+
//| Connect Variable Header                                          |
//+------------------------------------------------------------------+
/*
The Variable Header for the CONNECT Packet contains the following fields in this order:
Protocol Name, Protocol Level, Connect Flags, Keep Alive, and Properties.
*/

//+------------------------------------------------------------------+
//| Class CConnect                                                   |
//| Purpose: Class of MQTT Connect Control Packets                   |
//| Usage:   Used to build CONNECT packets with full v5.0 properties |
//+------------------------------------------------------------------+
class CConnect {
 private:
  //--- Remaining length of the packet
  uint m_remlen;

 protected:
  //--- Connection configuration
  uchar         m_connflags;   // Connect flags byte
  uchar         m_clientid[];  // Client identifier bytes
  MqttKeepAlive m_keepalive;   // Keep alive interval bytes

  //--- CONNECT properties (MQTT 5 §3.1.2.11)
  //--- Each property is staged in its own encoded buffer so Build() can emit only
  //--- the fields that are present without rebuilding unrelated properties.
  uchar         m_connprops[];       // Concatenated CONNECT Properties field built from the buffers below.
  uint          m_connprops_len;     // Encoded byte length of m_connprops[].
  uchar         m_sessionexp_int[];  // Session Expiry Interval property.
  uchar         m_receive_max[];     // Receive Maximum property.
  uchar         m_maxpkt_size[];     // Maximum Packet Size property.
  uchar         m_topicalias_max[];  // Topic Alias Maximum property.
  uchar         m_req_respinfo[];    // Request Response Information property.
  uchar         m_req_problinfo[];   // Request Problem Information property.
  uchar         m_userprops[];       // Concatenated User Property collection.
  uchar         m_authmethod[];      // Authentication Method property for enhanced auth negotiation.
  uchar         m_authdata[];        // Authentication Data property paired with m_authmethod[].

  //--- Will properties
  //--- MQTT 5 serializes Will Properties before the Will Topic/Payload, so the Will
  //--- property buffers are kept separate from CONNECT properties until Build().
  uchar         m_will_delayint[];       // Will Delay Interval property.
  uchar         m_will_payloadformat[];  // Payload Format Indicator property.
  uchar         m_will_msgexpint[];      // Message Expiry Interval for the Will publish.
  uchar         m_will_contenttype[];    // Content Type property.
  uchar         m_will_resptopic[];      // Response Topic property.
  uchar         m_will_corrdata[];       // Correlation Data property.
  uchar         m_will_userprops[];      // Concatenated Will User Property collection.

  uchar         m_willprops[];           // Concatenated Will Properties field built from the buffers above.
  uint          m_willprops_len;         // Encoded byte length of m_willprops[].
  uchar         m_will_topic[];          // Will topic UTF-8 encoded.
  uchar         m_will_payload[];        // Will payload binary data.

  //--- Payload
  uchar         m_payload[];    // Final CONNECT payload buffer built from client ID, Will, and credentials.
  uint          m_payload_len;  // Encoded byte length of m_payload[].
  uchar         m_user_name[];  // Username field with MQTT UTF-8 length prefix.
  uchar         m_password[];   // Password field with MQTT binary length prefix.

  //--- Scratch buffers reused across Build() calls for hot-path varint encoding.
  uchar         m_connprops_len_buf[];
  uchar         m_willprops_len_buf[];
  uchar         m_remlen_buf[];

  //--- Internal helpers for property length updates
  void          UpdateConnPropsLen();
  void          UpdateWillPropsLen();

 public:
  //--- Constructor declarations
  CConnect();

  //--- Destructor
  ~CConnect();

  //--- Methods for setting Connect Flags
  void SetCleanStart(const bool cleanStart = true);      // Enable/disable clean session
  void SetWillFlag(const bool willFlag = true);          // Enable/disable will message
  void SetWillQoS_1(const bool willQoS_1 = true);        // Set Will QoS bit 1
  void SetWillQoS_2(const bool willQoS_2 = true);        // Set Will QoS bit 2
  void SetWillRetain(const bool willRetain = true);      // Enable/disable will retain
  void SetPasswordFlag(const bool passwordFlag = true);  // Enable/disable password
  void SetUserNameFlag(const bool userNameFlag = true);  // Enable/disable username
  void SetKeepAlive(ushort seconds);                     // Set keep alive interval

  //--- Methods for setting Properties
  void SetSessionExpiryInterval(uint seconds);                 // Session expiry in seconds
  void SetReceiveMaximum(ushort receive_max);                  // Maximum QoS 1/2 messages
  void SetMaximumPacketSize(uint max_pkt_size);                // Maximum packet size
  void SetTopicAliasMaximum(ushort topic_alias_max);           // Maximum topic alias
  void SetRequestResponseInfo(uchar val);                      // Request response info (0/1)
  void SetRequestProblemInfo(uchar val);                       // Request problem info (0/1)
  void SetUserProperty(const string &key, const string &val);  // User property
  void SetAuthMethod(const string &auth_method);               // Authentication method
  void SetAuthData(const string &bindata);                     // Binary authentication data (string overload)
  void SetAuthData(const uchar &auth_data[]);                  // Binary authentication data (uchar array overload)

  //--- Methods for setting the Will Properties
  void SetWillDelayInterval(uint seconds);                         // Will delay interval
  void SetWillPayloadFormatIndicator(uchar val);                   // Payload format (0/1)
  void SetWillMessageExpiryInterval(uint seconds);                 // Message expiry interval
  void SetWillContentType(const string &content_type);             // Content type
  void SetWillResponseTopic(const string &resp_topic);             // Response topic
  void SetWillCorrelationData(const string &corr_data);            // Correlation data
  void SetWillCorrelationData(const uchar &corr_data[]);           // Correlation data (binary)
  void SetWillUserProperty(const string &key, const string &val);  // User property

  //--- Methods for setting the Payload
  void SetClientIdentifier(const string &clientId);  // Client identifier
  void SetWillTopic(const string &will_topic);       // Will topic
  void SetWillPayload(const string &will_payload);   // Will payload (string)
  void SetWillPayload(const uchar &will_payload[]);  // Will payload (binary) per §1.5.6
  void SetUserName(const string &username);          // Username
  void SetPassword(const string &password);          // Password (string)
  void SetPassword(const uchar &password[]);         // Password (binary) per §1.5.6

  //--- Method for building the final packet
  void Build(uchar &result[], CFlowControl *fc = NULL);
};

//+------------------------------------------------------------------+
//| Set password in payload                                          |
//| Parameters: password - password string                           |
//+------------------------------------------------------------------+
void CConnect::SetPassword(const string &password) {
  //--- Password in MQTT v5.0 is binary data with 2-byte length prefix
  //--- SECURITY: Do NOT use Print() with password data here.
  //---           Use MQTT_CREDENTIAL_PRINT() — it is a no-op unless
  //---           MQTT_LOG_CREDENTIALS is defined (debug builds only).
  //--- MQTT string credentials are UTF-8 encoded. For arbitrary binary tokens,
  //--- prefer SetPassword(const uchar &password[]) to preserve raw bytes exactly.
  uchar aux[];
  int   len = StringToCharArray(password, aux, 0, WHOLE_ARRAY, CP_UTF8);
  //--- StringToCharArray includes null terminator, MQTT does not
  if (len > 0 && aux[len - 1] == 0) {
    len--;
  }

  if (len > 65535) {
    MQTT_LOG_ERROR("Password exceeds 65535 bytes");
    return;
  }

  ArrayResize(m_password, len + 2);
  //--- Set length prefix (MSB, LSB)
  m_password[0] = (uchar)(len >> 8);
  m_password[1] = (uchar)(len & 0xFF);

  if (len > 0) {
    ArrayCopy(m_password, aux, 2, 0, len);
  }
};

//+------------------------------------------------------------------+
//| Set username in payload                                          |
//| Parameters: username - username string                           |
//+------------------------------------------------------------------+
void CConnect::SetUserName(const string &username) {
  //--- Encode username to UTF-8 format (includes 2-byte length prefix)
  //--- SECURITY: Do NOT use Print() with username data here.
  //---           Use MQTT_CREDENTIAL_PRINT() — it is a no-op unless
  //---           MQTT_LOG_CREDENTIALS is defined (debug builds only).
  EncodeUTF8String(username, m_user_name);
};

//+------------------------------------------------------------------+
//| Set will payload in payload                                      |
//| Parameters: will_payload - will message payload                  |
//+------------------------------------------------------------------+
void CConnect::SetWillPayload(const string &will_payload) {
  //--- Will payload in MQTT v5.0 is binary data with 2-byte length prefix
  uchar aux[];
  int   len = StringToCharArray(will_payload, aux, 0, WHOLE_ARRAY, CP_UTF8);
  //--- StringToCharArray includes null terminator, MQTT does not
  if (len > 0 && aux[len - 1] == 0) {
    len--;
  }

  if (len > 65535) {
    MQTT_LOG_ERROR("Will Payload exceeds 65535 bytes");
    return;
  }

  ArrayResize(m_will_payload, len + 2);
  //--- Set length prefix (MSB, LSB)
  m_will_payload[0] = (uchar)(len >> 8);
  m_will_payload[1] = (uchar)(len & 0xFF);

  if (len > 0) {
    ArrayCopy(m_will_payload, aux, 2, 0, len);
  }
};

//+------------------------------------------------------------------+
//| Set will topic in payload                                        |
//| Parameters: will_topic - will message topic                      |
//+------------------------------------------------------------------+
void CConnect::SetWillTopic(const string &will_topic) {
  //--- Will topics are topic names and must not contain wildcard characters per §3.1.3.2 and §4.7.1
  if (StringFind(will_topic, "#") >= 0 || StringFind(will_topic, "+") >= 0) {
    MQTT_LOG_ERROR("Will Topic must not contain wildcard characters (# or +) per MQTT §3.1.3.2 and §4.7.1");
    ArrayFree(m_will_topic);
    return;
  }
  //--- EncodeUTF8String handles all sizing internally; pre-resize is redundant.
  EncodeUTF8String(will_topic, m_will_topic);
};

//+------------------------------------------------------------------+
//| Set will user property (Allows multiple properties)              |
//| Parameters: key - property name                                  |
//|             val - property value                                 |
//+------------------------------------------------------------------+
void CConnect::SetWillUserProperty(const string &key, const string &val) {
  CPropertyEncoder::EncodeStringPairProperty(m_will_userprops, MQTT_PROP_IDENTIFIER_USER_PROPERTY, key, val);
};

//+------------------------------------------------------------------+
//| Set will correlation data                                        |
//| Parameters: corr_data - binary correlation data string           |
//+------------------------------------------------------------------+
void CConnect::SetWillCorrelationData(const string &corr_data) {
  uchar aux[];
  int   len = StringToCharArray(corr_data, aux, 0, WHOLE_ARRAY, CP_UTF8);
  if (len > 0 && aux[len - 1] == 0) {
    ArrayResize(aux, len - 1);
  }

  SetWillCorrelationData(aux);
};

//+------------------------------------------------------------------+
//| Set will correlation data                                        |
//| Parameters: corr_data - binary correlation data                  |
//+------------------------------------------------------------------+
void CConnect::SetWillCorrelationData(const uchar &corr_data[]) {
  if (ArraySize(corr_data) > 65535) {
    MQTT_LOG_ERROR("Correlation Data exceeds 65535 bytes");
    return;
  }

  //--- Encode only into the dedicated per-property buffer
  ArrayFree(m_will_corrdata);
  CPropertyEncoder::EncodeBinaryProperty(m_will_corrdata, MQTT_PROP_IDENTIFIER_CORRELATION_DATA, corr_data);
  UpdateWillPropsLen();
};

//+------------------------------------------------------------------+
//| Set will response topic                                          |
//| Parameters: resp_topic - response topic string                   |
//+------------------------------------------------------------------+
void CConnect::SetWillResponseTopic(const string &resp_topic) {
  ArrayFree(m_will_resptopic);
  CPropertyEncoder::EncodeStringProperty(m_will_resptopic, MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC, resp_topic);
  UpdateWillPropsLen();
};

//+------------------------------------------------------------------+
//| Set will content type                                            |
//| Parameters: content_type - MIME type string                      |
//+------------------------------------------------------------------+
void CConnect::SetWillContentType(const string &content_type) {
  ArrayFree(m_will_contenttype);
  CPropertyEncoder::EncodeStringProperty(m_will_contenttype, MQTT_PROP_IDENTIFIER_CONTENT_TYPE, content_type);
  UpdateWillPropsLen();
}

//+------------------------------------------------------------------+
//| Set will message expiry interval                                 |
//| Parameters: seconds - expiry interval in seconds                 |
//+------------------------------------------------------------------+
void CConnect::SetWillMessageExpiryInterval(uint seconds) {
  ArrayFree(m_will_msgexpint);
  CPropertyEncoder::EncodeFourByteIntegerProperty(m_will_msgexpint, MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL,
                                                  seconds);
  UpdateWillPropsLen();
}

//+------------------------------------------------------------------+
//| Set will payload format indicator                                |
//| Parameters: val - 0 for bytes, 1 for UTF-8                       |
//+------------------------------------------------------------------+
void CConnect::SetWillPayloadFormatIndicator(uchar val) {
  ArrayFree(m_will_payloadformat);
  CPropertyEncoder::EncodeByteProperty(m_will_payloadformat, MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR, val);
  UpdateWillPropsLen();
}

//+------------------------------------------------------------------+
//| Set will delay interval                                          |
//| Parameters: seconds - delay interval in seconds                  |
//+------------------------------------------------------------------+
void CConnect::SetWillDelayInterval(uint seconds) {
  ArrayFree(m_will_delayint);
  CPropertyEncoder::EncodeFourByteIntegerProperty(m_will_delayint, MQTT_PROP_IDENTIFIER_WILL_DELAY_INTERVAL, seconds);
  UpdateWillPropsLen();
}

//+------------------------------------------------------------------+
//| Set authentication data (string overload)                        |
//| Parameters: bindata - authentication data string                 |
//+------------------------------------------------------------------+
void CConnect::SetAuthData(const string &bindata) {
  uchar aux[];
  int   len = StringToCharArray(bindata, aux, 0, WHOLE_ARRAY, CP_UTF8);
  if (len > 0 && aux[len - 1] == 0) {
    ArrayResize(aux, len - 1);
  }

  ArrayFree(m_authdata);
  CPropertyEncoder::EncodeBinaryProperty(m_authdata, MQTT_PROP_IDENTIFIER_AUTHENTICATION_DATA, aux);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set authentication data (binary overload)                        |
//| Parameters: auth_data - binary authentication data               |
//+------------------------------------------------------------------+
void CConnect::SetAuthData(const uchar &auth_data[]) {
  ArrayFree(m_authdata);
  CPropertyEncoder::EncodeBinaryProperty(m_authdata, MQTT_PROP_IDENTIFIER_AUTHENTICATION_DATA, auth_data);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set authentication method                                        |
//| Parameters: auth_method - authentication method name             |
//+------------------------------------------------------------------+
void CConnect::SetAuthMethod(const string &auth_method) {
  ArrayFree(m_authmethod);
  CPropertyEncoder::EncodeStringProperty(m_authmethod, MQTT_PROP_IDENTIFIER_AUTHENTICATION_METHOD, auth_method);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set user property (Allows multiple properties)                   |
//| Parameters: key - property name                                  |
//|             val - property value                                 |
//+------------------------------------------------------------------+
void CConnect::SetUserProperty(const string &key, const string &val) {
  CPropertyEncoder::EncodeStringPairProperty(m_userprops, MQTT_PROP_IDENTIFIER_USER_PROPERTY, key, val);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set request problem information                                  |
//| Parameters: val - 0 or 1                                         |
//+------------------------------------------------------------------+
void CConnect::SetRequestProblemInfo(uchar val) {
  ArrayFree(m_req_problinfo);
  CPropertyEncoder::EncodeByteProperty(m_req_problinfo, MQTT_PROP_IDENTIFIER_REQUEST_PROBLEM_INFORMATION, val);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set request response information                                 |
//| Parameters: val - 0 or 1                                         |
//+------------------------------------------------------------------+
void CConnect::SetRequestResponseInfo(uchar val) {
  ArrayFree(m_req_respinfo);
  if (val > 1) {
    MQTT_LOG_ERROR("Request Response Information must be 0 or 1 per MQTT §3.1.2.11.7");
    UpdateConnPropsLen();
    return;
  }
  CPropertyEncoder::EncodeByteProperty(m_req_respinfo, MQTT_PROP_IDENTIFIER_REQUEST_RESPONSE_INFORMATION, (uchar)val);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set topic alias maximum                                          |
//| Parameters: topic_alias_max - maximum topic alias value          |
//+------------------------------------------------------------------+
void CConnect::SetTopicAliasMaximum(ushort topic_alias_max) {
  ArrayFree(m_topicalias_max);
  CPropertyEncoder::EncodeTwoByteIntegerProperty(m_topicalias_max, MQTT_PROP_IDENTIFIER_TOPIC_ALIAS_MAXIMUM,
                                                 topic_alias_max);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set maximum packet size                                          |
//| Parameters: max_pkt_s - maximum packet size in bytes             |
//+------------------------------------------------------------------+
void CConnect::SetMaximumPacketSize(uint max_pkt_s) {
  if (max_pkt_s == 0) {
    MQTT_LOG_ERROR("Maximum Packet Size cannot be 0");
    return;
  }
  ArrayFree(m_maxpkt_size);
  CPropertyEncoder::EncodeFourByteIntegerProperty(m_maxpkt_size, MQTT_PROP_IDENTIFIER_MAXIMUM_PACKET_SIZE, max_pkt_s);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set receive maximum                                              |
//| Parameters: receive_max - maximum QoS 1/2 messages               |
//+------------------------------------------------------------------+
void CConnect::SetReceiveMaximum(ushort receive_max) {
  if (receive_max == 0) {
    MQTT_LOG_ERROR("Receive Maximum cannot be 0");
    return;
  }
  ArrayFree(m_receive_max);
  CPropertyEncoder::EncodeTwoByteIntegerProperty(m_receive_max, MQTT_PROP_IDENTIFIER_RECEIVE_MAXIMUM, receive_max);
  UpdateConnPropsLen();
}

//--- Properties (§3.1.2.11)
//+------------------------------------------------------------------+
//| Set session expiry interval                                      |
//| Parameters: seconds - session expiry in seconds                  |
//+------------------------------------------------------------------+
void CConnect::SetSessionExpiryInterval(uint seconds) {
  ArrayFree(m_sessionexp_int);
  CPropertyEncoder::EncodeFourByteIntegerProperty(m_sessionexp_int, MQTT_PROP_IDENTIFIER_SESSION_EXPIRY_INTERVAL,
                                                  seconds);
  UpdateConnPropsLen();
}

//+------------------------------------------------------------------+
//| Set client identifier                                            |
//| Parameters: clientId - client identifier string                  |
//+------------------------------------------------------------------+
void CConnect::SetClientIdentifier(const string &clientId) {
  //--- EncodeUTF8String handles all sizing internally; pre-resize is redundant.
  EncodeUTF8String(clientId, m_clientid);
};

//+------------------------------------------------------------------+
//| Build - Assemble the complete CONNECT packet                     |
//| Purpose: Compile variable header and payload into binary form    |
//| Parameters: pkt - [OUT] the resulting CONNECT packet bytes       |
//|             fc  - [IN] flow control for packet size validation   |
//| Note: Implements the assembly sequence defined in MQTT 5.0 §3.1  |
//+------------------------------------------------------------------+
void CConnect::Build(uchar &pkt[], CFlowControl *fc) {
  //--- 1. Initial validation per MQTT Spec §3.1.2.6
  //--- If the Will Flag (bit 2) is 0, the Will QoS (bits 4-3) must be 0
  if ((m_connflags & WILL_FLAG) == 0) {
    if ((m_connflags & (WILL_QOS_1 | WILL_QOS_2)) != 0) {
      MQTT_LOG_ERROR("Will QoS must be 0 when Will Flag is 0 per MQTT §3.1.2.6");
      return;
    }
    //--- Will Retain must also be 0 when Will Flag is not set per §3.1.2.7
    if ((m_connflags & WILL_RETAIN) != 0) {
      MQTT_LOG_ERROR("Will Retain must be 0 when Will Flag is 0 per MQTT §3.1.2.7");
      return;
    }
  }

  //--- Will QoS=3 is forbidden per §3.1.2.6 regardless of how the bits were set
  uchar will_qos = (m_connflags >> 3) & 0x03;
  if (will_qos > 2) {
    MQTT_LOG_ERROR("Will QoS 3 is invalid per MQTT §3.1.2.12");
    return;
  }

  //--- Will Topic is mandatory when Will Flag is set and must not be zero-length.
  if ((m_connflags & WILL_FLAG) != 0 && ArraySize(m_will_topic) <= 2) {
    MQTT_LOG_ERROR("Will Topic must be a non-empty UTF-8 string when Will Flag is set per MQTT §3.1.3.2");
    return;
  }

  //--- 2. Assemble Connection Properties into a local buffer per §3.1.2.11
  uchar local_connprops[];
  if (ArraySize(m_sessionexp_int) > 0) {
    ArrayCopy(local_connprops, m_sessionexp_int, ArraySize(local_connprops));
  }
  if (ArraySize(m_receive_max) > 0) {
    ArrayCopy(local_connprops, m_receive_max, ArraySize(local_connprops));
  }
  if (ArraySize(m_maxpkt_size) > 0) {
    ArrayCopy(local_connprops, m_maxpkt_size, ArraySize(local_connprops));
  }
  if (ArraySize(m_topicalias_max) > 0) {
    ArrayCopy(local_connprops, m_topicalias_max, ArraySize(local_connprops));
  }
  if (ArraySize(m_req_respinfo) > 0) {
    ArrayCopy(local_connprops, m_req_respinfo, ArraySize(local_connprops));
  }
  if (ArraySize(m_req_problinfo) > 0) {
    ArrayCopy(local_connprops, m_req_problinfo, ArraySize(local_connprops));
  }
  if (ArraySize(m_userprops) > 0) {
    ArrayCopy(local_connprops, m_userprops, ArraySize(local_connprops));
  }
  if (ArraySize(m_authmethod) > 0) {
    ArrayCopy(local_connprops, m_authmethod, ArraySize(local_connprops));
  }
  if (ArraySize(m_authdata) > 0) {
    ArrayCopy(local_connprops, m_authdata, ArraySize(local_connprops));
  }
  uint  local_connprops_len = ArraySize(local_connprops);

  //--- 3. Assemble Will Properties into a local buffer (§3.1.3.2)
  uchar local_willprops[];
  if ((m_connflags & WILL_FLAG) != 0) {
    if (ArraySize(m_will_delayint) > 0) {
      ArrayCopy(local_willprops, m_will_delayint, ArraySize(local_willprops));
    }
    if (ArraySize(m_will_payloadformat) > 0) {
      ArrayCopy(local_willprops, m_will_payloadformat, ArraySize(local_willprops));
    }
    if (ArraySize(m_will_msgexpint) > 0) {
      ArrayCopy(local_willprops, m_will_msgexpint, ArraySize(local_willprops));
    }
    if (ArraySize(m_will_contenttype) > 0) {
      ArrayCopy(local_willprops, m_will_contenttype, ArraySize(local_willprops));
    }
    if (ArraySize(m_will_resptopic) > 0) {
      ArrayCopy(local_willprops, m_will_resptopic, ArraySize(local_willprops));
    }
    if (ArraySize(m_will_corrdata) > 0) {
      ArrayCopy(local_willprops, m_will_corrdata, ArraySize(local_willprops));
    }
    if (ArraySize(m_will_userprops) > 0) {
      ArrayCopy(local_willprops, m_will_userprops, ArraySize(local_willprops));
    }
  }
  uint local_willprops_len = ArraySize(local_willprops);

  //--- 4. Validate Will Message Size per §3.1.3
  if ((m_connflags & WILL_FLAG) != 0) {
    // Combined size of Will properties length (varint) + properties + topic + payload
    uint will_total_size = (uint)GetVarintBytes(local_willprops_len) + local_willprops_len + (uint)m_will_topic.Size()
                         + (uint)m_will_payload.Size();

    if (fc != NULL) {
      if (!fc.ValidateClientPacketSize(will_total_size)) {
        MQTT_LOG_ERROR("Will message section size (" + (string)will_total_size
                       + ") exceeds Maximum Packet Size per §3.1.3");
        return;
      }
    }
  }

  //--- 5. Calculate Total Remaining Length (MQTT 5.0 §3.1.1)
  EncodeVariableByteInteger(local_connprops_len, m_connprops_len_buf);

  uint local_remlen  = 10;  // Fixed size of Variable Header (Protocol Name, Level, Flags, Keep Alive)
  local_remlen      += (uint)ArraySize(m_connprops_len_buf) + local_connprops_len;
  local_remlen      += (uint)m_clientid.Size();

  if ((m_connflags & WILL_FLAG) != 0) {
    EncodeVariableByteInteger(local_willprops_len, m_willprops_len_buf);
    local_remlen += (uint)ArraySize(m_willprops_len_buf) + local_willprops_len + (uint)m_will_topic.Size()
                  + (uint)m_will_payload.Size();
  }
  if ((m_connflags & USER_NAME_FLAG) != 0) {
    local_remlen += (uint)m_user_name.Size();
  }
  if ((m_connflags & PASSWORD_FLAG) != 0) {
    local_remlen += (uint)m_password.Size();
  }

  //--- 6. Fixed Header and Total Size Validation
  EncodeVariableByteInteger(local_remlen, m_remlen_buf);
  uint total_packet_size = 1 + (uint)ArraySize(m_remlen_buf) + local_remlen;

  if (fc != NULL) {
    if (!fc.ValidateClientPacketSize(total_packet_size)) {
      MQTT_LOG_ERROR("CONNECT packet size (" + (string)total_packet_size
                     + ") exceeds Maximum Packet Size per §3.1.2.11.4");
      return;
    }
  }

  //--- 7. Final assembly: build the byte array
  m_remlen = local_remlen;
  ArrayCopy(m_connprops, local_connprops);
  m_connprops_len = local_connprops_len;
  ArrayCopy(m_willprops, local_willprops);
  m_willprops_len = local_willprops_len;

  ArrayResize(pkt, total_packet_size);
  pkt[0] = (uchar)CONNECT << 4;
  ArrayCopy(pkt, m_remlen_buf, 1);
  uint idx   = 1 + (uint)ArraySize(m_remlen_buf);

  //--- Variable Header (§3.1.2)
  pkt[idx++] = 0;
  pkt[idx++] = 4;                // Protocol Name Length (4)
  pkt[idx++] = 0x4D;             // 'M'
  pkt[idx++] = 0x51;             // 'Q'
  pkt[idx++] = 0x54;             // 'T'
  pkt[idx++] = 0x54;             // 'T' - Protocol Name
  pkt[idx++] = 0x05;             // Protocol Version v5.0
  pkt[idx++] = m_connflags;      // Connect Flags
  pkt[idx++] = m_keepalive.msb;  // Keep Alive
  pkt[idx++] = m_keepalive.lsb;

  //--- Properties (§3.1.2.11)
  ArrayCopy(pkt, m_connprops_len_buf, idx);
  idx += (uint)ArraySize(m_connprops_len_buf);
  ArrayCopy(pkt, m_connprops, idx);
  idx += m_connprops_len;

  //--- Payload (§3.1.3): Client ID is mandatory
  ArrayCopy(pkt, m_clientid, idx);
  idx += (uint)m_clientid.Size();

  //--- Payload (§3.1.3): Will Properties, Topic, and Payload
  if ((m_connflags & WILL_FLAG) != 0) {
    ArrayCopy(pkt, m_willprops_len_buf, idx);
    idx += (uint)ArraySize(m_willprops_len_buf);

    ArrayCopy(pkt, m_willprops, idx);
    idx += m_willprops_len;

    ArrayCopy(pkt, m_will_topic, idx);
    idx += (uint)m_will_topic.Size();

    ArrayCopy(pkt, m_will_payload, idx);
    idx += (uint)m_will_payload.Size();
  }

  //--- Payload (§3.1.3): User Name
  if ((m_connflags & USER_NAME_FLAG) != 0) {
    ArrayCopy(pkt, m_user_name, idx);
    idx += (uint)m_user_name.Size();
  }

  //--- Payload (§3.1.3): Password
  if ((m_connflags & PASSWORD_FLAG) != 0) {
    ArrayCopy(pkt, m_password, idx);
  }
}

//+------------------------------------------------------------------+
//| Set keep alive interval                                          |
//| Parameters: seconds - keep alive interval (max 65535)            |
//+------------------------------------------------------------------+
void CConnect::SetKeepAlive(ushort seconds) {  // MQTT max is 65,535 sec
  m_keepalive.msb = (uchar)(seconds >> 8) & 255;
  m_keepalive.lsb = (uchar)seconds & 255;
}

//+------------------------------------------------------------------+
//| Set password flag                                                |
//| Parameters: passwordFlag - true to include password              |
//+------------------------------------------------------------------+
void CConnect::SetPasswordFlag(const bool passwordFlag) {
  passwordFlag ? m_connflags |= PASSWORD_FLAG : m_connflags &= (uchar)~PASSWORD_FLAG;
}

//+------------------------------------------------------------------+
//| Set username flag                                                |
//| Parameters: userNameFlag - true to include username              |
//+------------------------------------------------------------------+
void CConnect::SetUserNameFlag(const bool userNameFlag) {
  userNameFlag ? m_connflags |= USER_NAME_FLAG : m_connflags &= (uchar)~USER_NAME_FLAG;
}

//+------------------------------------------------------------------+
//| Set will retain flag                                             |
//| Parameters: willRetain - true to retain will message             |
//+------------------------------------------------------------------+
void CConnect::SetWillRetain(const bool willRetain) {
  willRetain ? m_connflags |= WILL_RETAIN : m_connflags &= (uchar)~WILL_RETAIN;
}

//+------------------------------------------------------------------+
//| Set will QoS bit 2                                               |
//| Parameters: willQoS_2 - true to set QoS bit 2                    |
//+------------------------------------------------------------------+
void CConnect::SetWillQoS_2(const bool willQoS_2) {
  //--- Clear both QoS bits atomically so setting QoS_2 can never leave QoS_1 set,
  //--- which would silently create the forbidden Will QoS=3 value.
  m_connflags &= (uchar) ~(WILL_QOS_1 | WILL_QOS_2);
  if (willQoS_2) {
    m_connflags |= WILL_QOS_2;
  }
}

//+------------------------------------------------------------------+
//| Set will QoS bit 1                                               |
//| Parameters: willQoS_1 - true to set QoS bit 1                    |
//+------------------------------------------------------------------+
void CConnect::SetWillQoS_1(const bool willQoS_1) {
  //--- Clear both QoS bits atomically so setting QoS_1 can never leave QoS_2 set,
  //--- which would silently create the forbidden Will QoS=3 value.
  m_connflags &= (uchar) ~(WILL_QOS_1 | WILL_QOS_2);
  if (willQoS_1) {
    m_connflags |= WILL_QOS_1;
  }
}

//+------------------------------------------------------------------+
//| Set will flag                                                    |
//| Parameters: willFlag - true to include will message              |
//+------------------------------------------------------------------+
void CConnect::SetWillFlag(const bool willFlag) {
  willFlag ? m_connflags |= WILL_FLAG : m_connflags &= (uchar)~WILL_FLAG;
};

//+------------------------------------------------------------------+
//| Set clean start flag                                             |
//| Parameters: cleanStart - true for clean session                  |
//+------------------------------------------------------------------+
void CConnect::SetCleanStart(const bool cleanStart) {
  cleanStart ? m_connflags |= CLEAN_START : m_connflags &= (uchar)~CLEAN_START;
}

//+------------------------------------------------------------------+
//| Default constructor                                              |
//+------------------------------------------------------------------+
CConnect::CConnect()
    : m_connflags(0)
    , m_remlen(0)
    , m_connprops_len(0)
    , m_willprops_len(0)
    , m_payload_len(0) {
  m_keepalive.msb = 0;
  m_keepalive.lsb = 0;
  EncodeUTF8String("", m_clientid);
}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
CConnect::~CConnect() {
  //--- Securely zero credential buffers before deallocation
  SecureZeroArray(m_password);
  SecureZeroArray(m_user_name);
  SecureZeroArray(m_authdata);
  SecureZeroArray(m_authmethod);
}

//+------------------------------------------------------------------+
//| UpdateConnPropsLen                                               |
//| Purpose: Recalculate the total length of connection properties   |
//+------------------------------------------------------------------+
void CConnect::UpdateConnPropsLen() {
  m_connprops_len = (uint)ArraySize(m_sessionexp_int) + (uint)ArraySize(m_receive_max) + (uint)ArraySize(m_maxpkt_size)
                  + (uint)ArraySize(m_topicalias_max) + (uint)ArraySize(m_req_respinfo)
                  + (uint)ArraySize(m_req_problinfo) + (uint)ArraySize(m_userprops) + (uint)ArraySize(m_authmethod)
                  + (uint)ArraySize(m_authdata);
}

//+------------------------------------------------------------------+
//| UpdateWillPropsLen                                               |
//| Purpose: Recalculate the total length of Will properties         |
//+------------------------------------------------------------------+
void CConnect::UpdateWillPropsLen() {
  m_willprops_len = (uint)ArraySize(m_will_delayint) + (uint)ArraySize(m_will_payloadformat)
                  + (uint)ArraySize(m_will_msgexpint) + (uint)ArraySize(m_will_contenttype)
                  + (uint)ArraySize(m_will_resptopic) + (uint)ArraySize(m_will_corrdata)
                  + (uint)ArraySize(m_will_userprops);
}

//+------------------------------------------------------------------+
//| Set password in payload (binary data overload)                   |
//| Parameters: password - binary password data                      |
//+------------------------------------------------------------------+
void CConnect::SetPassword(const uchar &password[]) {
  //--- Password in MQTT v5.0 is binary data with 2-byte length prefix per §1.5.6
  uint len = ArraySize(password);

  if (len > 65535) {
    MQTT_LOG_ERROR("Password exceeds 65535 bytes");
    return;
  }

  ArrayResize(m_password, len + 2);
  //--- Set length prefix (MSB, LSB)
  m_password[0] = (uchar)(len >> 8);
  m_password[1] = (uchar)(len & 0xFF);

  if (len > 0) {
    ArrayCopy(m_password, password, 2, 0, len);
  }
}

//+------------------------------------------------------------------+
//| Set will payload in payload (binary data overload)               |
//| Parameters: will_payload - binary will message payload           |
//+------------------------------------------------------------------+
void CConnect::SetWillPayload(const uchar &will_payload[]) {
  //--- Will payload in MQTT v5.0 is binary data with 2-byte length prefix per §1.5.6
  uint len = ArraySize(will_payload);

  if (len > 65535) {
    MQTT_LOG_ERROR("Will Payload exceeds 65535 bytes");
    return;
  }

  ArrayResize(m_will_payload, len + 2);
  //--- Set length prefix (MSB, LSB)
  m_will_payload[0] = (uchar)(len >> 8);
  m_will_payload[1] = (uchar)(len & 0xFF);

  if (len > 0) {
    ArrayCopy(m_will_payload, will_payload, 2, 0, len);
  }
}

#endif  // MQTT_CONNECT_MQH
