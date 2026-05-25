//+------------------------------------------------------------------+
//|                                              PropertyEncoder.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| MQTT 5.0 Property Encoder utility class                          |
//| Consolidates property serialization and deserialization across   |
//| all control packets (CONNECT, PUBLISH, SUBSCRIBE, etc.)          |
//+------------------------------------------------------------------+
#ifndef MQTT_INTERNAL_UTIL_PROPERTYENCODER_MQH
#define MQTT_INTERNAL_UTIL_PROPERTYENCODER_MQH

//+------------------------------------------------------------------+
//| Class CPropertyEncoder                                           |
//| Purpose: Utility class for encoding MQTT 5.0 properties          |
//|          Provides static methods for serializing property values |
//|          in the correct wire format per MQTT 5.0 specification   |
//| Usage: Use static EncodeXxxProperty() methods to append          |
//|        properties to destination buffers during packet build     |
//| Note: All methods append to the destination buffer rather than   |
//|       replacing its contents, allowing multiple properties       |
//|       to be accumulated in a single buffer.                      |
//+------------------------------------------------------------------+
class CPropertyEncoder {
 public:
  //--- Serialization primitives (encode properties into wire format)
  //--- Each method appends a complete property (identifier + value) to dest[]
  static void AppendPropertyBytes(uchar &dest[], const uchar &src[], const uint len);
  static void EncodeByteProperty(uchar &dest[], const uchar prop_id, const uchar val);
  static void EncodeTwoByteIntegerProperty(uchar &dest[], const uchar prop_id, const ushort val);
  static void EncodeFourByteIntegerProperty(uchar &dest[], const uchar prop_id, const uint val);
  static void EncodeVariableByteIntegerProperty(uchar &dest[], const uchar prop_id, const uint val);
  static void EncodeStringProperty(uchar &dest[], const uchar prop_id, const string val);
  static void EncodeBinaryProperty(uchar &dest[], const uchar prop_id, const uchar &val[]);
  static void EncodeStringPairProperty(uchar &dest[], const uchar prop_id, const string key, const string val);

  //--- Deserialization support logic
  //--- Determines the byte length of a property value for parsing
  static bool GetPropertyValueLength(const uchar prop_id, const uchar &buf[], uint idx, uint &value_len);
};

#include "..\\..\\MQTT.mqh"

//+------------------------------------------------------------------+
//| AppendPropertyBytes                                              |
//| Purpose: Append raw property bytes to a destination buffer       |
//| Parameters: dest - [IN/OUT] destination buffer to append to      |
//|             src  - [IN] source bytes to copy                     |
//|             len  - [IN] number of bytes to copy                  |
//| Note: Uses exponential growth strategy to minimize reallocations |
//+------------------------------------------------------------------+
void CPropertyEncoder::AppendPropertyBytes(uchar &dest[], const uchar &src[], const uint len) {
  if (len == 0) {
    return;
  }
  uint current_size = (uint)ArraySize(dest);
  uint new_size     = current_size + len;
  //--- Optimize: Use 50% reserve for exponential growth to avoid fragmentation
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  ArrayCopy(dest, src, current_size, 0, len);
}

//+------------------------------------------------------------------+
//| EncodeByteProperty                                               |
//| Purpose: Encode a single-byte property (Property Type 1)         |
//| Parameters: dest    - [IN/OUT] destination buffer to append to   |
//|             prop_id - [IN] property identifier (e.g., 0x01)      |
//|             val     - [IN] single byte value                     |
//| Wire format: [prop_id:1][value:1]                                |
//| Used for: Payload Format Indicator, Request Problem Info, etc.   |
//+------------------------------------------------------------------+
void CPropertyEncoder::EncodeByteProperty(uchar &dest[], const uchar prop_id, const uchar val) {
  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 2;
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  dest[current_size]     = prop_id;
  dest[current_size + 1] = val;
}

//+------------------------------------------------------------------+
//| EncodeTwoByteIntegerProperty                                     |
//| Purpose: Encode a two-byte integer property (Property Type 2)    |
//| Parameters: dest    - [IN/OUT] destination buffer to append to   |
//|             prop_id - [IN] property identifier (e.g., 0x21)      |
//|             val     - [IN] 16-bit unsigned integer value         |
//| Wire format: [prop_id:1][value_msb:1][value_lsb:1]               |
//| Used for: Receive Maximum, Topic Alias, Server Keep Alive, etc.  |
//+------------------------------------------------------------------+
void CPropertyEncoder::EncodeTwoByteIntegerProperty(uchar &dest[], const uchar prop_id, const ushort val) {
  uchar aux[2];
  EncodeTwoByteInteger(val, aux);
  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 3;
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  dest[current_size] = prop_id;
  ArrayCopy(dest, aux, current_size + 1);
}

//+------------------------------------------------------------------+
//| EncodeFourByteIntegerProperty                                    |
//| Purpose: Encode a four-byte integer property (Property Type 4)   |
//| Parameters: dest    - [IN/OUT] destination buffer to append to   |
//|             prop_id - [IN] property identifier (e.g., 0x11)      |
//|             val     - [IN] 32-bit unsigned integer value         |
//| Wire format: [prop_id:1][value_b0:1][value_b1:1][value_b2:1][b3] |
//| Used for: Session Expiry Interval, Message Expiry, Max Pkt Size  |
//+------------------------------------------------------------------+
void CPropertyEncoder::EncodeFourByteIntegerProperty(uchar &dest[], const uchar prop_id, const uint val) {
  uchar aux[4];
  EncodeFourByteInteger(val, aux);
  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 5;
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  dest[current_size] = prop_id;
  ArrayCopy(dest, aux, current_size + 1);
}

//+------------------------------------------------------------------+
//| EncodeVariableByteIntegerProperty                                |
//| Purpose: Encode a variable byte integer property                 |
//| Parameters: dest    - [IN/OUT] destination buffer to append to   |
//|             prop_id - [IN] property identifier (e.g., 0x0B)      |
//|             val     - [IN] integer value (0 to 268,435,455)      |
//| Wire format: [prop_id:1][varint:1-4]                             |
//| Used for: Subscription Identifier                                |
//+------------------------------------------------------------------+
void CPropertyEncoder::EncodeVariableByteIntegerProperty(uchar &dest[], const uchar prop_id, const uint val) {
  uchar aux[];
  EncodeVariableByteInteger(val, aux);
  uint current_size = ArraySize(dest);
  uint aux_size     = ArraySize(aux);
  uint new_size     = current_size + 1 + aux_size;
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  dest[current_size] = prop_id;
  ArrayCopy(dest, aux, current_size + 1);
}

//+------------------------------------------------------------------+
//| EncodeStringProperty                                             |
//| Purpose: Encode a UTF-8 string property                          |
//| Parameters: dest    - [IN/OUT] destination buffer to append to   |
//|             prop_id - [IN] property identifier (e.g., 0x03)      |
//|             val     - [IN] string value to encode as UTF-8       |
//| Wire format: [prop_id:1][len_msb:1][len_lsb:1][utf8_data:N]      |
//| Used for: Content Type, Response Topic, Reason String, etc.      |
//+------------------------------------------------------------------+
void CPropertyEncoder::EncodeStringProperty(uchar &dest[], const uchar prop_id, const string val) {
  uchar aux[];
  if (!EncodeUTF8String(val, aux)) {
    return;
  }
  uint current_size = ArraySize(dest);
  uint aux_size     = ArraySize(aux);
  uint new_size     = current_size + 1 + aux_size;
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  dest[current_size] = prop_id;
  ArrayCopy(dest, aux, current_size + 1);
}

//+------------------------------------------------------------------+
//| EncodeStringPairProperty                                         |
//| Purpose: Encode a UTF-8 string pair property (key-value)         |
//| Parameters: dest    - [IN/OUT] destination buffer to append to   |
//|             prop_id - [IN] property identifier (0x26)            |
//|             key     - [IN] property key string                   |
//|             val     - [IN] property value string                 |
//| Wire format: [prop_id:1][key_len:2][key:N][val_len:2][val:M]     |
//| Used for: User Property                                          |
//+------------------------------------------------------------------+
void CPropertyEncoder::EncodeStringPairProperty(uchar &dest[], const uchar prop_id, const string key,
                                                const string val) {
  uchar key_buf[], val_buf[];
  if (!EncodeUTF8String(key, key_buf) || !EncodeUTF8String(val, val_buf)) {
    return;
  }
  uint key_len      = ArraySize(key_buf);
  uint val_len      = ArraySize(val_buf);

  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 1 + key_len + val_len;
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  dest[current_size] = prop_id;
  ArrayCopy(dest, key_buf, current_size + 1);
  ArrayCopy(dest, val_buf, current_size + 1 + key_len);
}

//+------------------------------------------------------------------+
//| EncodeBinaryProperty                                             |
//| Purpose: Encode a binary data property                           |
//| Parameters: dest    - [IN/OUT] destination buffer to append to   |
//|             prop_id - [IN] property identifier (e.g., 0x09)      |
//|             val     - [IN] binary data array                     |
//| Wire format: [prop_id:1][len_msb:1][len_lsb:1][binary_data:N]    |
//| Used for: Correlation Data, Authentication Data                  |
//+------------------------------------------------------------------+
void CPropertyEncoder::EncodeBinaryProperty(uchar &dest[], const uchar prop_id, const uchar &val[]) {
  uint val_len = ArraySize(val);
  if (val_len > 65535) {
    MQTT_LOG_ERROR("Binary property exceeds 65535 bytes per MQTT §1.5.6");
    return;
  }
  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 3 + val_len;
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);
  dest[current_size]     = prop_id;
  dest[current_size + 1] = (uchar)((val_len >> 8) & 0xFF);
  dest[current_size + 2] = (uchar)(val_len & 0xFF);
  if (val_len > 0) {
    ArrayCopy(dest, val, current_size + 3);
  }
}

//+------------------------------------------------------------------+
//| GetPropertyValueLength                                           |
//| Purpose: Determine the byte length of property value for parsing |
//| Parameters: prop_id   - [IN] property identifier                 |
//|             buf       - [IN] buffer containing property data     |
//|             idx       - [IN] starting index of property value    |
//|             value_len - [OUT] length of property value in bytes  |
//| Return: true if valid property and value fits in buffer          |
//| Note: Used during deserialization to skip over property values   |
//+------------------------------------------------------------------+
bool CPropertyEncoder::GetPropertyValueLength(const uchar prop_id, const uchar &buf[], uint idx, uint &value_len) {
  uint buf_size = (uint)ArraySize(buf);
  value_len     = 0;

  switch (prop_id) {
    case MQTT_PROP_IDENTIFIER_PAYLOAD_FORMAT_INDICATOR:
    case MQTT_PROP_IDENTIFIER_REQUEST_PROBLEM_INFORMATION:
    case MQTT_PROP_IDENTIFIER_REQUEST_RESPONSE_INFORMATION:
    case MQTT_PROP_IDENTIFIER_MAXIMUM_QOS:
    case MQTT_PROP_IDENTIFIER_RETAIN_AVAILABLE:
    case MQTT_PROP_IDENTIFIER_WILDCARD_SUBSCRIPTION_AVAILABLE:
    case MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER_AVAILABLE:
    case MQTT_PROP_IDENTIFIER_SHARED_SUBSCRIPTION_AVAILABLE:
      value_len = 1;
      return (idx + value_len <= buf_size);

    case MQTT_PROP_IDENTIFIER_SERVER_KEEP_ALIVE:
    case MQTT_PROP_IDENTIFIER_RECEIVE_MAXIMUM:
    case MQTT_PROP_IDENTIFIER_TOPIC_ALIAS_MAXIMUM:
    case MQTT_PROP_IDENTIFIER_TOPIC_ALIAS:
      value_len = 2;
      return (idx + value_len <= buf_size);

    case MQTT_PROP_IDENTIFIER_MESSAGE_EXPIRY_INTERVAL:
    case MQTT_PROP_IDENTIFIER_SESSION_EXPIRY_INTERVAL:
    case MQTT_PROP_IDENTIFIER_WILL_DELAY_INTERVAL:
    case MQTT_PROP_IDENTIFIER_MAXIMUM_PACKET_SIZE:
      value_len = 4;
      return (idx + value_len <= buf_size);

    case MQTT_PROP_IDENTIFIER_CONTENT_TYPE:
    case MQTT_PROP_IDENTIFIER_RESPONSE_TOPIC:
    case MQTT_PROP_IDENTIFIER_CORRELATION_DATA:
    case MQTT_PROP_IDENTIFIER_ASSIGNED_CLIENT_IDENTIFIER:
    case MQTT_PROP_IDENTIFIER_AUTHENTICATION_METHOD:
    case MQTT_PROP_IDENTIFIER_AUTHENTICATION_DATA:
    case MQTT_PROP_IDENTIFIER_RESPONSE_INFORMATION:
    case MQTT_PROP_IDENTIFIER_SERVER_REFERENCE:
    case MQTT_PROP_IDENTIFIER_REASON_STRING: {
      if (idx + 1 >= buf_size) {
        return false;
      }
      uint len  = (uint)((buf[idx] << 8) | buf[idx + 1]);
      value_len = 2 + len;
      return (idx + value_len <= buf_size);
    }
    case MQTT_PROP_IDENTIFIER_USER_PROPERTY: {
      if (idx + 1 >= buf_size) {
        return false;
      }
      uint key_len = (uint)((buf[idx] << 8) | buf[idx + 1]);
      if (idx + 2 + key_len + 1 >= buf_size) {
        return false;
      }
      uint val_len = (uint)((buf[idx + 2 + key_len] << 8) | buf[idx + 3 + key_len]);
      value_len    = 2 + key_len + 2 + val_len;
      return (idx + value_len <= buf_size);
    }
    case MQTT_PROP_IDENTIFIER_SUBSCRIPTION_IDENTIFIER: {
      uint temp_idx = idx;
      DecodeVariableByteInteger(buf, temp_idx);
      value_len = temp_idx - idx;
      return (idx + value_len <= buf_size);
    }
    default:
      return false;
  }
}

#endif  // MQTT_PROPERTYENCODER_MQH
