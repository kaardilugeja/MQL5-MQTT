//+------------------------------------------------------------------+
//|                                                         MQTT.mqh |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Core MQTT 5.0 protocol implementation including packet types,    |
//| encoding/decoding functions, and utility methods.                |
//+------------------------------------------------------------------+
#ifndef MQTT_MQH
#define MQTT_MQH

#include "Internal\Storage\SessionDatabase.mqh"
#include "Internal\Util\Defines.mqh"
#include "Internal\Connection\FlowControl.mqh"
#include "Internal\Session\TopicAliasManager.mqh"

//+------------------------------------------------------------------+
//| MQTT - Control Packet - Types                                    |
//+------------------------------------------------------------------+
/*
Position: byte 1, bits 7-4.
Represented as a 4-bit unsigned value, the values are shown below.
*/
enum ENUM_PKT_TYPE {
  CONNECT     = 0x01,  // Connection request
  CONNACK     = 0x02,  // Connection Acknowledgment
  PUBLISH     = 0x03,  // Publish message
  PUBACK      = 0x04,  // Publish acknowledgment (QoS 1)
  PUBREC      = 0x05,  // Publish received (QoS 2 delivery part 1)
  PUBREL      = 0x06,  // Publish release (QoS 2 delivery part 2)
  PUBCOMP     = 0x07,  // Publish complete (QoS 2 delivery part 3)
  SUBSCRIBE   = 0x08,  // Subscribe request
  SUBACK      = 0x09,  // Subscribe acknowledgment
  UNSUBSCRIBE = 0x0A,  // Unsubscribe request
  UNSUBACK    = 0x0B,  // Unsubscribe acknowledgment
  PINGREQ     = 0x0C,  // PING request
  PINGRESP    = 0x0D,  // PING response
  DISCONNECT  = 0x0E,  // Disconnect notification
  AUTH        = 0x0F,  // Authentication exchange
};

//+------------------------------------------------------------------+
//| PUBLISH - Fixed Header - Publish Flags                           |
//+------------------------------------------------------------------+
enum ENUM_PUBLISH_FLAGS {
  RETAIN_FLAG = 0x01,  // Retain message flag
  QoS_1_FLAG  = 0x02,  // QoS bit 1
  QoS_2_FLAG  = 0x04,  // QoS bit 2
  DUP_FLAG    = 0x08   // Duplicate delivery flag
};

//+------------------------------------------------------------------+
//| CONNECT - Variable Header - Connect Flags                        |
//+------------------------------------------------------------------+
/*
The Connect Flags byte contains several parameters specifying the behavior of the MQTT connection. It
also indicates the presence or absence of fields in the Payload.
*/
enum ENUM_CONNECT_FLAGS {
  RESERVED       = 0x00,  // Reserved bit (must be 0)
  CLEAN_START    = 0x02,  // Clean start session
  WILL_FLAG      = 0x04,  // Will message present
  WILL_QOS_1     = 0x08,  // Will QoS bit 1
  WILL_QOS_2     = 0x10,  // Will QoS bit 2
  WILL_RETAIN    = 0x20,  // Will message retain
  PASSWORD_FLAG  = 0x40,  // Password present
  USER_NAME_FLAG = 0x80   // Username present
};

//+------------------------------------------------------------------+
//| CONNECT - Variable Header - QoS Levels                           |
//+------------------------------------------------------------------+
/*
// Position: bits 4 and 3 of the Connect Flags.
// These two bits specify the QoS level to be used when publishing the Will Message.
// If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00) [MQTT-3.1.2-11].
// If the Will Flag is set to 1, the value of Will QoS can be 0 (0x00), 1 (0x01), or 2 (0x02) [MQTT-3.1.2-12].
// QoS level constants are defined as #defines in Defines.mqh:
//   QoS_0 = 0  (At most once)
//   QoS_1 = 1  (At least once)
//   QoS_2 = 2  (Exactly once)
*/

//+------------------------------------------------------------------+
//| EncodeTwoByteInteger                                             |
//| Purpose: Encode 2-byte big-endian integer                        |
//| Parameters: val - value to encode                                |
//|             dest_buf - output buffer                             |
//+------------------------------------------------------------------+
/*
Two Byte Integer data values are 16-bit unsigned integers in big-endian order: the high order byte
precedes the lower order byte. This means that a 16-bit word is presented on the network as Most
Significant Byte (MSB), followed by Least Significant Byte (LSB).
*/
void EncodeTwoByteInteger(uint val, uchar &dest_buf[]) {
  ArrayResize(dest_buf, 2);
  dest_buf[0] = (uchar)(val >> 8) & 0xff;  // MSB
  dest_buf[1] = (uchar)val & 0xff;         // LSB
}

//+------------------------------------------------------------------+
//| DecodeTwoByteIntAt                                               |
//| Purpose: Decode 2-byte big-endian integer at a given offset      |
//| Parameters: buf - input byte array                               |
//|             at  - byte offset to read from                       |
//|             val - [OUT] decoded 16-bit unsigned integer          |
//| Return: true on success, false if buffer is too short            |
//+------------------------------------------------------------------+
bool DecodeTwoByteIntAt(const uchar &buf[], uint at, ushort &val) {
  if (ArraySize(buf) < (int)(at + 2)) {
    MQTT_LOG_ERROR("DecodeTwoByteIntAt: buffer too short (size=" + (string)ArraySize(buf) + ", need=" + (string)(at + 2)
                   + ")");
    return false;
  }
  val = (ushort)(buf[at] << 8 | buf[at + 1]);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeFourByteIntAt                                              |
//| Purpose: Decode 4-byte big-endian integer at a given offset      |
//| Parameters: buf - input byte array                               |
//|             at  - byte offset to read from                       |
//|             val - [OUT] decoded 32-bit unsigned integer          |
//| Return: true on success, false if buffer is too short            |
//+------------------------------------------------------------------+
bool DecodeFourByteIntAt(const uchar &buf[], uint at, uint &val) {
  if (ArraySize(buf) < (int)(at + 4)) {
    MQTT_LOG_ERROR("DecodeFourByteIntAt: buffer too short (size=" + (string)ArraySize(buf)
                   + ", need=" + (string)(at + 4) + ")");
    return false;
  }
  val = ((uint)buf[at] << 24) | ((uint)buf[at + 1] << 16) | ((uint)buf[at + 2] << 8) | (uint)buf[at + 3];
  return true;
}

//+------------------------------------------------------------------+
//| EncodeFourByteInteger                                            |
//| Purpose: Encode 4-byte big-endian integer                        |
//| Parameters: val - value to encode                                |
//|             dest_buf - output buffer                             |
//+------------------------------------------------------------------+
/*
Four Byte Integer data values are 32-bit unsigned integers in big-endian order: the high order byte
precedes the successively lower order bytes. This means that a 32-bit word is presented on the network
as Most Significant Byte (MSB), followed by the next most Significant Byte (MSB), followed by the next
most Significant Byte (MSB), followed by Least Significant Byte (LSB).
*/
void EncodeFourByteInteger(uint val, uchar &dest_buf[]) {
  ArrayResize(dest_buf, 4);
  dest_buf[0] = (uchar)(val >> 24) & 0xff;  // Most significant byte
  dest_buf[1] = (uchar)(val >> 16) & 0xff;  // Second byte
  dest_buf[2] = (uchar)(val >> 8) & 0xff;   // Third byte
  dest_buf[3] = (uchar)val & 0xff;          // Least significant byte
}

/*
Position: starts at byte 2.
The Remaining Length is a Variable Byte Integer that represents the number of bytes remaining within the
current Control Packet, including data in the Variable Header and the Payload. The Remaining Length
does not include the bytes used to encode the Remaining Length. The packet size is the total number of
bytes in an MQTT Control Packet, this is equal to the length of the Fixed Header plus the Remaining
Length.
*/

//+------------------------------------------------------------------+
//| EncodeVariableByteInteger                                        |
//| Purpose: Encode an integer as MQTT variable byte integer         |
//| Parameters: value - value to encode                              |
//|             dest_buf - output buffer                             |
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
void EncodeVariableByteInteger(uint value, uchar &dest_buf[]) {
  //--- Validate input does not exceed maximum per MQTT spec §1.5.5
  //--- Variable Byte Integer can encode values 0 to 268,435,455 (0x0FFFFFFF)
  if (value > VARINT_MAX_FOUR_BYTES) {
    MQTT_LOG_ERROR("VarInt value " + (string)value + " exceeds maximum of 268,435,455 per MQTT spec §1.5.5");
    ArrayResize(dest_buf, 0);
    return;
  }

  ArrayResize(dest_buf, 4, 4);

  //-- Initialize counters
  uint num_bytes = 0;
  uint idx       = 0;
  do {
    //--- Extract lowest 7 bits (value % 128)
    uchar digit = (uchar)value % 128;

    //--- Prepare remaining value for next iteration
    value       = value / 128;

    //--- If more bytes follow, set the continuation bit (bit 7)
    if (value > 0) {
      digit |= 128;  // Set continuation bit
    }

    //--- Store encoded byte
    dest_buf[idx] = digit;
    idx++;
    num_bytes++;
  } while (value > 0 && num_bytes < 4);  // Max 4 bytes per MQTT spec

  ArrayResize(dest_buf, (int)num_bytes);
}

//+------------------------------------------------------------------+
//| DecodeVariableByteInteger                                        |
//| Purpose: Decode variable byte integer from buffer                |
//| Parameters: buf - input buffer                                   |
//|             idx - starting index (updated to reflect bytes read) |
//| Return: Decoded integer value, or UINT_MAX on error              |
//+------------------------------------------------------------------+
uint DecodeVariableByteInteger(const uchar &buf[], uint &idx) {
  uint multiplier = 1;  // Starts at 1, multiplies by 128 each iteration
  uint value      = 0;  // Accumulated decoded value
  uint bytes_used = 0;
  do {
    //--- Read current byte from buffer
    if (idx >= (uint)ArraySize(buf)) {
      MQTT_LOG_ERROR("Buffer overflow during VarInt decode");
      return UINT_MAX;
    }
    uint encodedByte  = buf[idx];

    //--- Extract lower 7 bits (data) and add to value with current multiplier
    value            += (encodedByte & 0x7F) * multiplier;
    multiplier       *= 128;
    idx++;
    bytes_used++;

    //--- Continuation bit clear: varint is complete and valid
    if ((encodedByte & 0x80) == 0) {
      return value;
    }

    //--- Detect malformed continuation bit on the 4th byte.
    //--- After consuming 4 bytes the continuation bit MUST be clear.
    if (bytes_used == 4) {
      MQTT_LOG_ERROR("Malformed Variable Byte Integer - too many bytes");
      return UINT_MAX;
    }
  } while (true);
  return UINT_MAX;  // Unreachable; satisfies compiler
}

//+------------------------------------------------------------------+
//|        Disallowed Unicode Code Points in UTF-8 Strings           |
//+------------------------------------------------------------------+
/*
In particular, the character data MUST NOT include encodings of code points
between U+D800 and U+DFFF

A UTF-8 Encoded String MUST NOT include an encoding of
the null character U+0000. [MQTT-1.5.4-2]

The data SHOULD NOT include encodings of the Unicode [Unicode] code points listed below.

U+0001..U+001F control characters

U+007F..U+009F control characters

Code points defined in the Unicode specification [Unicode] to be
non-characters (for example U+0FFFF)

A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF ("ZERO WIDTH NO-
BREAK SPACE") wherever it appears in a string and MUST NOT be skipped over or stripped off by a
packet receiver [MQTT-1.5.4-3]
*/

//+------------------------------------------------------------------+
//| IsUtf8MustRejectCodePoint                                        |
//| Purpose: Check if code point is hard-invalid in MQTT UTF-8       |
//| Parameters: code_point - Unicode code point to check             |
//| Return: true if code point must be rejected                      |
//+------------------------------------------------------------------+
bool IsUtf8MustRejectCodePoint(uint code_point) {
  //--- MQTT v5.0 Spec §1.5.4: MUST NOT include U+0000 NUL
  if (code_point == 0x0000) {
    return true;
  }
  //--- MQTT v5.0 Spec §1.5.4: MUST NOT include surrogates U+D800..U+DFFF
  if (code_point >= 0xD800 && code_point <= 0xDFFF) {
    return true;
  }
  return (code_point > 0x10FFFF);
}

//+------------------------------------------------------------------+
//| IsUtf8DiscouragedCodePoint                                       |
//| Purpose: Check if code point is MQTT-valid but discouraged       |
//| Parameters: code_point - Unicode code point to check             |
//| Return: true if code point should trigger a warning              |
//+------------------------------------------------------------------+
bool IsUtf8DiscouragedCodePoint(uint code_point) {
  //--- MQTT v5.0 Spec §1.5.4: SHOULD NOT include U+0001..U+001F control characters
  if (code_point >= 0x0001 && code_point <= 0x001F) {
    return true;
  }
  //--- MQTT v5.0 Spec §1.5.4: SHOULD NOT include U+007F..U+009F control characters
  if (code_point >= 0x007F && code_point <= 0x009F) {
    return true;
  }
  //--- Unicode noncharacters U+FDD0..U+FDEF (block of 32 noncharacters)
  if (code_point >= 0xFDD0 && code_point <= 0xFDEF) {
    return true;
  }
  //--- Unicode noncharacters U+xFFFE and U+xFFFF for all planes (LOW-word check)
  //--- Covers U+FFFE, U+FFFF, U+1FFFE, U+1FFFF, ..., U+10FFFE, U+10FFFF
  if ((code_point & 0xFFFF) >= 0xFFFE) {
    return true;
  }
  return false;
}

//+------------------------------------------------------------------+
//| IsUtf8ContinuationByte                                           |
//| Purpose: Check whether a byte is a UTF-8 continuation byte       |
//+------------------------------------------------------------------+
bool IsUtf8ContinuationByte(const uchar byte) { return (byte & 0xC0) == 0x80; }

//+------------------------------------------------------------------+
//| Encode UTF-8 String                                              |
//| Purpose: Encode a string as MQTT UTF-8 with length prefix        |
//| Parameters: str - string to encode                               |
//|             dest_buf - output buffer                             |
//+------------------------------------------------------------------+
bool EncodeUTF8String(string str, uchar &dest_buf[]) {
  //--- Check for Disallowed Unicode Code Points
  uint str_len = StringLen(str);
  if (str_len == 0) {
    //--- Per MQTT spec §1.5.4: zero-length UTF-8 string encodes as [0x00][0x00]
    ArrayResize(dest_buf, 2);
    dest_buf[0] = 0x00;
    dest_buf[1] = 0x00;
    return true;
  }

  for (uint i = 0; i < str_len; i++) {
    uint cp = (uint)StringGetCharacter(str, i);

    //--- Lone surrogate check (D800..DFFF)
    if (cp >= 0xD800 && cp <= 0xDFFF) {
      //--- Check if it's a valid surrogate pair
      if (cp >= 0xDC00 || i + 1 >= str_len) {
        MQTT_LOG_ERROR(StringFormat("Found lone surrogate or invalid surrogate pair at position %d", i));
        ArrayResize(dest_buf, 0);
        return false;
      }
      uint cp2 = (uint)StringGetCharacter(str, i + 1);
      if (cp2 < 0xDC00 || cp2 > 0xDFFF) {
        MQTT_LOG_ERROR(StringFormat("Found invalid surrogate pair at position %d", i));
        ArrayResize(dest_buf, 0);
        return false;
      }
      uint code_point = 0x10000 + (((cp - 0xD800) << 10) | (cp2 - 0xDC00));
      if (IsUtf8MustRejectCodePoint(code_point)) {
        MQTT_LOG_ERROR(StringFormat("Found invalid Unicode scalar 0x%06X at position %d", code_point, i));
        ArrayResize(dest_buf, 0);
        return false;
      }
      if (IsUtf8DiscouragedCodePoint(code_point)) {
        MQTT_LOG_WARN(StringFormat("UTF-8 string contains discouraged character 0x%06X at position %d", code_point, i));
      }
      i++;  // Valid pair representing a scalar value > U+FFFF
      continue;
    }

    if (IsUtf8MustRejectCodePoint(cp)) {
      MQTT_LOG_ERROR(StringFormat("Found disallowed character 0x%04X at position %d", cp, i));
      ArrayResize(dest_buf, 0);
      return false;
    }
    if (IsUtf8DiscouragedCodePoint(cp)) {
      MQTT_LOG_WARN(StringFormat("UTF-8 string contains discouraged character 0x%04X at position %d", cp, i));
    }
  }

  //--- Convert string to UTF-8 bytes
  uchar utf8_array[];
  int   utf8_len = StringToCharArray(str, utf8_array, 0, WHOLE_ARRAY, CP_UTF8);

  //--- StringToCharArray includes null terminator, MQTT does not
  if (utf8_len > 0 && utf8_array[utf8_len - 1] == 0) {
    utf8_len--;
  }

  //--- Check for length limits (max 65535 per spec §1.5.4)
  if (utf8_len > 65535) {
    MQTT_LOG_ERROR("String exceeds maximum MQTT length of 65,535 bytes");
    ArrayResize(dest_buf, 0);
    ArrayFree(utf8_array);
    return false;
  }

  //--- MQTT UTF-8 format: 2-byte length prefix + UTF-8 bytes
  ArrayResize(dest_buf, utf8_len + 2);
  dest_buf[0] = (uchar)(utf8_len >> 8);
  dest_buf[1] = (uchar)(utf8_len & 0xff);

  if (utf8_len > 0) {
    ArrayCopy(dest_buf, utf8_array, 2, 0, utf8_len);
  }
  ArrayFree(utf8_array);
  return true;
}

//+------------------------------------------------------------------+
//| StringToUTF8Bytes                                                |
//| Purpose: Convert a string to raw UTF-8 bytes WITHOUT the MQTT    |
//|          2-byte length prefix and WITHOUT the null terminator    |
//|          that MQL5's StringToCharArray appends.                  |
//| Parameters: str      - input string                              |
//|             dest_buf - output byte buffer (resized to fit)       |
//| Return: Number of bytes written to dest_buf                      |
//+------------------------------------------------------------------+
int StringToUTF8Bytes(const string str, uchar &dest_buf[]) {
  if (StringLen(str) == 0) {
    ArrayResize(dest_buf, 0);
    return 0;
  }
  int len = StringToCharArray(str, dest_buf, 0, WHOLE_ARRAY, CP_UTF8);
  //--- StringToCharArray includes null terminator, strip it
  if (len > 0 && dest_buf[len - 1] == 0) {
    len--;
    ArrayResize(dest_buf, len);
  }
  return len;
}

//+------------------------------------------------------------------+
//| ValidateUtf8Data                                                 |
//| Purpose: Validate raw UTF-8 bytes against MQTT §1.5.4 / §1.5.7   |
//| Parameters: buf   - input byte array                             |
//|             start - first byte of UTF-8 payload                  |
//|             count - number of bytes to validate                  |
//| Return: MQTT_OK, MQTT_ERROR_BUFFER_OVERFLOW, or                  |
//|         MQTT_ERROR_MALFORMED_PACKET                              |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR ValidateUtf8Data(uchar &buf[], uint start, uint count) {
  uint buf_size = (uint)ArraySize(buf);
  if (start > buf_size || count > buf_size - start) {
    MQTT_LOG_ERROR("UTF-8 data exceeds packet boundary");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  uint end = start + count;
  uint i   = start;
  while (i < end) {
    uchar lead       = buf[i];
    uint  code_point = 0;
    uint  seq_len    = 0;

    if (lead <= 0x7F) {
      code_point = lead;
      seq_len    = 1;
    } else if (lead >= 0xC2 && lead <= 0xDF) {
      if (i + 1 >= end || !IsUtf8ContinuationByte(buf[i + 1])) {
        MQTT_LOG_ERROR("Malformed UTF-8 continuation byte at offset " + (string)i);
        return MQTT_ERROR_MALFORMED_PACKET;
      }
      code_point = ((uint)(lead & 0x1F) << 6) | (uint)(buf[i + 1] & 0x3F);
      seq_len    = 2;
    } else if (lead == 0xE0) {
      if (i + 2 >= end || buf[i + 1] < 0xA0 || buf[i + 1] > 0xBF || !IsUtf8ContinuationByte(buf[i + 2])) {
        MQTT_LOG_ERROR("Malformed UTF-8 3-byte sequence at offset " + (string)i);
        return MQTT_ERROR_MALFORMED_PACKET;
      }
      code_point = ((uint)(lead & 0x0F) << 12) | ((uint)(buf[i + 1] & 0x3F) << 6) | (uint)(buf[i + 2] & 0x3F);
      seq_len    = 3;
    } else if ((lead >= 0xE1 && lead <= 0xEC) || (lead >= 0xEE && lead <= 0xEF)) {
      if (i + 2 >= end || !IsUtf8ContinuationByte(buf[i + 1]) || !IsUtf8ContinuationByte(buf[i + 2])) {
        MQTT_LOG_ERROR("Malformed UTF-8 3-byte sequence at offset " + (string)i);
        return MQTT_ERROR_MALFORMED_PACKET;
      }
      code_point = ((uint)(lead & 0x0F) << 12) | ((uint)(buf[i + 1] & 0x3F) << 6) | (uint)(buf[i + 2] & 0x3F);
      seq_len    = 3;
    } else if (lead == 0xED) {
      if (i + 2 >= end || buf[i + 1] < 0x80 || buf[i + 1] > 0x9F || !IsUtf8ContinuationByte(buf[i + 2])) {
        MQTT_LOG_ERROR("Malformed UTF-8 surrogate sequence at offset " + (string)i);
        return MQTT_ERROR_MALFORMED_PACKET;
      }
      code_point = ((uint)(lead & 0x0F) << 12) | ((uint)(buf[i + 1] & 0x3F) << 6) | (uint)(buf[i + 2] & 0x3F);
      seq_len    = 3;
    } else if (lead == 0xF0) {
      if (i + 3 >= end || buf[i + 1] < 0x90 || buf[i + 1] > 0xBF || !IsUtf8ContinuationByte(buf[i + 2])
          || !IsUtf8ContinuationByte(buf[i + 3])) {
        MQTT_LOG_ERROR("Malformed UTF-8 4-byte sequence at offset " + (string)i);
        return MQTT_ERROR_MALFORMED_PACKET;
      }
      code_point = ((uint)(lead & 0x07) << 18) | ((uint)(buf[i + 1] & 0x3F) << 12) | ((uint)(buf[i + 2] & 0x3F) << 6)
                 | (uint)(buf[i + 3] & 0x3F);
      seq_len    = 4;
    } else if (lead >= 0xF1 && lead <= 0xF3) {
      if (i + 3 >= end || !IsUtf8ContinuationByte(buf[i + 1]) || !IsUtf8ContinuationByte(buf[i + 2])
          || !IsUtf8ContinuationByte(buf[i + 3])) {
        MQTT_LOG_ERROR("Malformed UTF-8 4-byte sequence at offset " + (string)i);
        return MQTT_ERROR_MALFORMED_PACKET;
      }
      code_point = ((uint)(lead & 0x07) << 18) | ((uint)(buf[i + 1] & 0x3F) << 12) | ((uint)(buf[i + 2] & 0x3F) << 6)
                 | (uint)(buf[i + 3] & 0x3F);
      seq_len    = 4;
    } else if (lead == 0xF4) {
      if (i + 3 >= end || buf[i + 1] < 0x80 || buf[i + 1] > 0x8F || !IsUtf8ContinuationByte(buf[i + 2])
          || !IsUtf8ContinuationByte(buf[i + 3])) {
        MQTT_LOG_ERROR("Malformed UTF-8 4-byte sequence at offset " + (string)i);
        return MQTT_ERROR_MALFORMED_PACKET;
      }
      code_point = ((uint)(lead & 0x07) << 18) | ((uint)(buf[i + 1] & 0x3F) << 12) | ((uint)(buf[i + 2] & 0x3F) << 6)
                 | (uint)(buf[i + 3] & 0x3F);
      seq_len    = 4;
    } else {
      MQTT_LOG_ERROR("Malformed UTF-8 lead byte 0x" + StringFormat("%02X", lead) + " at offset " + (string)i);
      return MQTT_ERROR_MALFORMED_PACKET;
    }

    if (IsUtf8MustRejectCodePoint(code_point)) {
      MQTT_LOG_ERROR("Disallowed UTF-8 code point U+" + StringFormat("%04X", code_point) + " at offset " + (string)i);
      return MQTT_ERROR_MALFORMED_PACKET;
    }
    if (IsUtf8DiscouragedCodePoint(code_point)) {
      MQTT_LOG_WARN("Discouraged UTF-8 code point U+" + StringFormat("%04X", code_point) + " at offset " + (string)i);
    }
    i += seq_len;
  }
  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| TryReadUtf8String                                                |
//| Purpose: Read and strictly validate MQTT UTF-8 string            |
//| Parameters: char_array - input byte array                        |
//|             idx        - starting index (updated on success)     |
//|             out_str    - decoded string                          |
//| Return: MQTT_OK on success, or parse error code                  |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR TryReadUtf8String(uchar &char_array[], uint &idx, string &out_str) {
  out_str        = "";
  uint array_len = (uint)ArraySize(char_array);
  if (idx + 2 > array_len) {
    MQTT_LOG_ERROR("UTF-8 string length prefix exceeds packet boundary");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  uint count      = ((uint)char_array[idx] << 8) | (uint)char_array[idx + 1];
  uint data_start = idx + 2;
  if (data_start + count > array_len) {
    MQTT_LOG_ERROR("UTF-8 string payload exceeds packet boundary");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  ENUM_MQTT_ERROR utf8_err = ValidateUtf8Data(char_array, data_start, count);
  if (utf8_err != MQTT_OK) {
    return utf8_err;
  }

  out_str = CharArrayToString(char_array, data_start, count, CP_UTF8);
  idx     = data_start + count;
  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| ReadUtf8String                                                   |
//| Purpose: Read UTF-8 string from byte array and advance index     |
//| Parameters: char_array - input byte array                        |
//|             idx - starting index (updated to reflect bytes read) |
//| Return: Decoded string                                           |
//+------------------------------------------------------------------+
string ReadUtf8String(uchar &char_array[], uint &idx) {
  string str = "";
  if (TryReadUtf8String(char_array, idx, str) != MQTT_OK) {
    return "";
  }
  return str;
}

//+------------------------------------------------------------------+
//| GetQoSLevel                                                      |
//| Purpose: Extract QoS level from packet header                    |
//| Parameters: buf - packet buffer                                  |
//| Return: QoS level (0, 1, 2) or 255 for invalid QoS 3             |
//+------------------------------------------------------------------+
uchar GetQoSLevel(uchar &buf[]) {
  //--- Extract QoS bits (bits 1-2: mask 0x06)
  uchar qos_bits = (uchar)((buf[0] >> 1) & 0x03);

  //--- Check for invalid QoS 3
  if (qos_bits == 3) {
    MQTT_LOG_ERROR("Invalid QoS level 3 (0x06) detected");
    return 255;  // Invalid QoS
  }

  //--- Return valid QoS level (0, 1, or 2)
  return qos_bits;
}

//+------------------------------------------------------------------+
//| WritePacketIdentifier                                            |
//| Purpose: Write a known packet ID into a buffer                   |
//| Parameters: buf - packet buffer                                  |
//|             start_idx - index where to place packet ID           |
//|             pktid - the packet identifier to write               |
//+------------------------------------------------------------------+
void WritePacketIdentifier(uchar &buf[], int start_idx, ushort pktid) {
  if (ArraySize(buf) < start_idx + 2) {
    ArrayResize(buf, start_idx + 2);
  }

  buf[start_idx]     = (uchar)(pktid >> 8);    // MSB
  buf[start_idx + 1] = (uchar)(pktid & 0xFF);  // LSB
}

//+------------------------------------------------------------------+
//| Payload Format Indicator                                         |
//| Purpose: Indicates the format of the payload data                |
//|          Used in PUBLISH packets and Will properties             |
//+------------------------------------------------------------------+
enum PAYLOAD_FORMAT_INDICATOR {
  RAW_BYTES = 0x00,  // Payload is unspecified bytes (default)
  UTF8      = 0x01   // Payload is UTF-8 encoded character data
};

//+------------------------------------------------------------------+
//| ReadUserProperty                                                 |
//| Purpose: Read user property (key-value pair) from buffer         |
//| Parameters: buf - input buffer                                   |
//|             idx - starting index (updated to reflect bytes read) |
//|             dest_buf - output array for key and value            |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR TryReadUserProperty(uchar &buf[], uint &idx, string &dest_buf[]) {
  string          key = "";
  string          val = "";
  uint            cur = idx;

  ENUM_MQTT_ERROR err = TryReadUtf8String(buf, cur, key);
  if (err != MQTT_OK) {
    return err;
  }

  err = TryReadUtf8String(buf, cur, val);
  if (err != MQTT_OK) {
    return err;
  }

  ArrayResize(dest_buf, 2);
  dest_buf[0] = key;
  dest_buf[1] = val;
  idx         = cur;
  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| TryReadUtf8StringWithinBounds                                    |
//| Purpose: Read UTF-8 string constrained to a packet subsection    |
//| Return: MQTT_ERROR_INVALID_PROPS_LEN when the string would       |
//|         overrun the caller-declared subsection boundary          |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR TryReadUtf8StringWithinBounds(uchar &buf[], uint &idx, uint end, string &out_str) {
  out_str        = "";
  uint array_len = (uint)ArraySize(buf);
  if (idx > end || end > array_len) {
    MQTT_LOG_ERROR("UTF-8 string boundary exceeds packet size");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  uint remaining = end - idx;
  if (remaining < 2) {
    MQTT_LOG_ERROR("UTF-8 string length prefix exceeds declared boundary");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  uint count = ((uint)buf[idx] << 8) | (uint)buf[idx + 1];
  if (remaining - 2 < count) {
    MQTT_LOG_ERROR("UTF-8 string payload exceeds declared boundary");
    return MQTT_ERROR_INVALID_PROPS_LEN;
  }

  return TryReadUtf8String(buf, idx, out_str);
}

//+------------------------------------------------------------------+
//| TryReadUserPropertyWithinBounds                                  |
//| Purpose: Read User Property constrained to a packet subsection   |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR TryReadUserPropertyWithinBounds(uchar &buf[], uint &idx, uint end, string &dest_buf[]) {
  string          key = "";
  string          val = "";
  uint            cur = idx;

  ENUM_MQTT_ERROR err = TryReadUtf8StringWithinBounds(buf, cur, end, key);
  if (err != MQTT_OK) {
    return err;
  }

  err = TryReadUtf8StringWithinBounds(buf, cur, end, val);
  if (err != MQTT_OK) {
    return err;
  }

  ArrayResize(dest_buf, 2);
  dest_buf[0] = key;
  dest_buf[1] = val;
  idx         = cur;
  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| ReadUserProperty                                                 |
//| Purpose: Backward-compatible wrapper around TryReadUserProperty  |
//+------------------------------------------------------------------+
bool ReadUserProperty(uchar &buf[], uint &idx, string &dest_buf[]) {
  return TryReadUserProperty(buf, idx, dest_buf) == MQTT_OK;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength                                              |
//| Purpose: Read remaining length from packet                       |
//| Parameters: inpkt - input packet buffer                          |
//| Return: Decoded remaining length                                 |
//+------------------------------------------------------------------+
uint ReadRemainingLength(uchar &inpkt[]) {
  //--- Check for malformed Variable Byte Integer in Remaining Length (starts at index 1)
  uint start_idx = 1;
  if (HasInvalidBytes(inpkt, start_idx)) {
    return 0;
  }

  uint idx = start_idx;
  return DecodeVariableByteInteger(inpkt, idx);
}

//+------------------------------------------------------------------+
//| ReadPropertyLength                                               |
//| Purpose: Read property length from packet                        |
//| Parameters: inpkt - input packet buffer                          |
//|             idx - index where property length starts             |
//| Return: Decoded property length                                  |
//+------------------------------------------------------------------+
uint ReadPropertyLength(uchar &inpkt[], uint &idx) {
  //--- Check for malformed Variable Byte Integer at specified index
  if (HasInvalidBytes(inpkt, idx)) {
    return UINT_MAX;
  }

  //--- Decode property length and advance index
  return DecodeVariableByteInteger(inpkt, idx);
}

//+------------------------------------------------------------------+
//| HasInvalidBytes                                                  |
//| Purpose: Check for malformed Variable Byte Integers              |
//| Parameters: inpkt - input packet buffer                          |
//| Return: true if malformed varint found                           |
//+------------------------------------------------------------------+
bool HasInvalidBytes(uchar &inpkt[], uint start_idx) {
  //--- Check for malformed Variable Byte Integer
  //--- Malformed if:
  //--- 1. Exceeds 4 bytes limit
  //--- 2. Truncated (buffer ends while continuation bit is set)

  uint  idx         = start_idx;
  int   byte_count  = 0;
  uchar encodedByte = 0;

  while (idx < (uint)ArraySize(inpkt) && byte_count < 4) {
    encodedByte = inpkt[idx++];
    byte_count++;

    //--- If bit 7 is not set, this is a valid terminal byte
    if ((encodedByte & 128) == 0) {
      return false;
    }
  }

  //--- If we reached here, it's an error:
  if (byte_count >= 4 && (encodedByte & 128) != 0) {
    MQTT_LOG_ERROR("Malformed Variable Byte Integer - exceeds 4 bytes");
  } else {
    MQTT_LOG_ERROR("Buffer overflow during VarInt decode");
  }
  return true;
}

//+------------------------------------------------------------------+
//| Property Decoding Helpers                                        |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| ReadByte                                                         |
//| Purpose: Read a single byte and advance index                    |
//| Parameters: buf - input buffer                                   |
//|             idx - current index (advanced by 1)                  |
//| Return: The read byte, or 0 if out of bounds                     |
//+------------------------------------------------------------------+
uchar ReadByte(uchar &buf[], uint &idx) {
  if (idx >= (uint)ArraySize(buf)) {
    return 0;
  }
  return buf[idx++];
}

//+------------------------------------------------------------------+
//| ReadByte (safe overload)                                         |
//| Purpose: Read a single byte; sets ok=false on bounds failure so  |
//|          callers can distinguish from a legitimate 0x00 byte     |
//+------------------------------------------------------------------+
uchar ReadByte(uchar &buf[], uint &idx, bool &ok) {
  if (idx >= (uint)ArraySize(buf)) {
    ok = false;
    return 0;
  }
  ok = true;
  return buf[idx++];
}

//+------------------------------------------------------------------+
//| ReadTwoByteInt                                                   |
//| Purpose: Read a 2-byte big-endian integer and advance index      |
//| Note:    Returns 0 on bounds failure — use the bool &ok overload |
//|          for callers where 0 is a valid value (Packet ID, etc.)  |
//+------------------------------------------------------------------+
ushort ReadTwoByteInt(uchar &buf[], uint &idx) {
  if (idx + 2 > (uint)ArraySize(buf)) {
    return 0;
  }
  ushort val  = (ushort)((buf[idx] << 8) | buf[idx + 1]);
  idx        += 2;
  return val;
}

//+------------------------------------------------------------------+
//| ReadTwoByteInt (safe overload)                                   |
//| Purpose: Read a 2-byte big-endian integer; sets ok=false on      |
//|          bounds failure so callers can distinguish from val=0    |
//+------------------------------------------------------------------+
ushort ReadTwoByteInt(uchar &buf[], uint &idx, bool &ok) {
  if (idx + 2 > (uint)ArraySize(buf)) {
    ok = false;
    return 0;
  }
  ushort val  = (ushort)((buf[idx] << 8) | buf[idx + 1]);
  idx        += 2;
  ok          = true;
  return val;
}

//+------------------------------------------------------------------+
//| ReadFourByteInt                                                  |
//| Purpose: Read a 4-byte big-endian integer and advance index      |
//| Note:    Returns 0 on bounds failure — use the bool &ok overload |
//|          for callers where 0 is an ambiguous sentinel            |
//+------------------------------------------------------------------+
uint ReadFourByteInt(uchar &buf[], uint &idx) {
  if (idx + 4 > (uint)ArraySize(buf)) {
    return 0;
  }
  uint val  = ((uint)buf[idx] << 24) | ((uint)buf[idx + 1] << 16) | ((uint)buf[idx + 2] << 8) | (uint)buf[idx + 3];
  idx      += 4;
  return val;
}

//+------------------------------------------------------------------+
//| ReadFourByteInt (safe overload)                                  |
//| Purpose: Read a 4-byte big-endian integer; sets ok=false on      |
//|          bounds failure so callers can distinguish from val=0    |
//+------------------------------------------------------------------+
uint ReadFourByteInt(uchar &buf[], uint &idx, bool &ok) {
  if (idx + 4 > (uint)ArraySize(buf)) {
    ok = false;
    return 0;
  }
  uint val  = ((uint)buf[idx] << 24) | ((uint)buf[idx + 1] << 16) | ((uint)buf[idx + 2] << 8) | (uint)buf[idx + 3];
  idx      += 4;
  ok        = true;
  return val;
}

//+------------------------------------------------------------------+
//| ReadBinaryData                                                   |
//| Purpose: Read binary data (2-byte length + data) and advance idx |
//+------------------------------------------------------------------+
ENUM_MQTT_ERROR TryReadBinaryData(uchar &buf[], uint &idx, uchar &dest[]) {
  uint buf_size = (uint)ArraySize(buf);
  uint cur      = idx;
  if (cur + 2 > buf_size) {
    ArrayFree(dest);
    MQTT_LOG_ERROR("Binary data length prefix exceeds packet boundary");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  ushort len  = (ushort)((buf[cur] << 8) | buf[cur + 1]);
  cur        += 2;
  if (cur + len > buf_size) {
    ArrayFree(dest);
    MQTT_LOG_ERROR("Binary data payload exceeds packet boundary");
    return MQTT_ERROR_BUFFER_OVERFLOW;
  }

  ArrayResize(dest, len);
  if (len > 0) {
    ArrayCopy(dest, buf, 0, cur, len);
  }
  idx = cur + len;
  return MQTT_OK;
}

//+------------------------------------------------------------------+
//| ReadBinaryData                                                   |
//| Purpose: Backward-compatible wrapper around TryReadBinaryData    |
//+------------------------------------------------------------------+
bool ReadBinaryData(uchar &buf[], uint &idx, uchar &dest[]) { return TryReadBinaryData(buf, idx, dest) == MQTT_OK; }

//+------------------------------------------------------------------+
//| GetVarintBytes                                                   |
//| Purpose: Get the number of bytes needed to encode a varint       |
//| Parameters: varint - value to encode                             |
//| Return: Number of bytes (1-4)                                    |
//+------------------------------------------------------------------+
uint GetVarintBytes(uint varint) {
  //--- 4 bytes: encodes values 2,097,152-268,435,455 (28 bits of data, max for MQTT)
  if (varint >= VARINT_MIN_FOUR_BYTES && varint <= VARINT_MAX_FOUR_BYTES) {
    return 4;
  }

  //--- 3 bytes: encodes values 16,384-2,097,151 (21 bits of data)
  if (varint >= VARINT_MIN_THREE_BYTES && varint <= VARINT_MAX_THREE_BYTES) {
    return 3;
  }

  //--- 2 bytes: encodes values 128-16,383 (14 bits of data)
  if (varint >= VARINT_MIN_TWO_BYTES && varint <= VARINT_MAX_TWO_BYTES) {
    return 2;
  }

  //--- 1 byte: encodes values 0-127 (7 bits of data) - default case
  return 1;
}

//+------------------------------------------------------------------+
//| SetPacketIdentifierEx                                            |
//| Purpose: Enhanced packet ID allocation using session database    |
//|          Prevents reuse of in-flight packet IDs                  |
//| Parameters: buf - packet buffer                                  |
//|             start_idx - index where to place packet ID           |
//|             db - session database                                |
//| Return: Allocated packet ID (0 on failure)                       |
//+------------------------------------------------------------------+
long   g_mqtt_pktid_chart_ids[];
ushort g_mqtt_pktid_next_ids[];

//+------------------------------------------------------------------+
//| _MqttFindPacketIdChartSlot - Locate chart-scoped fallback state  |
//+------------------------------------------------------------------+
int    _MqttFindPacketIdChartSlot(long chart_id) {
  int count = ArraySize(g_mqtt_pktid_chart_ids);
  for (int i = 0; i < count; i++) {
    if (g_mqtt_pktid_chart_ids[i] == chart_id) {
      return i;
    }
  }
  return -1;
}

//+------------------------------------------------------------------+
//| _MqttNextFallbackPacketId - Per-chart fallback packet ID         |
//+------------------------------------------------------------------+
ushort _MqttNextFallbackPacketId() {
  long chart_id = (long)ChartID();
  int  idx      = _MqttFindPacketIdChartSlot(chart_id);
  if (idx < 0) {
    idx = ArraySize(g_mqtt_pktid_chart_ids);
    ArrayResize(g_mqtt_pktid_chart_ids, idx + 1);
    ArrayResize(g_mqtt_pktid_next_ids, idx + 1);
    g_mqtt_pktid_chart_ids[idx] = chart_id;
    g_mqtt_pktid_next_ids[idx]  = 0;
  }

  ushort next_id = (ushort)(g_mqtt_pktid_next_ids[idx] + 1);
  if (next_id == 0) {
    next_id = 1;
  }
  g_mqtt_pktid_next_ids[idx] = next_id;
  return next_id;
}

ushort SetPacketIdentifierEx(uchar &buf[], int start_idx, CSessionDatabase *db) {
  //--- Ensure buffer has space for 2-byte packet identifier
  if (ArraySize(buf) < start_idx + 2) {
    ArrayResize(buf, start_idx + 2);
  }

  ushort packet_id = 0;

  if (db != NULL) {
    //--- Use session database for proper ID management
    packet_id = db.AllocatePacketId();
    if (packet_id == 0) {
      MQTT_LOG_ERROR("Failed to allocate packet ID from session database");
      return 0;
    }
  } else {
    //--- Fallback: maintain an independent wrapping counter per chart when no
    //--- session database is available (e.g. standalone builders in tests).
    packet_id = _MqttNextFallbackPacketId();
  }

  //--- Set the packet ID in buffer at the specified index
  buf[start_idx]     = (uchar)(packet_id >> 8);    // MSB
  buf[start_idx + 1] = (uchar)(packet_id & 0xFF);  // LSB

  return packet_id;
}

//+------------------------------------------------------------------+
//| Shared Property Encoding Helpers                                 |
//+------------------------------------------------------------------+
//| These functions consolidate the repeated property encoding logic |
//| used by Disconnect, Auth, Connect, Subscribe, and Unsubscribe    |
//| packet classes. Each helper appends a fully encoded property     |
//| (identifier byte + encoded data) to the destination buffer.      |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| AppendUserProperty                                               |
//| Purpose: Append a User Property (0x26) to a properties buffer    |
//| Parameters: dest - properties buffer to append to                |
//|             key  - property key string                           |
//|             val  - property value string                         |
//| Return: New size of dest buffer                                  |
//+------------------------------------------------------------------+
uint AppendUserProperty(uchar &dest[], const string key, const string val) {
  //--- Encode key and value as UTF-8 strings
  uchar keyaux[], valaux[];
  if (!EncodeUTF8String(key, keyaux) || !EncodeUTF8String(val, valaux)) {
    return ArraySize(dest);
  }

  //--- Calculate current size and resize to accommodate [id][key][value]
  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 1 + ArraySize(keyaux) + ArraySize(valaux);
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);

  //--- Add property identifier
  dest[current_size] = MQTT_PROP_IDENTIFIER_USER_PROPERTY;

  //--- Copy key (after identifier) and value (after key)
  ArrayCopy(dest, keyaux, current_size + 1);
  ArrayCopy(dest, valaux, current_size + 1 + ArraySize(keyaux));

  return ArraySize(dest);
}

//+------------------------------------------------------------------+
//| AppendReasonString                                               |
//| Purpose: Append a Reason String (0x1F) to a properties buffer    |
//| Parameters: dest   - properties buffer to append to              |
//|             reason - human-readable reason string                |
//| Return: New size of dest buffer                                  |
//+------------------------------------------------------------------+
uint AppendReasonString(uchar &dest[], const string reason) {
  //--- Encode reason string as UTF-8
  uchar aux[];
  if (!EncodeUTF8String(reason, aux)) {
    return ArraySize(dest);
  }

  //--- Calculate current size and resize to accommodate [id][string]
  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 1 + ArraySize(aux);
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);

  //--- Add property identifier
  dest[current_size] = MQTT_PROP_IDENTIFIER_REASON_STRING;

  //--- Copy encoded string
  ArrayCopy(dest, aux, current_size + 1);

  return ArraySize(dest);
}

//+------------------------------------------------------------------+
//| AppendServerReference                                            |
//| Purpose: Append a Server Reference (0x1C) to a properties buffer |
//| Parameters: dest       - properties buffer to append to          |
//|             server_ref - server reference string                 |
//| Return: New size of dest buffer                                  |
//+------------------------------------------------------------------+
uint AppendServerReference(uchar &dest[], const string server_ref) {
  //--- Encode server reference as UTF-8
  uchar aux[];
  if (!EncodeUTF8String(server_ref, aux)) {
    return ArraySize(dest);
  }

  //--- Calculate current size and resize to accommodate [id][string]
  uint current_size = ArraySize(dest);
  uint new_size     = current_size + 1 + ArraySize(aux);
  uint reserve      = (new_size > 64) ? (new_size / 2) : 64;
  ArrayResize(dest, new_size, reserve);

  //--- Add property identifier
  dest[current_size] = MQTT_PROP_IDENTIFIER_SERVER_REFERENCE;

  //--- Copy encoded string
  ArrayCopy(dest, aux, current_size + 1);

  return ArraySize(dest);
}

//+------------------------------------------------------------------+
//| SecureZeroArray                                                  |
//| Purpose: Overwrite a byte buffer with zeros and free it          |
//| Parameters: buf - buffer to zero and free                        |
//| Note: Used to scrub sensitive data (passwords, auth data) from   |
//|       memory before deallocation per security best practices.    |
//+------------------------------------------------------------------+
void SecureZeroArray(uchar &buf[]) {
  int size = ArraySize(buf);
  if (size > 0) {
    ArrayFill(buf, 0, size, 0);
    ArrayFree(buf);
  }
}

//+------------------------------------------------------------------+
//| HandlePublishError                                               |
//| Purpose: Handle and log PUBLISH-related errors with reason code  |
//| Parameters: reason_code - MQTT reason code byte                  |
//| Note: Logs error description, can be extended for error handling |
//+------------------------------------------------------------------+
void HandlePublishError(uchar reason_code) { MQTT_LOG_ERROR("PUBLISH Error: " + MqttReasonCodeToString(reason_code)); }

//+------------------------------------------------------------------+
//| MqttReasonCodeToString                                           |
//| Purpose: Get human-readable description for MQTT reason code     |
//| Parameters: code - reason code byte                              |
//| Return: Descriptive string per spec §2.4                         |
//+------------------------------------------------------------------+
string MqttReasonCodeToString(uchar code) {
  switch (code) {
    case 0x00:
      return "Success / Normal disconnection";
    case 0x01:
      return "Granted QoS 1";
    case 0x02:
      return "Granted QoS 2";
    case 0x04:
      return "Disconnect with Will Message";
    case 0x10:
      return "No matching subscribers";
    case 0x11:
      return "No subscription existed";
    case 0x18:
      return "Continue authentication";
    case 0x19:
      return "Re-authenticate";
    case 0x80:
      return "Unspecified error";
    case 0x81:
      return "Malformed Packet";
    case 0x82:
      return "Protocol Error";
    case 0x83:
      return "Implementation specific error";
    case 0x84:
      return "Unsupported Protocol Version";
    case 0x85:
      return "Client Identifier not valid";
    case 0x86:
      return "Bad User Name or Password";
    case 0x87:
      return "Not authorized";
    case 0x88:
      return "Server unavailable";
    case 0x89:
      return "Server busy";
    case 0x8A:
      return "Banned";
    case 0x8B:
      return "Server shutting down";
    case 0x8C:
      return "Bad authentication method";
    case 0x8D:
      return "Keep Alive timeout";
    case 0x8E:
      return "Session taken over";
    case 0x8F:
      return "Topic Filter invalid";
    case 0x90:
      return "Topic Name invalid";
    case 0x91:
      return "Packet Identifier in use";
    case 0x92:
      return "Packet Identifier not found";
    case 0x93:
      return "Receive Maximum exceeded";
    case 0x94:
      return "Topic Alias invalid";
    case 0x95:
      return "Packet too large";
    case 0x96:
      return "Message rate too high";
    case 0x97:
      return "Quota exceeded";
    case 0x98:
      return "Administrative action";
    case 0x99:
      return "Payload format invalid";
    case 0x9A:
      return "Retain not supported";
    case 0x9B:
      return "QoS not supported";
    case 0x9C:
      return "Use another server";
    case 0x9D:
      return "Server moved";
    case 0x9E:
      return "Shared Subscriptions not supported";
    case 0x9F:
      return "Connection rate exceeded";
    case 0xA0:
      return "Maximum connect time";
    case 0xA1:
      return "Subscription Identifiers not supported";
    case 0xA2:
      return "Wildcard Subscriptions not supported";
    default:
      return "Unknown Reason Code (" + StringFormat("0x%02X", code) + ")";
  }
}

//+------------------------------------------------------------------+
//| Library Export Includes                                          |
//+------------------------------------------------------------------+
//| By including these here, a client EA or Script only needs to     |
//| #include <MQTT/MQTT.mqh> to access the entire library and        |
//| all of its classes natively.                                     |
//+------------------------------------------------------------------+
#include "Internal\Protocol\Auth.mqh"
#include "Internal\Connection\AutoReconnect.mqh"
#include "Internal\Queue\PublishQueue.mqh"
#include "Internal\Queue\PublishQueueCoordinator.mqh"
#include "Internal\Connection\ReconnectPolicy.mqh"
#include "Internal\Protocol\ConnAck.mqh"
#include "Internal\Protocol\Connect.mqh"
#include "Internal\Session\Context.mqh"
#include "Internal\Protocol\Disconnect.mqh"
#include "Internal\Transport\ITransport.mqh"
#include "Internal\Protocol\Publish.mqh"
#include "Internal\Client\MqttClient.mqh"
#include "Internal\Protocol\Pingreq.mqh"
#include "Internal\Protocol\Pingresp.mqh"
#include "Internal\Util\PropertyEncoder.mqh"
#include "Internal\Util\PropertyReader.mqh"
#include "Internal\Protocol\PubAck.mqh"
#include "Internal\Protocol\PubComp.mqh"
#include "Internal\Protocol\PubRec.mqh"
#include "Internal\Protocol\PubRel.mqh"
#include "Internal\Session\RetransmissionManager.mqh"
#include "Internal\Protocol\SubAck.mqh"
#include "Internal\Protocol\Subscribe.mqh"
#include "Internal\Util\TopicMatcher.mqh"
#include "Internal\Transport\Transport.mqh"
#include "Internal\Protocol\UnsubAck.mqh"
#include "Internal\Protocol\Unsubscribe.mqh"
#include "Internal\Transport\WebSocketTransport.mqh"

#endif  // MQTT_MQH
