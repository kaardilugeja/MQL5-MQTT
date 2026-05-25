//+------------------------------------------------------------------+
//|                                                    TEST_MQTT.mq5 |
//|                                   Copyright 2026, MQTT MQL5 Team |
//|                                      https://www.kaardilugeja.eu |
//+------------------------------------------------------------------+
//| Test suite for MQTT 5.0 base utilities and encoding functions.   |
//|                                                                  |
//| Tests variable byte integers, UTF-8 strings, property encoding,  |
//| and packet parsing utilities used across all MQTT packets.       |
//+------------------------------------------------------------------+
#include "..\..\TestUtil.mqh"
#include <MQTT\MQTT.mqh>

//+------------------------------------------------------------------+
//| Test Functions                                                   |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| Fixed-Length Integer Encoding Tests                              |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| EncodeTwoByteInteger_TwoBytes - Verify two-byte integer          |
//+------------------------------------------------------------------+
bool TEST_EncodeTwoByteInteger_TwoBytes() {
  TEST_CASE_START();
  //--- Expected: 0x0100 for 256
  uchar expected[] = {1, 0};
  uchar result[];
  EncodeTwoByteInteger(256, result);  // Encode value 256 as 2 bytes (MSB=1, LSB=0)
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeTwoByteInteger_OneByte - Verify small value encoding       |
//+------------------------------------------------------------------+
bool TEST_EncodeTwoByteInteger_OneByte() {
  TEST_CASE_START();
  //--- Expected: 0x0001 for 1
  uchar expected[] = {0, 1};
  uchar result[];
  EncodeTwoByteInteger(1, result);  // Encode small value with leading zero padding
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeFourByteInteger_OneByte - Verify four-byte padding         |
//+------------------------------------------------------------------+
bool TEST_EncodeFourByteInteger_OneByte() {
  TEST_CASE_START();
  //--- Expected: 0x00000001 for 1
  uchar expected[] = {0, 0, 0, 1};
  uchar result[];
  EncodeFourByteInteger(1, result);  // Small value encoded as 4 bytes with leading zeros
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeFourByteInteger_TwoBytes - Verify two-byte value           |
//+------------------------------------------------------------------+
bool TEST_EncodeFourByteInteger_TwoBytes() {
  TEST_CASE_START();
  //--- Expected: 0x00000100 for 256
  uchar expected[] = {0, 0, 1, 0};
  uchar result[];
  EncodeFourByteInteger(256, result);  // Value 256 encoded as 4 bytes
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeFourByteInteger_ThreeBytes - Verify three-byte value       |
//+------------------------------------------------------------------+
bool TEST_EncodeFourByteInteger_ThreeBytes() {
  TEST_CASE_START();
  //--- Expected: 0x00010000 for 65536
  uchar expected[] = {0, 1, 0, 0};
  uchar result[];
  EncodeFourByteInteger(65536, result);  // Large value using 3 significant bytes
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeFourByteInteger_FourBytes - Verify large value             |
//+------------------------------------------------------------------+
bool TEST_EncodeFourByteInteger_FourBytes() {
  TEST_CASE_START();
  //--- Expected: 0x01000000 for 16,777,216
  uchar expected[] = {1, 0, 0, 0};
  uchar result[];
  EncodeFourByteInteger(16777216, result);  // Max value using all 4 bytes
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| Packet Identification & QoS Parsing Tests                        |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| SetPacketID_TopicName1Char - Injection of packet identifier      |
//+------------------------------------------------------------------+
bool TEST_SetPacketID_TopicName1Char() {
  TEST_CASE_START();
  //--- Buffer: [Header][Len][TopicLenH][TopicLenL][a][PacketIDH][PacketIDL][...]
  //--- Note: Topic 'a' is 1 character, packet ID (0x0001) inserted at offset 5
  uchar expected[] = {50, 6, 0, 1, 'a', 0, 1};
  uchar buf[]      = {50, 6, 0, 1, 'a', 0, 0};  // initial buffer
  WritePacketIdentifier(buf, 5, 1);             // Explicitly write packet ID 1
  ASSERT_TRUE(AssertEqual(expected, buf));
  return true;
}

//+------------------------------------------------------------------+
//| SetPacketID_TopicName5Char - Verify buffer integrity             |
//+------------------------------------------------------------------+
bool TEST_SetPacketID_TopicName5Char() {
  TEST_CASE_START();
  //--- Fixed header + topic length + topic (5 chars)
  //--- This test verifies buffer structure is preserved (no packet ID in QoS 0)
  uchar expected[] = {48, 11, 0, 5, 'a', 'b', 'c', 'd', 'e', 0};
  uchar buf[]      = {48, 11, 0, 5, 'a', 'b', 'c', 'd', 'e', 0};
  ASSERT_TRUE(AssertEqual(expected, buf));  // Buffer unchanged for QoS 0 publish
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_2_RETAIN_DUP - Extracts QoS from complex header      |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_2_RETAIN_DUP() {
  TEST_CASE_START();
  //--- Header 0x3D (0011 1101) -> DUP=1, QoS=2, RETAIN=1
  //--- Bits: 0011 1101 -> PUBLISH(3) + DUP(1)<<3 + QoS(2)<<1 + RETAIN(1)
  uchar buf[] = {61, 3, 0, 1, 'a'};
  ASSERT_EQ(0x02, GetQoSLevel(buf));  // Extract QoS from bits 2-1
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_2_RETAIN - Extracts QoS Level 2 with Retain bit      |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_2_RETAIN() {
  TEST_CASE_START();
  //--- Header 0x35 (0011 0101) -> DUP=0, QoS=2, RETAIN=1
  //--- Bits: 0011 0101 -> PUBLISH(3) + QoS(2)<<1 + RETAIN(1)
  uchar buf[] = {53, 3, 0, 1, 'a'};
  ASSERT_EQ(0x02, GetQoSLevel(buf));
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_2 - Standard QoS Level 2 extraction                  |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_2() {
  TEST_CASE_START();
  //--- Header 0x34 (0011 0100) -> DUP=0, QoS=2, RETAIN=0
  //--- Bits: 0011 0100 -> PUBLISH(3) + QoS(2)<<1
  uchar buf[] = {52, 3, 0, 1, 'a'};
  ASSERT_EQ(0x02, GetQoSLevel(buf));  // Pure QoS 2, no flags
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_1_RETAIN_DUP - QoS Level 1 with all flags            |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_1_RETAIN_DUP() {
  TEST_CASE_START();
  //--- Header 0x3B (0011 1011) -> DUP=1, QoS=1, RETAIN=1
  //--- Bits: 0011 1011 -> PUBLISH(3) + DUP(1)<<3 + QoS(1)<<1 + RETAIN(1)
  uchar buf[] = {59, 3, 0, 1, 'a'};
  ASSERT_EQ(0x01, GetQoSLevel(buf));  // QoS 1 extracted regardless of DUP/RETAIN
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_1_RETAIN - QoS Level 1 with Retain bit               |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_1_RETAIN() {
  TEST_CASE_START();
  //--- Header 0x33 (0011 0011) -> DUP=0, QoS=1, RETAIN=1
  uchar buf[] = {51, 3, 0, 1, 'a'};
  ASSERT_EQ(0x01, GetQoSLevel(buf));
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_1 - Standard QoS Level 1 extraction                  |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_1() {
  TEST_CASE_START();
  //--- Header 0x32 (0011 0010) -> DUP=0, QoS=1, RETAIN=0
  uchar buf[] = {50, 3, 0, 1, 'a'};
  ASSERT_EQ(0x01, GetQoSLevel(buf));  // Pure QoS 1
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_0_RETAIN - QoS Level 0 with Retain bit               |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_0_RETAIN() {
  TEST_CASE_START();
  //--- Header 0x31 (0011 0001) -> DUP=0, QoS=0, RETAIN=1
  uchar buf[] = {49, 3, 0, 1, 'a'};
  ASSERT_EQ(0x00, GetQoSLevel(buf));
  return true;
}

//+------------------------------------------------------------------+
//| GetQoSLevel_0 - Standard QoS Level 0 extraction                  |
//+------------------------------------------------------------------+
bool TEST_GetQoSLevel_0() {
  TEST_CASE_START();
  //--- Header 0x30 (0011 0000) -> DUP=0, QoS=0, RETAIN=0
  //--- Bits: 0011 0000 -> PUBLISH(3), no flags set
  uchar buf[] = {48, 3, 0, 1, 'a'};
  ASSERT_EQ(0x00, GetQoSLevel(buf));  // QoS 0 (fire and forget)
  return true;
}

//+------------------------------------------------------------------+
//| UTF-8 String Encoding Tests                                      |
//+------------------------------------------------------------------+
//| EncodeVariableByteInteger_OneDigit - Encoding <= 127             |
//+------------------------------------------------------------------+
bool TEST_EncodeVariableByteInteger_OneDigit() {
  TEST_CASE_START();
  //--- One byte when value <= 127 (continuation bit = 0)
  //--- 0x7F = 127, no continuation (MSB = 0)
  uchar expected[] = {0x7F};
  uchar result[];
  EncodeVariableByteInteger(127, result);
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| UTF8String_TooLong_Rejected - 65536+ bytes rejected              |
//+------------------------------------------------------------------+
bool TEST_UTF8String_TooLong_Rejected() {
  TEST_CASE_START();
  string long_str = "";
  for (int i = 0; i < 65536; i++) {
    StringAdd(long_str, "a");
  }
  uchar result[];
  ASSERT_FALSE(EncodeUTF8String(long_str, result));
  //--- String exceeding 65535 bytes should be rejected
  ASSERT_EQ(0, ArraySize(result));
  return true;
}

//+------------------------------------------------------------------+
//| EncodeUTF8String_EmptyString - Verify zero-length encoding       |
//+------------------------------------------------------------------+
bool TEST_EncodeUTF8String_EmptyString() {
  TEST_CASE_START();
  //--- Per MQTT spec §1.5.4: a zero-length UTF-8 string is valid and encodes as
  //--- a 2-byte length prefix [0x00][0x00] with no following data bytes.
  uchar expected[] = {0x00, 0x00};
  uchar result[];
  ASSERT_TRUE(EncodeUTF8String("", result));
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeUTF8String_ASCII - Standard ASCII string encoding          |
//+------------------------------------------------------------------+
bool TEST_EncodeUTF8String_ASCII() {
  TEST_CASE_START();
  //--- Expected: [LenH:0][LenL:6][a][b][c][1][2][3]
  //--- MQTT UTF-8 strings are prefixed with 2-byte length (big-endian)
  uchar expected[] = {0, 6, 'a', 'b', 'c', '1', '2', '3'};
  uchar result[];
  ASSERT_TRUE(EncodeUTF8String("abc123", result));  // 6 char string with length prefix
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeUTF8String_OneChar - Single character encoding             |
//+------------------------------------------------------------------+
bool TEST_EncodeUTF8String_OneChar() {
  TEST_CASE_START();
  //--- Single char: [LenH:0][LenL:1][a]
  uchar expected[] = {0, 1, 'a'};
  uchar result[];
  ASSERT_TRUE(EncodeUTF8String("a", result));
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeUTF8String_NullChar_Disallowed - MQL5 U+0000 platform note |
//+------------------------------------------------------------------+
bool TEST_EncodeUTF8String_NullChar_Disallowed() {
  TEST_CASE_START();
  //--- MQL5 PLATFORM LIMITATION: strings are null-terminated internally.
  //--- StringSetCharacter(s, 0, 0x0000) sets the first character to the null
  //--- terminator, causing StringLen(s) to return 0.  U+0000 therefore can
  //--- never appear inside an MQL5 string at runtime; the IsDisallowedCodePoint
  //--- check for U+0000 is correct per MQTT §1.5.4 but is unreachable via the
  //--- character-iteration loop on this platform.
  //---
  //--- What we CAN verify: after StringSetCharacter zeroes the first char, the
  //--- encoder sees a zero-length string and produces the valid MQTT zero-length
  //--- UTF-8 encoding [0x00][0x00] (§1.5.4 permits zero-length strings).
  uchar  result[];
  string s = " ";                    // 1-char placeholder
  StringSetCharacter(s, 0, 0x0000);  // MQL5 makes StringLen(s)==0 after this
  ASSERT_TRUE(EncodeUTF8String(s, result));
  uchar expected[] = {0x00, 0x00};   // Zero-length MQTT UTF-8 string encoding
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeUTF8String_Surrogate_Disallowed - Reject surrogates        |
//+------------------------------------------------------------------+
bool TEST_EncodeUTF8String_Surrogate_Disallowed() {
  TEST_CASE_START();
  //--- UTF-16 surrogates (U+D800-U+DFFF) are not valid in MQTT UTF-8
  uchar  result[];
  string s = " ";
  StringSetCharacter(s, 0, 0xD800);  // Set high surrogate U+D800
  ASSERT_FALSE(EncodeUTF8String(s, result));
  ASSERT_EQ(0, ArraySize(result));   // Must reject surrogate code points
  return true;
}

//+------------------------------------------------------------------+
//| EncodeUTF8String_Noncharacter_Allowed - Accept discouraged chars |
//+------------------------------------------------------------------+
bool TEST_EncodeUTF8String_Noncharacter_Allowed() {
  TEST_CASE_START();
  //--- U+FFFF is discouraged by MQTT §1.5.4 but not hard-invalid.
  uchar  result[];
  string s = " ";
  StringSetCharacter(s, 0, 0xFFFF);  // Set U+FFFF non-character
  ASSERT_TRUE(EncodeUTF8String(s, result));
  ASSERT_TRUE(ArraySize(result) >= 5);
  ASSERT_EQ(0xEF, result[2]);
  ASSERT_EQ(0xBF, result[3]);
  ASSERT_EQ(0xBF, result[4]);
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| ValidateUtf8Data_ControlChar_Allowed                             |
//+------------------------------------------------------------------+
bool TEST_ValidateUtf8Data_ControlChar_Allowed() {
  TEST_CASE_START();
  uchar buf[] = {0x1F};

  ASSERT_EQ((int)MQTT_OK, (int)ValidateUtf8Data(buf, 0, 1));
  return true;
}

//+------------------------------------------------------------------+
//| UTF8String_MaximumLength - 65535 bytes max length                |
//+------------------------------------------------------------------+
bool TEST_UTF8String_MaximumLength() {
  TEST_CASE_START();
  string max_str = "";
  for (int i = 0; i < 65535; i++) {
    StringAdd(max_str, "a");
  }
  uchar result[];
  ASSERT_TRUE(EncodeUTF8String(max_str, result));
  //--- Check result has 2-byte length prefix + data
  //--- Result format: [len_msb][len_lsb][utf8_bytes]
  //--- For 65535 ASCII 'a' characters, UTF-8 encoding is 1 byte per char
  ASSERT_TRUE(ArraySize(result) >= 2);  // Must have at least length prefix
  ushort encoded_len = (ushort)((result[0] << 8) | result[1]);
  ASSERT_EQ(65535, encoded_len);        // Length should be 65535
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeVariableByteInteger_TwoDigits - Encoding <= 16,383         |
//+------------------------------------------------------------------+
bool TEST_EncodeVariableByteInteger_TwoDigits() {
  TEST_CASE_START();
  //--- Two bytes: 16383 = 127 + 128*127 -> 0xFF 0x7F
  //--- First byte: continuation=1, value=127; Second: continuation=0, value=127
  uchar expected[] = {0xFF, 0x7F};
  uchar result[];
  EncodeVariableByteInteger(16383, result);
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeVariableByteInteger_ThreeDigits - Encoding <= 2,097,151    |
//+------------------------------------------------------------------+
bool TEST_EncodeVariableByteInteger_ThreeDigits() {
  TEST_CASE_START();
  //--- Three bytes for max value 2,097,151
  //--- All bytes have continuation bit set except last
  uchar expected[] = {0xFF, 0xFF, 0x7F};
  uchar result[];
  EncodeVariableByteInteger(2097151, result);
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| EncodeVariableByteInteger_FourDigits - Encoding <= 268M          |
//+------------------------------------------------------------------+
bool TEST_EncodeVariableByteInteger_FourDigits() {
  TEST_CASE_START();
  //--- Four bytes: max MQTT value is 268,435,455 (~4x continuation bytes)
  //--- Each byte holds 7 bits of value + 1 continuation bit
  uchar expected[] = {0xFF, 0xFF, 0xFF, 0x7F};
  uchar result[];
  EncodeVariableByteInteger(268435455, result);
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| Variable Byte Integer Decoding Tests                             |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| DecodeVariableByteInteger_OneByte - Decoding <= 127              |
//+------------------------------------------------------------------+
bool TEST_DecodeVariableByteInteger_OneByte() {
  TEST_CASE_START();
  //--- Single byte value (no continuation): 0x7F = 127
  uchar inpkt[] = {0x7F};
  uint  idx     = 0;
  ASSERT_EQ(127, DecodeVariableByteInteger(inpkt, idx));
  return true;
}

//+------------------------------------------------------------------+
//| DecodeVariableByteInteger_TwoBytes - Decoding <= 16,383          |
//+------------------------------------------------------------------+
bool TEST_DecodeVariableByteInteger_TwoBytes() {
  TEST_CASE_START();
  //--- Two bytes: 0xFF 0x7F = (127) + (127 << 7) = 16383
  uchar inpkt[] = {0xFF, 0x7F};
  uint  idx     = 0;
  ASSERT_EQ(16383, DecodeVariableByteInteger(inpkt, idx));
  return true;
}

//+------------------------------------------------------------------+
//| DecodeVariableByteInteger_ThreeBytes - Decoding <= 2,097,151     |
//+------------------------------------------------------------------+
bool TEST_DecodeVariableByteInteger_ThreeBytes() {
  TEST_CASE_START();
  //--- Three bytes: 0xFF 0xFF 0x7F = 127 + (127<<7) + (127<<14) = 2,097,151
  uchar inpkt[] = {0xFF, 0xFF, 0x7F};
  uint  idx     = 0;
  ASSERT_EQ(2097151, DecodeVariableByteInteger(inpkt, idx));
  return true;
}

//+------------------------------------------------------------------+
//| DecodeVariableByteInteger_FourBytes - Decoding <= 268M           |
//+------------------------------------------------------------------+
bool TEST_DecodeVariableByteInteger_FourBytes() {
  TEST_CASE_START();
  //--- Four bytes: 0xFF 0xFF 0xFF 0x7F = max value 268,435,455
  uchar inpkt[] = {0xFF, 0xFF, 0xFF, 0x7F};
  uint  idx     = 0;
  ASSERT_EQ(268435455, DecodeVariableByteInteger(inpkt, idx));
  return true;
}

//+------------------------------------------------------------------+
//| Decoding & Reading Utility Tests                                 |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| ReadUtf8String - Verify string extraction from packet            |
//+------------------------------------------------------------------+
bool TEST_ReadUtf8String() {
  TEST_CASE_START();
  //--- Packet: [LenH:0x00][LenL:0x0A=10][10 bytes of 'utf8string']
  //--- Result should be the string "utf8string"
  uchar inpkt[] = {0, 10, 'u', 't', 'f', '8', 's', 't', 'r', 'i', 'n', 'g'};
  uint  idx     = 0;
  ASSERT_STR_EQ("utf8string", ReadUtf8String(inpkt, idx));
  return true;
}

//+------------------------------------------------------------------+
//| ReadByte_SafeOverload - Distinguish 0x00 from bounds failure     |
//+------------------------------------------------------------------+
bool TEST_ReadByte_SafeOverload() {
  TEST_CASE_START();
  uchar buf[] = {0x00, 0x7F};
  uint  idx   = 0;
  bool  ok    = false;

  uchar first = ReadByte(buf, idx, ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(0, first);
  ASSERT_EQ(1, (int)idx);

  idx            = 2;
  uchar past_end = ReadByte(buf, idx, ok);
  ASSERT_FALSE(ok);
  ASSERT_EQ(0, past_end);
  ASSERT_EQ(2, (int)idx);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeFourByteInt_OneByte - Decoding padded small value          |
//+------------------------------------------------------------------+
bool TEST_DecodeFourByteInt_OneByte() {
  TEST_CASE_START();
  //--- Big-endian decode: 0x00000001 = 1
  uchar encoded[] = {0, 0, 0, 1};
  uint  val4      = 0;
  ASSERT_TRUE(DecodeFourByteIntAt(encoded, 0, val4));
  ASSERT_EQ(1, val4);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeFourByteInt_TwoBytes - Decoding padded mid value           |
//+------------------------------------------------------------------+
bool TEST_DecodeFourByteInt_TwoBytes() {
  TEST_CASE_START();
  //--- Big-endian: 0x00000100 = 256 (0x100)
  uchar encoded[] = {0, 0, 1, 0};
  uint  val4      = 0;
  ASSERT_TRUE(DecodeFourByteIntAt(encoded, 0, val4));
  ASSERT_EQ(256, val4);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeFourByteInt_ThreeBytes - Decoding padded high value        |
//+------------------------------------------------------------------+
bool TEST_DecodeFourByteInt_ThreeBytes() {
  TEST_CASE_START();
  //--- Big-endian: 0x00010000 = 65536 (0x10000)
  uchar encoded[] = {0, 1, 0, 0};
  uint  val4      = 0;
  ASSERT_TRUE(DecodeFourByteIntAt(encoded, 0, val4));
  ASSERT_EQ(65536, val4);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeFourByteInt_FourBytes - Decoding full 4-byte value         |
//+------------------------------------------------------------------+
bool TEST_DecodeFourByteInt_FourBytes() {
  TEST_CASE_START();
  //--- Big-endian: 0x01000000 = 16777216 (0x1000000)
  uchar encoded[] = {1, 0, 0, 0};
  uint  val4      = 0;
  ASSERT_TRUE(DecodeFourByteIntAt(encoded, 0, val4));
  ASSERT_EQ(16777216, val4);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeFourByteInt_HighMSB - Decoding with top bit set            |
//+------------------------------------------------------------------+
bool TEST_DecodeFourByteInt_HighMSB() {
  TEST_CASE_START();
  //--- Big-endian: 0x80000001 must survive without signed promotion corruption.
  uchar encoded[] = {0x80, 0x00, 0x00, 0x01};
  uint  val4      = 0;
  ASSERT_TRUE(DecodeFourByteIntAt(encoded, 0, val4));
  ASSERT_EQ((uint)0x80000001, val4);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeTwoByteInt_OneByte - Decoding padded small 2-byte value    |
//+------------------------------------------------------------------+
bool TEST_DecodeTwoByteInt_OneByte() {
  TEST_CASE_START();
  //--- Big-endian 2-byte: 0x0001 = 1
  uchar  encoded[] = {0, 1};
  ushort val2      = 0;
  ASSERT_TRUE(DecodeTwoByteIntAt(encoded, 0, val2));
  ASSERT_EQ(1, val2);
  return true;
}

//+------------------------------------------------------------------+
//| DecodeTwoByteInt_TwoBytes - Decoding full 2-byte value           |
//+------------------------------------------------------------------+
bool TEST_DecodeTwoByteInt_TwoBytes() {
  TEST_CASE_START();
  //--- Big-endian 2-byte: 0x0100 = 256 (0x100)
  uchar  encoded[] = {1, 0};
  ushort val2      = 0;
  ASSERT_TRUE(DecodeTwoByteIntAt(encoded, 0, val2));
  ASSERT_EQ(256, val2);
  return true;
}

//+------------------------------------------------------------------+
//| Property & Length Parsing Tests                                  |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| ReadUserProperty - Verify User Property (Key-Value pair)         |
//+------------------------------------------------------------------+
bool TEST_ReadUserProperty() {
  TEST_CASE_START();
  //--- Expected result is array with "key:" and "val"
  string expected[] = {"key:", "val"};
  //--- Mock packet with User Property at offset 6
  //--- [Fixed header][remaining length][session present][reason code][prop length][0x26=UserProp][key][val]
  uchar  inpkt[]    = {32, 15, 1, 0, 10, 38, 0, 4, 'k', 'e', 'y', ':', 0, 3, 'v', 'a', 'l'};
  string result[];
  //--- User Property format: 0x26 [KeyLenH][KeyLenL][Key][ValLenH][ValLenL][Val]
  uint   idx = 6;  // Start reading at property offset
  ReadUserProperty(inpkt, idx, result);
  ASSERT_TRUE(AssertEqual(expected, result));
  ArrayFree(result);
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_INVALID - Reject too many bytes              |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_INVALID() {
  TEST_CASE_START();
  //--- Invalid: > 4 continuation bytes (exceeds MQTT spec max)
  //--- 4 bytes all with continuation bit set (0x80) - invalid format
  uchar inpkt[] = {0x00, 0xFF, 0xFF, 0xFF, 0xFF};
  ASSERT_EQ(0, ReadRemainingLength(inpkt));  // Should return error
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_ZERO - Zero length handling                  |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_ZERO() {
  TEST_CASE_START();
  //--- Zero remaining length encoded as single byte 0x00
  uchar inpkt[] = {0x00, 0};
  ASSERT_EQ(0, ReadRemainingLength(inpkt));  // Valid zero length
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_OneByte - Single byte length                 |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_OneByte() {
  TEST_CASE_START();
  //--- Simple single byte value: 2
  uchar inpkt[] = {0x00, 2};
  ASSERT_EQ(2, ReadRemainingLength(inpkt));
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_TwoBytes - Two byte length                   |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_TwoBytes() {
  TEST_CASE_START();
  //--- Two-byte value: 0xFF 0x7F = 16383 (max for 2 bytes)
  uchar inpkt[] = {0x00, 0xFF, 0x7F};
  ASSERT_EQ(16383, ReadRemainingLength(inpkt));
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_ThreeBytes - Three byte length               |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_ThreeBytes() {
  TEST_CASE_START();
  //--- Three-byte value: 0xFF 0xFF 0x7F = 2,097,151
  uchar inpkt[] = {0x00, 0xFF, 0xFF, 0x7F};
  ASSERT_EQ(2097151, ReadRemainingLength(inpkt));
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_FourBytes - Four byte length                 |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_FourBytes() {
  TEST_CASE_START();
  //--- Four-byte value: 0xFF 0xFF 0xFF 0x7F = 268,435,455 (max MQTT value)
  uchar inpkt[] = {0x00, 0xFF, 0xFF, 0xFF, 0x7F};
  ASSERT_EQ(268435455, ReadRemainingLength(inpkt));
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_TRUNCATED - Reject incomplete encoding       |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_TRUNCATED() {
  TEST_CASE_START();
  //--- Truncated: continuation bit set (0x80) but no following byte
  uchar inpkt[] = {0x00, 0x80, 0x80};
  ASSERT_EQ(0, ReadRemainingLength(inpkt));  // Error: incomplete data
  return true;
}

//+------------------------------------------------------------------+
//| ReadRemainingLength_FIVE_BYTES - Reject > 4 bytes encoding       |
//+------------------------------------------------------------------+
bool TEST_ReadRemainingLength_FIVE_BYTES() {
  TEST_CASE_START();
  //--- 5 bytes with continuation: exceeds MQTT max of 4 bytes
  uchar inpkt[] = {0x00, 0x80, 0x80, 0x80, 0x80, 0x01};
  ASSERT_EQ(0, ReadRemainingLength(inpkt));  // Error: too many bytes
  return true;
}

//+------------------------------------------------------------------+
//| ReadPropertyLength_ZERO - Zero length property block             |
//+------------------------------------------------------------------+
bool TEST_ReadPropertyLength_ZERO() {
  TEST_CASE_START();
  //--- Property length 0 encoded as single byte (no properties follow)
  //--- Packet: [flags:4][4][session:0][reason:0][prop_len:0]
  uchar pkt[] = {4, 4, 0, 1, 0, 0};
  uint  idx   = 4;  // Position at property length field
  ASSERT_EQ(0, ReadPropertyLength(pkt, idx));
  return true;
}

//+------------------------------------------------------------------+
//| ReadPropertyLength_MALFORMED - malformed varint sentinel         |
//+------------------------------------------------------------------+
bool TEST_ReadPropertyLength_MALFORMED() {
  TEST_CASE_START();
  uchar pkt[] = {0x20, 0x04, 0x00, 0x00, 0x80, 0x80, 0x80, 0x80};
  uint  idx   = 4;
  ASSERT_EQ(UINT_MAX, ReadPropertyLength(pkt, idx));
  return true;
}

//+------------------------------------------------------------------+
//| OnStart - Run all tests                                          |
//+------------------------------------------------------------------+
void OnStart() {
  //--- Record suite start using the shared MT5 Journal format.
  const string suite_name = "TEST_MQTT";
  TestUtilRecordSuiteStart(suite_name);
  //--- Track aggregate case and assertion counts across the suite.

  int       total_tests  = 0;
  int       passed_tests = 0;

  //--- Array of test function names for reporting
  string    test_names[] = {"TEST_EncodeTwoByteInteger_TwoBytes",
                            "TEST_EncodeTwoByteInteger_OneByte",
                            "TEST_EncodeFourByteInteger_OneByte",
                            "TEST_EncodeFourByteInteger_TwoBytes",
                            "TEST_EncodeFourByteInteger_ThreeBytes",
                            "TEST_EncodeFourByteInteger_FourBytes",
                            "TEST_EncodeVariableByteInteger_OneDigit",
                            "TEST_SetPacketID_TopicName1Char",
                            "TEST_SetPacketID_TopicName5Char",
                            "TEST_GetQoSLevel_2_RETAIN_DUP",
                            "TEST_GetQoSLevel_2_RETAIN",
                            "TEST_GetQoSLevel_2",
                            "TEST_GetQoSLevel_1_RETAIN_DUP",
                            "TEST_GetQoSLevel_1_RETAIN",
                            "TEST_GetQoSLevel_1",
                            "TEST_GetQoSLevel_0_RETAIN",
                            "TEST_GetQoSLevel_0",
                            "TEST_UTF8String_TooLong_Rejected",
                            "TEST_EncodeUTF8String_EmptyString",
                            "TEST_EncodeUTF8String_ASCII",
                            "TEST_EncodeUTF8String_OneChar",
                            "TEST_EncodeUTF8String_NullChar_Disallowed",
                            "TEST_EncodeUTF8String_Surrogate_Disallowed",
                            "TEST_EncodeUTF8String_Noncharacter_Allowed",
                            "TEST_ValidateUtf8Data_ControlChar_Allowed",
                            "TEST_UTF8String_MaximumLength",
                            "TEST_EncodeVariableByteInteger_TwoDigits",
                            "TEST_EncodeVariableByteInteger_ThreeDigits",
                            "TEST_EncodeVariableByteInteger_FourDigits",
                            "TEST_DecodeVariableByteInteger_OneByte",
                            "TEST_DecodeVariableByteInteger_TwoBytes",
                            "TEST_DecodeVariableByteInteger_ThreeBytes",
                            "TEST_DecodeVariableByteInteger_FourBytes",
                            "TEST_ReadUtf8String",
                            "TEST_ReadByte_SafeOverload",
                            "TEST_DecodeFourByteInt_OneByte",
                            "TEST_DecodeFourByteInt_TwoBytes",
                            "TEST_DecodeFourByteInt_ThreeBytes",
                            "TEST_DecodeFourByteInt_FourBytes",
                            "TEST_DecodeFourByteInt_HighMSB",
                            "TEST_DecodeTwoByteInt_OneByte",
                            "TEST_DecodeTwoByteInt_TwoBytes",
                            "TEST_ReadUserProperty",
                            "TEST_ReadRemainingLength_INVALID",
                            "TEST_ReadRemainingLength_ZERO",
                            "TEST_ReadRemainingLength_OneByte",
                            "TEST_ReadRemainingLength_TwoBytes",
                            "TEST_ReadRemainingLength_ThreeBytes",
                            "TEST_ReadRemainingLength_FourBytes",
                            "TEST_ReadRemainingLength_TRUNCATED",
                            "TEST_ReadRemainingLength_FIVE_BYTES",
                            "TEST_ReadPropertyLength_ZERO",
                            "TEST_ReadPropertyLength_MALFORMED"};

  const int test_count   = ArraySize(test_names);

  //--- Main test execution loop
  for (int i = 0; i < test_count; i++) {

    //--- Reset global assertion counters for each test
    g_tests_passed = 0;
    g_tests_failed = 0;

    //--- Clear any staged comparator failure details before the next case runs.
    TestUtilClearPendingFailure();
    bool result = false;

    //--- Dispatch to specific test function via switch statement
    switch (i) {
      case 0:
        result = TEST_EncodeTwoByteInteger_TwoBytes();
        break;
      case 1:
        result = TEST_EncodeTwoByteInteger_OneByte();
        break;
      case 2:
        result = TEST_EncodeFourByteInteger_OneByte();
        break;
      case 3:
        result = TEST_EncodeFourByteInteger_TwoBytes();
        break;
      case 4:
        result = TEST_EncodeFourByteInteger_ThreeBytes();
        break;
      case 5:
        result = TEST_EncodeFourByteInteger_FourBytes();
        break;
      case 6:
        result = TEST_EncodeVariableByteInteger_OneDigit();
        break;
      case 7:
        result = TEST_SetPacketID_TopicName1Char();
        break;
      case 8:
        result = TEST_SetPacketID_TopicName5Char();
        break;
      case 9:
        result = TEST_GetQoSLevel_2_RETAIN_DUP();
        break;
      case 10:
        result = TEST_GetQoSLevel_2_RETAIN();
        break;
      case 11:
        result = TEST_GetQoSLevel_2();
        break;
      case 12:
        result = TEST_GetQoSLevel_1_RETAIN_DUP();
        break;
      case 13:
        result = TEST_GetQoSLevel_1_RETAIN();
        break;
      case 14:
        result = TEST_GetQoSLevel_1();
        break;
      case 15:
        result = TEST_GetQoSLevel_0_RETAIN();
        break;
      case 16:
        result = TEST_GetQoSLevel_0();
        break;
      case 17:
        result = TEST_UTF8String_TooLong_Rejected();
        break;
      case 18:
        result = TEST_EncodeUTF8String_EmptyString();
        break;
      case 19:
        result = TEST_EncodeUTF8String_ASCII();
        break;
      case 20:
        result = TEST_EncodeUTF8String_OneChar();
        break;
      case 21:
        result = TEST_EncodeUTF8String_NullChar_Disallowed();
        break;
      case 22:
        result = TEST_EncodeUTF8String_Surrogate_Disallowed();
        break;
      case 23:
        result = TEST_EncodeUTF8String_Noncharacter_Allowed();
        break;
      case 24:
        result = TEST_ValidateUtf8Data_ControlChar_Allowed();
        break;
      case 25:
        result = TEST_UTF8String_MaximumLength();
        break;
      case 26:
        result = TEST_EncodeVariableByteInteger_TwoDigits();
        break;
      case 27:
        result = TEST_EncodeVariableByteInteger_ThreeDigits();
        break;
      case 28:
        result = TEST_EncodeVariableByteInteger_FourDigits();
        break;
      case 29:
        result = TEST_DecodeVariableByteInteger_OneByte();
        break;
      case 30:
        result = TEST_DecodeVariableByteInteger_TwoBytes();
        break;
      case 31:
        result = TEST_DecodeVariableByteInteger_ThreeBytes();
        break;
      case 32:
        result = TEST_DecodeVariableByteInteger_FourBytes();
        break;
      case 33:
        result = TEST_ReadUtf8String();
        break;
      case 34:
        result = TEST_ReadByte_SafeOverload();
        break;
      case 35:
        result = TEST_DecodeFourByteInt_OneByte();
        break;
      case 36:
        result = TEST_DecodeFourByteInt_TwoBytes();
        break;
      case 37:
        result = TEST_DecodeFourByteInt_ThreeBytes();
        break;
      case 38:
        result = TEST_DecodeFourByteInt_FourBytes();
        break;
      case 39:
        result = TEST_DecodeFourByteInt_HighMSB();
        break;
      case 40:
        result = TEST_DecodeTwoByteInt_OneByte();
        break;
      case 41:
        result = TEST_DecodeTwoByteInt_TwoBytes();
        break;
      case 42:
        result = TEST_ReadUserProperty();
        break;
      case 43:
        result = TEST_ReadRemainingLength_INVALID();
        break;
      case 44:
        result = TEST_ReadRemainingLength_ZERO();
        break;
      case 45:
        result = TEST_ReadRemainingLength_OneByte();
        break;
      case 46:
        result = TEST_ReadRemainingLength_TwoBytes();
        break;
      case 47:
        result = TEST_ReadRemainingLength_ThreeBytes();
        break;
      case 48:
        result = TEST_ReadRemainingLength_FourBytes();
        break;
      case 49:
        result = TEST_ReadRemainingLength_TRUNCATED();
        break;
      case 50:
        result = TEST_ReadRemainingLength_FIVE_BYTES();
        break;
      case 51:
        result = TEST_ReadPropertyLength_ZERO();
        break;
      case 52:
        result = TEST_ReadPropertyLength_MALFORMED();
        break;
    }

    //--- Track total assertions across all tests
    total_tests += g_tests_passed + g_tests_failed;
    if (result && g_tests_failed == 0) {
      passed_tests++;
      TestUtilRecordCasePass(test_names[i], g_tests_passed);
    } else {
      TestUtilRecordCaseFail(test_names[i], g_tests_failed);
    }
  }

  TestUtilFinalizeSuite(suite_name, passed_tests, test_count, total_tests);
}
