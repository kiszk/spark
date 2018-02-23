/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.unsafe.Platform;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
  private byte[] buffer;
  private int offset;
  private int bitOffset; // Only used for booleans.
  private byte booleanData;
  private ByteBuffer byteBuffer; // used to wrap the byte array buffer

  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  public VectorizedPlainValuesReader() {
  }

  @Override
  public void initFromPage(int valueCount, byte[] bytes, int offset) throws IOException {
    this.buffer = bytes;
    this.offset = offset + Platform.BYTE_ARRAY_OFFSET;
    if (bigEndianPlatform) {
      byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void readBooleans(int total, WritableColumnVector c, int rowId) {
    int index = 0;
    if (total >= 8 && ((bitOffset % 8) == 0)) {
      for (; index + 8 <= total; index += 8) {
        byte b = Platform.getByte(buffer, offset);
        boolean b0 = (b & 0x01) != 0;
        boolean b1 = (b & 0x02) != 0;
        boolean b2 = (b & 0x04) != 0;
        boolean b3 = (b & 0x08) != 0;
        boolean b4 = (b & 0x10) != 0;
        boolean b5 = (b & 0x20) != 0;
        boolean b6 = (b & 0x40) != 0;
        boolean b7 = (b & 0x80) != 0;
        int row = rowId + index;
        c.putBoolean(row, b0);
        c.putBoolean(row + 1, b1);
        c.putBoolean(row + 2, b2);
        c.putBoolean(row + 3, b3);
        c.putBoolean(row + 4, b4);
        c.putBoolean(row + 5, b5);
        c.putBoolean(row + 6, b6);
        c.putBoolean(row + 7, b7);
        offset++;
      }
    }
    for (; index < total; index++) {
      c.putBoolean(rowId + index, readBoolean());
    }
  }

  @Override
  public final void readIntegers(int total, WritableColumnVector c, int rowId) {
    c.putIntsLittleEndian(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 4 * total;
  }

  @Override
  public final void readLongs(int total, WritableColumnVector c, int rowId) {
    c.putLongsLittleEndian(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 8 * total;
  }

  @Override
  public final void readFloats(int total, WritableColumnVector c, int rowId) {
    c.putFloats(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 4 * total;
  }

  @Override
  public final void readDoubles(int total, WritableColumnVector c, int rowId) {
    c.putDoubles(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 8 * total;
  }

  @Override
  public final void readBytes(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      // Bytes are stored as a 4-byte little endian int. Just read the first byte.
      // TODO: consider pushing this in ColumnVector by adding a readBytes with a stride.
      c.putByte(rowId + i, Platform.getByte(buffer, offset));
      offset += 4;
    }
  }

  @Override
  public final boolean readBoolean() {
    if ((bitOffset % 8) == 0) {
      booleanData = Platform.getByte(buffer, offset);
      bitOffset = 0;
      offset++;
    }
    boolean v = (booleanData & (1 << bitOffset)) != 0;
    bitOffset++;
    return v;
  }

  @Override
  public final int readInteger() {
    int v = Platform.getInt(buffer, offset);
    if (bigEndianPlatform) {
      v = java.lang.Integer.reverseBytes(v);
    }
    offset += 4;
    return v;
  }

  @Override
  public final long readLong() {
    long v = Platform.getLong(buffer, offset);
    if (bigEndianPlatform) {
      v = java.lang.Long.reverseBytes(v);
    }
    offset += 8;
    return v;
  }

  @Override
  public final byte readByte() {
    return (byte)readInteger();
  }

  @Override
  public final float readFloat() {
    float v;
    if (!bigEndianPlatform) {
      v = Platform.getFloat(buffer, offset);
    } else {
      v = byteBuffer.getFloat(offset - Platform.BYTE_ARRAY_OFFSET);
    }
    offset += 4;
    return v;
  }

  @Override
  public final double readDouble() {
    double v;
    if (!bigEndianPlatform) {
      v = Platform.getDouble(buffer, offset);
    } else {
      v = byteBuffer.getDouble(offset - Platform.BYTE_ARRAY_OFFSET);
    }
    offset += 8;
    return v;
  }

  @Override
  public final void readBinary(int total, WritableColumnVector v, int rowId) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      int start = offset;
      offset += len;
      v.putByteArray(rowId + i, buffer, start - Platform.BYTE_ARRAY_OFFSET, len);
    }
  }

  @Override
  public final Binary readBinary(int len) {
    Binary result = Binary.fromConstantByteArray(buffer, offset - Platform.BYTE_ARRAY_OFFSET, len);
    offset += len;
    return result;
  }
}
