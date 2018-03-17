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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.bitset.BitSetMethods;

/**
 * A helper class to write data into global row buffer using `UnsafeRow` format.
 *
 * It will remember the offset of row buffer which it starts to write, and move the cursor of row
 * buffer while writing.  If new data(can be the input record if this is the outermost writer, or
 * nested struct if this is an inner writer) comes, the starting cursor of row buffer may be
 * changed, so we need to call `UnsafeRowWriter.reset` before writing, to update the
 * `startingOffset` and clear out null bits.
 *
 * Note that if this is the outermost writer, which means we will always write from the very
 * beginning of the global row buffer, we don't need to update `startingOffset` and can just call
 * `zeroOutNullBytes` before writing new data.
 */
public final class UnsafeRowWriter extends UnsafeWriter {

  private final UnsafeRow row;

  private final int nullBitsSize;
  private final int fixedSize;

  public UnsafeRowWriter(UnsafeRow row, int initialBufferSize) {
    this(row, new BufferHolder(row, initialBufferSize), row.numFields());
  }

  public UnsafeRowWriter(UnsafeRow row) {
    this(row, new BufferHolder(row), row.numFields());
  }

  public UnsafeRowWriter(UnsafeWriter writer, int numFields) {
    this(null, writer.getBufferHolder(), numFields);
  }

  private UnsafeRowWriter(UnsafeRow row, BufferHolder holder, int numFields) {
    super(holder);
    this.row = row;
    this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
    this.fixedSize = nullBitsSize + 8 * numFields;
    this.startingOffset = cursor();
  }

  public void setTotalSize() {
    row.setTotalSize(totalSize());
  }

  /**
   * Resets the `startingOffset` according to the current cursor of row buffer, and clear out null
   * bits.  This should be called before we write a new nested struct to the row buffer.
   */
  public void resetRowWriter() {
    this.startingOffset = cursor();

    // grow the global buffer to make sure it has enough space to write fixed-length data.
    grow(fixedSize);
    addCursor(fixedSize);

    zeroOutNullBytes();
  }

  /**
   * Clears out null bits.  This should be called before we write a new row to row buffer.
   */
  public void zeroOutNullBytes() {
    for (int i = 0; i < nullBitsSize; i += 8) {
      Platform.putLong(buffer(), startingOffset + i, 0L);
    }
  }

  public boolean isNullAt(int ordinal) {
    return BitSetMethods.isSet(buffer(), startingOffset, ordinal);
  }

  public void setNullAt(int ordinal) {
    BitSetMethods.set(buffer(), startingOffset, ordinal);
    Platform.putLong(buffer(), getFieldOffset(ordinal), 0L);
  }

  @Override
  public void setNull1Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull2Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull4Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull8Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  protected final long getOffset(int oridinal, int elementSize) {
    return getFieldOffset(oridinal);
  }

  public long getFieldOffset(int ordinal) {
    return startingOffset + nullBitsSize + 8 * ordinal;
  }

  @Override
  public void setOffsetAndSize(int ordinal, int currentCursor, int size) {
    _setOffsetAndSize(ordinal, currentCursor, size);
  }

  public void write(int ordinal, boolean value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer(), offset, 0L);
    _write(offset, value);
  }

  public void write(int ordinal, byte value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer(), offset, 0L);
    _write(offset, value);
  }

  public void write(int ordinal, short value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer(), offset, 0L);
    _write(offset, value);
  }

  public void write(int ordinal, int value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer(), offset, 0L);
    _write(offset, value);
  }

  public void write(int ordinal, long value) {
    _write(getFieldOffset(ordinal), value);
  }

  public void write(int ordinal, float value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer(), offset, 0L);
    _write(offset, value);
  }

  public void write(int ordinal, double value) {
    _write(getFieldOffset(ordinal), value);
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // make sure Decimal object has the same scale as DecimalType
      if (input.changePrecision(precision, scale)) {
        write(ordinal, input.toUnscaledLong());
      } else {
        setNullAt(ordinal);
      }
    } else {
      // grow the global buffer before writing data.
      holder.grow(16);

      // Make sure Decimal object has the same scale as DecimalType.
      // Note that we may pass in null Decimal object to set null for it.
      if (input == null || !input.changePrecision(precision, scale)) {
        // zero-out the bytes
        Platform.putLong(buffer(), cursor(), 0L);
        Platform.putLong(buffer(), cursor() + 8, 0L);

        BitSetMethods.set(buffer(), startingOffset, ordinal);
        // keep the offset for future update
        setOffsetAndSize(ordinal, 0);
      } else {
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        final int numBytes = bytes.length;
        assert numBytes <= 16;

        zeroOutPaddingBytes(numBytes);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, buffer(), cursor(), numBytes);
        setOffsetAndSize(ordinal, bytes.length);
      }

      // move the cursor forward.
      addCursor(16);
    }
  }
}
