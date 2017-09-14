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

package org.apache.spark.unsafe.memory;

import org.apache.spark.unsafe.Platform;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import sun.nio.ch.DirectBuffer;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

  private static Method bufAddrMethod;
  static {
    try {
      Class cb = UnsafeMemoryAllocator.class.getClassLoader().loadClass("java.nio.DirectByteBuffer");
      bufAddrMethod = cb.getMethod("address");
      bufAddrMethod.setAccessible(true);
    }
    catch(Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  @Override
  public OffHeapMemoryBlock allocate(long size) throws OutOfMemoryError {
    Object buffer = ByteBuffer.allocateDirect((int)size);
    if (buffer instanceof DirectBuffer) {
      long addr = ((DirectBuffer) buffer).address();
      return new OffHeapMemoryBlock(buffer, addr, size);
    }
    throw new UnsupportedOperationException("A ByteBuffer does not have an address in off-heap");
  }

  @Override
  public void free(MemoryBlock memory) {
    assert(memory instanceof OffHeapMemoryBlock);
    assert (memory.getBaseObject() == null) :
      "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
    assert (memory.getPageNumber() != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    assert ((memory.getPageNumber() == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.getPageNumber() == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must be freed via TMM.freePage(), not directly in allocator free()";

    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }

    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to reset its pointer.
    ((OffHeapMemoryBlock)memory).setBaseOffset(0);
    // Mark the page as freed (so we can detect double-frees).
    memory.setPageNumber(MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER);

    // DirectByteBuffers are deallocated automatically by JVM when they become
    // unreachable much like normal Objects in heap
  }

  public OffHeapMemoryBlock reallocate(OffHeapMemoryBlock block, long oldSize, long newSize) {
    OffHeapMemoryBlock mb = this.allocate(newSize);
    if (block.getBaseOffset() != 0)
      MemoryBlock.copyMemory(block, block.getBaseOffset(), mb, mb.getBaseOffset(), oldSize);

    return mb;
  }
}
