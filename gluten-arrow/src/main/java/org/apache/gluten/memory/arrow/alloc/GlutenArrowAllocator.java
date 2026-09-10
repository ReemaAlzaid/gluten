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
package org.apache.gluten.memory.arrow.alloc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Enumeration;

/**
 * Creates Arrow root allocators that remain compatible with the Netty version supplied by Spark.
 */
public final class GlutenArrowAllocator {
  private static final String ALLOCATION_MANAGER_TYPE_ENV = "ARROW_ALLOCATION_MANAGER_TYPE";
  private static final String ALLOCATION_MANAGER_TYPE_PROPERTY = "arrow.allocation.manager.type";
  private static final String DEFAULT_FACTORY_FIELD = "DEFAULT_ALLOCATION_MANAGER_FACTORY";
  private static final String[] FACTORY_RESOURCES = {
    "org/apache/arrow/memory/DefaultAllocationManagerFactory.class",
    "org/apache/arrow/memory/unsafe/DefaultAllocationManagerFactory.class",
    "org/apache/arrow/memory/netty/DefaultAllocationManagerFactory.class"
  };

  private static final AllocationManager.Factory NETTY_FACTORY =
      new AllocationManager.Factory() {
        @Override
        public AllocationManager create(BufferAllocator accountingAllocator, long size) {
          return new NettyAllocationManager(accountingAllocator, size);
        }

        @Override
        public ArrowBuf empty() {
          return new ArrowBuf(ReferenceManager.NO_OP, null, 0, 0);
        }
      };

  private GlutenArrowAllocator() {}

  public static RootAllocator createRootAllocator(AllocationListener listener, long limit) {
    if (!usesNettyAllocationManager()) {
      return new RootAllocator(listener, limit);
    }
    return createNettyRootAllocator(listener, limit);
  }

  static RootAllocator createNettyRootAllocator(AllocationListener listener, long limit) {
    installNettyAllocationManagerFactory();
    return new RootAllocator(listener, limit);
  }

  private static void installNettyAllocationManagerFactory() {
    try {
      Field factoryField =
          DefaultAllocationManagerOption.class.getDeclaredField(DEFAULT_FACTORY_FIELD);
      factoryField.setAccessible(true);
      synchronized (DefaultAllocationManagerOption.class) {
        factoryField.set(null, NETTY_FACTORY);
      }
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to configure Arrow's Netty allocation manager", e);
    }
  }

  private static boolean usesNettyAllocationManager() {
    String configuredType = System.getenv(ALLOCATION_MANAGER_TYPE_ENV);
    String propertyType = System.getProperty(ALLOCATION_MANAGER_TYPE_PROPERTY);
    if (propertyType != null) {
      configuredType = propertyType;
    }
    if ("Netty".equalsIgnoreCase(configuredType)) {
      return true;
    }
    if ("Unsafe".equalsIgnoreCase(configuredType)) {
      return false;
    }

    URL factory = findAllocationManagerFactory();
    return factory != null && factory.getPath().contains("memory-netty");
  }

  private static URL findAllocationManagerFactory() {
    ClassLoader loader = RootAllocator.class.getClassLoader();
    for (String resource : FACTORY_RESOURCES) {
      try {
        Enumeration<URL> factories =
            loader == null
                ? ClassLoader.getSystemResources(resource)
                : loader.getResources(resource);
        if (factories.hasMoreElements()) {
          return factories.nextElement();
        }
      } catch (IOException e) {
        throw new IllegalStateException("Unable to locate Arrow's allocation manager", e);
      }
    }
    return null;
  }

  private static final class NettyAllocationManager extends AllocationManager {
    private final ByteBuf buffer;
    private final long allocatedAddress;
    private final long allocatedSize;

    private NettyAllocationManager(BufferAllocator accountingAllocator, long requestedSize) {
      super(accountingAllocator);
      if (requestedSize > Integer.MAX_VALUE) {
        buffer = null;
        allocatedAddress = PlatformDependent.allocateMemory(requestedSize);
        allocatedSize = requestedSize;
        return;
      }

      ByteBuf allocated =
          PooledByteBufAllocator.DEFAULT.directBuffer((int) requestedSize, (int) requestedSize);
      try {
        if (!allocated.hasMemoryAddress()) {
          throw new UnsupportedOperationException(
              "Netty returned a buffer without a memory address");
        }
        allocatedAddress = allocated.memoryAddress();
        allocatedSize = allocated.capacity();
        buffer = allocated;
      } catch (Throwable t) {
        allocated.release();
        throw t;
      }
    }

    @Override
    public long getSize() {
      return allocatedSize;
    }

    @Override
    protected long memoryAddress() {
      return allocatedAddress;
    }

    @Override
    protected void release0() {
      if (buffer == null) {
        PlatformDependent.freeMemory(allocatedAddress);
      } else {
        buffer.release();
      }
    }
  }
}
