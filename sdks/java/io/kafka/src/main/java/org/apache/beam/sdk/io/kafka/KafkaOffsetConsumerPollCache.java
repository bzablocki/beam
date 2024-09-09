/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaOffsetConsumerPollCache {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetConsumerPollCache.class);

  private static class OffsetCacheKey {
    final KafkaSourceDescriptor descriptor;

    OffsetCacheKey(KafkaSourceDescriptor descriptor) {
      this.descriptor = descriptor;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null) {
        return false;
      }
      if (!(other instanceof OffsetCacheKey)) {
        return false;
      }
      OffsetCacheKey otherKey = (OffsetCacheKey) other;
      return descriptor.equals(otherKey.descriptor);
    }

    @Override
    public int hashCode() {
      return Objects.hash(descriptor);
    }
  }


  private static class CacheOffsetEntry {

    final Consumer<byte[], byte[]> consumer;

    private CacheOffsetEntry(Consumer<byte[], byte[]> consumer) {
      this.consumer = consumer;
    }
  }

  private final Duration offsetCacheDuration = Duration.ofMinutes(10);
  private final Cache<OffsetCacheKey, CacheOffsetEntry> offsetCache;

  @SuppressWarnings("method.invocation")
  KafkaOffsetConsumerPollCache() {
    this.offsetCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(offsetCacheDuration.toMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<OffsetCacheKey, CacheOffsetEntry> notification) -> {
                  if (notification.getCause() != RemovalCause.EXPLICIT) {
                    LOG.info(
                        "bzablockilogkafka Asynchronously closing offset reader for {} as it has been idle for over {}",
                        notification.getKey(),
                        offsetCacheDuration);
                    asyncCloseOffsetConsumer(
                        checkNotNull(notification.getKey()), checkNotNull(notification.getValue()));
                  }
                })
            .build();
  }


  Consumer<byte[], byte[]> acquireOffsetTrackerConsumer(
      Map<String, Object> consumerConfig,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
      KafkaSourceDescriptor kafkaSourceDescriptor) {
    OffsetCacheKey key = new OffsetCacheKey(kafkaSourceDescriptor);
    try {
      LOG.info("bzablockilogkafka Get offset consumer for {}", key.descriptor.getTopicPartition());
      CacheOffsetEntry entry =
          offsetCache.get(
              key,
              () -> {
                LOG.info(
                    "bzablockilogkafka creating a new offset consumer {}",
                    key.descriptor.getTopicPartition());
                Consumer<byte[], byte[]> offsetConsumer = consumerFactoryFn.apply(consumerConfig);
                ConsumerSpEL.evaluateAssign(
                    offsetConsumer, ImmutableList.of(kafkaSourceDescriptor.getTopicPartition()));
                return new CacheOffsetEntry(offsetConsumer);
              });
      return entry.consumer;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /** Close the reader and log a warning if close fails. */
  private void asyncCloseOffsetConsumer(OffsetCacheKey key, CacheOffsetEntry entry) {
    LOG.info(
        "bzablockilogkafka closing offset consumer for {}",
        key.descriptor.getTopicPartition());
    entry.consumer.close();
  }


}
