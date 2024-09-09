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

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaOffsetConsumerPollThreadCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(KafkaOffsetConsumerPollThreadCache.class);

  private final ExecutorService invalidationExecutor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaOffsetConsumerPollCache-invalidation-%d")
              .build());
  private final ExecutorService backgroundThreads =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaOffsetConsumerPollCache-poll-%d")
              .build());

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

    final KafkaOffsetConsumerPollThread offsetThread;

    private CacheOffsetEntry(KafkaOffsetConsumerPollThread thread) {
      this.offsetThread = thread;
    }
  }

  private final Duration offsetClientCacheDuration = Duration.ofMinutes(5);
  private final Cache<OffsetCacheKey, CacheOffsetEntry> offsetCache;

  @SuppressWarnings("method.invocation")
  KafkaOffsetConsumerPollThreadCache() {
    this.offsetCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(offsetClientCacheDuration.toMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<OffsetCacheKey, CacheOffsetEntry> notification) -> {
                  if (notification.getCause() != RemovalCause.EXPLICIT) {
                    LOG.info(
                        "bzablockilogkafka Asynchronously closing offset reader for {} as it has been idle for over {}",
                        notification.getKey(),
                        offsetClientCacheDuration);
                    asyncCloseOffsetConsumer(
                        checkNotNull(notification.getKey()), checkNotNull(notification.getValue()));
                  }
                })
            .build();
  }

  KafkaOffsetConsumerPollThread acquireOffsetTrackerConsumer(
      Map<String, Object> consumerConfig,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
      KafkaSourceDescriptor kafkaSourceDescriptor) {
    OffsetCacheKey key = new OffsetCacheKey(kafkaSourceDescriptor);
    try {
      // LOG.info("bzablockilogkafka Get offset consumer for {}", key.descriptor.getTopicPartition());
      CacheOffsetEntry entry =
          offsetCache.get(
              key,
              () -> {
                LOG.info(
                    "bzablockilogkafka creating a new offset consumer {}",
                    key.descriptor.getTopicPartition());
                Consumer<byte[], byte[]> consumer = consumerFactoryFn.apply(consumerConfig);
                ConsumerSpEL.evaluateAssign(
                    consumer, ImmutableList.of(kafkaSourceDescriptor.getTopicPartition()));
                KafkaOffsetConsumerPollThread pollThread = new KafkaOffsetConsumerPollThread();
                pollThread.startOnExecutor(
                    backgroundThreads, consumer, kafkaSourceDescriptor.getTopicPartition());
                return new CacheOffsetEntry(pollThread);
              });
      return entry.offsetThread;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /** Close the reader and log a warning if close fails. */
  private void asyncCloseOffsetConsumer(OffsetCacheKey key, CacheOffsetEntry entry) {
    invalidationExecutor.execute(
        () -> {
          try {
            entry.offsetThread.close();
            LOG.info("Finished closing consumer for {}", key);
          } catch (IOException e) {
            LOG.warn("Failed to close consumer for {}", key, e);
          }
        });
  }
}
