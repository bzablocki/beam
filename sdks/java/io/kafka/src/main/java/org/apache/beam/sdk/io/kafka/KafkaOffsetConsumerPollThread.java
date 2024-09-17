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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaOffsetConsumerPollThread {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetConsumerPollThread.class);
  private @Nullable Consumer<byte[], byte[]> consumer;
  private @Nullable TopicPartition topicPartition;
  private final AtomicLong collectedEndOffset = new AtomicLong();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private @Nullable Future<?> offsetTrackerFuture;

  KafkaOffsetConsumerPollThread() {
    consumer = null;
    offsetTrackerFuture = null;
    topicPartition = null;
  }

  void startOnExecutor(
      ExecutorService executorService,
      Consumer<byte[], byte[]> consumer,
      TopicPartition topicPartition) {
    this.consumer = consumer;
    this.topicPartition = topicPartition;
    // Use a separate thread to read Kafka messages. Kafka Consumer does all its work including
    // network I/O inside poll(). Polling only inside #advance(), especially with a small timeout
    // like 100 milliseconds does not work well. This along with large receive buffer for
    // consumer achieved best throughput in tests (see `defaultConsumerProperties`).
    offsetTrackerFuture = executorService.submit(this::endOffsetTrackerLoop);
  }

  private void endOffsetTrackerLoop() {
    Consumer<byte[], byte[]> consumer = Preconditions.checkStateNotNull(this.consumer);
    TopicPartition topicPartition = Preconditions.checkStateNotNull(this.topicPartition);

    while (!closed.get()) {
      Long currentEndOffset =
          consumer.endOffsets(ImmutableList.of(topicPartition)).get(topicPartition);
      if (currentEndOffset != null) {
        collectedEndOffset.set(currentEndOffset);
      } else {
        LOG.error("bzablockilogkafka unable to get end offset for {}", topicPartition);
      }
      try { // todo limit number of calls
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // todo Ignore?
      }
    }
  }

  void close() throws IOException {
    if (consumer == null) {
      LOG.debug("Closing consumer poll thread that was never started.");
      return;
    }
    Preconditions.checkStateNotNull(offsetTrackerFuture);
    closed.set(true);
    Closeables.close(consumer, true); // todo not sure about the order
    Thread.currentThread().interrupt();
  }

  long readEndOffset() {
    return collectedEndOffset.get();
  }
}
