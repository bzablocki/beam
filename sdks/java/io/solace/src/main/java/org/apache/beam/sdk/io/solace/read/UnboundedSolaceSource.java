/*
 * Copyright 2023 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.dce.io.solace.read;

import com.google.cloud.dataflow.dce.io.solace.broker.SempClientFactory;
import com.google.cloud.dataflow.dce.io.solace.broker.SessionServiceFactory;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Queue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class UnboundedSolaceSource<T> extends UnboundedSource<T, SolaceCheckpointMark> {
    private static final long serialVersionUID = 42L;
    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSolaceSource.class);
    private final Queue queue;
    private final Integer maxNumConnections;
    private final Coder<T> coder;
    private final boolean enableDeduplication;
    private final SempClientFactory sempClientFactory;
    private final SessionServiceFactory sessionServiceFactory;
    private final SerializableFunction<T, Instant> timestampFn;
    private final SerializableFunction<BytesXMLMessage, T> parseFn;

    public Queue getQueue() {
        return queue;
    }

    public SessionServiceFactory getSessionServiceFactory() {
        return sessionServiceFactory;
    }

    public SempClientFactory getSempClientFactory() {
        return sempClientFactory;
    }

    public SerializableFunction<T, Instant> getTimestampFn() {
        return timestampFn;
    }

    public SerializableFunction<BytesXMLMessage, T> getParseFn() {
        return parseFn;
    }

    public UnboundedSolaceSource(
            Queue queue,
            SempClientFactory sempClientFactory,
            SessionServiceFactory sessionServiceFactory,
            Integer maxNumConnections,
            boolean enableDeduplication,
            Coder<T> coder,
            SerializableFunction<T, Instant> timestampFn,
            SerializableFunction<BytesXMLMessage, T> parseFn) {
        this.queue = queue;
        this.sempClientFactory = sempClientFactory;
        this.sessionServiceFactory = sessionServiceFactory;
        this.maxNumConnections = maxNumConnections;
        this.enableDeduplication = enableDeduplication;
        this.coder = coder;
        this.timestampFn = timestampFn;
        this.parseFn = parseFn;
    }

    @Override
    public UnboundedReader<T> createReader(
            PipelineOptions options, @Nullable SolaceCheckpointMark checkpointMark) {
        // it makes no sense to resume a Solace Session with the previous checkpoint
        // so don't need the pass a checkpoint to new a Solace Reader
        return new UnboundedSolaceReader<>(this);
    }

    @Override
    public List<UnboundedSolaceSource<T>> split(int desiredNumSplits, PipelineOptions options)
            throws IOException {
        boolean queueNonExclusive = sempClientFactory.create().isQueueNonExclusive(queue.getName());
        if (queueNonExclusive) {
            return getSolaceSources(desiredNumSplits, maxNumConnections);
        } else {
            LOG.warn(
                    "SolaceIO.Read: The queue {} is exclusive. Provisioning only 1 read client.",
                    queue);
            return getSolaceSources(desiredNumSplits, 1);
        }
    }

    private List<UnboundedSolaceSource<T>> getSolaceSources(
            int desiredNumSplits, Integer maxNumConnections) {
        List<UnboundedSolaceSource<T>> sourceList = new ArrayList<>();
        int numSplits =
                maxNumConnections != null
                        ? Math.min(desiredNumSplits, maxNumConnections)
                        : desiredNumSplits;
        LOG.info("SolaceIO.Read: UnboundedSolaceSource: creating {} read connections.", numSplits);
        for (int i = 0; i < numSplits; i++) {
            UnboundedSolaceSource<T> source =
                    new UnboundedSolaceSource<>(
                            queue,
                            sempClientFactory,
                            sessionServiceFactory,
                            maxNumConnections,
                            enableDeduplication,
                            coder,
                            timestampFn,
                            parseFn);
            sourceList.add(source);
        }
        return sourceList;
    }

    @Override
    public Coder<SolaceCheckpointMark> getCheckpointMarkCoder() {
        return AvroCoder.of(SolaceCheckpointMark.class);
    }

    @Override
    public Coder<T> getOutputCoder() {
        return coder;
    }

    @Override
    public boolean requiresDeduping() {
        return enableDeduplication;
    }
}
