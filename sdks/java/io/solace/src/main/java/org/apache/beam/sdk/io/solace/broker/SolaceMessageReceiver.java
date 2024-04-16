/*
 * Copyright 2024 Google.
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
package com.google.cloud.dataflow.dce.io.solace.broker;

import com.google.cloud.dataflow.dce.io.solace.RetryCallableManager;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

public class SolaceMessageReceiver implements MessageReceiver, Serializable {

    public static final int DEFAULT_ADVANCE_TIMEOUT_IN_MILLIS = 100;
    private final FlowReceiver flowReceiver;
    private final RetryCallableManager retryCallableManager = RetryCallableManager.create();

    public SolaceMessageReceiver(FlowReceiver flowReceiver) {
        this.flowReceiver = flowReceiver;
    }

    @Override
    public void start() {
        retryCallableManager.retryCallable(
                () -> {
                    flowReceiver.start();
                    return 0;
                },
                Set.of(JCSMPException.class));
    }

    @Override
    public boolean isClosed() {
        return flowReceiver == null || flowReceiver.isClosed();
    }

    @Override
    public BytesXMLMessage receive() throws IOException {
        try {
            return flowReceiver.receive(DEFAULT_ADVANCE_TIMEOUT_IN_MILLIS);
        } catch (JCSMPException e) {
            throw new IOException(e);
        }
    }
}
