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
package com.google.cloud.dataflow.dce.io.solace;

import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auto.value.AutoValue;
import com.google.cloud.ExceptionHandler;
import com.google.cloud.ExceptionHandler.Interceptor;
import com.google.cloud.RetryHelper;
import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.beam.sdk.annotations.Internal;

@Internal
@AutoValue
public abstract class RetryCallableManager implements Serializable {

    private static final int NUMBER_OF_RETRIES = 4;
    private static final int RETRY_INTERVAL_SECONDS = 1;
    private static final int RETRY_MULTIPLIER = 2;
    private static final int MAX_DELAY =
            NUMBER_OF_RETRIES * RETRY_MULTIPLIER * RETRY_INTERVAL_SECONDS + 1;

    public static RetryCallableManager create() {
        return builder().build();
    }
    /**
     * Method that executes and repeats the execution of the callable argument, if it throws one of
     * the exceptions from the exceptionsToIntercept Set.
     */
    public <V> V retryCallable(
            Callable<V> callable, Set<Class<? extends Exception>> exceptionsToIntercept) {
        return RetryHelper.runWithRetries(
                callable,
                getRetrySettings(),
                getExceptionHandlerForExceptions(exceptionsToIntercept),
                NanoClock.getDefaultClock());
    }

    private ExceptionHandler getExceptionHandlerForExceptions(
            Set<Class<? extends Exception>> exceptionsToIntercept) {
        return ExceptionHandler.newBuilder()
                .abortOn(RuntimeException.class)
                .addInterceptors(new ExceptionSetInterceptor(Set.copyOf(exceptionsToIntercept)))
                .build();
    }

    abstract RetrySettings getRetrySettings();

    abstract Builder toBuilder();

    static Builder builder() {
        return new AutoValue_RetryCallableManager.Builder()
                .setRetrySettings(
                        RetrySettings.newBuilder()
                                .setInitialRetryDelay(
                                        org.threeten.bp.Duration.ofSeconds(RETRY_INTERVAL_SECONDS))
                                .setMaxAttempts(NUMBER_OF_RETRIES)
                                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(MAX_DELAY))
                                .setRetryDelayMultiplier(RETRY_MULTIPLIER)
                                .build());
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setRetrySettings(RetrySettings retrySettings);

        abstract RetryCallableManager build();
    }

    private static class ExceptionSetInterceptor implements Interceptor {
        private static final long serialVersionUID = -8429573586820467828L;
        private final Set<Class<? extends Exception>> exceptionsToIntercept;

        public ExceptionSetInterceptor(Set<Class<? extends Exception>> exceptionsToIntercept) {
            this.exceptionsToIntercept = exceptionsToIntercept;
        }

        @Override
        public RetryResult afterEval(Exception exception, RetryResult retryResult) {
            return Interceptor.RetryResult.CONTINUE_EVALUATION;
        }

        @Override
        public RetryResult beforeEval(Exception exceptionToEvaluate) {
            for (Class<? extends Exception> exceptionToIntercept : exceptionsToIntercept) {
                if (isOf(exceptionToIntercept, exceptionToEvaluate)) {
                    return Interceptor.RetryResult.RETRY;
                }
            }
            return Interceptor.RetryResult.CONTINUE_EVALUATION;
        }

        private boolean isOf(Class<?> clazz, Object obj) {
            return clazz.isInstance(obj);
        }
    }
}
