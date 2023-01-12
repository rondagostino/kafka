/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;


import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class ControllerRequestContext {

    public static OptionalLong requestTimeoutMsToDeadlineNs(
        Time time,
        int millisecondsOffset
    ) {
        return OptionalLong.of(time.nanoseconds() + NANOSECONDS.convert(millisecondsOffset, MILLISECONDS));
    }

    private final KafkaPrincipal principal;
    private final OptionalLong deadlineNs;
    private final RequestHeaderData requestHeader;

    private final Optional<Consumer<Double>> requestedPartitionCountRecorder;

    public ControllerRequestContext(
        RequestHeaderData requestHeader,
        KafkaPrincipal principal,
        OptionalLong deadlineNs
    ) {
        this(requestHeader, principal, deadlineNs, Optional.empty());
    }

    public ControllerRequestContext(
        RequestHeaderData requestHeader,
        KafkaPrincipal principal,
        OptionalLong deadlineNs,
        Optional<Consumer<Double>> requestedPartitionCountRecorder
    ) {
        this.requestHeader = requestHeader;
        this.principal = principal;
        this.deadlineNs = deadlineNs;
        this.requestedPartitionCountRecorder = requestedPartitionCountRecorder;
    }

    public ControllerRequestContext(
        AuthorizableRequestContext requestContext,
        OptionalLong deadlineNs
    ) {
        this(requestContext, deadlineNs, Optional.empty());
    }

    public ControllerRequestContext(
        AuthorizableRequestContext requestContext,
        OptionalLong deadlineNs,
        Optional<Consumer<Double>> requestedPartitionCountRecorder
    ) {
        this(
            new RequestHeaderData()
                .setRequestApiKey((short) requestContext.requestType())
                .setRequestApiVersion((short) requestContext.requestVersion())
                .setCorrelationId(requestContext.correlationId())
                .setClientId(requestContext.clientId()),
            requestContext.principal(),
            deadlineNs,
            requestedPartitionCountRecorder
        );
    }

    public RequestHeaderData requestHeader() {
        return requestHeader;
    }

    public KafkaPrincipal principal() {
        return principal;
    }

    public OptionalLong deadlineNs() {
        return deadlineNs;
    }

    /**
     *
     * @param requestedPartitionCount the value to record
     * @throws ThrottlingQuotaExceededException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double requestedPartitionCount) {
        this.requestedPartitionCountRecorder.ifPresent(recorder -> recorder.accept(requestedPartitionCount));
    }
}
