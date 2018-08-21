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
package org.apache.kafka.common.security.authenticator;

import java.util.Objects;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;

/**
 * A request related to authentication of connections to brokers
 */
public class AuthenticationRequest {
    private final int nodeId;
    private final AbstractRequest.Builder<?> requestBuilder;
    private final AuthenticationRequestCompletionHandler authenticationRequestCompletionHandler;

    /**
     * Constructor
     * 
     * @param nodeId
     *            the mandatory nodeId; it must convert to an integer
     * @param requestBuilder
     *            the mandatory request builder; it must be assignable to
     *            {@code ApiVersionsRequest.Builder},
     *            {@code SaslHandshakeRequest.Builder}, or
     *            {@code SaslAuthenticateRequest.Builder}
     * @param authenticationRequestCompletionHandler
     *            the mandatory callback
     */
    public AuthenticationRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder,
            AuthenticationRequestCompletionHandler authenticationRequestCompletionHandler) {
        this(Integer.parseInt(nodeId), requestBuilder, authenticationRequestCompletionHandler);
    }

    /**
     * Constructor
     * 
     * @param nodeId
     *            the mandatory nodeId
     * @param requestBuilder
     *            the mandatory request builder; it must be assignable to
     *            {@code ApiVersionsRequest.Builder},
     *            {@code SaslHandshakeRequest.Builder}, or
     *            {@code SaslAuthenticateRequest.Builder}
     * @param authenticationRequestCompletionHandler
     *            the mandatory callback
     */
    public AuthenticationRequest(int nodeId, AbstractRequest.Builder<?> requestBuilder,
            AuthenticationRequestCompletionHandler authenticationRequestCompletionHandler) {
        this.nodeId = nodeId;
        this.requestBuilder = Objects.requireNonNull(requestBuilder);
        if (!ApiVersionsRequest.Builder.class.isAssignableFrom(requestBuilder.getClass())
                && !SaslHandshakeRequest.Builder.class.isAssignableFrom(requestBuilder.getClass())
                && !SaslAuthenticateRequest.Builder.class.isAssignableFrom(requestBuilder.getClass()))
            throw new IllegalArgumentException(
                    String.format("invalid requestBuilder class: %s", requestBuilder.getClass()));
        this.authenticationRequestCompletionHandler = Objects.requireNonNull(authenticationRequestCompletionHandler);
    }

    /**
     * Return the node ID
     * 
     * @return the node ID
     */
    public int nodeId() {
        return nodeId;
    }

    /**
     * Return the always non-null request builder. It will be assignable to
     * {@code ApiVersionsRequest.Builder}, {@code SaslHandshakeRequest.Builder}, or
     * {@code SaslAuthenticateRequest.Builder}.
     * 
     * @return the always non-null request builder
     */
    public AbstractRequest.Builder<?> requestBuilder() {
        return requestBuilder;
    }

    /**
     * Return the always non-null callback
     * 
     * @return the always non-null callback
     */
    public AuthenticationRequestCompletionHandler authenticationRequestCompletionHandler() {
        return authenticationRequestCompletionHandler;
    }
}
