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

import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;

/**
 * Enqueues requests related to authentication of connections to brokers
 */
public interface AuthenticationRequestEnqueuer {
    /**
     * Create and enqueue a new {@link ApiVersionsRequest}
     *
     * @param nodeId
     *            the mandatory node to send to
     * @param apiVersionsRequestBuilder
     *            the mandatory request builder to use
     * @param callback
     *            the mandatory callback to invoke when we get a response. Note that
     *            this callback must not throw any instances of
     *            {@code RuntimeException}; instead, it must notify its
     *            {@link AuthenticationRequestCompletionHandler#authenticationSuccessOrFailureReceiver()}
     *            about any information related to success or failure.
     */
    void enqueueRequest(String nodeId, ApiVersionsRequest.Builder apiVersionsRequestBuilder,
            AuthenticationRequestCompletionHandler callback);

    /**
     * Create and enqueue a new {@link SaslHandshakeRequest}
     *
     * @param nodeId
     *            the mandatory node to send to
     * @param saslHandshakeRequestBuilder
     *            the mandatory request builder to use
     * @param callback
     *            the mandatory callback to invoke when we get a response. Note that
     *            this callback must not throw any instances of
     *            {@code RuntimeException}; instead, it must notify its
     *            {@link AuthenticationRequestCompletionHandler#authenticationSuccessOrFailureReceiver()}
     *            about any information related to success or failure.
     */
    void enqueueRequest(String nodeId, SaslHandshakeRequest.Builder saslHandshakeRequestBuilder,
            AuthenticationRequestCompletionHandler callback);

    /**
     * Create and enqueue a new {@link SaslAuthenticateRequest}
     *
     * @param nodeId
     *            the mandatory node to send to
     * @param saslAuthenticateRequestBuilder
     *            the mandatory request builder to use
     * @param callback
     *            the mandatory callback to invoke when we get a response. Note that
     *            this callback must not throw any instances of
     *            {@code RuntimeException}; instead, it must notify its
     *            {@link AuthenticationRequestCompletionHandler#authenticationSuccessOrFailureReceiver()}
     *            about any information related to success or failure.
     */
    void enqueueRequest(String nodeId, SaslAuthenticateRequest.Builder saslAuthenticateRequestBuilder,
            AuthenticationRequestCompletionHandler callback);
}
