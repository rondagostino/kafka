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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.clients.consumer.internals.RequestFutureListener;

/**
 * Receives information related to success or failure of requests related to
 * re-authentication.
 */
public interface AuthenticationRequestCompletionHandler extends RequestCompletionHandler {
    /**
     * Return the {@link AuthenticationSuccessOrFailureReceiver} instance where
     * notification related to the ultimate success or failure of re-authentication
     * should be sent
     * 
     * @return the {@link AuthenticationSuccessOrFailureReceiver} instance where
     *         notification related to the ultimate success or failure of
     *         re-authentication should be sent
     */
    AuthenticationSuccessOrFailureReceiver authenticationSuccessOrFailureReceiver();

    /**
     * Sometimes the {@code ClientRequest} is not available when a failure occurs
     * (for example, in the case of a {@link RequestFuture}). This method will be
     * invoked rather than {@link #onComplete(ClientResponse)} in such cases, and
     * implementations must deal with both possibilities.
     * 
     * @param e
     *            the exception that caused the failure
     */
    void onException(RuntimeException e);

    /**
     * Return a {@link RequestFutureListener}, appropriate for use with a
     * {@link RequestFuture}, that will translate the success or failure of the
     * underlying request to this instance.
     * 
     * @return a {@link RequestFutureListener}, appropriate for use with a
     *         {@link RequestFuture}, that will translate the success or failure of
     *         the underlying request to this instance.
     */
    default RequestFutureListener<ClientResponse> requestFutureListener() {
        return new RequestFutureListener<ClientResponse>() {
            @Override
            public void onSuccess(ClientResponse value) {
                onComplete(value);
            }

            @Override
            public void onFailure(RuntimeException e) {
                onException(e);
            }
        };
    }
}
