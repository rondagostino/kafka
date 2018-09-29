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
package org.apache.kafka.common.network;

import java.util.Objects;

/**
 * Defines the context in which an {@link Authenticator} is to be created during
 * a re-authentication.
 */
public class ReauthenticationContext {
    private final NetworkReceive saslHandshakeReceive;
    private final Authenticator previousAuthenticator;
    private final NetworkReceive inProgressResponse;
    private final long reauthenticationStartMs;

    /**
     * Constructor to be used on the server-side
     * 
     * @param saslHandshakeReceive
     *            the mandatory {@link NetworkReceive} containing the
     *            {@code SaslHandshakeRequest} that has been received on the server
     *            and that initiates re-authentication.
     * @param now
     *            the current time in milliseconds since the epoch
     */
    public ReauthenticationContext(NetworkReceive saslHandshakeReceive, long now) {
        this(Objects.requireNonNull(saslHandshakeReceive), null, null, now);
    }

    /**
     * Constructor to be used on the client-side
     * 
     * @param previousAuthenticator
     *            the mandatory {@link Authenticator} that was previously used to
     *            authenticate the channel
     * @param inProgressResponse
     *            a response that has been partially read, if any, otherwise null
     * @param now
     *            the current time in milliseconds since the epoch
     */
    public ReauthenticationContext(Authenticator previousAuthenticator, NetworkReceive inProgressResponse, long now) {
        this(null, Objects.requireNonNull(previousAuthenticator), inProgressResponse, now);
    }

    /**
     * Return the {@link NetworkReceive} containing the {@code SaslHandshakeRequest}
     * that initiates re-authentication on the server, otherwise null if this is a
     * client-side context
     * 
     * @return the {@link NetworkReceive} containing the
     *         {@code SaslHandshakeRequest} that initiates re-authentication on the
     *         server, otherwise null if this is a client-side context
     */
    public NetworkReceive saslHandshakeReceive() {
        return saslHandshakeReceive;
    }

    /**
     * Return the {@link Authenticator} that was previously used to authenticate the
     * channel on the client, otherwise null if this is a server-side context
     * 
     * @return the {@link Authenticator} that was previously used to authenticate
     *         the channel on the client, otherwise null if this is a server-side
     *         context
     */
    public Authenticator previousAuthenticator() {
        return previousAuthenticator;
    }

    /**
     * Return the response that has been partially read, if any, otherwise null
     * 
     * @return the response that has been partially read, if any, otherwise null
     */
    public NetworkReceive inProgressResponse() {
        return inProgressResponse;
    }

    /**
     * Return the time when re-authentication started, in milliseconds since the
     * epoch
     * 
     * @return the time when re-authentication started, in milliseconds since the
     *         epoch
     */
    public long reauthenticationStartMs() {
        return reauthenticationStartMs;
    }

    private ReauthenticationContext(NetworkReceive saslHandshakeReceive, Authenticator previousAuthenticator,
            NetworkReceive inProgressResponse, long now) {
        this.saslHandshakeReceive = saslHandshakeReceive;
        this.previousAuthenticator = previousAuthenticator;
        this.inProgressResponse = inProgressResponse;
        this.reauthenticationStartMs = now;
    }
}
