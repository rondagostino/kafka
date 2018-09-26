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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;

/**
 * Authentication for Channel
 */
public interface Authenticator extends Closeable {
    /**
     * Implements any authentication mechanism. Use transportLayer to read or write tokens.
     * For security protocols PLAINTEXT and SSL, this is a no-op since no further authentication
     * needs to be done. For SASL_PLAINTEXT and SASL_SSL, this performs the SASL authentication.
     *
     * @throws AuthenticationException if authentication fails due to invalid credentials or
     *      other security configuration errors
     * @throws IOException if read/write fails due to an I/O error
     */
    void authenticate() throws AuthenticationException, IOException;

    /**
     * Perform any processing related to authentication failure. This is invoked when the channel is about to be closed
     * because of an {@link AuthenticationException} thrown from a prior {@link #authenticate()} call.
     * @throws IOException if read/write fails due to an I/O error
     */
    default void handleAuthenticationFailure() throws IOException {
    }

    /**
     * Returns Principal using PrincipalBuilder
     */
    KafkaPrincipal principal();

    /**
     * returns true if authentication is complete otherwise returns false;
     */
    boolean complete();

    /**
     * Return true if this instance is a client-side authenticator that supports
     * re-authentication, otherwise false
     * 
     * @return true if this instance is a client-side authenticator that supports
     *         re-authentication, otherwise false
     */
    default boolean supportsClientReauth() {
        return false;
    }

    /**
     * Return true if this instance is a server-side authenticator that supports
     * re-authentication, otherwise false
     * 
     * @return true if this instance is a server-side authenticator that supports
     *         re-authentication, otherwise false
     */
    default boolean supportsServerReauth() {
        return false;
    }

    /**
     * Begins re-authentication. Uses transportLayer to read or write tokens as is
     * done for {@link #authenticate()}. For security protocols PLAINTEXT and SSL,
     * this is a no-op since re-authentication does not apply/is not supported,
     * respectively. For SASL_PLAINTEXT and SASL_SSL, this performs a SASL
     * authentication. Any in-flight responses from prior requests can/will be read
     * and collected for later processing as required. There must not be partially
     * written requests; any request queued for writing (for which zero bytes have
     * been written) remains queued until after re-authentication succeeds. .
     * 
     * @throws AuthenticationException
     *             if authentication fails due to invalid credentials or other
     *             security configuration errors
     * @throws IOException
     *             if read/write fails due to an I/O error
     */
    default void reauthenticate(ReauthenticationContext reauthenticationContext) throws IOException {
        // empty
    }

    /**
     * Return the time, in milliseconds since the epoch, when the session began.
     * Valid on both client- and server-side.
     * 
     * @return the time, in milliseconds since the epoch, when the session began
     * @throws IllegalStateException
     *             if the session is not established
     * @see #complete()
     */
    long sessionBeginTimeMs();

    /**
     * Return the client-side session expiration time, in milliseconds since the
     * epoch, after which re-authentication must occur before the connection is used
     * for anything else; or return null if the session has no expiration or it is
     * on the server-side. Note that any non-null value does not take into account
     * either the possibility of clock drift between the client and the server or
     * the inherent latency introduced by the fact that messages have to traverse
     * the network between the client and the server. It is therefore prudent to
     * assume the session expires at some point before the given time (e.g. 90% of
     * the way between the {@link #sessionBeginTimeMs()} and this time).
     * 
     * @return the client-side session expiration time, in milliseconds since the
     *         epoch, after which re-authentication must occur before the connection
     *         is used for anything else; or return null if the session has no
     *         expiration
     */
    default Long sessionExpirationTimeMs() {
        return null;
    }

    /**
     * Return the client-side {@link NetworkReceive} responses that arrived during
     * re-authentication that are unrelated to re-authentication, if any, otherwise
     * null. These correspond to requests sent prior to the beginning of
     * re-authentication; the requests were made when the channel was successfully
     * authenticated, and the responses arrived during the re-authentication
     * process.
     * 
     * @return the client-side {@link NetworkReceive} responses that arrived during
     *         re-authentication that are unrelated to re-authentication, if any,
     *         otherwise null
     */
    default Deque<NetworkReceive> getAndClearResponsesReceivedDuringReauthentication() {
        return null;
    }
    
    /**
     * Return true if this is a server-side authenticator and the connected client
     * supports re-authentication, otherwise false
     * 
     * @return true if this is a server-side authenticator and the connected client
     *         supports re-authentication, otherwise false
     */
    default boolean clientSupportsReauthentication() {
        return false;
    }
}
