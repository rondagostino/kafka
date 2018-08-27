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
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.AuthenticationSuccessOrFailureReceiver;
import org.apache.kafka.common.security.authenticator.AuthenticationSuccessOrFailureReceiver.RetryIndication;
import org.apache.kafka.common.security.expiring.ExpiringCredential;
import org.apache.kafka.common.security.expiring.internals.ClientChannelCredentialEvent;
import org.apache.kafka.common.security.expiring.internals.ClientChannelCredentialTracker;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import javax.security.auth.Subject;

public class KafkaChannel {
    private static final Logger log = LoggerFactory.getLogger(KafkaChannel.class);
    /**
     * Mute States for KafkaChannel:
     * <ul>
     *   <li> NOT_MUTED: Channel is not muted. This is the default state. </li>
     *   <li> MUTED: Channel is muted. Channel must be in this state to be unmuted. </li>
     *   <li> MUTED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted and SocketServer has not sent a response
     *                                    back to the client yet (acks != 0) or is currently waiting to receive a
     *                                    response from the API layer (acks == 0). </li>
     *   <li> MUTED_AND_THROTTLED: (SocketServer only) Channel is muted and throttling is in progress due to quota
     *                             violation. </li>
     *   <li> MUTED_AND_THROTTLED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted, throttling is in progress,
     *                                                  and a response is currently pending. </li>
     * </ul>
     */
    public enum ChannelMuteState {
        NOT_MUTED,
        MUTED,
        MUTED_AND_RESPONSE_PENDING,
        MUTED_AND_THROTTLED,
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    };

    /** Socket server events that will change the mute state:
     * <ul>
     *   <li> REQUEST_RECEIVED: A request has been received from the client. </li>
     *   <li> RESPONSE_SENT: A response has been sent out to the client (ack != 0) or SocketServer has heard back from
     *                       the API layer (acks = 0) </li>
     *   <li> THROTTLE_STARTED: Throttling started due to quota violation. </li>
     *   <li> THROTTLE_ENDED: Throttling ended. </li>
     * </ul>
     *
     * Valid transitions on each event are:
     * <ul>
     *   <li> REQUEST_RECEIVED: MUTED => MUTED_AND_RESPONSE_PENDING </li>
     *   <li> RESPONSE_SENT:    MUTED_AND_RESPONSE_PENDING => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED </li>
     *   <li> THROTTLE_STARTED: MUTED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED_AND_RESPONSE_PENDING </li>
     *   <li> THROTTLE_ENDED:   MUTED_AND_THROTTLED => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_RESPONSE_PENDING </li>
     * </ul>
     */
    public enum ChannelMuteEvent {
        REQUEST_RECEIVED,
        RESPONSE_SENT,
        THROTTLE_STARTED,
        THROTTLE_ENDED
    };

    private final String id;
    private final TransportLayer transportLayer;
    private final Supplier<Authenticator> authenticatorSupplier;
    private Authenticator authenticatedAuthenticator;
    private Authenticator notYetAuthenticatedAuthenticator;
    // Tracks accumulated network thread time. This is updated on the network thread.
    // The values are read and reset after each response is sent.
    private long networkThreadTimeNanos;
    private final int maxReceiveSize;
    private final MemoryPool memoryPool;
    private NetworkReceive receive;
    private Send send;
    // Track connection and mute state of channels to enable outstanding requests on channels to be
    // processed after the channel is disconnected.
    private boolean disconnected;
    private ChannelMuteState muteState;
    private ChannelState state;
    private final Time time = Time.SYSTEM;

    public KafkaChannel(String id, TransportLayer transportLayer, Supplier<Authenticator> authenticatorSupplier, int maxReceiveSize, MemoryPool memoryPool) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticatorSupplier = authenticatorSupplier;
        this.networkThreadTimeNanos = 0L;
        this.maxReceiveSize = maxReceiveSize;
        this.memoryPool = memoryPool;
        this.disconnected = false;
        this.muteState = ChannelMuteState.NOT_MUTED;
        this.state = ChannelState.NOT_CONNECTED;
    }

    public void close() throws IOException {
        if (!disconnected && clientExpiringCredential() != null) {
            ClientChannelCredentialTracker clientChannelCredentialTracker = clientChannelCredentialTracker();
            if (clientChannelCredentialTracker != null)
                clientChannelCredentialTracker.offer(ClientChannelCredentialEvent.channelDisconnected(time, this));
        }
        this.disconnected = true;
        Utils.closeAll(transportLayer, authenticatedAuthenticator, notYetAuthenticatedAuthenticator, receive);
    }

    /**
     * Returns the authenticated principal, or ANONYMOUS if authentication has not
     * yet occurred
     */
    public KafkaPrincipal principal() {
        return authenticatedAuthenticator != null ? authenticatedAuthenticator.principal() : KafkaPrincipal.ANONYMOUS;
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator.
     * For SSL with client authentication enabled, {@link TransportLayer#handshake()} performs
     * authentication. For SASL, authentication is performed by {@link Authenticator#authenticate()}.
     */
    public void prepare() throws AuthenticationException, IOException {
        try {
            if (!transportLayer.ready())
                transportLayer.handshake();
            if (transportLayer.ready() && authenticatedAuthenticator == null) {
                if (notYetAuthenticatedAuthenticator == null)
                    notYetAuthenticatedAuthenticator = authenticatorSupplier.get();
                if (!notYetAuthenticatedAuthenticator.complete())
                    notYetAuthenticatedAuthenticator.authenticate();
                if (notYetAuthenticatedAuthenticator.complete())
                    /*
                     * Initial authentication (could be either on the SASL Client side or on the
                     * SASL Server side) is complete. There won't be any previously-authenticated
                     * authenticator, but the below method takes that possibility into account.
                     */
                    replaceAnyPreviouslyAuthenticatedAuthenticatorWithNewOne();
            }
        } catch (AuthenticationException e) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e);
            throw e;
        }
        if (ready()) {
            state = ChannelState.READY;
            ExpiringCredential clientExpiringCredential = clientExpiringCredential();
            if (clientExpiringCredential != null) {
                ClientChannelCredentialTracker clientChannelCredentialTracker = clientChannelCredentialTracker();
                if (clientChannelCredentialTracker != null)
                    clientChannelCredentialTracker.offer(ClientChannelCredentialEvent
                            .channelInitiallyAuthenticated(time, this, clientExpiringCredential));
            }
        }
    }

    /**
     * Respond to a SASL re-authentication initial handshake. This occurs on the
     * SASL Server side of the re-authentication dance (i.e. on the broker).
     * 
     * @param requestHeader
     *            the request header
     * @param saslHandshakeRequest
     *            the initial handshake request to process
     * @return the response to return to the client
     */
    public SaslHandshakeResponse respondToReauthenticationSaslHandshakeRequest(RequestHeader requestHeader,
            SaslHandshakeRequest saslHandshakeRequest) {
        if (!ready()) {
            log.debug("Illegal state: client tried to re-authenticate when the connection is not ready");
            return new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE, Collections.emptySet());
        }
        if (!(authenticatedAuthenticator instanceof SaslServerAuthenticator)) {
            log.debug("Invalid request: client tried to re-authenticate to a non-SASL Server authenticator: {}",
                    authenticatedAuthenticator.getClass().getName());
            return new SaslHandshakeResponse(Errors.INVALID_REQUEST, Collections.emptySet());
        }
        // make sure we are using the same mechanism as before
        String originalSaslMechanism = ((SaslServerAuthenticator) authenticatedAuthenticator).saslMechanism();
        if (!originalSaslMechanism.equals(saslHandshakeRequest.mechanism())) {
            log.debug(
                    "Invalid request: client tried to re-authenticate a connection originally authenticated via mechanism {} with a different mechanism: {}",
                    originalSaslMechanism, saslHandshakeRequest.mechanism());
            return new SaslHandshakeResponse(Errors.INVALID_REQUEST,
                    new HashSet<>(Arrays.asList(originalSaslMechanism)));
        }
        if (notYetAuthenticatedAuthenticator == null)
            notYetAuthenticatedAuthenticator = authenticatorSupplier.get();
        if (!(notYetAuthenticatedAuthenticator instanceof SaslServerAuthenticator)) {
            // should never happen
            String errMsg = "notYetAuthenticatedAuthenticator of incorrect type: "
                    + (notYetAuthenticatedAuthenticator == null ? "null"
                            : notYetAuthenticatedAuthenticator.getClass().getName());
            log.error(errMsg);
            Utils.closeQuietly(notYetAuthenticatedAuthenticator, errMsg);
            notYetAuthenticatedAuthenticator = null;
            return new SaslHandshakeResponse(Errors.UNKNOWN_SERVER_ERROR, Collections.emptySet());
        }
        try {
            SaslServerAuthenticator notYetAuthenticatedSaslServerAuthenticator = (SaslServerAuthenticator) notYetAuthenticatedAuthenticator;
            SaslHandshakeResponse retvalSaslHandshakeResponse = notYetAuthenticatedSaslServerAuthenticator
                    .respondToReauthenticationSaslHandshakeRequest(requestHeader, saslHandshakeRequest);
            if (retvalSaslHandshakeResponse != null) {
                if (notYetAuthenticatedSaslServerAuthenticator.failed()) {
                    Utils.closeQuietly(notYetAuthenticatedAuthenticator,
                            "notYetAuthenticatedAuthenticator in failed state");
                    notYetAuthenticatedAuthenticator = null;
                }
                return retvalSaslHandshakeResponse;
            }
            // should never happen
            log.error("notYetAuthenticatedAuthenticator returned null handshake response");
            Utils.closeQuietly(notYetAuthenticatedAuthenticator, "notYetAuthenticatedAuthenticator");
            notYetAuthenticatedAuthenticator = null;
            return new SaslHandshakeResponse(Errors.UNKNOWN_SERVER_ERROR, Collections.emptySet());
        } catch (Exception e) {
            // deal with IOException or unexpected RuntimeException
            log.error(e.getMessage(), e);
            Utils.closeQuietly(notYetAuthenticatedAuthenticator, "notYetAuthenticatedAuthenticator");
            notYetAuthenticatedAuthenticator = null;
            return new SaslHandshakeResponse(Errors.UNKNOWN_SERVER_ERROR,
                    new HashSet<>(Arrays.asList(originalSaslMechanism)));
        }
    }

    /**
     * Respond to a SASL re-authentication token exchange. This occurs on the SASL
     * Server side of the re-authentication dance (i.e. on the broker).
     * 
     * @param requestHeader
     *            the request header
     * @param saslAuthenticateRequest
     *            the token exchange request to process
     * @return the response to return to the client
     */
    public SaslAuthenticateResponse respondToReauthenticationSaslAuthenticateRequest(RequestHeader requestHeader,
            SaslAuthenticateRequest saslAuthenticateRequest) {
        if (!ready()) {
            log.debug("Illegal state: client tried to re-authenticate when the connection was not ready");
            return new SaslAuthenticateResponse(Errors.ILLEGAL_SASL_STATE,
                    "Cannot re-authenticate: not yet ready/authenticated");
        }
        if (!(authenticatedAuthenticator instanceof SaslServerAuthenticator)) {
            log.debug("Invalid request: client tried to re-authenticate to a non-SASL authenticator: {}",
                    authenticatedAuthenticator.getClass().getName());
            return new SaslAuthenticateResponse(Errors.INVALID_REQUEST,
                    "Only SASL-enabled broker listeners can respond to re-authentication requests");
        }
        if (notYetAuthenticatedAuthenticator == null) {
            log.debug("Invalid request: client tried to re-authenticate without an initial handshake");
            return new SaslAuthenticateResponse(Errors.INVALID_REQUEST,
                    "Must provide a handshake request to start re-authentication");
        }
        try {
            SaslServerAuthenticator notYetAuthenticatedSaslServerAuthenticator = (SaslServerAuthenticator) notYetAuthenticatedAuthenticator;
            SaslAuthenticateResponse retvalSaslAuthenticateResponse = notYetAuthenticatedSaslServerAuthenticator
                    .respondToReauthenticationSaslAuthenticateRequest(requestHeader, saslAuthenticateRequest);
            if (retvalSaslAuthenticateResponse == null) {
                // should never happen
                String errMsg = "Expected SaslAuthenticateResponse: null";
                log.error(errMsg);
                Utils.closeQuietly(notYetAuthenticatedAuthenticator, errMsg);
                notYetAuthenticatedAuthenticator = null;
                return new SaslAuthenticateResponse(Errors.UNKNOWN_SERVER_ERROR, errMsg);
            }
            /*
             * The re-authentication on the SASL Server side can either be completed,
             * failed, or still in progress.
             */
            if (notYetAuthenticatedAuthenticator.complete()) {
                if (!authenticatedAuthenticator.principal().getPrincipalType()
                        .equals(notYetAuthenticatedAuthenticator.principal().getPrincipalType())
                        || !authenticatedAuthenticator.principal().getName()
                                .equals(notYetAuthenticatedAuthenticator.principal().getName())) {
                    // disallow changing identities upon re-authentication
                    String errMsg = String.format(
                            "Not allowed to change identities during re-authentication from %s:%s: %s:%s",
                            authenticatedAuthenticator.principal().getPrincipalType(),
                            authenticatedAuthenticator.principal().getName(),
                            notYetAuthenticatedAuthenticator.principal().getPrincipalType(),
                            notYetAuthenticatedAuthenticator.principal().getName());
                    log.error(errMsg);
                    Utils.closeQuietly(notYetAuthenticatedAuthenticator, errMsg);
                    notYetAuthenticatedAuthenticator = null;
                    return new SaslAuthenticateResponse(Errors.SASL_AUTHENTICATION_FAILED, errMsg);
                }
                replaceAnyPreviouslyAuthenticatedAuthenticatorWithNewOne();
            } else if (notYetAuthenticatedSaslServerAuthenticator.failed()) {
                Utils.closeQuietly(notYetAuthenticatedAuthenticator, "failed notYetAuthenticatedAuthenticator");
                notYetAuthenticatedAuthenticator = null;
            }
            return retvalSaslAuthenticateResponse;
        } catch (RuntimeException e) {
            // deal with unexpected RuntimeException
            log.error(e.getMessage(), e);
            Utils.closeQuietly(notYetAuthenticatedAuthenticator, "notYetAuthenticatedAuthenticator");
            notYetAuthenticatedAuthenticator = null;
            return new SaslAuthenticateResponse(Errors.UNKNOWN_SERVER_ERROR, e.getMessage());
        }
    }

    public void disconnect() {
        if (!disconnected) {
            if (clientExpiringCredential() != null) {
                ClientChannelCredentialTracker clientChannelCredentialTracker = clientChannelCredentialTracker();
                if (clientChannelCredentialTracker != null)
                    clientChannelCredentialTracker.offer(ClientChannelCredentialEvent.channelDisconnected(time, this));
            }
        }
        disconnected = true;
        transportLayer.disconnect();
    }

    public void state(ChannelState state) {
        this.state = state;
    }

    public ChannelState state() {
        return this.state;
    }

    public boolean finishConnect() throws IOException {
        boolean connected = transportLayer.finishConnect();
        if (connected)
            state = ready() ? ChannelState.READY : ChannelState.AUTHENTICATE;
        return connected;
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    /**
     * externally muting a channel should be done via selector to ensure proper state handling
     */
    void mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.MUTED;
        }
    }

    /**
     * Unmute the channel. The channel can be unmuted only if it is in the MUTED state. For other muted states
     * (MUTED_AND_*), this is a no-op.
     *
     * @return Whether or not the channel is in the NOT_MUTED state after the call
     */
    boolean maybeUnmute() {
        if (muteState == ChannelMuteState.MUTED) {
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.NOT_MUTED;
        }
        return muteState == ChannelMuteState.NOT_MUTED;
    }

    // Handle the specified channel mute-related event and transition the mute state according to the state machine.
    public void handleChannelMuteEvent(ChannelMuteEvent event) {
        boolean stateChanged = false;
        switch (event) {
            case REQUEST_RECEIVED:
                if (muteState == ChannelMuteState.MUTED) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case RESPONSE_SENT:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED;
                    stateChanged = true;
                }
                break;
            case THROTTLE_STARTED:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case THROTTLE_ENDED:
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
        }
        if (!stateChanged) {
            throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
        }
    }

    public ChannelMuteState muteState() {
        return muteState;
    }

    /**
     * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
     */
    public boolean isMute() {
        return muteState != ChannelMuteState.NOT_MUTED;
    }

    public boolean isInMutableState() {
        //some requests do not require memory, so if we do not know what the current (or future) request is
        //(receive == null) we dont mute. we also dont mute if whatever memory required has already been
        //successfully allocated (if none is required for the currently-being-read request
        //receive.memoryAllocated() is expected to return true)
        if (receive == null || receive.memoryAllocated())
            return false;
        //also cannot mute if underlying transport is not in the ready state
        return transportLayer.ready();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticatedAuthenticator != null;
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
        this.send = send;
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
        }

        receive(receive);
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        } else if (receive.requiredMemoryAmountKnown() && !receive.memoryAllocated() && isInMutableState()) {
            //pool must be out of memory, mute ourselves.
            mute();
        }
        return result;
    }

    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    /**
     * Accumulates network thread time for this channel.
     */
    public void addNetworkThreadTimeNanos(long nanos) {
        networkThreadTimeNanos += nanos;
    }

    /**
     * Returns accumulated network thread time for this channel and resets
     * the value to zero.
     */
    public long getAndResetNetworkThreadTimeNanos() {
        long current = networkThreadTimeNanos;
        networkThreadTimeNanos = 0;
        return current;
    }

    @Override
    public String toString() {
        return super.toString() + " id=" + id;
    }

    private ExpiringCredential clientExpiringCredential() {
        return authenticatedAuthenticator instanceof SaslClientAuthenticator
                ? ((SaslClientAuthenticator) authenticatedAuthenticator).expiringCredential()
                : null;
    }

    private ClientChannelCredentialTracker clientChannelCredentialTracker() {
        if (!(authenticatedAuthenticator instanceof SaslClientAuthenticator))
            return null;
        Subject subject = ((SaslClientAuthenticator) authenticatedAuthenticator).subject();
        Set<ClientChannelCredentialTracker> instances = subject
                .getPrivateCredentials(ClientChannelCredentialTracker.class);
        return instances.isEmpty() ? null : instances.iterator().next();
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        if (send.completed())
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }

    /**
     * @return true if underlying transport has bytes remaining to be read from any underlying intermediate buffers.
     */
    public boolean hasBytesBuffered() {
        return transportLayer.hasBytesBuffered();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaChannel that = (KafkaChannel) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Initiate a re-authentication of this channel. This method must enqueue the
     * initial request in the re-authentication dance and return immediately while
     * ensuring that the ultimate success or failure of the re-authentication is
     * reported back via the {@link ClientChannelCredentialTracker} instance
     * available in the private credentials of the {@code Subject} associated with
     * the SASL mechanism.
     * 
     * @param time
     *            the require {@link Time}
     */
    public void initiateReauthentication(Time time) {
        /*
         * We must have a client channel/credential tracker since that is where this
         * request originates from and that is who we need to notify after the
         * re-authentication ultimately succeeds or fails.
         */
        ClientChannelCredentialTracker clientChannelCredentialTracker = clientChannelCredentialTracker();
        if (clientChannelCredentialTracker == null) {
            // should never happen; don't retry
            String errorMessage = String
                    .format("Channel told to re-authenticate but it could not find its client channel/credential tracker");
            log.error(errorMessage);
            return;
        }
        /*
         * This channel must be connected now even if the re-authentication is scheduled
         * for sometime in the future.
         */
        if (!ready()) {
            // should never happen; don't retry
            failReauthenticationDoNotRetry(clientChannelCredentialTracker,
                    "Channel told to re-authenticate when the connection was not ready");
            return;
        }
        /*
         * Re-authentication can only be initiated from the client side of a SASL
         * connection.
         */
        if (!(authenticatedAuthenticator instanceof SaslClientAuthenticator)) {
            // should never happen; don't retry
            failReauthenticationDoNotRetry(clientChannelCredentialTracker,
                    String.format(
                            "Attempt to re-authenticate to a non-SASL Client authenticator  (should never happen): %s",
                            authenticatedAuthenticator.getClass().getName()));
            return;
        }
        /*
         * Instantiate a second authenticator (which also must be for the SASL client
         * side) and tell it to initiate re-authentication. Provide some code in the
         * form of an authentication success/failure receiver that will adjust this
         * channel's state based on the ultimate success or failure of the
         * re-authentication attempt.
         */
        if (notYetAuthenticatedAuthenticator != null)
            failReauthenticationDoNotRetry(clientChannelCredentialTracker,
                    "Channel seems to already be re-authenticating (should nver happen)");
        notYetAuthenticatedAuthenticator = authenticatorSupplier.get();
        if (!(notYetAuthenticatedAuthenticator instanceof SaslClientAuthenticator)) {
            // should never happen; don't retry
            String errorMessage = "notYetAuthenticatedAuthenticator of incorrect type: "
                    + (notYetAuthenticatedAuthenticator == null ? "null"
                            : notYetAuthenticatedAuthenticator.getClass().getName());
            Utils.closeQuietly(notYetAuthenticatedAuthenticator, errorMessage);
            notYetAuthenticatedAuthenticator = null;
            failReauthenticationDoNotRetry(clientChannelCredentialTracker, errorMessage);
            return;
        }
        SaslClientAuthenticator notYetAuthenticatedSaslClientAuthenticator = (SaslClientAuthenticator) notYetAuthenticatedAuthenticator;
        notYetAuthenticatedSaslClientAuthenticator.initiateReauthentication(time,
                new AuthenticationSuccessOrFailureReceiver() {
                    @Override
                    public void reauthenticationFailed(RetryIndication retry, String errorMessage) {
                        log.warn(errorMessage);
                        /*
                         * Indicate that re-authentication failed along with whether or not it should be
                         * retried.
                         */
                        Utils.closeQuietly(notYetAuthenticatedAuthenticator, "notYetAuthenticatedAuthenticator");
                        notYetAuthenticatedAuthenticator = null;
                        clientChannelCredentialTracker.offer(ClientChannelCredentialEvent
                                .channelFailedReauthentication(time, KafkaChannel.this, errorMessage, retry));
                    }

                    @Override
                    public void reauthenticationSucceeded() {
                        ExpiringCredential expiringCredential = notYetAuthenticatedSaslClientAuthenticator
                                .expiringCredential();
                        if (notYetAuthenticatedSaslClientAuthenticator.complete() && expiringCredential != null) {
                            /*
                             * Re-authentication succeeded on the SASL client side, and internal state makes
                             * sense.
                             */
                            clientChannelCredentialTracker.offer(ClientChannelCredentialEvent
                                    .channelReauthenticated(time, KafkaChannel.this, expiringCredential));
                            replaceAnyPreviouslyAuthenticatedAuthenticatorWithNewOne();
                            return;
                        }
                        /*
                         * Re-authentication supposedly succeeded, but something doesn't make sense in
                         * terms of internal state.
                         */
                        String errorMessage = "";
                        if (!notYetAuthenticatedSaslClientAuthenticator.complete()) {
                            errorMessage = "authenticator not complete despite re-authentication supposedly succeeding, ignoring and not retrying (this should never happen)";
                        }
                        if (expiringCredential == null) {
                            errorMessage = errorMessage.isEmpty() ? "" : (errorMessage + "; ");
                            errorMessage = errorMessage
                                    + "no expiring credential available despite re-authentication supposedly succeeding, ignoring and not retrying (this should never happen)";
                        }
                        log.error(errorMessage);
                        /*
                         * Indicate that re-authentication should be considered failed and should not be
                         * retried.
                         */
                        Utils.closeQuietly(notYetAuthenticatedAuthenticator, "notYetAuthenticatedAuthenticator");
                        notYetAuthenticatedAuthenticator = null;
                        failReauthenticationDoNotRetry(clientChannelCredentialTracker, errorMessage);
                    }
                });
    }

    private void failReauthenticationDoNotRetry(ClientChannelCredentialTracker clientChannelCredentialTracker,
            String errorMessage) {
        log.error(errorMessage);
        clientChannelCredentialTracker.offer(ClientChannelCredentialEvent.channelFailedReauthentication(time, this,
                errorMessage, RetryIndication.DO_NOT_RETRY));
    }

    private void replaceAnyPreviouslyAuthenticatedAuthenticatorWithNewOne() {
        // swap the old authenticator (if any) for the new one
        Authenticator tmp = authenticatedAuthenticator;
        authenticatedAuthenticator = notYetAuthenticatedAuthenticator;
        notYetAuthenticatedAuthenticator = null;
        if (tmp != null)
            Utils.closeQuietly(tmp, "previousAuthenticatedAuthenticator");
    }
}
