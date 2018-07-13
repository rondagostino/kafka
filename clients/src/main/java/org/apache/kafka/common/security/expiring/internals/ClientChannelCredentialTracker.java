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
package org.apache.kafka.common.security.expiring.internals;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.security.authenticator.AuthenticationSuccessOrFailureReceiver.RetryIndication;
import org.apache.kafka.common.security.expiring.ExpiringCredential;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the authentication/refresh/re-authentication state transitions for
 * instances of {@link ExpiringCredential} and the {@link KafkaChannel}
 * instances on the SASL Client side that use them.
 * <p>
 * An instance of this class is added to the private credentials of the
 * {@code Subject} associated with the client JAAS configuration where is can be
 * retrieved and leveraged by the {@link KafkaChannel} when it
 * authenticates/re-authenticates/disconnects and by the
 * {@link ExpiringCredentialRefreshingLogin} when it refreshes an
 * {@link ExpiringCredential}.
 * <p>
 * Incoming events of type {@link ClientChannelCredentialEvent} are enqueued via
 * {@link #offer(ClientChannelCredentialEvent)}. Events of interest can be
 * instantiated via the following methods:
 * <ul>
 * <li>{@link ClientChannelCredentialEvent#channelInitiallyAuthenticated(Time, KafkaChannel, ExpiringCredential)}</li>
 * <li>{@link ClientChannelCredentialEvent#credentialRefreshed(Time, ExpiringCredential, ExpiringCredential)}</li>
 * <li>{@link ClientChannelCredentialEvent#channelReauthenticated(Time, KafkaChannel, ExpiringCredential)}</li>
 * <li>{@link ClientChannelCredentialEvent#channelFailedReauthentication(Time, KafkaChannel, String, RetryIndication)}</li>
 * <li>{@link ClientChannelCredentialEvent#channelDisconnected(Time, KafkaChannel)}</li>
 * </ul>
 * Incoming events are processed asynchronously in a separate thread in a
 * serialized manner in the order in which they are received. The incoming event
 * processing thread invokes {@link KafkaChannel#initiateReauthentication(Time)}
 * when it determines that a {@link KafkaChannel} needs to begin
 * re-authenticating immediately; it is then up to the {@code KafkaChannel}
 * instance to enqueue the initial request in the re-authentication dance and
 * return immediately while ensuring that the ultimate success or failure of the
 * re-authentication is reported back to this instance.
 * <p>
 * The incoming event processing thread enqueues outgoing re-authentication
 * events that need to be delayed to some future time (due to a failed
 * re-authentication attempt with an ensuing back-off) in a separate prioritized
 * queue, and an outgoing event processor thread is responsible for invoking
 * {@link KafkaChannel#initiateReauthentication(Time)} at the appropriate time
 * for each such enqueued event.
 */
public class ClientChannelCredentialTracker {
    /**
     * The state of a credential: ACTIVE or REFRESHED
     */
    private enum CredentialState {
        ACTIVE, REFRESHED;
    }

    /**
     * Information related to a channel that has failed re-authentication one or
     * more times since it was last successfully authenticated and has yet to
     * successfully re-authenticate since then
     */
    private static class FailedReauthenticationInfo {
        private final int numReauthenticationFailures;

        /**
         * Constructor; the re-authentication failure count is 1 plus the prior
         * re-authentication failure count, if any
         * 
         * @param priorFailedReauthenticationInfo
         *            the optional information describing the prior re-authentication
         *            failure
         */
        public FailedReauthenticationInfo(FailedReauthenticationInfo priorFailedReauthenticationInfo) {
            this.numReauthenticationFailures = priorFailedReauthenticationInfo == null ? 1
                    : 1 + priorFailedReauthenticationInfo.numReauthenticationFailures;
        }

        /**
         * Return the (always positive) number of times a re-authentication failure has
         * occurred
         * 
         * @return the (always positive) number of times a re-authentication failure has
         *         occurred
         */
        public int numReauthenticationFailures() {
            return numReauthenticationFailures;
        }

        @Override
        public String toString() {
            return String.format("failed re-authentication count = %d", numReauthenticationFailures);
        }
    }

    /**
     * The state of a channel, including the credential that authenticated it and
     * any failed re-authentication information associated with it
     */
    private static class ChannelState {
        private final ExpiringCredential credential;
        private final FailedReauthenticationInfo failedReauthenticationInfo;

        /**
         * Constructor to use when an initial authentication occurs
         * 
         * @param credential
         *            the mandatory credential that authenticated a channel
         */
        public ChannelState(ExpiringCredential credential) {
            this(requireNonNull(credential), null);
        }

        /**
         * Constructor to use when a subsequent re-authentication failure occurs
         * 
         * @param credential
         *            the mandatory credential that authenticated a channel
         * @param failedReauthenticationInfo
         *            the optional failed re-authentication information
         */
        public ChannelState(ExpiringCredential credential, FailedReauthenticationInfo failedReauthenticationInfo) {
            this.credential = requireNonNull(credential);
            this.failedReauthenticationInfo = failedReauthenticationInfo;
        }

        /**
         * Return the always non-null credential that authenticated this channel
         * 
         * @return the always non-null credential that authenticated this channel
         */
        public ExpiringCredential credential() {
            return credential;
        }

        /**
         * Return the (potentially null) failed re-authentication information
         * 
         * @return the (potentially null) failed re-authentication information
         */
        public FailedReauthenticationInfo failedReauthenticationInfo() {
            return failedReauthenticationInfo;
        }

        @Override
        public int hashCode() {
            return credential.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!(obj instanceof ChannelState))
                return false;
            ChannelState that = (ChannelState) obj;
            if (failedReauthenticationInfo == null && that.failedReauthenticationInfo == null)
                return credential.equals(that.credential);
            if (failedReauthenticationInfo == null || that.failedReauthenticationInfo == null)
                return false;
            return failedReauthenticationInfo.numReauthenticationFailures() == that.failedReauthenticationInfo
                    .numReauthenticationFailures() && credential.equals(that.credential);
        }

        @Override
        public String toString() {
            return String.format("credential principal=%s expiring at %s, %s", credential.principalName(),
                    new Date(credential.expireTimeMs()), failedReauthenticationInfo == null ? "not yet re-authenticated"
                            : failedReauthenticationInfo.toString());
        }
    }

    /*
     * Runnable that processes enqueued incoming events
     */
    private class IncomingEventProcessor implements Runnable {
        public static final long MIN_MILLIS_BEFORE_REAUTHENTICATION_FAILURE_RETRY = 1000L * 60; // 1 minute
        // 10 minutes is long enough to wait before purging an expired credential
        private static final int MIN_MILLIS_DELAY_BEFORE_EXPIRED_CREDENTIAL_IS_PURGED = 1000 * 60 * 10;
        /*
         * Every channel that we know about appears as a key in this map; the channel
         * state, including the credential that authenticated the channel (regardless of
         * whether that credential is the ACTIVE state or not) and any failed
         * re-authentication information, is its value.
         */
        private final Map<KafkaChannel, ChannelState> channelStates = new HashMap<>();
        /*
         * Every credential we know about appears as a key in this map; the credential's
         * state (ACTIVE vs. REFRESHED) is its value.
         */
        private final Map<ExpiringCredential, CredentialState> credentialStates = new HashMap<>();

        @Override
        public void run() {
            while (true)
                try {
                    ClientChannelCredentialEvent event = incomingEventQueue.take();
                    log.info("Processing {}", event);
                    switch (event.type()) {
                        case CHANNEL_INITIALLY_AUTHENTICATED:
                            handleChannelInitiallyAuthenticatedEvent(event);
                            break;
                        case CREDENTIAL_REFRESHED:
                            handleCredentialRefreshedEvent(event);
                            break;
                        case CHANNEL_REAUTHENTICATED:
                            handleChannelReauthenticatedEvent(event);
                            break;
                        case CHANNEL_FAILED_REAUTHENTICATION:
                            handleChannelFailedReauthenticationEvent(event);
                            break;
                        case CHANNEL_DISCONNECTED:
                            handleChannelDisconnectedEvent(event);
                            break;
                        default:
                            log.warn("Ignoring channel credential event of unknown type {} (should not happen): {}",
                                    event.type(), event);
                    }
                } catch (InterruptedException e) {
                    // exit the thread
                    log.info(
                            "Channel credential manager thread interrupted; exiting with current incoming event queue size = {}",
                            incomingEventQueue.size());
                    return;
                } catch (Exception e) {
                    // log the exception but keep the thread alive
                    log.error(String.format("Ignoring exception and continuing to process events: %s", e.getMessage()),
                            e);
                }
        }

        private void handleChannelInitiallyAuthenticatedEvent(ClientChannelCredentialEvent event) {
            if (event.authenticatingCredential() == null || event.channel() == null)
                ignoreMalformed(event);
            else if (Integer.parseInt(event.channel().id()) < 0)
                log.info("Ignoring event for bootstrap connection (with id < 0): {}", event);
            else
                recordInitialAuthenticationEvent(event.authenticatingCredential(), event.channel(), event.createMs());
        }

        /**
         * Record the fact that the given channel has been initially authenticated with
         * the given credential. The credential state will first be set to
         * {@link CredentialState#ACTIVE} if the credential is not yet known. The
         * channel state will be set to the given credential with no re-authentication
         * failure information. If the credential state is
         * {@link CredentialState#REFRESHED} then the channel will told to immediately
         * re-authenticate.
         * 
         * @param credential
         *            the mandatory credential that initially authenticated the channel
         * @param channel
         *            the mandatory channel that has been initially authenticated with
         *            the given credential
         * @param whenMs
         *            when the event occurred, expressed as the number of milliseconds
         *            since the epoch
         */
        private void recordInitialAuthenticationEvent(ExpiringCredential credential, KafkaChannel channel,
                long whenMs) {
            requireNonNull(credential);
            requireNonNull(channel);
            Supplier<String> logMsgSupplierForImmediateReauthentication = () -> String.format(
                    "Told channel with id=%s that it should immediately re-authenticate after initially authenticating with already-refreshed credential (principal=%s, expiration=%s)",
                    channel.id(), credential.principalName(), new Date(credential.expireTimeMs()));
            Supplier<String> logMsgSupplierForAuthenticationWithAnActiveCredential = () -> String.format(
                    "Initially authenticated channel with id=%s using active credential (principal=%s, expiration=%s)",
                    channel.id(), credential.principalName(), new Date(credential.expireTimeMs()));
            Supplier<String> logMsgSupplier = recordAuthenticationEvent(credential, channel, whenMs,
                    logMsgSupplierForImmediateReauthentication, logMsgSupplierForAuthenticationWithAnActiveCredential);
            if (logMsgSupplier != null)
                log.info(logMsgSupplier.get());
            purgeExpiredCredentials();
        }

        private void handleCredentialRefreshedEvent(ClientChannelCredentialEvent event) {
            if (event.fromCredential() == null || event.toCredential() == null)
                ignoreMalformed(event);
            else
                recordRefreshedEvent(event.fromCredential(), event.toCredential(), event.createMs());
        }

        /**
         * Record the fact that a credential has been refreshed and is to be replaced
         * with another credential. Any channels associated with the refreshed
         * credential will be told to re-authenticate.
         * 
         * @param fromCredential
         *            the mandatory credential that was refreshed
         * @param toCredential
         *            the mandatory credential that replaces the refreshed one
         * @param whenMs
         *            when the event occurred, expressed as the number of milliseconds
         *            since the epoch
         */
        private void recordRefreshedEvent(ExpiringCredential fromCredential, ExpiringCredential toCredential,
                long whenMs) {
            requireNonNull(fromCredential);
            requireNonNull(toCredential);
            CredentialState alreadyRecordedCredentialStateForRefreshedCredential = credentialStates.put(fromCredential,
                    CredentialState.REFRESHED);
            if (CredentialState.ACTIVE != alreadyRecordedCredentialStateForRefreshedCredential)
                // either null or REFRESHED, so no channels were associated with it
                return;
            /*
             * Tell all of the channels authenticated with the refreshed credential to
             * re-authenticate
             */
            for (Entry<KafkaChannel, ChannelState> channelState : channelStates.entrySet()) {
                KafkaChannel channel = channelState.getKey();
                if (fromCredential.equals(channelState.getValue().credential())) {
                    initiateReauthentication(channel, whenMs);
                    log.info(
                            "Told channel with id={} to re-authenticate due to refresh of credential (principal={}, expiration={}) wth new credential (principal={}, expiration={})",
                            channel.id(), fromCredential.principalName(), new Date(fromCredential.expireTimeMs()),
                            toCredential.principalName(), new Date(toCredential.expireTimeMs()));
                }
            }
            purgeExpiredCredentials();
        }

        private void handleChannelReauthenticatedEvent(ClientChannelCredentialEvent event) {
            if (event.authenticatingCredential() == null || event.channel() == null)
                ignoreMalformed(event);
            else
                recordChannelReauthenticationEvent(event.authenticatingCredential(), event.channel(), event.createMs());
        }

        /**
         * Record the fact that a channel has been successfully re-authenticated with a
         * credential. The channel state will be set to the given credential with no
         * re-authentication failure information. If the credential state is
         * {@link CredentialState#REFRESHED} then the channel will told to immediately
         * re-authenticate.
         * 
         * @param credential
         *            the mandatory credential that re-authenticated the channel
         * @param channel
         *            the mandatory channel that has been re-authenticated with the
         *            given credential
         * @param whenMs
         *            when the event occurred, expressed as the number of milliseconds
         *            since the epoch
         */
        private void recordChannelReauthenticationEvent(ExpiringCredential credential, KafkaChannel channel,
                long whenMs) {
            requireNonNull(credential);
            requireNonNull(channel);
            Supplier<String> logMsgSupplierForImmediateReauthentication = () -> String.format(
                    "Told channel with id=%s that it should immediately re-authenticate after upon re-authenticating with already-refreshed credential (principal=%s, expiration=%s)",
                    channel.id(), credential.principalName(), new Date(credential.expireTimeMs()));
            Supplier<String> logMsgSupplierForAuthenticationWithAnActiveCredential = () -> String.format(
                    "Re-authenticated channel with id=%s using active credential (principal=%s, expiration=%s)",
                    channel.id(), credential.principalName(), new Date(credential.expireTimeMs()));
            Supplier<String> logMsgSupplier = recordAuthenticationEvent(credential, channel, whenMs,
                    logMsgSupplierForImmediateReauthentication, logMsgSupplierForAuthenticationWithAnActiveCredential);
            if (logMsgSupplier != null)
                log.info(logMsgSupplier.get());
            purgeExpiredCredentials();
        }

        private void handleChannelFailedReauthenticationEvent(ClientChannelCredentialEvent event) {
            if (event.channel() == null || event.retryIndication() == null)
                ignoreMalformed(event);
            else
                recordChannelFailedReauthenticationEvent(event.channel(), event.createMs(), event.retryIndication(),
                        event.errorMessage());
        }

        /**
         * Record the fact that a channel failed re-authenticated. The channel state
         * will be adjusted to reflect the correct number of failed re-authentications,
         * and if retry is allowed the channel will be told to re-authenticate in 1, 2,
         * or 4 minutes depending on whether this is the first, second, or third
         * re-authentication failure, with subsequent attempts only allowed for certain
         * classes of failure.
         * 
         * @param channel
         *            the mandatory channel that has been re-authenticated with the
         *            given credential
         * @param whenMs
         *            when the event occurred, expressed as the number of milliseconds
         *            since the epoch
         * @param retryIndication
         *            whether or not re-authentication should be retried
         * @param errorMessage
         *            the optional error message describing why re-authentication failed
         */
        private void recordChannelFailedReauthenticationEvent(KafkaChannel channel, long whenMs,
                RetryIndication retryIndication, String errorMessage) {
            requireNonNull(channel);
            ChannelState alreadyRecordedChannelState = channelStates.get(channel);
            if (alreadyRecordedChannelState == null) {
                log.debug(String.format(
                        "Unknown channel id=%s (it must have already closed); ignoring failed re-authentication event enqueued at %s",
                        channel.id(), new Date(whenMs)));
                return;
            }
            log.warn(String.format("Channel with id=%s failed re-authentication, retry=%s: %s", channel.id(),
                    retryIndication, errorMessage));
            if (retryIndication == RetryIndication.DO_NOT_RETRY || (retryIndication == RetryIndication.RETRY_LIMITED
                    && alreadyRecordedChannelState.failedReauthenticationInfo().numReauthenticationFailures > 2)) {
                log.debug(String.format(
                        "Not retrying re-authentication for channel id=%s; ignoring failed re-authentication event enqueued at %s with retry=%s",
                        channel.id(), new Date(whenMs), retryIndication));
                channelStates.remove(channel);
                purgeExpiredCredentials();
                return;
            }
            ChannelState newChannelState = new ChannelState(alreadyRecordedChannelState.credential(),
                    new FailedReauthenticationInfo(alreadyRecordedChannelState.failedReauthenticationInfo()));
            channelStates.put(channel, newChannelState);
            long whenReauthenticateMs;
            switch (newChannelState.failedReauthenticationInfo().numReauthenticationFailures) {
                case 1:
                    whenReauthenticateMs = whenMs + 1000 * 60 * 1;
                    break;
                case 2:
                    whenReauthenticateMs = whenMs + 1000 * 60 * 2;
                    break;
                default:
                    whenReauthenticateMs = whenMs + 1000 * 60 * 4;
            }
            initiateReauthentication(channel, whenReauthenticateMs);
            purgeExpiredCredentials();
        }

        private void handleChannelDisconnectedEvent(ClientChannelCredentialEvent event) {
            if (event.channel() == null)
                ignoreMalformed(event);
            else
                recordChannelDisconnectedOrClosedEvent(event.channel(), event.createMs());
        }

        /**
         * Record the fact that a channel has been disconnected and/or closed and
         * therefore we no longer need to track it
         * 
         * @param channel
         *            the mandatory channel that was disconnected and/or closed
         * @param whenMs
         *            when the event occurred, expressed as the number of milliseconds
         *            since the epoch
         */
        private void recordChannelDisconnectedOrClosedEvent(KafkaChannel channel, long whenMs) {
            requireNonNull(channel);
            channelStates.remove(channel);
            purgeExpiredCredentials();
        }

        private void purgeExpiredCredentials() {
            // remove credentials that have been expired for some time
            long now = time.milliseconds();
            Set<ExpiringCredential> purgedCredentials = new HashSet<>();
            for (Iterator<ExpiringCredential> iterator = this.credentialStates.keySet().iterator(); iterator
                    .hasNext();) {
                ExpiringCredential credential = iterator.next();
                if (credentialConsideredExpiredIncludingAnyExtraBufferTimeJustToBeSafe(credential, now)) {
                    iterator.remove();
                    purgedCredentials.add(credential);
                }
            }
            if (purgedCredentials.isEmpty())
                return;
            /*
             * Tell any channel still authenticated with a purged credential to
             * re-authenticate. This should never happen in general, but we do it just to be
             * safe.
             */
            for (Entry<KafkaChannel, ChannelState> entry : channelStates.entrySet()) {
                ExpiringCredential credential = entry.getValue().credential();
                if (purgedCredentials.contains(credential)) {
                    KafkaChannel channel = entry.getKey();
                    initiateReauthentication(channel, now);
                    log.warn(
                            "Told channel with id={} to re-authenticate because it is still using an expired credential (principal={}, expiration={}); this should generally not happen",
                            channel.id(), credential.principalName(), new Date(credential.expireTimeMs()));
                }
            }
        }

        private Supplier<String> recordAuthenticationEvent(ExpiringCredential credential, KafkaChannel channel,
                long whenMs, Supplier<String> logMsgSupplierForImmediateReauthentication,
                Supplier<String> logMsgSupplierForAuthenticationWithAnActiveCredential) {
            ChannelState newChannelState = new ChannelState(credential);
            ChannelState alreadyRecordedChannelState = channelStates.put(channel, newChannelState);
            Supplier<String> logMsgSuffixSupplier = null;
            if (alreadyRecordedChannelState != null) {
                if (newChannelState.equals(alreadyRecordedChannelState))
                    // be idempotent
                    return null;
                if (credential.equals(alreadyRecordedChannelState.credential())) {
                    log.warn(String.format(
                            "Channel with id=%s was recorded as being re-authenticated with the same credential (principal=%s, expiration=%s); ignoring this ocurrence (which generally should not happen)",
                            channel.id(), credential.principalName(), new Date(credential.expireTimeMs())));
                    return null;
                }
                logMsgSuffixSupplier = () -> String.format(
                        " (original credential prior to re-authentication was principal=%s, expiration=%s)",
                        alreadyRecordedChannelState.credential().principalName(),
                        new Date(alreadyRecordedChannelState.credential().expireTimeMs()));
            }
            boolean credentialConsideredExpired = credentialConsideredExpiredIncludingAnyExtraBufferTimeJustToBeSafe(
                    credential, whenMs);
            CredentialState alreadyRecordedCredentialState = null;
            if (!credentialConsideredExpired)
                alreadyRecordedCredentialState = credentialStates.putIfAbsent(credential, CredentialState.ACTIVE);
            if (credentialConsideredExpired || CredentialState.REFRESHED == alreadyRecordedCredentialState) {
                // tell the channel to re-authenticate
                initiateReauthentication(channel, whenMs);
                if (logMsgSuffixSupplier == null)
                    return logMsgSupplierForImmediateReauthentication;
                final Supplier<String> logMsgSuffixSupplierFinal = logMsgSuffixSupplier;
                return () -> String.format("%s%s", logMsgSupplierForImmediateReauthentication.get(),
                        logMsgSuffixSupplierFinal.get());
            }
            if (logMsgSuffixSupplier == null)
                return logMsgSupplierForAuthenticationWithAnActiveCredential;
            final Supplier<String> logMsgSuffixSupplierFinal = logMsgSuffixSupplier;
            return () -> String.format("%s%s", logMsgSupplierForAuthenticationWithAnActiveCredential.get(),
                    logMsgSuffixSupplierFinal.get());
        }

        /*
         * It is not straightforward for us to definitively know that a channel won't
         * authenticate with a refreshed (or even an expired) credential. The reason is
         * because it takes some time for channel to authenticate, so an authentication
         * handshake could start before a credential is refreshed and then the
         * authentication could finish after the credential is refreshed. Also, the
         * broker may consider an expired token to be valid for some additional amount
         * of time to allow for potential clock drift. Therefore we decide to purge a
         * credential after some additional time has elapsed past its expiration.
         */
        private boolean credentialConsideredExpiredIncludingAnyExtraBufferTimeJustToBeSafe(
                ExpiringCredential credential, long asOfMs) {
            return credential.expireTimeMs() + MIN_MILLIS_DELAY_BEFORE_EXPIRED_CREDENTIAL_IS_PURGED <= asOfMs;
        }

        private void ignoreMalformed(ClientChannelCredentialEvent event) {
            log.error(String.format("Ignoring malformed event: %s", event));
        }

        private void initiateReauthentication(KafkaChannel channel, long whenMs) {
            if (time.milliseconds() >= whenMs)
                // initiate now to keep the queue empty if possible
                channel.initiateReauthentication(time);
            else
                // schedule it for the given future time
                outgoingScheduledEventQueue.add(new ChannelReauthenticationEvent(channel, whenMs));
        }
    }

    private static class ChannelReauthenticationEvent implements Comparable<ChannelReauthenticationEvent> {
        private final KafkaChannel channel;
        private final long onOrAfter;

        public ChannelReauthenticationEvent(KafkaChannel channel, long onOrAfter) {
            this.onOrAfter = onOrAfter;
            this.channel = requireNonNull(channel);
        }

        @Override
        public int compareTo(ChannelReauthenticationEvent o) {
            return Long.compare(onOrAfter, o.onOrAfter);
        }

        public KafkaChannel channel() {
            return channel;
        }

        public long onOrAfter() {
            return onOrAfter;
        }
    }

    /*
     * Runnable that processes enqueued outgoing events in a scheduled fashion
     */
    private class OutgoingScheduledEventProcessor implements Runnable {
        @Override
        public void run() {
            while (true)
                try {
                    ChannelReauthenticationEvent nextScheduledEvent = outgoingScheduledEventQueue.poll(Long.MAX_VALUE,
                            TimeUnit.NANOSECONDS);
                    long now = time.milliseconds();
                    if (nextScheduledEvent.onOrAfter() <= now)
                        nextScheduledEvent.channel().initiateReauthentication(time);
                    else {
                        // put it back in the priority queue and sleep a bit
                        outgoingScheduledEventQueue.add(nextScheduledEvent);
                        Thread.sleep(Math.max(nextScheduledEvent.onOrAfter() - now,
                                IncomingEventProcessor.MIN_MILLIS_BEFORE_REAUTHENTICATION_FAILURE_RETRY));
                    }
                } catch (InterruptedException e) {
                    // exit the thread
                    log.info(
                            "Channel credential manager thread interrupted; exiting with current incoming event queue size = {}",
                            incomingEventQueue.size());
                    return;
                } catch (Exception e) {
                    // log the exception but keep the thread alive
                    log.error(String.format("Ignoring exception and continuing to process events: %s", e.getMessage()),
                            e);
                }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClientChannelCredentialTracker.class);
    private final Time time;
    /*
     * Make this queue big enough so that it will never fill up under normal
     * circumstances but not so big that in case of a bug and it does fill up it
     * won't cause an out-of-memory situation.
     */
    private final LinkedBlockingQueue<ClientChannelCredentialEvent> incomingEventQueue = new LinkedBlockingQueue<>(
            1000);
    private final Thread incomingEventProcessorThread;
    private final PriorityBlockingQueue<ChannelReauthenticationEvent> outgoingScheduledEventQueue = new PriorityBlockingQueue<>();
    private final Thread outgoingEventProcessorThread;

    /**
     * Default constructor
     */
    public ClientChannelCredentialTracker() {
        this(Time.SYSTEM);
    }

    /**
     * Constructor for testing
     * 
     * @param time
     *            the mandatory {@code Time}
     */
    ClientChannelCredentialTracker(Time time) {
        this.time = Objects.requireNonNull(time);
        incomingEventProcessorThread = KafkaThread.daemon(
                "kafka-channel-credential-manager-incoming-event-processor-thread", new IncomingEventProcessor());
        incomingEventProcessorThread.start();
        outgoingEventProcessorThread = KafkaThread.daemon(
                "kafka-channel-credential-manager-outgoinging-event-processor-thread",
                new OutgoingScheduledEventProcessor());
        outgoingEventProcessorThread.start();
    }

    public void close() {
        if (incomingEventProcessorThread.isAlive()) {
            incomingEventProcessorThread.interrupt();
            try {
                incomingEventProcessorThread.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for event processor thread to shutdown.", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Enqueue the given event for asynchronous processing. Return false only if the
     * event could not be enqueued; this should never happen, and it signals an
     * internal error if it occurs.
     * 
     * @param event
     *            the mandatory event
     * @return true if the given event was able to be enqueued, otherwise false
     */
    public boolean offer(ClientChannelCredentialEvent event) {
        return incomingEventQueue.offer(Objects.requireNonNull(event, "event must not be null"));
    }

    private static ExpiringCredential requireNonNull(ExpiringCredential credential) {
        return Objects.requireNonNull(credential, "credential must not be null");
    }

    private static KafkaChannel requireNonNull(KafkaChannel channel) {
        return Objects.requireNonNull(channel, "channel must not be null");
    }
}
