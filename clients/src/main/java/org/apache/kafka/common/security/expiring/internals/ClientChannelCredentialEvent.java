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

import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.security.authenticator.AuthenticationSuccessOrFailureReceiver.RetryIndication;
import org.apache.kafka.common.security.expiring.ExpiringCredential;
import org.apache.kafka.common.utils.Time;

/**
 * An event that occurred related to channels and credentials
 */
public class ClientChannelCredentialEvent {
    public enum EventType {
        CHANNEL_INITIALLY_AUTHENTICATED,
        CREDENTIAL_REFRESHED,
        CHANNEL_REAUTHENTICATED,
        CHANNEL_FAILED_REAUTHENTICATION,
        CHANNEL_DISCONNECTED
    }

    public enum EventAttributeKey {
        CHANNEL(KafkaChannel.class),
        AUTHENTICATING_CREDENTIAL(ExpiringCredential.class),
        FROM_CREDENTIAL(ExpiringCredential.class),
        TO_CREDENTIAL(ExpiringCredential.class),
        RETRY_INDICATION(RetryIndication.class),
        ERROR_MESSAGE(String.class);

        private final Class<?> valueClass;

        private EventAttributeKey(Class<?> valueClass) {
            this.valueClass = valueClass;
        }

        public Class<?> valueClass() {
            return valueClass;
        }

        public Object confirmCorrectType(Object obj) {
            if (!valueClass.isAssignableFrom(obj.getClass()))
                throw new IllegalArgumentException(String.format("Incorrect type (expected %s): %s",
                        valueClass.getSimpleName(), obj.getClass().getSimpleName()));
            return obj;
        }
    }

    private final EventType type;
    private final long createMs;
    private final Map<EventAttributeKey, Object> data;

    /**
     * Return the always non-null type
     * 
     * @return the always non-null type
     */
    public EventType type() {
        return type;
    }

    /**
     * Return the potentially null channel
     * 
     * @return the potentially null channel
     */
    public KafkaChannel channel() {
        return (KafkaChannel) data.get(EventAttributeKey.CHANNEL);
    }

    /**
     * Return the always non-null and unmodifiable (but possible empty) map defining
     * the attributes related to the event
     * 
     * @return the always non-null and unmodifiable (but possible empty) map
     *         defining the attributes related to the event
     */
    public Map<EventAttributeKey, Object> data() {
        return data;
    }

    /**
     * Return the potentially null credential that a channel used to successfully
     * either initially authenticate or re-authenticate
     * 
     * @return the potentially null credential that a channel used to successfully
     *         either initially authenticate or re-authenticate
     */
    public ExpiringCredential authenticatingCredential() {
        return (ExpiringCredential) data.get(EventAttributeKey.AUTHENTICATING_CREDENTIAL);
    }

    /**
     * Return the potentially null credential that was refreshed
     * 
     * @return the potentially null credential that was refreshed
     */
    public ExpiringCredential fromCredential() {
        return (ExpiringCredential) data.get(EventAttributeKey.FROM_CREDENTIAL);
    }

    /**
     * Return the potentially null credential that replaces a refreshed credential
     * 
     * @return the potentially null credential that replaces a refreshed credential
     */
    public ExpiringCredential toCredential() {
        return (ExpiringCredential) data.get(EventAttributeKey.TO_CREDENTIAL);
    }

    /**
     * Return when this event was created, expressed as the number of milliseconds
     * since the epoch
     * 
     * @return when this event was created, expressed as the number of milliseconds
     *         since the epoch
     */
    public long createMs() {
        return createMs;
    }

    /**
     * Return the potentially null indication as to whether or not to retry
     * re-authentication
     * 
     * @return the potentially null indication as to whether or not to retry
     *         re-authentication
     */
    RetryIndication retryIndication() {
        return (RetryIndication) data.get(EventAttributeKey.RETRY_INDICATION);
    }

    /**
     * Return the potentially null error message associated with a failed
     * re-authentication
     * 
     * @return the potentially null error message associated with a failed
     *         re-authentication
     */
    String errorMessage() {
        return (String) data.get(EventAttributeKey.ERROR_MESSAGE);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type);
        sb.append(" created at ").append(new Date(createMs));
        sb.append(": ").append(data.toString());
        return sb.toString();
    }

    /**
     * A channel initially authenticated with a credential
     * 
     * @param time
     *            the mandatory time
     * @param channel
     *            the mandatory channel
     * @param credential
     *            the mandatory credential
     * @return the corresponding event
     */
    public static ClientChannelCredentialEvent channelInitiallyAuthenticated(Time time, KafkaChannel channel,
            ExpiringCredential credential) {
        return new ClientChannelCredentialEvent(requireNonNull(time), EventType.CHANNEL_INITIALLY_AUTHENTICATED,
                EventAttributeKey.CHANNEL, requireNonNull(channel), EventAttributeKey.AUTHENTICATING_CREDENTIAL,
                requireNonNull(credential));
    }

    /**
     * A credential was refreshed
     * 
     * @param time
     *            the mandatory time
     * @param fromCredential
     *            the mandatory credential that was refreshed
     * @param toCredential
     *            the mandatory credential that replaces the refreshed credential
     * @return the corresponding event
     */
    public static ClientChannelCredentialEvent credentialRefreshed(Time time, ExpiringCredential fromCredential,
            ExpiringCredential toCredential) {
        return new ClientChannelCredentialEvent(requireNonNull(time), EventType.CREDENTIAL_REFRESHED,
                EventAttributeKey.FROM_CREDENTIAL, requireNonNull(fromCredential), EventAttributeKey.TO_CREDENTIAL,
                requireNonNull(toCredential));
    }

    /**
     * A channel successfully re-authenticated with a credential
     * 
     * @param time
     *            the mandatory time
     * @param channel
     *            the mandatory channel
     * @param credential
     *            the mandatory credential
     * @return the corresponding event
     */
    public static ClientChannelCredentialEvent channelReauthenticated(Time time, KafkaChannel channel,
            ExpiringCredential credential) {
        return new ClientChannelCredentialEvent(requireNonNull(time), EventType.CHANNEL_REAUTHENTICATED,
                EventAttributeKey.CHANNEL, requireNonNull(channel), EventAttributeKey.AUTHENTICATING_CREDENTIAL,
                requireNonNull(credential));
    }

    /**
     * A channel failed to re-authenticated
     * 
     * @param time
     *            the mandatory time
     * @param channel
     *            the mandatory channel
     * @param errorMessage
     *            the optional error message describing why re-authentication failed
     * @param retryIndication
     *            whether a retry should be attempted or not
     * @return the corresponding event
     */
    public static ClientChannelCredentialEvent channelFailedReauthentication(Time time, KafkaChannel channel,
            String errorMessage, RetryIndication retryIndication) {
        return new ClientChannelCredentialEvent(requireNonNull(time), EventType.CHANNEL_FAILED_REAUTHENTICATION,
                EventAttributeKey.CHANNEL, requireNonNull(channel), EventAttributeKey.ERROR_MESSAGE, errorMessage,
                EventAttributeKey.RETRY_INDICATION, Objects.requireNonNull(retryIndication));
    }

    /**
     * A channel was disconnected
     * 
     * @param time
     *            the mandatory time
     * @param channel
     *            the mandatory channel
     * @return the corresponding event
     */
    public static ClientChannelCredentialEvent channelDisconnected(Time time, KafkaChannel channel) {
        return new ClientChannelCredentialEvent(requireNonNull(time), EventType.CHANNEL_DISCONNECTED,
                EventAttributeKey.CHANNEL, requireNonNull(channel));
    }

    private ClientChannelCredentialEvent(Time time, EventType type, Object... optionalDataValues) {
        this.createMs = time.milliseconds();
        this.type = type;
        EnumMap<EventAttributeKey, Object> optionalData = new EnumMap<>(EventAttributeKey.class);
        for (int i = 0; i < optionalDataValues.length - 1; i = i + 2) {
            Object value = optionalDataValues[i + 1];
            if (value != null) {
                EventAttributeKey key = (EventAttributeKey) optionalDataValues[i];
                optionalData.put(key, key.confirmCorrectType(value));
            }
        }
        this.data = Collections.unmodifiableMap(optionalData);
    }

    private static Time requireNonNull(Time time) {
        return Objects.requireNonNull(time, "time must not be null");
    }

    private static KafkaChannel requireNonNull(KafkaChannel channel) {
        return Objects.requireNonNull(channel, "channel must not be null");
    }

    private static ExpiringCredential requireNonNull(ExpiringCredential credential) {
        return Objects.requireNonNull(credential, "credential must not be null");
    }
}