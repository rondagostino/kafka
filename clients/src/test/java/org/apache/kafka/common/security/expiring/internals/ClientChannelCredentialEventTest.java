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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Date;

import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.security.authenticator.AuthenticationSuccessOrFailureReceiver.RetryIndication;
import org.apache.kafka.common.security.expiring.ExpiringCredential;
import org.apache.kafka.common.security.expiring.internals.ClientChannelCredentialEvent.EventAttributeKey;
import org.apache.kafka.common.security.expiring.internals.ClientChannelCredentialEvent.EventType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

public class ClientChannelCredentialEventTest {
    @Test
    public void shouldCreateCorrectEventWhenChannelInitiallyAuthenticated() {
        Time time = new MockTime();
        KafkaChannel channel = EasyMock.mock(KafkaChannel.class);
        ExpiringCredential credential = EasyMock.mock(ExpiringCredential.class);
        ClientChannelCredentialEvent event = ClientChannelCredentialEvent.channelInitiallyAuthenticated(time, channel,
                credential);
        assertEquals(time.milliseconds(), event.createMs());
        assertSame(channel, event.channel());
        assertSame(credential, event.authenticatingCredential());
        assertSame(EventType.CHANNEL_INITIALLY_AUTHENTICATED, event.type());
        assertNull(event.fromCredential());
        assertNull(event.toCredential());
        assertNull(event.errorMessage());
        assertNull(event.retryIndication());
        assertEquals(2, event.data().size());
        assertSame(channel, event.data().get(EventAttributeKey.CHANNEL));
        assertSame(credential, event.data().get(EventAttributeKey.AUTHENTICATING_CREDENTIAL));
        assertEquals(expectedEventToStringValue(event), event.toString());
    }

    @Test
    public void shouldCreateCorrectEventWhenCredentialRefreshed() {
        Time time = new MockTime();
        ExpiringCredential fromCredential = EasyMock.mock(ExpiringCredential.class);
        ExpiringCredential toCredential = EasyMock.mock(ExpiringCredential.class);
        ClientChannelCredentialEvent event = ClientChannelCredentialEvent.credentialRefreshed(time, fromCredential,
                toCredential);
        assertEquals(time.milliseconds(), event.createMs());
        assertNull(event.channel());
        assertNull(event.authenticatingCredential());
        assertSame(EventType.CREDENTIAL_REFRESHED, event.type());
        assertSame(fromCredential, event.fromCredential());
        assertSame(toCredential, event.toCredential());
        assertNull(event.errorMessage());
        assertNull(event.retryIndication());
        assertEquals(2, event.data().size());
        assertSame(fromCredential, event.data().get(EventAttributeKey.FROM_CREDENTIAL));
        assertSame(toCredential, event.data().get(EventAttributeKey.TO_CREDENTIAL));
        assertEquals(expectedEventToStringValue(event), event.toString());
    }

    @Test
    public void shouldCreateCorrectEventWhenChannelReauthenticated() {
        Time time = new MockTime();
        KafkaChannel channel = EasyMock.mock(KafkaChannel.class);
        ExpiringCredential credential = EasyMock.mock(ExpiringCredential.class);
        ClientChannelCredentialEvent event = ClientChannelCredentialEvent.channelReauthenticated(time, channel,
                credential);
        assertEquals(time.milliseconds(), event.createMs());
        assertSame(channel, event.channel());
        assertSame(credential, event.authenticatingCredential());
        assertSame(EventType.CHANNEL_REAUTHENTICATED, event.type());
        assertNull(event.fromCredential());
        assertNull(event.toCredential());
        assertNull(event.errorMessage());
        assertNull(event.retryIndication());
        assertEquals(2, event.data().size());
        assertSame(channel, event.data().get(EventAttributeKey.CHANNEL));
        assertSame(credential, event.data().get(EventAttributeKey.AUTHENTICATING_CREDENTIAL));
        assertEquals(expectedEventToStringValue(event), event.toString());
    }

    @Test
    public void shouldCreateCorrectEventWhenChannelFailedReauthenticated() {
        Time time = new MockTime();
        KafkaChannel channel = EasyMock.mock(KafkaChannel.class);
        for (String errorMessage : new String[] {"errorMessage", null}) {
            for (RetryIndication retryIndication : RetryIndication.values()) {
                ClientChannelCredentialEvent event = ClientChannelCredentialEvent.channelFailedReauthentication(time,
                        channel, errorMessage, retryIndication);
                assertEquals(time.milliseconds(), event.createMs());
                assertSame(channel, event.channel());
                assertNull(event.authenticatingCredential());
                assertSame(EventType.CHANNEL_FAILED_REAUTHENTICATION, event.type());
                assertNull(event.fromCredential());
                assertNull(event.toCredential());
                if (errorMessage == null)
                    assertNull(event.errorMessage());
                else
                    assertEquals(errorMessage, event.errorMessage());
                assertEquals(retryIndication, event.retryIndication());
                assertEquals(errorMessage == null ? 2 : 3, event.data().size());
                assertSame(channel, event.data().get(EventAttributeKey.CHANNEL));
                if (errorMessage != null)
                    assertEquals(errorMessage, event.data().get(EventAttributeKey.ERROR_MESSAGE));
                assertSame(retryIndication, event.data().get(EventAttributeKey.RETRY_INDICATION));
                assertEquals(expectedEventToStringValue(event), event.toString());
            }
        }
    }

    @Test
    public void shouldCreateCorrectEventWhenChannelDisconnectedc() {
        Time time = new MockTime();
        KafkaChannel channel = EasyMock.mock(KafkaChannel.class);
        ClientChannelCredentialEvent event = ClientChannelCredentialEvent.channelDisconnected(time, channel);
        assertEquals(time.milliseconds(), event.createMs());
        assertSame(channel, event.channel());
        assertNull(event.authenticatingCredential());
        assertSame(EventType.CHANNEL_DISCONNECTED, event.type());
        assertNull(event.fromCredential());
        assertNull(event.toCredential());
        assertNull(event.errorMessage());
        assertNull(event.retryIndication());
        assertEquals(1, event.data().size());
        assertSame(channel, event.data().get(EventAttributeKey.CHANNEL));
        assertEquals(expectedEventToStringValue(event), event.toString());
    }

    private static String expectedEventToStringValue(ClientChannelCredentialEvent event) {
        return event.type() + " created at " + new Date(event.createMs()) + ": " + event.data();
    }
}