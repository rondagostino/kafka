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
package org.apache.kafka.common.security.oauthbearer.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.easymock.EasyMockSupport;
import org.junit.Test;

public class OAuthBearerSaslClientTest extends EasyMockSupport {
    private static final byte[] EMPTY_CHALLENGE = "".getBytes();
    private static final OAuthBearerToken TEST_TOKEN = new OAuthBearerToken() {
        @Override
        public String value() {
            return "value";
        }

        @Override
        public Set<String> scope() {
            return Collections.emptySet();
        }

        @Override
        public long lifetimeMs() {
            return 100;
        }

        @Override
        public String principalName() {
            return "principalName";
        }

        @Override
        public Long startTimeMs() {
            return null;
        }
    };
    private static final OAuthBearerToken TEST_TOKEN_ALSO_AS_EXPIRING_CREDENTIAL = new OAuthBearerTokenExpiringCredential(
            TEST_TOKEN);
    private static final OAuthBearerToken TEST_TOKEN_ALSO_AS_EXPIRING_CREDENTIAL2 = new OAuthBearerTokenExpiringCredential(
            TEST_TOKEN);
    private static final SaslExtensions TEST_EXTENTIONS = new SaslExtensions(new HashMap<String, String>() {
        private static final long serialVersionUID = 7699471367006937629L;
        {
            put("One", "1");
        }
    });

    private static class TestCallbackHandler implements AuthenticateCallbackHandler {
        private final OAuthBearerToken mandatoryToken;
        private final SaslExtensions optionalExtensionsNullImpliesUnsupportedCallback;
        private final OAuthBearerToken mandatorySecondToken;
        private int handleCount = 0;

        public TestCallbackHandler(OAuthBearerToken mandatoryToken,
                SaslExtensions optionalExtensionsNullImpliesUnsupportedCallback) {
            this(mandatoryToken, optionalExtensionsNullImpliesUnsupportedCallback, mandatoryToken);
        }

        public TestCallbackHandler(OAuthBearerToken mandatoryToken,
                SaslExtensions optionalExtensionsNullImpliesUnsupportedCallback,
                OAuthBearerToken mandatorySecondToken) {
            this.mandatoryToken = Objects.requireNonNull(mandatoryToken);
            this.optionalExtensionsNullImpliesUnsupportedCallback = optionalExtensionsNullImpliesUnsupportedCallback;
            this.mandatorySecondToken = Objects.requireNonNull(mandatorySecondToken);
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism,
                List<AppConfigurationEntry> jaasConfigEntries) {
            // empty
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuthBearerTokenCallback) {
                    ((OAuthBearerTokenCallback) callback)
                            .token(++handleCount == 1 ? mandatoryToken : mandatorySecondToken);
                } else if (callback instanceof SaslExtensionsCallback) {
                    if (optionalExtensionsNullImpliesUnsupportedCallback == null)
                        throw new UnsupportedCallbackException(callback);
                    ((SaslExtensionsCallback) callback).extensions(optionalExtensionsNullImpliesUnsupportedCallback);
                } else
                    throw new UnsupportedCallbackException(callback);
            }
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testAttachesExtensionsToFirstClientMessage() throws Exception {
        String expectedInitialResponse = responseString(
                new OAuthBearerClientInitialResponse(TEST_TOKEN.value(), TEST_EXTENTIONS));
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new TestCallbackHandler(TEST_TOKEN, TEST_EXTENTIONS));
        String receivedInitialResponse = evaluateChallengeAndReturnAsString(client, EMPTY_CHALLENGE);
        assertEquals(expectedInitialResponse, receivedInitialResponse);
    }

    @Test
    public void testNoInputExtensionsDoesNotAttachAnyExtensionsToFirstClientMessage() throws Exception {
        String expectedInitialResponse = responseString(
                new OAuthBearerClientInitialResponse(TEST_TOKEN.value(), SaslExtensions.NO_SASL_EXTENSIONS));
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(
                new TestCallbackHandler(TEST_TOKEN, SaslExtensions.NO_SASL_EXTENSIONS));
        String receivedInitialResponse = evaluateChallengeAndReturnAsString(client, EMPTY_CHALLENGE);
        assertEquals(expectedInitialResponse, receivedInitialResponse);
    }

    @Test
    public void testIgnoresUnsupportedSaslExtensionsCallback() throws SaslException {
        // null tells the handler to raise UnsupportedCallbackException
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(new TestCallbackHandler(TEST_TOKEN, null));
        // make sure the client ignores the exception; no exception should occur here
        client.evaluateChallenge(EMPTY_CHALLENGE);
    }

    @Test
    public void testCorrectMechanismName() throws SaslException {
        // tell the handler to raise UnsupportedCallbackException
        assertEquals("OAUTHBEARER",
                new OAuthBearerSaslClient(new TestCallbackHandler(TEST_TOKEN, TEST_EXTENTIONS)).getMechanismName());
    }

    @Test
    public void testIndicatesHasInitialResponse() throws SaslException {
        // tell the handler to raise UnsupportedCallbackException
        assertTrue(
                new OAuthBearerSaslClient(new TestCallbackHandler(TEST_TOKEN, TEST_EXTENTIONS)).hasInitialResponse());
    }

    @Test
    public void testRecognizesInitialExpiringCredentialIfExpiringCredentialChangesBeforeReceivingServerSecondChallenge()
            throws SaslException {
        OAuthBearerSaslClient client = new OAuthBearerSaslClient(
                new TestCallbackHandler(TEST_TOKEN_ALSO_AS_EXPIRING_CREDENTIAL, SaslExtensions.NO_SASL_EXTENSIONS,
                        TEST_TOKEN_ALSO_AS_EXPIRING_CREDENTIAL2));
        client.evaluateChallenge(EMPTY_CHALLENGE);
        client.evaluateChallenge(EMPTY_CHALLENGE);
        assertSame(TEST_TOKEN_ALSO_AS_EXPIRING_CREDENTIAL, client.token());
    }

    private static String responseString(OAuthBearerClientInitialResponse oauthBearerClientInitialResponse) {
        return new String(oauthBearerClientInitialResponse.toBytes(), StandardCharsets.UTF_8);
    }

    private static String evaluateChallengeAndReturnAsString(OAuthBearerSaslClient client, byte[] challenge)
            throws SaslException {
        byte[] retvalBytes = client.evaluateChallenge(challenge);
        return retvalBytes == null ? null : new String(retvalBytes, StandardCharsets.UTF_8);
    }
}
