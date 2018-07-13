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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.easymock.EasyMock;
import org.junit.Test;

public class AuthenticationRequestTest {
    @Test
    public void shouldExposePropertiesCorrectly() {
        String nodeId = "0";
        AuthenticationRequestCompletionHandler authenticationRequestCompletionHandler = EasyMock
                .mock(AuthenticationRequestCompletionHandler.class);
        for (AbstractRequest.Builder<?> requestBuilder : new AbstractRequest.Builder<?>[] {
            EasyMock.mock(ApiVersionsRequest.Builder.class), EasyMock.mock(SaslHandshakeRequest.Builder.class),
            EasyMock.mock(SaslAuthenticateRequest.Builder.class)}) {
            AuthenticationRequest authenticationRequest = new AuthenticationRequest(nodeId, requestBuilder,
                    authenticationRequestCompletionHandler);
            assertEquals(Integer.parseInt(nodeId), authenticationRequest.nodeId());
            assertSame(requestBuilder, authenticationRequest.requestBuilder());
            assertSame(authenticationRequestCompletionHandler,
                    authenticationRequest.authenticationRequestCompletionHandler());
        }
    }

    @Test(expected = NumberFormatException.class)
    public void shouldRejectNullNodeId() {
        new AuthenticationRequest(null, EasyMock.mock(ApiVersionsRequest.Builder.class),
                EasyMock.mock(AuthenticationRequestCompletionHandler.class));
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullRequestBuilder() {
        new AuthenticationRequest(0, null, EasyMock.mock(AuthenticationRequestCompletionHandler.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectIllegalTypeOfRequestBuilder() {
        new AuthenticationRequest(0, EasyMock.mock(AddOffsetsToTxnRequest.Builder.class),
                EasyMock.mock(AuthenticationRequestCompletionHandler.class));
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullAuthenticationRequestCompletionHandler() {
        new AuthenticationRequest(0, EasyMock.mock(ApiVersionsRequest.Builder.class), null);
    }
}
