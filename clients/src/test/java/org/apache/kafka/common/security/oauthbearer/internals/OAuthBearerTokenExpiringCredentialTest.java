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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.junit.Test;

public class OAuthBearerTokenExpiringCredentialTest {
    private static final Set<String> SCOPE = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("scope1")));
    private static final OAuthBearerToken TEST_TOKEN = new OAuthBearerToken() {
        @Override
        public String value() {
            return "value";
        }

        @Override
        public Set<String> scope() {
            return SCOPE;
        }

        @Override
        public long lifetimeMs() {
            return 100L;
        }

        @Override
        public String principalName() {
            return "principalName";
        }

        @Override
        public Long startTimeMs() {
            return 200L;
        }
    };

    @Test(expected = NullPointerException.class)
    public void rejectsNullToken() {
        new OAuthBearerTokenExpiringCredential(null);
    }

    @Test
    public void delegatesAppropriately() {
        OAuthBearerTokenExpiringCredential oauthBearerTokenExpiringCredential = new OAuthBearerTokenExpiringCredential(
                TEST_TOKEN);
        assertSame(TEST_TOKEN, oauthBearerTokenExpiringCredential.underlyingToken());
        assertSame(TEST_TOKEN.value(), oauthBearerTokenExpiringCredential.value());
        assertSame(TEST_TOKEN.scope(), oauthBearerTokenExpiringCredential.scope());
        assertEquals(TEST_TOKEN.lifetimeMs(), oauthBearerTokenExpiringCredential.lifetimeMs());
        assertSame(TEST_TOKEN.principalName(), oauthBearerTokenExpiringCredential.principalName());
        assertEquals(TEST_TOKEN.startTimeMs(), oauthBearerTokenExpiringCredential.startTimeMs());
    }
}
