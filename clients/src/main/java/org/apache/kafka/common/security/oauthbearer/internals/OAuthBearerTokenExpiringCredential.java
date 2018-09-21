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

import java.util.Date;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredential;

/**
 * An {@link OAuthBearerToken} that also implements {@link ExpiringCredential}
 */
public class OAuthBearerTokenExpiringCredential implements OAuthBearerToken, ExpiringCredential {
    private final OAuthBearerToken delegate;

    /**
     * Constructor
     * 
     * @param delegate
     *            the mandatory, underlying token to which invocations are delegated
     */
    public OAuthBearerTokenExpiringCredential(OAuthBearerToken delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public long expireTimeMs() {
        return delegate.lifetimeMs();
    }

    @Override
    public Long absoluteLastRefreshTimeMs() {
        return null;
    }

    @Override
    public String value() {
        return delegate.value();
    }

    @Override
    public Set<String> scope() {
        return delegate.scope();
    }

    @Override
    public long lifetimeMs() {
        return expireTimeMs();
    }

    @Override
    public String principalName() {
        return delegate.principalName();
    }

    @Override
    public Long startTimeMs() {
        return delegate.startTimeMs();
    }

    @Override
    public int hashCode() {
        return value().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!(obj instanceof OAuthBearerTokenExpiringCredential))
            return false;
        return value().equals(((OAuthBearerTokenExpiringCredential) obj).value());
    }

    @Override
    public String toString() {
        return String.format("Principal=%s, Scope=%s, StartTime=%s, ExpireTime=%s", principalName(), scope(),
                new Date(startTimeMs()), new Date(expireTimeMs()));
    }

    /**
     * Return the always non-null underlying token
     * 
     * @return the always non-null underlying token
     */
    public OAuthBearerToken underlyingToken() {
        return delegate;
    }
}
