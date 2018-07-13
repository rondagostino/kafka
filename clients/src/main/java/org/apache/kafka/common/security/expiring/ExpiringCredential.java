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
package org.apache.kafka.common.security.expiring;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

/**
 * A credential that expires and that can potentially be refreshed; such a
 * refreshed credential can also potentially be used to re-authenticate an
 * existing connection.
 * <p>
 * The parameters that impact how the refresh algorithm operates are specified
 * as part of the producer/consumer/broker configuration and are as follows. See
 * the documentation for these properties elsewhere for details.
 * <table>
 * <tr>
 * <th>Producer/Consumer/Broker Configuration Property</th>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.window.factor}</td>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.window.jitter}</td>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.min.period.seconds}</td>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.min.buffer.seconds}</td>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.reauthenticate.enable}</td>
 * </tr>
 * </table>
 * <p>
 * This interface was introduced in 2.1.0 and, while it feels stable, it could
 * evolve. We will try to evolve the API in a compatible manner, but we reserve
 * the right to make breaking changes in minor releases, if necessary. We will
 * update the {@code InterfaceStability} annotation and this notice once the API
 * is considered stable.
 * 
 * @see OAuthBearerLoginModule
 * @see SaslConfigs#SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_REAUTHENTICATE_ENABLE_DOC
 */
@InterfaceStability.Evolving
public interface ExpiringCredential {
    /**
     * The value to use when asking a {@code SaslClient} instance for the
     * {@code ExpiringCredential} that was used to authenticate, if any
     */
    public static final String SASL_CLIENT_NEGOTIATED_PROPERTY_NAME = "Expiring.Credential";

    /**
     * The name of the principal to which this credential applies (used only for
     * logging)
     * 
     * @return the always non-null/non-empty principal name
     */
    String principalName();

    /**
     * When the credential became valid, in terms of the number of milliseconds
     * since the epoch, if known, otherwise null. An expiring credential may not
     * necessarily indicate when it was created -- just when it expires -- so we
     * need to support a null return value here.
     * 
     * @return the time when the credential became valid, in terms of the number of
     *         milliseconds since the epoch, if known, otherwise null
     */
    Long startTimeMs();

    /**
     * When the credential expires, in terms of the number of milliseconds since the
     * epoch. All expiring credentials by definition must indicate their expiration
     * time -- thus, unlike other methods, we do not support a null return value
     * here.
     * 
     * @return the time when the credential expires, in terms of the number of
     *         milliseconds since the epoch
     */
    long expireTimeMs();

    /**
     * The point after which the credential can no longer be refreshed, in terms of
     * the number of milliseconds since the epoch, if any, otherwise null. Some
     * expiring credentials can be refreshed over and over again without limit, so
     * we support a null return value here.
     * 
     * @return the point after which the credential can no longer be refreshed, in
     *         terms of the number of milliseconds since the epoch, if any,
     *         otherwise null
     */
    Long absoluteLastRefreshTimeMs();
}
