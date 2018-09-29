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
package org.apache.kafka.common.security;

import javax.security.auth.login.Configuration;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JaasUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JaasUtils.class);
    public static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";

    public static final String SERVICE_NAME = "serviceName";

    public static final String ZK_SASL_CLIENT = "zookeeper.sasl.client";
    public static final String ZK_LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig";
    
    /**
     * The server (broker) specifies a positive session length in milliseconds to a
     * SASL client when {@link BrokerSecurityConfigs#CONNECTIONS_MAX_REAUTH_MS} is
     * positive as per <a href=
     * "https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate">KIP
     * 368: Allow SASL Connections to Periodically Re-Authenticate</a>. The session
     * length is the minimum of the configured value and any session length implied
     * by the credential presented during authentication. The lifetime defined by
     * the credential, in terms of milliseconds since the epoch, is available via a
     * negotiated property on the SASL Server instance, and that value can be
     * converted to a session length by subtracting the time at which authentication
     * occurred. This variable defines the negotiated property key that is used to
     * communicate the credential lifetime in milliseconds since the epoch.
     */
    public static final String CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY = "CREDENTIAL.LIFETIME.MS";

    private JaasUtils() {}

    public static boolean isZkSecurityEnabled() {
        boolean zkSaslEnabled = Boolean.parseBoolean(System.getProperty(ZK_SASL_CLIENT, "true"));
        String zkLoginContextName = System.getProperty(ZK_LOGIN_CONTEXT_NAME_KEY, "Client");

        boolean isSecurityEnabled;
        try {
            Configuration loginConf = Configuration.getConfiguration();
            isSecurityEnabled = loginConf.getAppConfigurationEntry(zkLoginContextName) != null;
        } catch (Exception e) {
            throw new KafkaException("Exception while loading Zookeeper JAAS login context '" + zkLoginContextName + "'", e);
        }
        if (isSecurityEnabled && !zkSaslEnabled) {
            LOG.error("JAAS configuration is present, but system property " +
                        ZK_SASL_CLIENT + " is set to false, which disables " +
                        "SASL in the ZooKeeper client");
            throw new KafkaException("Exception while determining if ZooKeeper is secure");
        }

        return isSecurityEnabled;
    }
}

