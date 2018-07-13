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

/**
 * Receives information related to re-authentication success or failure
 */
public interface AuthenticationSuccessOrFailureReceiver {
    /**
     * Indicate whether or not to retry a failed re-authentication attempt
     */
    public enum RetryIndication {
        /**
         * Retry at least as long as the current credential remains valid
         */
        RETRY_LIMITED,
        /**
         * Do not retry
         */
        DO_NOT_RETRY
    }

    /**
     * Indicate that re-authentication failed
     * 
     * @param retryIndication
     *            whether or not to retry
     * @param errorMessage
     *            the nature of the current failure
     */
    void reauthenticationFailed(RetryIndication retryIndication, String errorMessage);

    /**
     * Indicate that re-authentication succeeded
     */
    void reauthenticationSucceeded();
}
