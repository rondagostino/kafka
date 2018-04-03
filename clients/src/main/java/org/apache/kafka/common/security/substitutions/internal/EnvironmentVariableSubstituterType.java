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
package org.apache.kafka.common.security.substitutions.internal;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.security.substitutions.RedactableObject;
import org.apache.kafka.common.security.substitutions.SubstitutableValues;
import org.apache.kafka.common.security.substitutions.SubstituterType;
import org.apache.kafka.common.security.substitutions.SubstituterTypeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code SubstituterType} that handles environment variable substitution. The
 * identifier typically indicates the environment variable to read. This
 * substitution type recognizes the following modifiers:
 * <ul>
 * <li>{@code redact} -- when enabled, the value of the environment variable is
 * stored such that it is prevented from being logged</li>
 * <li>{@code notBlank} -- when enabled, blank (only whitespace) or non-existent
 * environment variables are replaced by default values. Implies
 * {@code notEmpty}.</li>
 * <li>{@code notEmpty} -- when enabled, either explicitly or via
 * {@code notBlank}, empty ({@code ""}) or non-existent environment variables
 * are replaced by default values.</li>
 * <li>{@code fromValueOfKey} -- provides a level of indirection so that the
 * environment variable to be read, instead of being literally specified by the
 * identifier, can be determined via another key's value. The identifier is
 * interpreted as the key whose value identifies the environment variable to
 * read. This allows the environment variable name to potentially be generated
 * from multiple substitutions concatenated together.</li>
 * <li>{@code defaultValue=<value>} -- when enabled, the provided literal value
 * is used as a default value in case the environment variable either does not
 * exist or its value is disallowed via {@code notBlank} or
 * {@code notEmpty}</li>
 * <li>{@code defaultKey=<key>} -- when enabled, the indicated key is evaluated
 * as a default value in case the environment variable either does not exist or
 * its value is disallowed via {@code notBlank} or {@code notEmpty}</li>
 * </ul>
 * 
 * Flags (modifiers without an equal sign) are trimmed, so "{@code redact}" and
 * "{@code  redact }" are recognized as being the same. Arguments (modifiers
 * with an equal sign) have their name trimmed but not their value, so
 * "{@code name=value}" and "{@code  name = value }" are both recognized as
 * setting the {@code name} argument (though their values do not match due to
 * whitespace differences).
 * <p>
 * It is an error to set the same named argument multiple times (even if the
 * values are the same). Redundantly specifying the same flag is acceptable.
 * <p>
 * Unrecognized flags and arguments are ignored.
 */
public class EnvironmentVariableSubstituterType implements SubstituterType {
    private static final Logger log = LoggerFactory.getLogger(EnvironmentVariableSubstituterType.class);
    private final SubstituterTypeHelper delegate;

    /**
     * Explicit default constructor
     */
    public EnvironmentVariableSubstituterType() {
        this(null);
    }

    EnvironmentVariableSubstituterType(final Map<String, RedactableObject> injectedValuesForTesting) {
        delegate = new SubstituterTypeHelper() {
            @Override
            public RedactableObject retrieveResult(String type, String identifier, boolean redact,
                    Set<String> additionalFlags, Map<String, String> additionalArgs,
                    SubstitutableValues substitutableOptions) {
                if (!additionalFlags.isEmpty() || !additionalArgs.isEmpty())
                    log.warn(
                            String.format("Ignoring unrecognized flags/args: %s; %s", additionalFlags, additionalArgs));
                if (injectedValuesForTesting != null) {
                    RedactableObject injectedValueForTesting = injectedValuesForTesting.get(identifier);
                    return injectedValueForTesting != null && redact ? injectedValueForTesting.redactedVersion()
                            : injectedValueForTesting;
                }
                String retval = System.getenv(identifier);
                return retval != null ? new RedactableObject(retval, redact) : null;
            }
        };
    }

    @Override
    public RedactableObject doSubstitution(String type, List<String> modifiers, String value,
            SubstitutableValues substitutableOptions) throws IOException {
        return delegate.doSubstitution(type, modifiers, value, substitutableOptions);
    }
}
