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
 * A {@code SubstituterType} that handles option substitution.
 * <p>
 * Note that the {@code fromValueOfKey} modifier, if present, still adds an
 * additional level of indirection: the identifier indicates the key whose
 * value, in turn, indicates the key whose value will be retrieved. For example,
 * given these key/value pairs:
 * <p>
 * a="b"<br>
 * b="c"<br>
 * x1="$[keyValue=a]"<br>
 * x2="$[keyValue/fromValueOfKey/=a]"
 * <p>
 * The results will be:
 * <p>
 * {x1=b, x2=c}
 */
public class KeyValueSubstituterType implements SubstituterType {
    private static final Logger log = LoggerFactory.getLogger(KeyValueSubstituterType.class);
    private final SubstituterTypeHelper delegate;

    /**
     * Explicit default constructor
     */
    public KeyValueSubstituterType() {
        delegate = new SubstituterTypeHelper() {
            @Override
            public RedactableObject retrieveResult(String type, String identifier, boolean redact, Set<String> flags,
                    Map<String, String> args, SubstitutableValues substitutableOptions) throws IOException {
                if (!flags.isEmpty() || !args.isEmpty())
                    log.warn(String.format("Ignoring unrecognized flags/args: %s; %s", flags, args));
                RedactableObject substitutionResult = substitutableOptions.getSubstitutionResult(identifier, false);
                return substitutionResult != null && redact ? substitutionResult.redactedVersion() : substitutionResult;
            }
        };
    }

    @Override
    public RedactableObject doSubstitution(String type, List<String> modifiers, String value,
            SubstitutableValues substitutableValues) throws IOException {
        return delegate.doSubstitution(type, modifiers, value, substitutableValues);
    }
}
