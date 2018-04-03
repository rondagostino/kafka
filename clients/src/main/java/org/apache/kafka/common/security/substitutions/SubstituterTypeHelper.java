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
package org.apache.kafka.common.security.substitutions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.security.substitutions.internal.NameEqualsValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template {@code SubstituterType} that handles the following modifiers:
 * <ul>
 * <li>{@code redact} -- when enabled, results are stored such that they are
 * prevented from being logged</li>
 * <li>{@code notBlank} -- when enabled, blank (only whitespace) or non-existent
 * results are replaced by default values. Implies {@code notEmpty}.</li>
 * <li>{@code notEmpty} -- when enabled, either explicitly or via
 * {@code notBlank}, empty ({@code ""}) or non-existent results are replaced by
 * default values.</li>
 * <li>{@code fromValueOfKey} -- provides a level of indirection so that the
 * identifier, instead of always being literally specified (i.e. read this
 * particular file, or this particular environment variable), can be determined
 * via some other key's value. This allows, for example, the filename, system
 * property name, etc. to potentially be generated from multiple substitutions
 * concatenated together.</li>
 * <li>{@code defaultValue=<value>} -- when enabled, the provided literal value
 * is used as a default value in case the result either does not exist or is
 * disallowed via {@code notBlank} or {@code notEmpty}</li>
 * <li>{@code defaultKey=<key>} -- when enabled, the indicated key is evaluated
 * as a default value in case the result either does not exist or is disallowed
 * via {@code notBlank} or {@code notEmpty}</li>
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
 * Flags and arguments are presented to the substitution type's implementation
 * via the
 * {@link #retrieveResult(String, String, boolean, Set, Map, SubstitutableValues)}
 * method.
 * <p>
 * Implementations of the {@link SubstituterType} interface that wish to
 * leverage the help provided by this class can either extend this class
 * directly or delegate to an anonymous class that extends this one.
 */
public abstract class SubstituterTypeHelper implements SubstituterType {
    private static final Logger log = LoggerFactory.getLogger(SubstituterTypeHelper.class);

    /**
     * Retrieve the substitution result associated with the given identifier, if
     * any, otherwise null
     * 
     * @param type
     *            the (always non-null/non-blank) type of substitution being
     *            performed
     * @param identifier
     *            the required (though potentially empty) identifier as interpreted
     *            by the substitution implementation for the given type. For
     *            example, it may be a filename, system property name, environment
     *            variable name, etc.
     * @param redact
     *            if the result must be redacted regardless of any information to
     *            the contrary
     * @param additionalFlags
     *            the flags specified, if any, beyond the standard {@code redact},
     *            {@code notBlank}, {@code notEmpty}, and {@code fromValueOfKey}
     *            flags
     * @param additionalArgs
     *            the arguments specified, if any, beyond the standard
     *            {@code defaultValue} and {@code defaultKey} arguments
     * @param substitutableValues
     *            the key/value mappings and their current substitution state
     * @return the substitution result associated with the given identifier, if any,
     *         otherwise null
     * @throws IOException
     *             if the request cannot be performed such that the use of a default
     *             value would be inappropriate
     */
    public abstract RedactableObject retrieveResult(String type, String identifier, boolean redact,
            Set<String> additionalFlags, Map<String, String> additionalArgs, SubstitutableValues substitutableValues)
            throws IOException;

    @Override
    public RedactableObject doSubstitution(String type, List<String> modifiers, String identifier,
            SubstitutableValues substitutableValues) throws IOException {
        Objects.requireNonNull(type);
        Objects.requireNonNull(modifiers);
        Objects.requireNonNull(identifier);
        Objects.requireNonNull(substitutableValues);
        boolean redact = false;
        boolean notBlank = false;
        boolean notEmpty = false;
        boolean fromValueOfKey = false;
        NameEqualsValue providedDefaultValue = null;
        NameEqualsValue providedDefaultKey = null;
        Map<String, String> additionalArgs = new HashMap<>();
        Set<String> additionalFlags = new HashSet<>();
        for (String modifierText : modifiers) {
            switch (modifierText.trim()) {
                case "redact":
                    redact = true;
                    break;
                case "notBlank":
                    notBlank = true;
                    break;
                case "notEmpty":
                    notEmpty = true;
                    break;
                case "fromValueOfKey":
                    fromValueOfKey = true;
                    break;
                default:
                    NameEqualsValue nameEqualsValue = NameEqualsValue.from(modifierText, false);
                    switch (nameEqualsValue != null ? nameEqualsValue.name() : "") {
                        case "defaultValue":
                            if (providedDefaultValue != null)
                                throw new IOException("Cannot specify default value multiple times");
                            providedDefaultValue = nameEqualsValue;
                            break;
                        case "defaultKey":
                            if (providedDefaultKey != null)
                                throw new IOException("Cannot specify default key multiple times");
                            providedDefaultKey = nameEqualsValue;
                            break;
                        default:
                            collectAdditionalArgOrFlag(additionalArgs, nameEqualsValue, additionalFlags,
                                    modifierText.trim());
                    }
            }
        }
        RedactableObject potentiallyRedactedDefaultValue = providedDefaultValue != null
                ? new RedactableObject(providedDefaultValue.value(), redact)
                : null;
        RedactableObject potentiallyRedactedIdentifier = new RedactableObject(identifier, redact);
        String defaultKey = providedDefaultKey != null ? providedDefaultKey.value() : null;
        if (log.isDebugEnabled()) {
            log.debug(String.format(
                    "type=%s, redact=%b, notBlank=%b, notEmpty=%b, fromValueOfKey=%b, defaultValue=%s, defaultKey=%s, flags=%s, args=%s, identifier=%s",
                    type, redact, notBlank, notEmpty, fromValueOfKey, potentiallyRedactedDefaultValue, defaultKey,
                    additionalFlags, additionalArgs, potentiallyRedactedIdentifier));
        }
        if (potentiallyRedactedDefaultValue != null && defaultKey != null)
            throw new IOException(String.format("Type=%s: cannot specify both defaultValue and defaultKey", type));
        return doSubstitution(type, redact, notBlank, notEmpty, fromValueOfKey, potentiallyRedactedDefaultValue,
                defaultKey, Collections.unmodifiableSet(additionalFlags), Collections.unmodifiableMap(additionalArgs),
                potentiallyRedactedIdentifier, substitutableValues);
    }

    private static void collectAdditionalArgOrFlag(Map<String, String> argsBeingCollected, NameEqualsValue potentialArg,
            Set<String> flagsBeingCollected, String potentialFlag) throws IOException {
        if (potentialArg == null)
            flagsBeingCollected.add(potentialFlag);
        else {
            String prev = argsBeingCollected.put(potentialArg.name(), potentialArg.value());
            if (prev != null)
                throw new IOException(
                        String.format("Cannot specify name=value argument multiple times: %s", potentialArg.name()));
        }
    }

    private RedactableObject doSubstitution(String type, boolean redact, boolean notBlank, boolean notEmpty,
            boolean fromValueOfKey, RedactableObject defaultValue, String defaultKey, Set<String> additionalFlags,
            Map<String, String> additionalArgs, RedactableObject identifier, SubstitutableValues substitutableValues)
            throws IOException {
        /*
         * Get the identifier, honoring the "fromValueOfKey" modifier if it is present.
         * Ignore any redact modifiers because secrets should not appear in identifiers.
         * It would be onerous to support that; every method that accepts
         * "String identifier" would instead have to accept
         * "RedactableObject identifier". We explicitly decide to not support this.
         */
        RedactableObject redactableIdentifierToUse = !fromValueOfKey ? identifier
                : substitutableValues.getSubstitutionResult(identifier.value(), true);
        String identifierToUse = redactableIdentifierToUse.value();
        if (log.isDebugEnabled())
            log.debug(String.format("Type=%s: identifier=%s", type, identifierToUse));
        // get the substitution value
        RedactableObject retrievedValue = retrieveResult(type, identifierToUse, redact, additionalFlags, additionalArgs,
                substitutableValues);
        if (log.isDebugEnabled())
            log.debug(String.format("Type=%s: identifier=%s: value=%s", type, identifierToUse, retrievedValue));
        return processRetrievedValueRelativeToModifiersAndDefault(type, redact, notBlank, notEmpty, defaultValue,
                defaultKey, identifierToUse, retrievedValue, substitutableValues);
    }

    private static RedactableObject processRetrievedValueRelativeToModifiersAndDefault(String type, boolean redact,
            boolean notBlank, boolean notEmpty, RedactableObject defaultValue, String defaultKey, String identifier,
            RedactableObject retrievedValue, SubstitutableValues substitutableValues) throws IOException {
        boolean requireDefault = retrievedValue == null || ((notEmpty || notBlank) && retrievedValue.isEmpty())
                || (notBlank && retrievedValue.isBlank());
        if (!requireDefault)
            return retrievedValue;
        if (requireDefault && defaultKey == null && defaultValue == null)
            throwMissingDefaultException(type, redact, notBlank, notEmpty, identifier, retrievedValue);
        return processRequiredDefault(type, redact, notBlank, notEmpty, defaultValue, defaultKey, identifier,
                substitutableValues);
    }

    private static RedactableObject processRequiredDefault(String type, boolean redact, boolean notBlank,
            boolean notEmpty, RedactableObject defaultValue, String defaultKey, String identifier,
            SubstitutableValues substitutableValues) throws IOException {
        if (log.isDebugEnabled())
            log.debug(String.format("Type=%s: requiring default value for identifier=%s", type, identifier));
        RedactableObject redactableDefaultToUse;
        if (defaultValue != null)
            redactableDefaultToUse = defaultValue;
        else {
            redactableDefaultToUse = substitutableValues.getSubstitutionResult(defaultKey, true);
            if (redact)
                redactableDefaultToUse = redactableDefaultToUse.redactedVersion();
        }
        if ((notEmpty || notBlank) && redactableDefaultToUse.isEmpty())
            throw new IOException(
                    String.format("Type=%s: default value for identifier=%s is empty (notBlank=%b, notEmpty=%b)", type,
                            identifier, notBlank, notEmpty));
        if (notBlank && redactableDefaultToUse.isBlank())
            throw new IOException(String.format(
                    "Type=%s: Default value for identifier=%s is non-empty but still blank (notBlank=%b, notEmpty=%b)",
                    type, identifier, notBlank, notEmpty));
        return redactableDefaultToUse;
    }

    private static void throwMissingDefaultException(String type, boolean redact, boolean notBlank, boolean notEmpty,
            String identifier, RedactableObject retrievedValue) throws IOException {
        if (retrievedValue == null)
            throw new IOException(
                    String.format("Type=%s: no default for identifier that does not exist: %s", type, identifier));
        if ((notEmpty || notBlank) && retrievedValue.isEmpty())
            throw new IOException(String.format(
                    "Type=%s: no default for identifier with value that is empty (notBlank=%b, notEmpty=%b): %s", type,
                    notBlank, notEmpty, identifier));
        throw new IOException(String.format(
                "Type=%s: no default for identifier with value that is non-empty but still blank (notBlank=%b, notEmpty=%b): %s",
                type, notBlank, notEmpty, identifier));
    }
}
