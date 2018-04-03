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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.substitutions.internal.EnvironmentVariableSubstituterType;
import org.apache.kafka.common.security.substitutions.internal.FileContentSubstituterType;
import org.apache.kafka.common.security.substitutions.internal.KeyValueSubstituterType;
import org.apache.kafka.common.security.substitutions.internal.SystemPropertySubstituterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds support for substitution within the values of an instance of
 * {@link UnderlyingValues}. Substitution is accomplished via delimited text
 * within a value of the following form:
 * 
 * <pre>
 * &lt;OPENING_DELIMITER&gt;&lt;TYPE&gt;&lt;OPTIONAL_MODIFIERS&gt;=&lt;OPTIONAL_IDENTIFIER&gt;&lt;CLOSING_DELIMITER&gt;
 * </pre>
 * 
 * Where the above elements are defined as follows:
 * 
 * <pre>
 * OPENING_DELIMITER: $[, $[[, $[[[, $[[[[, or $[[[[[
 * CLOSING_DELIMITER:  ],  ]],  ]]],  ]]]], or  ]]]]] (number of brackets must match)
 * 
 * TYPE: everything up to (but not including) the first punctuation character
 * 
 * OPTIONAL_MODIFIERS: the optional section immediately after the TYPE, starting with any
 *                     punctuation character except for the equal sign (=), and ending
 *                     with the same punctuation character followed immediately by an
 *                     equal sign. The same punctuation character delimits individual
 *                     modifiers, which come in two flavors: flags, which do not contain
 *                     an equal sign, and name=value arguments, which do.
 *                       
 * OPTIONAL_IDENTIFIER: the optional section immediately after any modifiers and the equal
 *                      sign (=).
 * </pre>
 * 
 * For example:
 * 
 * <pre>
 * $[envVar=THE_ENV_VAR]
 * $[envVar/notBlank/redact/=THE_ENV_VAR]
 * $[envVar/defaultValue = theDefaultValue/=THE_ENV_VAR]
 * $[envVar/defaultKey = theDefaultKey/=THE_ENV_VAR]
 * $[file|redact|notBlank|=/the/path/to/the/file]
 * </pre>
 * 
 * Once an underlying value is retrieved and its calculated value -- whether
 * different from the raw underlying value due to a substitution or not -- is
 * determined, this instance will not retrieve the underlying value again; the
 * calculated value will be used if it is referred to. This means if the
 * underlying values are expected to change then to see those changes a new
 * instance of this class must be allocated.
 * <p>
 * Working left to right, once the delimiters are defined for a value (for
 * example, {@code $[} and {@code ]}), only those delimiters are recognized for
 * the rest of that value (and they are always recognized as meaning
 * substitution for the rest of that value). A special "empty" substitution does
 * nothing except, when it appears to the left of every other occurrence of
 * matching delimiters, it serves to force the delimiter for that value to the
 * one indicated. For example, to force the delimiter to {@code $[[} and
 * {@code ]]} (and prevent {@code $[} and {@code ]} from causing substitution)
 * for a value:
 * 
 * <pre>
 * someKey = "$[[]]These $[ and ] delimiters do not cause substitution"
 * </pre>
 * 
 * The following built-in substitution types are supported, though it is
 * straightforward to add others (see below):
 * 
 * <ul>
 * <li>{@code envVar}: substitute an environment variable, typically indicated
 * by the identifier</li>
 * <li>{@code sysProp}: substitute a system property, typically indicated by the
 * identifier</li>
 * <li>{@code file}: substitute the contents of a file, typically indicated by
 * the identifier</li>
 * <li>{@code keyValue}: substitute the contents of another key's value,
 * typically indicated by the identifier (and that key's value has substitution
 * performed on it if necessary)</li>
 * </ul>
 * 
 * The built-in substitution types support the following flags, which are
 * trimmed and may be redundantly specified:
 * <ul>
 * <li>{@code redact}: prevent values from being logged</li>
 * <li>{@code notEmpty}: the value must not be empty</li>
 * <li>{@code notBlank}: the value must not be blank (i.e. consisting only of
 * whitespace); implies {@code notEmpty}.</li>
 * <li>{@code fromValueOfKey}: provides a level of indirection so that the
 * identifier, instead of being used directly (i.e. read the indicated file, or
 * the indicated environment variable), identifies another key whose value is
 * used as the identifier instead. This allows, for example, the filename,
 * system property name, etc. to potentially be generated from multiple
 * substitutions concatenated together.</li>
 * </ul>
 * The built-in substitution types support the following arguments, whose names
 * are trimmed but whose values are not; it is an error to specify the same
 * named argument multiple times (even if the values are identical):
 * <ul>
 * <li>{@code defaultValue=<value>}: substitute the given literal value if the
 * substitution cannot otherwise be made (either because the identifier
 * indicates something that does not exist or the determined value was
 * disallowed because it was empty or blank). The substituted default value must
 * satisfy any {@code notBlank} or {@code notEmpty} modifiers that act as
 * constraints, otherwise it is an error.</li>
 * <li>{@code defaultKey=<key>}: substitute the value associated with the
 * indicated key if the substitution cannot otherwise be made (either because
 * the identifier indicates something that does not exist or the determined
 * value was disallowed because it was empty or blank). The value that is
 * ultimately substituted must satisfy any {@code notBlank} or {@code notEmpty}
 * modifiers that act as constraints, otherwise it is an error.</li>
 * </ul>
 * 
 * To add new substitutions beyond the built-in ones mentioned above simply
 * define a key/value pair of the following form:
 * 
 * <pre>
 * [optionalTypeDefinitionKeyPrefix]&lt;type&gt;SubstituterType = "fully.qualified.class.name"
 * </pre>
 * 
 * For example:
 * 
 * <pre>
 * fooSubstituterType = "org.example.FooSubstituterType"
 * </pre>
 * 
 * The indicated class must implement {@link SubstituterType}, and you can
 * invoke the substitution in a value like this:
 * 
 * <pre>
 * $[foo/optional/modifiers/=optionalValue]
 * </pre>
 * 
 * The type definition prefix is defined at construction time and may be empty.
 * 
 * A parsing error within recognized delimiters results in the delimiters and
 * the intervening text that could not be parsed being left alone. For example,
 * the following text would be passed through unchanged because the delimited
 * text cannot be parsed as a valid substitution request:
 * {@code qw$[asd_4Q!]uH6}.
 * 
 * @see SubstituterType
 * @see SubstituterTypeHelper
 */
public class SubstitutableValues {
    private static final String TYPE_DEFINITION_KEY_SUFFIX = "SubstituterType";
    private static final Logger log = LoggerFactory.getLogger(SubstitutableValues.class);
    /*
     * Use the reluctant qualifier (*?) here instead of the greedy one (*),
     * otherwise we will match through to the last opening delimiter instead of
     * matching up to just the first one
     */
    private static final String PREFIX_REGEX = "(?<beforeOpeningDelimiter>(?=\\A\\$\\[)|(?!\\$\\[).*?)";
    /*
     * Continue to use the reluctant qualifier (+?) so that we continue to match one
     * character forward at a time. Note that this only matches one opening bracket
     * by default, but we will use a negative lookahead in the next part of the
     * regex to push it forward through all of the contiguous opening brackets.
     */
    private static final String OPENING_DELIMITER_DISCOVERY_REGEX = "\\$(?<openingBrackets>[\\[]+?)";
    /*
     * Force the matcher forward through all the opening brackets with a negative
     * lookahead (a character that isn't an opening bracket). Then grab everything
     * after that.
     */
    private static final String AFTER_OPENING_DELIMITER_REGEX = "(?!\\[)(?<afterOpeningDelimiter>.*)\\z";
    private static final Pattern DELIMITER_DISCOVERY_PATTERN = Pattern
            .compile(PREFIX_REGEX + OPENING_DELIMITER_DISCOVERY_REGEX + AFTER_OPENING_DELIMITER_REGEX);

    private static final String LONGEST_CLOSING_DELIMITER = "]]]]]";
    private static final int MAX_DELIMITERS = LONGEST_CLOSING_DELIMITER.length();
    /*
     * One regex for every supported opening delimiter
     */
    private static final String[] OPENING_DELIMITER_REGEX;
    static {
        OPENING_DELIMITER_REGEX = new String[MAX_DELIMITERS];
        for (int i = 0; i < MAX_DELIMITERS; ++i)
            OPENING_DELIMITER_REGEX[i] = "\\$\\[{" + (i + 1) + "}?";
    }
    /*
     * One regex for every supported closing delimiter
     */
    private static final String[] CLOSING_DELIMITER_REGEX;
    static {
        CLOSING_DELIMITER_REGEX = new String[MAX_DELIMITERS];
        for (int i = 0; i < MAX_DELIMITERS; ++i)
            CLOSING_DELIMITER_REGEX[i] = "]{" + (i + 1) + "}?";
    }
    /*
     * One regex for every supported opening delimiter to identify what comes before
     * and what comes after the opening delimiter. Again, be sure to use the
     * reluctant qualifier (*?) here instead of the greedy one (*), otherwise we
     * will match through to the last opening delimiter instead of matching up to
     * just the first one.
     */
    private static final Pattern[] IDENTIFY_OPENING_DELIMITER_PREFIX_AND_SUFFIX_PATTERN;
    static {
        IDENTIFY_OPENING_DELIMITER_PREFIX_AND_SUFFIX_PATTERN = new Pattern[MAX_DELIMITERS];
        for (int i = 0; i < MAX_DELIMITERS; ++i)
            IDENTIFY_OPENING_DELIMITER_PREFIX_AND_SUFFIX_PATTERN[i] = Pattern
                    .compile("(?<beforeOpeningDelimiter>\\A.*?)" + OPENING_DELIMITER_REGEX[i]
                            + "(?<afterOpeningDelimiter>.*\\z)");
    }
    /*
     * One regex for every supported closing delimiter to identify what comes before
     * the closing delimiter and what comes after it. Again, be sure to use the
     * reluctant qualifier (*?) here instead of the greedy one (*), otherwise we
     * will match through to the last closing delimiter instead of matching up to
     * just the first one.
     */
    private static final Pattern[] CHECK_FOR_SUBSTITUTION_COMMAND_PATTERN;
    static {
        CHECK_FOR_SUBSTITUTION_COMMAND_PATTERN = new Pattern[MAX_DELIMITERS];
        for (int i = 0; i < MAX_DELIMITERS; ++i)
            CHECK_FOR_SUBSTITUTION_COMMAND_PATTERN[i] = Pattern.compile(
                    "(?<substitutionCommand>(\\A?=" + CLOSING_DELIMITER_REGEX[i] + ")|(?!" + CLOSING_DELIMITER_REGEX[i]
                            + ").*?)" + CLOSING_DELIMITER_REGEX[i] + "(?<afterClosingDelimiter>.*)\\z");
    }

    /*
     * Regex to apply to the text between the substitution delimiters to identify
     * the type, the first punctuation character, and what comes after that so we
     * can identify any optional modifiers and the optional identifier.
     */
    private static final Pattern DISCOVER_SUBSTITUTION_COMMAND_PUNCTUATION_CHAR_PATTERN = Pattern
            .compile("\\A(?<type>[^\\p{Punct}]+?)(?=\\p{Punct})(?<afterType>.*)");

    private static class SubstitutionParseException extends Exception {
        private static final long serialVersionUID = 4961195373549192353L;

        public SubstitutionParseException(String msg) {
            super(msg);
        }
    }

    private final String typeDefinitionKeyPrefix;
    private final UnderlyingValues underlyingValues;
    private final ConcurrentHashMap<String, RedactableObject> substitutionResults;
    private final ConcurrentHashMap<String, Boolean> keyResolutionMap;
    private final SubstitutableValues delegate;
    private final String keyCausingCircularReference;
    private final boolean forceDebugLogForTesting;

    /**
     * Constructor where the type definition key prefix is empty
     * 
     * @param underlyingMap
     *            the mandatory underlying map. It is not copied, so it should be
     *            immutable. Results are unspecified if it is mutated in any manner.
     */
    public SubstitutableValues(UnderlyingValues underlyingValues) {
        this("", underlyingValues, false);
    }

    /**
     * Constructor with the given type definition key prefix
     * 
     * @param typeDefinitionKeyPrefix
     *            the mandatory (but possibly empty) type definition key prefix
     * @param underlyingMap
     *            the mandatory underlying map. It is not copied, so it should be
     *            immutable. Results are unspecified if it is mutated in any manner.
     */
    public SubstitutableValues(String typeDefinitionKeyPrefix, UnderlyingValues underlyingValues) {
        this(typeDefinitionKeyPrefix, underlyingValues, false);
    }

    SubstitutableValues(String typeDefinitionKeyPrefix, UnderlyingValues underlyingValues,
            boolean forceDebugLogForTesting) {
        this.typeDefinitionKeyPrefix = Objects.requireNonNull(typeDefinitionKeyPrefix).trim();
        this.underlyingValues = Objects.requireNonNull(underlyingValues);
        this.substitutionResults = new ConcurrentHashMap<>();
        this.keyResolutionMap = new ConcurrentHashMap<>();
        this.delegate = null;
        this.keyCausingCircularReference = null;
        this.forceDebugLogForTesting = forceDebugLogForTesting;
    }

    /**
     * Constructor to create a new instance based on the given instance except that
     * the given key is marked as causing a circular reference if an attempt is made
     * to evaluate it
     * 
     * @param delegate
     *            the mandatory instance from which the view will be created
     * @param keyCausingCircularReference
     *            the mandatory key that causes a circular reference if it is
     *            evaluated
     * @see #withEvaluationCausingCircularReference(String)
     * @see #evaluationCausesCircularReference(String)
     */
    SubstitutableValues(SubstitutableValues delegate, String keyCausingCircularReference) {
        this.typeDefinitionKeyPrefix = Objects.requireNonNull(delegate).typeDefinitionKeyPrefix;
        this.underlyingValues = delegate.underlyingValues;
        this.substitutionResults = delegate.substitutionResults;
        this.keyResolutionMap = delegate.keyResolutionMap;
        this.delegate = delegate;
        this.keyCausingCircularReference = keyCausingCircularReference;
        this.forceDebugLogForTesting = delegate.forceDebugLogForTesting;
    }

    /**
     * Return the always non-null (but possibly empty) type definition key prefix
     * 
     * @return the always non-null (but possibly empty) type definition key prefix
     */
    public String typeDefinitionKeyPrefix() {
        return typeDefinitionKeyPrefix;
    }

    /**
     * Return the underlying values provided during construction
     * 
     * @return the underlying values provided during construction
     */
    public UnderlyingValues underlyingValues() {
        return underlyingValues;
    }

    /**
     * Return an unmodifiable map identifying which keys have been processed for
     * substitution and the corresponding result (if any). A key is guaranteed to
     * have been processed for substitution and its name will appear as a key in the
     * returned map only after {@link #getSubstitutionResult(String)} has been
     * invoked for that key either directly or indirectly because some other key's
     * substitution result depends on the substitution result of the key.
     * 
     * @return an unmodifiable map identifying which keys have been processed for
     *         substitution and the corresponding result (if any)
     */
    public Map<String, RedactableObject> substitutionResults() {
        return Collections.unmodifiableMap(substitutionResults);
    }

    /**
     * Perform substitution if necessary and return the resulting value for the
     * given key
     * 
     * @param key
     *            the mandatory requested key
     * @param requiredToExist
     *            if true then the requested key is required to exist
     * @return the given key's substitution result, after any required substitution
     *         is applied, or null if the key does not exist and it was not required
     *         to exist
     * @throws IOException
     *             if a required substitution cannot be performed, including if the
     *             given (or any other) required key does not exist
     */
    public RedactableObject getSubstitutionResult(String key, boolean requiredToExist) throws IOException {
        boolean debugLogEnabled = forceDebugLogForTesting || log.isDebugEnabled();
        if (debugLogEnabled)
            log.debug(String.format("Retrieve key=%s, requiredToExist=%b", key, requiredToExist));
        Boolean keyResolutionStatus = keyResolutionMap.get(key);
        // get the underlying value now if we haven't retrieved it before
        Object rawObjectRequestedNow = keyResolutionStatus == null ? underlyingValues.get(key) : null;
        if (keyResolutionStatus == null && rawObjectRequestedNow == null) {
            // we just determined that there is no underlying value associated with the key
            keyResolutionStatus = keyResolutionMap.putIfAbsent(key, Boolean.FALSE);
            if (keyResolutionStatus == null)
                keyResolutionStatus = Boolean.FALSE;
        }
        // handle the cases where we already know the answer
        if (keyResolutionStatus != null) {
            if (!keyResolutionStatus.booleanValue()) {
                // it doesn't exist
                if (!requiredToExist) {
                    if (debugLogEnabled)
                        log.debug(String.format("Retrieved key=%s, requiredToExist=%b, value=<null>", key,
                                requiredToExist));
                    return null;
                }
                throw new IOException(String.format("Mandatory key does not exist: %s", key));
            }
            // we resolved it before
            RedactableObject alreadyDeterminedSubstitutionResult = substitutionResults.get(key);
            if (debugLogEnabled)
                log.debug(String.format("Previosuly retrieved: key=%s, requiredToExist=%b, value=%s", key,
                        requiredToExist, alreadyDeterminedSubstitutionResult));
            return alreadyDeterminedSubstitutionResult;
        }
        // we do not yet know; check for circular reference before resolving
        if (evaluationCausesCircularReference(key))
            throw new IOException(String.format("Circular reference trying to evaluate key: %s", key));
        RedactableObject substitutionResult = withEvaluationCausingCircularReference(key)
                .getAlwaysNonNullSubstitutionResultFromRawObject(rawObjectRequestedNow);
        if (debugLogEnabled)
            log.debug(String.format("Retrieved: key=%s, requiredToExist=%b, value=%s", key, requiredToExist,
                    substitutionResult));
        try {
            setSubstitutionResult(key, substitutionResult);
            return substitutionResult;
        } catch (IllegalArgumentException e) {
            RedactableObject valueFromAnotherThread = substitutionResults.get(key);
            log.warn(String.format(
                    "Another thread calculated a different value: key=%s, requiredToExist=%b, rejected value=%s, value to be returned=%s",
                    key, requiredToExist, substitutionResult, valueFromAnotherThread));
            return valueFromAnotherThread;
        }
    }

    /**
     * Indicate if, in the context of this instance's view, the given key is in the
     * process of being evaluated; return true if an attempt to evaluate the key's
     * substitution value would cause a circular reference, otherwise false
     * 
     * @param key
     *            the mandatory key
     * @return true if an attempt to evaluate the key's substitution value would
     *         cause a circular reference, otherwise false
     * @see #withEvaluationCausingCircularReference(String)
     */
    boolean evaluationCausesCircularReference(String key) {
        return Objects.requireNonNull(key).equals(keyCausingCircularReference)
                || (delegate != null && delegate.evaluationCausesCircularReference(key));
    }

    /**
     * Identify that the key with the given name has had substitution performed for
     * it yielding the given result. This method is idempotent; invoking it with a
     * substitution result equal to the current substitution result (as defined by
     * {@code Object.equals()}) has no effect. The substitution result for a key
     * cannot be changed (again, as defined by {@code Object.equals()}) once it has
     * been set; an attempt to do so will raise an exception.
     * 
     * @param key
     *            the mandatory key, which must exist in the underlying map
     * @param substitutionResult
     *            the mandatory substitution result to set
     * @throws IllegalArgumentException
     *             if the value for the given key had been previously set to a
     *             different value
     */
    void setSubstitutionResult(String key, RedactableObject substitutionResult) throws IllegalArgumentException {
        RedactableObject priorSubstitutionResult = substitutionResults.putIfAbsent(key,
                Objects.requireNonNull(substitutionResult));
        if (priorSubstitutionResult == null)
            keyResolutionMap.put(key, Boolean.TRUE);
        else if (!priorSubstitutionResult.equals(substitutionResult))
            throw new IllegalArgumentException(String.format("Cannot change the substitution result for key=%s", key));
    }

    /**
     * Create and return a new view of this instance where in that context the given
     * key is in the process of being evaluated such that an attempt to evaluate its
     * value again within the same context would cause a circular reference
     * 
     * @param key
     *            the mandatory key
     * @return a new view of this instance where in that context the given key is in
     *         the process of being evaluated such that an attempt to evaluate its
     *         value again within the same context would cause a circular
     * @see #evaluationCausesCircularReference(String)
     */
    private SubstitutableValues withEvaluationCausingCircularReference(String key) {
        return new SubstitutableValues(this, Objects.requireNonNull(key));
    }

    private RedactableObject getAlwaysNonNullSubstitutionResultFromRawObject(Object rawObject) throws IOException {
        if (rawObject instanceof String)
            return getAlwaysNonNullSubstitutionResultFromRawText((String) rawObject, false, null);
        if (rawObject instanceof Password)
            return getAlwaysNonNullSubstitutionResultFromRawText(((Password) rawObject).value(), true, null);
        return rawObject instanceof RedactableObject ? (RedactableObject) rawObject : new RedactableObject(rawObject);
    }

    private RedactableObject getAlwaysNonNullSubstitutionResultFromRawText(String rawText, boolean forceRedact,
            Integer forcedDelimiterLength) throws IOException {
        String beforeOpeningDelimiterText;
        int delimiterLength;
        String afterOpeningDelimiter;
        boolean debugLogEnabled = forceDebugLogForTesting || log.isDebugEnabled();
        if (forcedDelimiterLength == null) {
            // identify the opening and closing delimiters
            Matcher delimiterDiscoveryMatcher = DELIMITER_DISCOVERY_PATTERN.matcher(Objects.requireNonNull(rawText));
            if (!delimiterDiscoveryMatcher.matches())
                return new RedactableObject(rawText, forceRedact);
            beforeOpeningDelimiterText = delimiterDiscoveryMatcher.group("beforeOpeningDelimiter");
            String openingBrackets = delimiterDiscoveryMatcher.group("openingBrackets");
            delimiterLength = openingBrackets.length();
            afterOpeningDelimiter = delimiterDiscoveryMatcher.group("afterOpeningDelimiter");
            if (debugLogEnabled)
                log.debug(String.format("Identified opening delimiter: $%s", openingBrackets));
            if (delimiterLength > MAX_DELIMITERS)
                throw new IOException(String.format("Cannot force the delimiter length to greater than %d: %d",
                        MAX_DELIMITERS, delimiterLength));
        } else {
            delimiterLength = forcedDelimiterLength.intValue();
            if (delimiterLength < 1 || delimiterLength > MAX_DELIMITERS)
                throw new IllegalArgumentException(String.format(
                        "BUG (should never happen): Cannot force the delimiter length to less than 1 or greater than %d: %d",
                        MAX_DELIMITERS, delimiterLength));
            Matcher identifyOpeningDelimiterPrefixAndSuffixMatcher = IDENTIFY_OPENING_DELIMITER_PREFIX_AND_SUFFIX_PATTERN[delimiterLength
                    - 1].matcher(rawText);
            if (!identifyOpeningDelimiterPrefixAndSuffixMatcher.matches())
                return new RedactableObject(rawText, forceRedact);
            beforeOpeningDelimiterText = identifyOpeningDelimiterPrefixAndSuffixMatcher.group("beforeOpeningDelimiter");
            afterOpeningDelimiter = identifyOpeningDelimiterPrefixAndSuffixMatcher.group("afterOpeningDelimiter");
        }
        RedactableObject firstSubstitutionResult;
        String afterClosingDelimiterText;
        /*
         * Special-case the empty command
         */
        if (afterOpeningDelimiter.startsWith(LONGEST_CLOSING_DELIMITER.substring(0, delimiterLength))) {
            firstSubstitutionResult = new RedactableObject("", forceRedact);
            afterClosingDelimiterText = afterOpeningDelimiter.substring(delimiterLength);
        } else {
            /*
             * We've found the opening delimiter; now look for the closing delimiter with
             * the same number of (closing) brackets, which closes the substitution command.
             * Use a reluctant qualifier (*?) instead of the greedy one (*) just before the
             * closing delimiter match, otherwise we will match through to the last closing
             * delimiter instead of matching up to just the first one.
             */
            Matcher checkForSubstitutionCommandMatcher = CHECK_FOR_SUBSTITUTION_COMMAND_PATTERN[delimiterLength - 1]
                    .matcher(afterOpeningDelimiter);
            if (!checkForSubstitutionCommandMatcher.matches())
                return new RedactableObject(rawText, forceRedact);
            String substitutionCommandText = checkForSubstitutionCommandMatcher.group("substitutionCommand");
            afterClosingDelimiterText = checkForSubstitutionCommandMatcher.group("afterClosingDelimiter");
            try {
                firstSubstitutionResult = invokeSubstitution(substitutionCommandText, forceRedact);
            } catch (SubstitutionParseException e) {
                log.warn(e.getMessage());
                return new RedactableObject(rawText, forceRedact);
            }
        }
        if (debugLogEnabled)
            log.debug(String.format("Retrieved substitution: %s", firstSubstitutionResult));
        if (afterClosingDelimiterText.isEmpty()) {
            if (beforeOpeningDelimiterText.isEmpty())
                return firstSubstitutionResult;
            else
                return new RedactableObject(beforeOpeningDelimiterText + firstSubstitutionResult.value(),
                        firstSubstitutionResult.isRedacted());
        }
        RedactableObject afterClosingDelimiterResult = getAlwaysNonNullSubstitutionResultFromRawText(
                afterClosingDelimiterText, forceRedact, delimiterLength);
        if (debugLogEnabled)
            log.debug(String.format("Retrieved after closing delimiter: %s", afterClosingDelimiterResult));
        return new RedactableObject(
                beforeOpeningDelimiterText + firstSubstitutionResult.value() + afterClosingDelimiterResult.value(),
                firstSubstitutionResult.isRedacted() || afterClosingDelimiterResult.isRedacted());
    }

    private RedactableObject invokeSubstitution(String substitutionCommandText, boolean forceRedact)
            throws IOException, SubstitutionParseException {
        /*
         * Text must be of the form <TYPE><OPTIONAL_MODIFIERS>=<OPTIONAL_IDENTIFIER>
         */
        boolean debugLogEnabled = forceDebugLogForTesting || log.isDebugEnabled();
        Matcher matcher = DISCOVER_SUBSTITUTION_COMMAND_PUNCTUATION_CHAR_PATTERN.matcher(substitutionCommandText);
        if (!matcher.matches())
            throwUnableToParseException(forceRedact ? RedactableObject.REDACTED : substitutionCommandText);

        String type = matcher.group("type").trim();
        String afterType = matcher.group("afterType");
        List<String> modifiers;
        String identifier;
        char punctuationChar = afterType.charAt(0);
        if (punctuationChar == '=') {
            modifiers = forceRedact ? Arrays.asList("redact") : Collections.<String>emptyList();
            identifier = afterType.substring(1);
        } else {
            int indexOfModifierClosingPuncAndEqualSign = afterType.indexOf(punctuationChar + "=");
            if (indexOfModifierClosingPuncAndEqualSign < 2 || afterType.charAt(1) == punctuationChar
                    || afterType.charAt(indexOfModifierClosingPuncAndEqualSign - 1) == punctuationChar)
                throwUnableToParseException(forceRedact ? RedactableObject.REDACTED : substitutionCommandText);
            String modifiersTextToParse = afterType.substring(1, indexOfModifierClosingPuncAndEqualSign);
            String punctuationRegex = String.format("\\x%02x", (int) punctuationChar);
            String[] modifiersSplit = modifiersTextToParse.split(punctuationRegex);
            modifiers = new ArrayList<>(modifiersSplit.length);
            boolean redactModifierExplicitlySpecified = false;
            for (String modifier : modifiersSplit) {
                if (modifier.trim().isEmpty())
                    throwUnableToParseException(forceRedact ? RedactableObject.REDACTED : substitutionCommandText);
                modifiers.add(modifier);
                redactModifierExplicitlySpecified = redactModifierExplicitlySpecified
                        || "redact".equals(modifier.trim());
            }
            if (forceRedact && !redactModifierExplicitlySpecified)
                modifiers.add("redact");
            modifiers = Collections.unmodifiableList(modifiers);
            identifier = afterType.substring(indexOfModifierClosingPuncAndEqualSign + 2);
        }
        SubstituterType substituter = getSubstituterTypeImplementation(type);
        if (debugLogEnabled)
            log.debug(String.format("Performing substitution of type=%s", type));
        RedactableObject substitutionResult = substituter.doSubstitution(type, modifiers, identifier, this);
        if (substitutionResult == null)
            throw new IOException(String.format("Substituter returned null: %s", substituter.getClass().getName()));
        return substitutionResult;
    }

    private static void throwUnableToParseException(String substitutionCommandText) throws SubstitutionParseException {
        log.error(String.format("Unable to parse substitution command text: %s", substitutionCommandText));
        throw new SubstitutionParseException("Unable to parse substitution command text");
    }

    private SubstituterType getSubstituterTypeImplementation(String requestedType) throws SubstitutionParseException {
        // allow default implementations to be overridden
        String typeDefinitionKey = typeDefinitionKeyPrefix + requestedType + TYPE_DEFINITION_KEY_SUFFIX;
        Boolean keyResolvedStatus = keyResolutionMap.get(typeDefinitionKey);
        // get the underlying value now if we haven't retrieved it before
        if (keyResolvedStatus == null)
            keyResolvedStatus = Boolean.valueOf(resolveKey(typeDefinitionKey));
        Object substituterTypeClassOrName = keyResolvedStatus.booleanValue()
                ? substitutionResults.get(typeDefinitionKey)
                : null;
        if (substituterTypeClassOrName == null)
            substituterTypeClassOrName = getDefaultSubstituterTypeClassName(requestedType);
        if (substituterTypeClassOrName == null)
            throw new SubstitutionParseException(String.format("No such substituter type (set it via the '%s' key): %s",
                    typeDefinitionKey, requestedType));
        try {
            if (substituterTypeClassOrName instanceof Class)
                return SubstituterType.class.cast(((Class<?>) substituterTypeClassOrName).newInstance());
            else
                return SubstituterType.class.cast(Class.forName(substituterTypeClassOrName.toString()).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new SubstitutionParseException(e.getMessage());
        }
    }

    private boolean resolveKey(String typeDefinitionKey) {
        Object rawObjectRequestedNow = underlyingValues.get(typeDefinitionKey);
        if (rawObjectRequestedNow == null) {
            // no underlying value associated with the key
            Boolean retvalKeyResolvedStatus = keyResolutionMap.putIfAbsent(typeDefinitionKey, Boolean.FALSE);
            return retvalKeyResolvedStatus != null ? retvalKeyResolvedStatus.booleanValue() : false;
        }
        // we just retrieved the value
        try {
            setSubstitutionResult(typeDefinitionKey, new RedactableObject(rawObjectRequestedNow));
        } catch (IllegalArgumentException e) {
            RedactableObject valueFromAnotherThread = substitutionResults.get(typeDefinitionKey);
            log.warn(String.format(
                    "Another thread calculated a different value: key=%s, rejected value=%s, value to be used=%s",
                    typeDefinitionKey, rawObjectRequestedNow, valueFromAnotherThread));
        }
        return true;
    }

    private static String getDefaultSubstituterTypeClassName(String requestedType) {
        String retval = null;
        switch (requestedType) {
            case "file":
                retval = FileContentSubstituterType.class.getName();
                break;
            case "envVar":
                retval = EnvironmentVariableSubstituterType.class.getName();
                break;
            case "sysProp":
                retval = SystemPropertySubstituterType.class.getName();
                break;
            case "keyValue":
                retval = KeyValueSubstituterType.class.getName();
                break;
            default:
                break;
        }
        return retval;
    }
}
