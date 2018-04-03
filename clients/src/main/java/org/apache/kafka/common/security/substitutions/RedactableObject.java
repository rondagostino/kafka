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

import java.util.Objects;

import org.apache.kafka.common.config.types.Password;

/**
 * An object whose text value can be redacted
 */
public class RedactableObject {
    static final String REDACTED = "[redacted]";
    private final Object object;
    private final boolean redacted;

    /**
     * Constructor for an instance that will be redacted only if the given object is
     * of type {@link Password}.
     * 
     * @param object
     *            the mandatory object
     */
    public RedactableObject(Object object) {
        this(Objects.requireNonNull(object), object instanceof Password);
    }

    /**
     * Constructor
     * 
     * @param object
     *            the mandatory object
     * @param redact
     *            when true the object's value will be redacted in
     *            {@link #redactedText()}
     */
    public RedactableObject(Object object, boolean redact) {
        this.object = Objects.requireNonNull(object);
        this.redacted = redact;
    }

    /**
     * Return the (always non-null) underlying object provided during instance
     * construction
     * 
     * @return the (always non-null) underlying object provided during instance
     *         construction
     */
    public Object object() {
        return object;
    }

    /**
     * Return true if this instance contains information that will be redacted when
     * {@link #redactedText()} is invoked, otherwise false
     * 
     * @return true if this instance contains information that will be redacted when
     *         {@link #redactedText()} is invoked, otherwise false
     */
    public boolean isRedacted() {
        return redacted;
    }

    /**
     * Return the redacted text for this instance, if redaction is required,
     * otherwise return the {@link #value()}
     * 
     * @return the redacted text for this instance, if redaction is required,
     *         otherwise return the {@link #value()}
     */
    public String redactedText() {
        return redacted ? REDACTED : value();
    }

    /**
     * Return the {@code String} value of this instance, including information that
     * would otherwise be redacted
     * 
     * @return the {@code String} value of this instance, including information that
     *         would otherwise be redacted
     */
    public String value() {
        if (object instanceof String)
            return (String) object;
        if (object instanceof Password)
            return ((Password) object).value();
        return object.toString();
    }

    /**
     * Return true if this result is considered to be empty, otherwise false
     * 
     * @return true if this result is considered to be empty, otherwise false
     */
    public boolean isEmpty() {
        return value().isEmpty();
    }

    /**
     * Return true if this result is considered to be blank (containing at most just
     * whitespace), otherwise false
     * 
     * @return true if this result is considered to be blank (containing at most
     *         just whitespace), otherwise false
     */
    public boolean isBlank() {
        return value().trim().isEmpty();
    }

    /**
     * Return this instance if it is redacted according to {@link #isRedacted()},
     * otherwise return a new, redacted instance with the same underlying object
     * 
     * @return this instance if it is redacted according to {@link #isRedacted()},
     *         otherwise return a new, redacted instance with the same underlying
     *         object
     */
    public RedactableObject redactedVersion() {
        return redacted ? this : new RedactableObject(object, true);
    }

    @Override
    public String toString() {
        // be sure to redact information as required
        return redactedText();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RedactableObject))
            return false;
        RedactableObject other = (RedactableObject) obj;
        /*
         * Note that differences in redaction cause inequality
         */
        return redacted == other.redacted && object.equals(other.object);
    }

    @Override
    public int hashCode() {
        return object.hashCode();
    }
}
