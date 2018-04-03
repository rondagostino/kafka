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

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The representation of a modifier with textual form {@code <name>=value}. The
 * {@code name} is always trimmed; the {@code value} can optionally be trimmed.
 */
public class NameEqualsValue {
    private static final Pattern NAME_EQUALS_VALUE_PATTERN = Pattern.compile("\\A\\s*?(?!\\s)([^=]+?)=(.+)\\z");
    private final String name;
    private final String value;

    /**
     * Constructor
     * 
     * @param name
     *            the mandatory name; it will be trimmed, and the result must not be
     *            empty and must not contain the equal sign (=)
     * @param value
     *            the mandatory value
     */
    public NameEqualsValue(String name, String value) {
        this.name = Objects.requireNonNull(name).trim();
        this.value = Objects.requireNonNull(value);
        if (this.name.isEmpty() || this.name.contains("="))
            throw new IllegalArgumentException(String.format("Illegal name: %s", name));
    }

    /**
     * Return the (always non-null and trimmed) name
     * 
     * @return the (always non-null and trimmed) name
     */
    public String name() {
        return name;
    }

    /**
     * Return the (always non-null) value
     * 
     * @return the (always non-null) value
     */
    public String value() {
        return value;
    }

    /**
     * Create and return a new instance if the given modifier text matches the
     * required syntax, with value being trimmed, otherwise return null
     * 
     * @param modifierText
     *            the text to examine
     * @return a new instance if the given modifier text matches the required
     *         syntax, with value being trimmed, otherwise null
     */
    public static NameEqualsValue from(String modifierText) {
        return from(modifierText, true);
    }

    /**
     * Create and return a new instance if the given modifier text matches the
     * required syntax, otherwise return null
     * 
     * @param modifierText
     *            the text to examine
     * @param trimValue
     *            if true the matched value will be trimmed, otherwise not
     * @return a new instance if the given modifier text matches the required
     *         syntax, otherwise null
     */
    public static NameEqualsValue from(String modifierText, boolean trimValue) {
        Matcher matcher = NAME_EQUALS_VALUE_PATTERN.matcher(Objects.requireNonNull(modifierText));
        if (!matcher.matches())
            return null;
        String parsedName = matcher.group(1);
        String parsedValue = matcher.group(2);
        return new NameEqualsValue(parsedName.trim(), trimValue ? parsedValue.trim() : parsedValue);
    }

    @Override
    public String toString() {
        return name + "=" + value;
    }
}
