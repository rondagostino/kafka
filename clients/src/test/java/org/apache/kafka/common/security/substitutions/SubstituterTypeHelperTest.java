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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class SubstituterTypeHelperTest {
    private static final SubstitutableValues NO_SUBSTITUTABLE_VALUES = new SubstitutableValues(
            underlyingValuesFrom(Collections.<String, Object>emptyMap()));

    public static class TestSubstituterType extends SubstituterTypeHelper {
        private final Map<String, RedactableObject> injectedMapForTesting;

        public TestSubstituterType() {
            this(null);
        }

        public TestSubstituterType(Map<String, RedactableObject> injectedMapForTesting) {
            this.injectedMapForTesting = injectedMapForTesting;
        }

        @Override
        public RedactableObject retrieveResult(String type, String identifier, boolean redact,
                Set<String> additionalFlags, Map<String, String> additionalArgs,
                SubstitutableValues substitutableOptions) {
            if (injectedMapForTesting == null)
                return null;
            RedactableObject retval = injectedMapForTesting.get(identifier);
            return retval != null && redact ? retval.redactedVersion() : retval;
        }
    }

    public static class FlagsAndArgsSubstituterType extends SubstituterTypeHelper {
        @Override
        public RedactableObject retrieveResult(String type, String identifier, boolean redact,
                Set<String> additionalFlags, Map<String, String> additionalArgs,
                SubstitutableValues substitutableOptions) {
            return new RedactableObject(additionalFlags.toString() + additionalArgs.toString(), false);
        }
    }

    @Test
    public void doSubstitution() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        RedactableObject expected = new RedactableObject("b", false);
        injectedMapForTesting.put("a", expected);
        assertEquals(expected, new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Collections.<String>emptyList(), "a", NO_SUBSTITUTABLE_VALUES));
    }

    @Test
    public void doSubstitutionRedact() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        RedactableObject inputNotRedacted = new RedactableObject("b", false);
        injectedMapForTesting.put("a", inputNotRedacted);
        assertEquals(inputNotRedacted.redactedVersion(), new TestSubstituterType(injectedMapForTesting)
                .doSubstitution("type", Arrays.asList("redact"), "a", NO_SUBSTITUTABLE_VALUES));
    }

    @Test
    public void doSubstitutionMultipleRedundantFlags() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        RedactableObject inputNotRedacted = new RedactableObject("b", false);
        injectedMapForTesting.put("a", inputNotRedacted);
        assertEquals(inputNotRedacted.redactedVersion(), new TestSubstituterType(injectedMapForTesting)
                .doSubstitution("type", Arrays.asList("redact", "redact", " redact "), "a", NO_SUBSTITUTABLE_VALUES));
    }

    @Test
    public void doSubstitutionDefaultValue() throws IOException {
        for (boolean needDefault : new Boolean[] {true, false}) {
            Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
            RedactableObject expected = new RedactableObject("b", false);
            if (!needDefault)
                injectedMapForTesting.put("a", expected);
            assertEquals(expected, new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                    Arrays.asList("defaultValue=b"), "a", NO_SUBSTITUTABLE_VALUES));
        }
    }

    @Test
    public void doSubstitutionDefaultOption() throws IOException {
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("b", "c");
        assertEquals(new RedactableObject("c", false),
                new TestSubstituterType(Collections.<String, RedactableObject>emptyMap()).doSubstitution("type",
                        Arrays.asList("defaultKey=b"), "a",
                        new SubstitutableValues(underlyingValuesFrom(underlyingMap))));
    }

    @Test
    public void doSubstitutionFromOption() throws IOException {
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("a", "b");
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        RedactableObject expected = new RedactableObject("c", false);
        injectedMapForTesting.put("b", expected);
        assertEquals(expected, new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Arrays.asList("fromValueOfKey"), "a", new SubstitutableValues(underlyingValuesFrom(underlyingMap))));
    }

    @Test
    public void doSubstitutionFromOptionContainingSubstitution() throws IOException {
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("testSubstituterType", TestSubstituterType.class.getName());
        underlyingMap.put("a", "$[test/defaultValue=x/=w]");
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        RedactableObject expected = new RedactableObject("y", false);
        injectedMapForTesting.put("x", expected);
        assertEquals(expected, new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Arrays.asList("fromValueOfKey"), "a", new SubstitutableValues(underlyingValuesFrom(underlyingMap))));
    }

    @Test
    public void doSubstitutionFromOptionWithRedact() throws IOException {
        /*
         * Note that we explicitly decided to not propagate the redact flag when it
         * applies to option names
         */
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("testSubstituterType", TestSubstituterType.class.getName());
        underlyingMap.put("a", "$[test/redact/defaultValue=x/=w]");
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        RedactableObject expected = new RedactableObject("y", false);
        injectedMapForTesting.put("x", expected);
        assertEquals(expected, new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Arrays.asList("fromValueOfKey"), "a", new SubstitutableValues(underlyingValuesFrom(underlyingMap))));
    }

    @Test
    public void doSubstitutionEmpty() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        RedactableObject expected = new RedactableObject("", false);
        injectedMapForTesting.put("a", expected);
        assertEquals(expected, new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Collections.<String>emptyList(), "a", NO_SUBSTITUTABLE_VALUES));
    }

    @Test
    public void doSubstitutionNotEmptyOrNotBlankOnEmptyValue() throws IOException {
        for (String notEmptyOrNotBlank : new String[] {"notEmpty", "notBlank"}) {
            Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
            injectedMapForTesting.put("a", new RedactableObject("", false));
            assertEquals(new RedactableObject("b", false),
                    new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                            Arrays.asList(notEmptyOrNotBlank, "defaultValue=b"), "a", NO_SUBSTITUTABLE_VALUES));
        }
    }

    @Test
    public void doSubstitutionBlank() throws IOException {
        for (boolean notEmpty : new boolean[] {true, false}) {
            Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
            RedactableObject expected = new RedactableObject(" ", false);
            injectedMapForTesting.put("a", expected);
            assertEquals(expected,
                    new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                            notEmpty ? Arrays.asList("notEmpty") : Collections.<String>emptyList(), "a",
                            NO_SUBSTITUTABLE_VALUES));
        }
    }

    @Test
    public void doSubstitutionNotBlankOnBlankValue() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        injectedMapForTesting.put("a", new RedactableObject(" ", false));
        assertEquals(new RedactableObject("b", false), new TestSubstituterType(injectedMapForTesting)
                .doSubstitution("type", Arrays.asList("notBlank", "defaultValue=b"), "a", NO_SUBSTITUTABLE_VALUES));
    }

    @Test(expected = IOException.class)
    public void defaultValueTwice() throws IOException {
        new TestSubstituterType(Collections.<String, RedactableObject>emptyMap()).doSubstitution("type",
                Arrays.asList("defaultValue=b", "defaultValue=b"), "a", NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void defaultOptionTwice() throws IOException {
        new TestSubstituterType(Collections.<String, RedactableObject>emptyMap()).doSubstitution("type",
                Arrays.asList("defaultOption=b", "defaultOption=b"), "a", NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void defaultOptionDoesNotExist() throws IOException {
        new TestSubstituterType(Collections.<String, RedactableObject>emptyMap()).doSubstitution("type",
                Arrays.asList("defaultOption=b"), "a", NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void fromOptionDoesNotExist() throws IOException {
        new TestSubstituterType(Collections.<String, RedactableObject>emptyMap()).doSubstitution("type",
                Arrays.asList("fromOption"), "a", NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void envVarDoesNotExist() throws IOException {
        new TestSubstituterType(Collections.<String, RedactableObject>emptyMap()).doSubstitution("type",
                Collections.<String>emptyList(), "a", NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void defaultValueAndDefaultKey() throws IOException {
        new TestSubstituterType(Collections.<String, RedactableObject>emptyMap()).doSubstitution("type",
                Arrays.asList("defaultValue=b", "defaultKey=b"), "a", NO_SUBSTITUTABLE_VALUES);
    }

    @Test
    public void unknownModifiers() throws IOException {
        assertEquals(new RedactableObject("[foo]{a= b}", false), new FlagsAndArgsSubstituterType()
                .doSubstitution("type", Arrays.asList(" foo ", "a = b"), "", NO_SUBSTITUTABLE_VALUES));
    }

    @Test(expected = IOException.class)
    public void notEmptyOnEmptyValue() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        injectedMapForTesting.put("a", new RedactableObject("", false));
        new TestSubstituterType(injectedMapForTesting).doSubstitution("type", Arrays.asList("notEmpty"), "a",
                NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void notBlankOnEmptyValue() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        injectedMapForTesting.put("a", new RedactableObject("", false));
        new TestSubstituterType(injectedMapForTesting).doSubstitution("type", Arrays.asList("notBlank"), "a",
                NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void notBlankOnBlankValue() throws IOException {
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        injectedMapForTesting.put("a", new RedactableObject(" ", false));
        new TestSubstituterType(injectedMapForTesting).doSubstitution("type", Arrays.asList("notBlank"), "a",
                NO_SUBSTITUTABLE_VALUES);
    }

    @Test(expected = IOException.class)
    public void notEmptyOnEmptyDefaultValue() throws IOException {
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("b", "");
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        injectedMapForTesting.put("a", new RedactableObject("", false));
        new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Arrays.asList("notEmpty", "defaultOption=b"), "a",
                new SubstitutableValues(underlyingValuesFrom(underlyingMap)));
    }

    @Test(expected = IOException.class)
    public void notBlankOnEmptyDefaultValue() throws IOException {
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("b", "");
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        injectedMapForTesting.put("a", new RedactableObject("", false));
        new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Arrays.asList("notBlank", "defaultOption=b"), "a",
                new SubstitutableValues(underlyingValuesFrom(underlyingMap)));
    }

    @Test(expected = IOException.class)
    public void notBlankOnBlankDefaultValue() throws IOException {
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("b", " ");
        Map<String, RedactableObject> injectedMapForTesting = new HashMap<>();
        injectedMapForTesting.put("a", new RedactableObject(" ", false));
        new TestSubstituterType(injectedMapForTesting).doSubstitution("type",
                Arrays.asList("notBlank", "defaultOption=b"), "a",
                new SubstitutableValues(underlyingValuesFrom(underlyingMap)));
    }

    private static UnderlyingValues underlyingValuesFrom(final Map<String, Object> map) {
        return new UnderlyingValues() {
            @Override
            public Object get(String key) {
                return map.get(key);
            }
        };
    }
}
