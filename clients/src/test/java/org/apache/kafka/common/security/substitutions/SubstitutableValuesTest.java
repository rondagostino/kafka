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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.kafka.common.config.types.Password;
import org.junit.Test;

public class SubstitutableValuesTest {
    public static class TestSubstituter implements SubstituterType {
        @Override
        public RedactableObject doSubstitution(String type, List<String> modifiers, String value,
                SubstitutableValues substitutableValues) throws IOException {
            return new RedactableObject(value + modifiers.toString(), modifiers.contains("redact"));
        }
    }

    public static class TestRedactingSubstituter extends TestSubstituter {
        @Override
        public RedactableObject doSubstitution(String type, List<String> modifiers, String value,
                SubstitutableValues substitutableValues) throws IOException {
            return super.doSubstitution(type, modifiers, value, substitutableValues).redactedVersion();
        }
    }

    /**
     * Bad substituter class that illegally returns null
     */
    public static class BadSubstituter implements SubstituterType {
        @Override
        public RedactableObject doSubstitution(String type, List<String> modifiers, String value,
                SubstitutableValues substitutableValues) throws IOException {
            return null;
        }
    }

    public static class CircularReferenceSubstituter implements SubstituterType {
        // make sure the first instance we create will work and subsequent instances
        // will cause a circular reference
        static int instanceCount = 0;
        int maxCallsAllowed = ++instanceCount;
        int numCalls = 0;

        @Override
        public RedactableObject doSubstitution(String type, List<String> modifiers, String value,
                SubstitutableValues substitutableValues) throws IOException {
            if (++numCalls < maxCallsAllowed)
                return substitutableValues.getSubstitutionResult("b", false);
            return new RedactableObject("a", false);
        }
    }

    public static class FunkyObjectSubstituter implements SubstituterType {
        @Override
        public RedactableObject doSubstitution(String type, List<String> modifiers, final String value,
                SubstitutableValues substitutableValues) throws IOException {
            final boolean redact = modifiers.contains("redact");
            return new RedactableObject(SubstitutableValuesTest.class, redact) {
                @Override
                public String value() {
                    return value + ":" + SubstitutableValuesTest.class.getName();
                }
            };
        }
    }

    @Test
    public void optionsMap() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("a", "b");
        UnderlyingValues underlyingValues = underlyingValuesFrom(optionsMap);
        SubstitutableValues substitutableValues = new SubstitutableValues(underlyingValues);
        assertSame(underlyingValues, substitutableValues.underlyingValues());
        assertFalse(substitutableValues.evaluationCausesCircularReference("a"));
        assertFalse(substitutableValues.evaluationCausesCircularReference("b"));
    }

    @Test
    public void substitutionResults() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("a", "b");
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(optionsMap));
        assertTrue(substitutableValues.substitutionResults().isEmpty());
        assertFalse(substitutableValues.evaluationCausesCircularReference("a"));
    }

    @Test
    public void substitutionResultsForNonStringNonPassword() throws IOException {
        final Object[] objects = new Object[] {Boolean.TRUE, Integer.valueOf("1"), Short.valueOf("2"),
            Long.valueOf("3"), BigDecimal.valueOf(4.5), Arrays.asList("1", "b"), getClass()};
        for (Object object : objects) {
            Map<String, Object> optionsMap = new HashMap<>();
            optionsMap.put("a", object);
            SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(optionsMap));
            RedactableObject substitutionResult = substitutableValues.getSubstitutionResult("a", true);
            assertFalse(substitutionResult.isRedacted());
            assertEquals(object, substitutionResult.object());
        }
    }

    @Test
    public void substitutionResultForPassword() throws IOException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, new Password("$[testSubstituter=foo]"));
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject actual = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(new RedactableObject("foo[redact]", true), actual);
    }

    @Test
    public void setSubstitutionResultNotRedacted() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("a", "b");
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(optionsMap));
        RedactableObject expected = new RedactableObject("b", false);
        substitutableValues.setSubstitutionResult("a", expected);
        assertEquals(expected, substitutableValues.substitutionResults().get("a"));
        substitutableValues.setSubstitutionResult("a", expected);
        try {
            substitutableValues.setSubstitutionResult("a", new RedactableObject("c", false));
            fail();
        } catch (Exception ignore) {
            // ignore
        }
        try {
            substitutableValues.setSubstitutionResult("a", expected.redactedVersion());
            fail();
        } catch (Exception ignore) {
            // ignore
        }
        assertFalse(substitutableValues.evaluationCausesCircularReference("a"));
        assertFalse(substitutableValues.evaluationCausesCircularReference("b"));
    }

    @Test
    public void setSubstitutionResultRedacted() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("a", "b");
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(optionsMap));
        RedactableObject expected = new RedactableObject("b", true);
        substitutableValues.setSubstitutionResult("a", expected);
        assertEquals(expected, substitutableValues.substitutionResults().get("a"));
        substitutableValues.setSubstitutionResult("a", expected);
        try {
            substitutableValues.setSubstitutionResult("a", new RedactableObject("c", true));
            fail();
        } catch (Exception ignore) {
            // ignore
        }
        try {
            substitutableValues.setSubstitutionResult("a", new RedactableObject("b", false));
            fail();
        } catch (Exception ignore) {
            // ignore
        }
        assertFalse(substitutableValues.evaluationCausesCircularReference("a"));
        assertFalse(substitutableValues.evaluationCausesCircularReference("b"));
    }

    @Test
    public void optionEvaluationCausesCircularReference() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("a", "a");
        optionsMap.put("b", "b");
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(optionsMap));
        assertFalse(substitutableValues.evaluationCausesCircularReference("a"));
        assertFalse(substitutableValues.evaluationCausesCircularReference("b"));
        SubstitutableValues aInProgress = new SubstitutableValues(substitutableValues, "a");
        assertTrue(aInProgress.evaluationCausesCircularReference("a"));
        assertFalse(aInProgress.evaluationCausesCircularReference("b"));
        SubstitutableValues bInProgress = new SubstitutableValues(substitutableValues, "b");
        assertFalse(bInProgress.evaluationCausesCircularReference("a"));
        assertTrue(bInProgress.evaluationCausesCircularReference("b"));
        assertTrue(aInProgress.evaluationCausesCircularReference("a"));
        assertFalse(aInProgress.evaluationCausesCircularReference("b"));
        SubstitutableValues abInProgress1 = new SubstitutableValues(aInProgress, "b");
        SubstitutableValues abInProgress2 = new SubstitutableValues(bInProgress, "a");
        assertTrue(abInProgress1.evaluationCausesCircularReference("a"));
        assertTrue(abInProgress1.evaluationCausesCircularReference("b"));
        assertTrue(abInProgress2.evaluationCausesCircularReference("a"));
        assertTrue(abInProgress2.evaluationCausesCircularReference("b"));
    }

    @Test
    public void handleBuiltinSubstituters() throws IOException, UnsupportedCallbackException {
        RedactableObject expected = new RedactableObject("foo", false);
        String optionName = "a";
        boolean optionRequiredToExist = true;
        for (String type : new String[] {"file", "keyValue", "sysProp", "envVar"}) {
            Map<String, Object> moduleOptionsMap = new HashMap<>();
            moduleOptionsMap.put(optionName, "$[" + type + "/defaultValue=foo/=doesNotExist]");
            SubstitutableValues substitutableValues = new SubstitutableValues("",
                    underlyingValuesFrom(moduleOptionsMap));
            RedactableObject actual = substitutableValues.getSubstitutionResult(optionName, optionRequiredToExist);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void simpleSubstutution() throws IOException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, "b");
        boolean optionRequiredToExist = true;
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        for (int i = 1; i <= 2; ++i) {
            RedactableObject expected = new RedactableObject("b", false);
            assertEquals(expected, substitutableValues.getSubstitutionResult(optionName, optionRequiredToExist));
            assertEquals(expected, substitutableValues.substitutionResults().get(optionName));
        }
    }

    @Test
    public void withoutClosingSubstitutionDelimiter() throws IOException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, "$[foo");
        boolean optionRequiredToExist = true;
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap));
        for (int i = 1; i <= 2; ++i) {
            RedactableObject expected = new RedactableObject("$[foo", false);
            assertEquals(expected, substitutableValues.getSubstitutionResult(optionName, optionRequiredToExist));
            assertEquals(expected, substitutableValues.substitutionResults().get(optionName));
        }
    }

    @Test
    public void optionThatDoesNotExist() throws IOException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionNameThatDoesNotExist = "doesNotExist";
        for (boolean optionRequiredToExist : new boolean[] {true, false}) {
            SubstitutableValues substitutableValues = new SubstitutableValues("",
                    underlyingValuesFrom(moduleOptionsMap), true);
            try {
                RedactableObject substitutionResult = substitutableValues
                        .getSubstitutionResult(optionNameThatDoesNotExist, optionRequiredToExist);
                if (optionRequiredToExist)
                    fail("Option was required to exist, but an exception was not generated");
                else
                    assertNull(substitutionResult);
                assertNull(substitutableValues.substitutionResults().get(optionNameThatDoesNotExist));
                assertNull(
                        substitutableValues.getSubstitutionResult(optionNameThatDoesNotExist, optionRequiredToExist));
            } catch (final Exception e) {
                if (!optionRequiredToExist)
                    fail("Option was not required to exist, but an exception was generated anyway");
            }
        }
    }

    @Test
    public void handleSubstitutionNoPrefixOrSuffix() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, "$[testSubstituter=foo]");
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject actual = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(new RedactableObject("foo[]", false), actual);
    }

    @Test
    public void handleSubstitutionEqualSignInModifiers() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, "$[testSubstituter/a=1/ b = 2 /c=3/=foo]");
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject actual = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(new RedactableObject("foo[a=1,  b = 2 , c=3]", false), actual);
    }

    @Test
    public void handleEmptySubstitutionSpecialCase() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        RedactableObject expected = new RedactableObject("$[testSubstituter=foo1]" + "foo2[]", false);
        moduleOptionsMap.put(optionName, "$[[]]$[testSubstituter=foo1]$[[testSubstituter=foo2]]");
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject actual = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(expected, actual);
    }

    @Test
    public void handleTwoSubstitutionsNoPrefixOrSuffix() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, "$[testSubstituter=foo1=1]$[testSubstituter=foo2=2]");
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject actual = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(new RedactableObject("foo1=1[]foo2=2[]", false), actual);
    }

    @Test
    public void handleSubstitution() throws IOException, UnsupportedCallbackException {
        // any punctuation except for the equal sign (=) is valid
        for (char modifierDelimiter : new char[] {'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.',
            '/', ':', ';', '<', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~'}) {
            // up to 5 brackets each in the opening/closing delimiters
            for (String openingDelimiter : new String[] {"$[", "$[[", "$[[[", "$[[[[", "$[[[[["}) {
                if (openingDelimiter.equals("$[") && modifierDelimiter == ']')
                    continue;
                String closingDelimiter = openingDelimiter.substring(1).replace('[', ']');
                for (boolean redact : new boolean[] {false, true}) {
                    for (String modifiersText : new String[] {"", "" + modifierDelimiter + "a" + modifierDelimiter,
                        "" + modifierDelimiter + "a" + modifierDelimiter + "bb" + modifierDelimiter}) {
                        String expectedModifiersText = modifiersText.isEmpty() ? "[]"
                                : modifiersText.length() == 3 ? "[a]" : "[a, bb]";
                        Map<String, Object> moduleOptionsMap = new HashMap<>();
                        String optionName = "a";
                        moduleOptionsMap.put(optionName, String.format(
                                " prefix %1$stestSubstituter%2$s=middle1 %3$s%1$stestSubstituter%2$s= middle2 %3$s_%1$stestSubstituter%2$s=middle3%3$s suffix%1$stestSubstituter%2$s=end%3$s",
                                openingDelimiter, modifiersText, closingDelimiter));
                        moduleOptionsMap.put("testSubstituterSubstituterType",
                                redact ? TestRedactingSubstituter.class.getName() : TestSubstituter.class.getName());
                        SubstitutableValues substitutableValues = new SubstitutableValues("",
                                underlyingValuesFrom(moduleOptionsMap), true);
                        RedactableObject actual = substitutableValues.getSubstitutionResult(optionName, true);
                        RedactableObject expected = new RedactableObject(
                                String.format(" prefix middle1 %1$s middle2 %1$s_middle3%1$s suffixend%1$s",
                                        expectedModifiersText),
                                redact);
                        assertEquals(expected, actual);
                    }
                }
            }
        }
    }

    @Test
    public void handleMalformedSubstitutionCommandTextUnknownType() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        String value = "$[unknownType=foo]"; // legal except unknown type
        moduleOptionsMap.put(optionName, value);
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject substitutionResult = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(value, substitutionResult.value());
    }

    @Test
    public void handleMalformedSubstitutionCommandTextMissingEqualSign()
            throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        String value = "$[testSubstituter]"; // missing '=' at the end
        moduleOptionsMap.put(optionName, value);
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject substitutionResult = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(value, substitutionResult.value());
    }

    @Test
    public void handleMalformedSubstitutionCommandTextPunctuationBeforeType()
            throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        String value = "$['testSubstituter=]";
        moduleOptionsMap.put(optionName, value);
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        RedactableObject substitutionResult = substitutableValues.getSubstitutionResult(optionName, true);
        assertEquals(value, substitutionResult.value());
    }

    @Test(expected = IOException.class)
    public void handleMalformedSubstitutionCommandTooManyDelimiters() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, "$[[[[[[testSubstituter=]]]]]]"); // 6 brackets is illegal
        moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        substitutableValues.getSubstitutionResult(optionName, true);
    }

    @Test
    public void handleMalformedSubstitutionCommandTextMissingConstrainDelimiter()
            throws IOException, UnsupportedCallbackException {
        for (String malformedModifier : new String[] {"/", "//", "/a", "/a//", "/a/b", "/a/b//", "/a//b/"}) {
            Map<String, Object> moduleOptionsMap = new HashMap<>();
            String optionName = "a";
            String value = "$[testSubstituter" + malformedModifier + "=]";
            moduleOptionsMap.put(optionName, value);
            moduleOptionsMap.put("testSubstituterSubstituterType", TestSubstituter.class.getName());
            SubstitutableValues substitutableValues = new SubstitutableValues("",
                    underlyingValuesFrom(moduleOptionsMap), true);
            RedactableObject substitutionResult = substitutableValues.getSubstitutionResult(optionName, true);
            assertEquals(value, substitutionResult.value());
        }
    }

    @Test(expected = IOException.class)
    public void handleBadSubstituter() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionName = "a";
        moduleOptionsMap.put(optionName, "$[badSubstituter=foo]");
        moduleOptionsMap.put("badSubstituterSubstituterType", BadSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        substitutableValues.getSubstitutionResult(optionName, true);
    }

    @Test
    public void handleCircularReference() throws IOException, UnsupportedCallbackException {
        Map<String, Object> moduleOptionsMap = new HashMap<>();
        String optionNameA = "a";
        moduleOptionsMap.put(optionNameA, "$[circularReferenceSubstituter=a]");
        String optionNameB = "b";
        moduleOptionsMap.put(optionNameB, "$[circularReferenceSubstituter=b]");
        moduleOptionsMap.put("circularReferenceSubstituterSubstituterType",
                CircularReferenceSubstituter.class.getName());
        SubstitutableValues substitutableValues = new SubstitutableValues("", underlyingValuesFrom(moduleOptionsMap),
                true);
        try {
            // first one should succeed
            substitutableValues.getSubstitutionResult(optionNameA, true);
            try {
                substitutableValues.getSubstitutionResult(optionNameB, true);
                fail("did not detect circular reference");
            } catch (IOException expected) {
                // empty
            }
        } catch (IOException e) {
            fail("Should not have generated ewrror on first attempt");
        }
    }

    @Test
    public void handleFunkyRedactableObject() throws IOException, UnsupportedCallbackException {
        for (boolean redact : new boolean[] {true, false}) {
            Map<String, Object> moduleOptionsMap = new HashMap<>();
            moduleOptionsMap.put("funky", "$[funkySubstituter" + (redact ? "/redact/" : "") + "=foo]");
            moduleOptionsMap.put("notFunky", "begin $[funkySubstituter" + (redact ? "/redact/" : "") + "=foo] end");
            moduleOptionsMap.put("funkySubstituterSubstituterType", FunkyObjectSubstituter.class.getName());
            boolean optionRequiredToExist = true;
            SubstitutableValues substitutableValues = new SubstitutableValues("",
                    underlyingValuesFrom(moduleOptionsMap), true);
            RedactableObject funkyResult = substitutableValues.getSubstitutionResult("funky", optionRequiredToExist);
            RedactableObject notFunkyResult = substitutableValues.getSubstitutionResult("notFunky",
                    optionRequiredToExist);
            assertTrue(funkyResult.object() instanceof Class);
            assertEquals(redact, funkyResult.isRedacted());
            if (redact)
                assertEquals(RedactableObject.REDACTED, funkyResult.toString());
            assertEquals("foo:" + getClass().getName(), funkyResult.value());
            assertTrue(notFunkyResult.object() instanceof String);
            assertEquals(redact, notFunkyResult.isRedacted());
            if (redact)
                assertEquals(RedactableObject.REDACTED, notFunkyResult.toString());
            assertEquals("begin foo:" + getClass().getName() + " end", notFunkyResult.value());
        }
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
