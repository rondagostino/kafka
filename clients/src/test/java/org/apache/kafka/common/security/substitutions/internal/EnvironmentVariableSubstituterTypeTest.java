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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.security.substitutions.RedactableObject;
import org.apache.kafka.common.security.substitutions.SubstitutableValues;
import org.apache.kafka.common.security.substitutions.UnderlyingValues;
import org.junit.Test;

public class EnvironmentVariableSubstituterTypeTest {
    @Test
    public void doSubstitution() throws IOException {
        List<String> noModifiers = Collections.<String>emptyList();
        Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
        for (Entry<String, String> entry : System.getenv().entrySet()) {
            if (entry.getValue() != null && !entry.getValue().trim().isEmpty()) {
                assertEquals(new RedactableObject(entry.getValue(), false),
                        new EnvironmentVariableSubstituterType().doSubstitution("envvar", noModifiers, entry.getKey(),
                                new SubstitutableValues(underlyingValuesFrom(noOptions))));
            }
        }
        Map<String, RedactableObject> injectedValuesForTesting = new HashMap<>();
        RedactableObject expected = new RedactableObject("b", false);
        injectedValuesForTesting.put("a", expected);
        assertEquals(expected, new EnvironmentVariableSubstituterType(injectedValuesForTesting).doSubstitution("envar",
                noModifiers, "a", new SubstitutableValues(underlyingValuesFrom(noOptions))));
    }

    @Test
    public void ignoreExtraFlag() throws IOException {
        List<String> unknownModifiers = Arrays.asList("foo");
        Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
        for (Entry<String, String> entry : System.getenv().entrySet()) {
            if (entry.getValue() != null && !entry.getValue().trim().isEmpty()) {
                new EnvironmentVariableSubstituterType().doSubstitution("envvar", unknownModifiers, entry.getKey(),
                        new SubstitutableValues(underlyingValuesFrom(noOptions)));
            }
        }
    }

    @Test
    public void ignoreExtraArgument() throws IOException {
        List<String> unknownModifiers = Arrays.asList("foo=123");
        Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
        for (Entry<String, String> entry : System.getenv().entrySet()) {
            if (entry.getValue() != null && !entry.getValue().trim().isEmpty()) {
                new EnvironmentVariableSubstituterType().doSubstitution("envvar", unknownModifiers, entry.getKey(),
                        new SubstitutableValues(underlyingValuesFrom(noOptions)));
            }
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
