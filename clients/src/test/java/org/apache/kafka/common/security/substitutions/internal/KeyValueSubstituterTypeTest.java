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

import org.apache.kafka.common.security.substitutions.RedactableObject;
import org.apache.kafka.common.security.substitutions.SubstitutableValues;
import org.apache.kafka.common.security.substitutions.UnderlyingValues;
import org.junit.Test;

public class KeyValueSubstituterTypeTest {
    @Test
    public void doSubstitution() throws IOException {
        List<String> noModifiers = Collections.<String>emptyList();
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("a", "b");
        assertEquals(new RedactableObject("b", false), new KeyValueSubstituterType().doSubstitution("key", noModifiers,
                "a", new SubstitutableValues(underlyingValuesFrom(underlyingMap))));
    }

    @Test
    public void doSubstitutionWithFromOptionModifier() throws IOException {
        List<String> fromOptionModifier = Arrays.asList("fromValueOfKey");
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("a", "b");
        underlyingMap.put("b", "c");
        assertEquals(new RedactableObject("c", false), new KeyValueSubstituterType().doSubstitution("keyValue",
                fromOptionModifier, "a", new SubstitutableValues(underlyingValuesFrom(underlyingMap))));
    }

    @Test
    public void ignoreExtraFlag() throws IOException {
        List<String> unknownModifiers = Arrays.asList("foo");
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("a", "b");
        new KeyValueSubstituterType().doSubstitution("key", unknownModifiers, "a",
                new SubstitutableValues(underlyingValuesFrom(underlyingMap)));
    }

    @Test
    public void ignoreExtraArgument() throws IOException {
        List<String> unknownModifiers = Arrays.asList("foo=123");
        Map<String, Object> underlyingMap = new HashMap<>();
        underlyingMap.put("a", "b");
        new KeyValueSubstituterType().doSubstitution("key", unknownModifiers, "a",
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
