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
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class NameEqualsValueTest {
    @Test
    public void name() {
        assertEquals("name", new NameEqualsValue("name", "value").name());
    }

    @Test
    public void value() {
        assertEquals("value", new NameEqualsValue("name", "value").value());
    }

    @Test
    public void fromIllegalValue() {
        assertNull(NameEqualsValue.from("name"));
    }

    @Test
    public void fromLegalValue() {
        NameEqualsValue from = NameEqualsValue.from("name=value=extra");
        assertEquals("name", from.name());
        assertEquals("value=extra", from.value());
        assertEquals("name=value=extra", from.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void blankName() {
        new NameEqualsValue(" ", "foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nameWithEqualSign() {
        new NameEqualsValue("a=b", "foo");
    }

    @Test
    public void fromLegalValueWithVariousTrimCombinations() {
        String name = " name ";
        String value = " value ";
        NameEqualsValue from = NameEqualsValue.from(name + "=" + value);
        assertEquals(name.trim(), from.name());
        assertEquals(value.trim(), from.value());
        for (boolean trimValue : new boolean[] {true, false}) {
            NameEqualsValue from2 = NameEqualsValue.from(name + "=" + value, trimValue);
            assertEquals(name.trim(), from2.name());
            assertEquals(trimValue ? value.trim() : value, from2.value());
        }
    }
}
