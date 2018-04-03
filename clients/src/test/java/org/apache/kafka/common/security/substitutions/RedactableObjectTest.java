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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.kafka.common.config.types.Password;
import org.junit.Test;

public class RedactableObjectTest {
    @Test
    public void passwordObject() throws IOException {
        String expected = "b";
        Password passwordObject = new Password(expected);
        RedactableObject redactableObject = new RedactableObject(passwordObject);
        assertEquals(passwordObject, redactableObject.object());
        assertEquals(RedactableObject.REDACTED, redactableObject.toString());
        assertEquals(expected, redactableObject.value());
        assertTrue(redactableObject.isRedacted());
    }

    @Test
    public void nonPasswordObject() throws IOException {
        String expected = "b";
        RedactableObject redactableObject = new RedactableObject(expected);
        assertEquals(expected, redactableObject.object());
        assertFalse(redactableObject.isRedacted());
    }

    @Test
    public void object() throws IOException {
        String expected = "b";
        RedactableObject redactableObject = new RedactableObject(expected, false);
        assertEquals(expected, redactableObject.object());
    }

    @Test
    public void isRedacted() throws IOException {
        for (boolean expected : new boolean[] {true, false}) {
            RedactableObject redactableObject = new RedactableObject("a", expected);
            assertEquals(expected, redactableObject.isRedacted());
        }
    }

    @Test
    public void redactedTextAndToString() throws IOException {
        for (boolean redacted : new boolean[] {true, false}) {
            String value = "a";
            RedactableObject redactableObject = new RedactableObject(value, redacted);
            String expected = redacted ? RedactableObject.REDACTED : value;
            assertEquals(expected, redactableObject.redactedText());
            assertEquals(expected, redactableObject.toString());
        }
    }

    @Test
    public void value() throws IOException {
        for (boolean redacted : new boolean[] {true, false}) {
            String value = "a";
            RedactableObject redactableObject = new RedactableObject(value, redacted);
            String expected = value;
            assertEquals(expected, redactableObject.value());
        }
    }

    @Test
    public void isEmpty() throws IOException {
        for (boolean redacted : new boolean[] {true, false}) {
            for (String value : new String[] {"", " ", "a"}) {
                RedactableObject redactableObject = new RedactableObject(value, redacted);
                boolean expected = value.isEmpty();
                assertEquals(expected, redactableObject.isEmpty());
            }
        }
    }

    @Test
    public void isBlank() throws IOException {
        for (boolean redacted : new boolean[] {true, false}) {
            for (String value : new String[] {"", " ", "a"}) {
                RedactableObject redactableObject = new RedactableObject(value, redacted);
                boolean expected = value.trim().isEmpty();
                assertEquals(expected, redactableObject.isBlank());
            }
        }
    }

    @Test
    public void redactedVersion() throws IOException {
        for (boolean redacted : new boolean[] {true, false}) {
            String value = "a";
            RedactableObject redactableObject = new RedactableObject(value, redacted);
            RedactableObject redactedObject = redactableObject.redactedVersion();
            assertEquals(new RedactableObject(value, true), redactedObject);
            if (redactableObject.isRedacted())
                assertSame(redactedObject, redactableObject);
        }
    }

    @Test
    public void equalsAndHashCode() throws IOException {
        String value1 = "a";
        String value2 = "b";
        RedactableObject notRedactedObject1 = new RedactableObject(value1, false);
        RedactableObject notRedactedObject1b = new RedactableObject(value1, false);
        RedactableObject notRedactedObject2 = new RedactableObject(value2, false);
        RedactableObject redactedObject1 = new RedactableObject(value1, true);
        RedactableObject redactedObject1b = new RedactableObject(value1, true);
        RedactableObject redactedObject2 = new RedactableObject(value2, true);
        assertTrue(notRedactedObject1.equals(notRedactedObject1b));
        assertTrue(notRedactedObject1.hashCode() == notRedactedObject1b.hashCode());
        assertFalse(notRedactedObject1.equals(null));
        assertFalse(notRedactedObject1.equals(notRedactedObject2));
        assertFalse(notRedactedObject1.equals(redactedObject1));
        assertFalse(notRedactedObject1.equals(redactedObject2));
        assertTrue(redactedObject1.hashCode() == redactedObject1b.hashCode());
        assertFalse(redactedObject1.equals(redactedObject2));
    }

    @Test
    public void nonStringObjectType() throws IOException {
        RedactableObject redactableObject = new RedactableObject(this, true);
        assertEquals(RedactableObject.REDACTED, redactableObject.redactedText());
        assertEquals(RedactableObject.REDACTED, redactableObject.toString());
        assertEquals(toString(), redactableObject.value());
    }

    @Test
    public void subclass() throws IOException {
        final String expected = RedactableObjectTest.class.getName();
        RedactableObject redactableObject = new RedactableObject(this, true) {
            @Override
            public String value() {
                return object() instanceof RedactableObjectTest ? expected : super.value();
            }
        };
        assertEquals(RedactableObject.REDACTED, redactableObject.redactedText());
        assertEquals(RedactableObject.REDACTED, redactableObject.toString());
        assertEquals(expected, redactableObject.value());
    }
}
