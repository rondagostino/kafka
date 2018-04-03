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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.security.substitutions.RedactableObject;
import org.apache.kafka.common.security.substitutions.SubstitutableValues;
import org.apache.kafka.common.security.substitutions.UnderlyingValues;
import org.junit.Test;

public class FileContentSubstituterTypeTest {
    @Test
    public void doSubstitution() throws IOException {
        Path tempFile = Files.createTempFile(null, null);
        try {
            List<String> noModifiers = Collections.<String>emptyList();
            Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
            Files.write(tempFile, "hello".getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE);
            assertEquals(new RedactableObject("hello", false),
                    new FileContentSubstituterType().doSubstitution("file", noModifiers,
                            tempFile.toAbsolutePath().toString(),
                            new SubstitutableValues(underlyingValuesFrom(noOptions))));
        } finally {
            Files.delete(tempFile);
        }
    }

    @Test
    public void ignoreExtraFlag() throws IOException {
        List<String> unknownModifiers = Arrays.asList("defaultValue=wasNotThere", "foo");
        Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
        new FileContentSubstituterType().doSubstitution("file", unknownModifiers, "fileThatIsNotThere",
                new SubstitutableValues(underlyingValuesFrom(noOptions)));
    }

    @Test
    public void ignoreExtraArgument() throws IOException {
        List<String> unknownModifiers = Arrays.asList("defaultValue=wasNotThere", "foo=123");
        Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
        new FileContentSubstituterType().doSubstitution("file", unknownModifiers, "fileThatIsNotThere",
                new SubstitutableValues(underlyingValuesFrom(noOptions)));
    }

    @Test
    public void doSubstitutionFileDoesNotExist() throws IOException {
        List<String> modifiers = Arrays.asList("defaultValue=wasNotThere");
        Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
        assertEquals(new RedactableObject("wasNotThere", false), new FileContentSubstituterType().doSubstitution("file",
                modifiers, "fileThatIsNotThere", new SubstitutableValues(underlyingValuesFrom(noOptions))));
    }

    @Test
    public void doSubstitutionDirectory() throws IOException {
        List<String> modifiers = Arrays.asList("defaultValue=wasDirectory");
        Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
        assertEquals(new RedactableObject("wasDirectory", false), new FileContentSubstituterType()
                .doSubstitution("file", modifiers, ".", new SubstitutableValues(underlyingValuesFrom(noOptions))));
    }

    @Test(expected = IOException.class)
    public void doSubstitutionFileTooBig() throws IOException {
        Path tempFile = Files.createTempFile(null, null);
        try {
            List<String> noModifiers = Collections.<String>emptyList();
            Map<String, Object> noOptions = Collections.<String, Object>emptyMap();
            for (int i = 0; i <= 1024 * 1024 / 100; ++i) {
                Files.write(tempFile,
                        "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                                .getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.APPEND);
            }
            new FileContentSubstituterType().doSubstitution("file", noModifiers, tempFile.toAbsolutePath().toString(),
                    new SubstitutableValues(underlyingValuesFrom(noOptions)));
        } finally {
            Files.delete(tempFile);
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
