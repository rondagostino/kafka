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
import java.util.List;

/**
 * The single-method interface that pluggable substituter types must implement.
 */
public interface SubstituterType {
    /**
     * Perform the substitution of the given type using the given modifiers and
     * value on the given options
     * 
     * @param type
     *            the (always non-null) type of substitution to perform
     * @param modifiers
     *            the (always non-null but potentially empty) modifiers to apply, if
     *            any. They are presented exactly as they appear in the
     *            configuration, with no whitespace trimming applied.
     * @param identifier
     *            the always non-null (but potentially empty) identifier, which is
     *            interpreted in the context of the substitution of the indicated
     *            type. For example, it may indicate an environment variable name, a
     *            filename, etc.
     * @param substitutableValues
     *            the values and their current substitution state
     * @return the (always non-null) result of performing the substitution
     * @throws IOException
     *             if the substitution cannot be performed
     */
    RedactableObject doSubstitution(String type, List<String> modifiers, String identifier,
            SubstitutableValues substitutableValues) throws IOException;
}
