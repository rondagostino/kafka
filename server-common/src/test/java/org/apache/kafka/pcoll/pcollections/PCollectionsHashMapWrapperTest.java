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

package org.apache.kafka.pcoll.pcollections;

import org.apache.kafka.pcoll.DelegationChecker;
import org.apache.kafka.server.util.TranslatedValueMapView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pcollections.HashPMap;
import org.pcollections.HashTreePMap;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class PCollectionsHashMapWrapperTest {
    private static final HashPMap<Object, Object> SINGLETON_MAP = HashTreePMap.singleton(new Object(), new Object());

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIsEmpty(boolean expectedResult) {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(expectedResult)
            .recordsInvocationAndAnswers(when(mock.isEmpty()))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).isEmpty());
    }


    @Test
    public void testUnderlying() {
        assertSame(SINGLETON_MAP, new PCollectionsHashMapWrapper<>(SINGLETON_MAP).underlying());
    }

    @Test
    public void testAsJava() {
        assertSame(SINGLETON_MAP, new PCollectionsHashMapWrapper<>(SINGLETON_MAP).asJava());
    }

    @Test
    public void testAsJavaWithValueTranslation() {
        HashPMap<Integer, Integer> singletonMap = HashTreePMap.singleton(1, 1);
        Map<Integer, Integer> mapWithValuesTranslated = new PCollectionsHashMapWrapper<>(singletonMap).asJava(value -> value + 1);
        assertTrue(mapWithValuesTranslated instanceof TranslatedValueMapView);
        assertEquals(2, mapWithValuesTranslated.get(1));
    }

    @Test
    public void testAfterAdding() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(SINGLETON_MAP)
            .recordsInvocationAndAnswers(when(mock.plus(eq(this), eq(this))))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).afterAdding(this, this).underlying());
    }

    @Test
    public void testAfterRemoving() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(SINGLETON_MAP)
            .recordsInvocationAndAnswers(when(mock.minus(eq(this))))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).afterRemoving(this).underlying());
    }

    @Test
    public void testGet() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(new Object())
            .recordsInvocationAndAnswers(when(mock.get(eq(this))))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).get(this));
    }

    @Test
    public void testEntrySet() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(Collections.emptySet())
            .recordsInvocationAndAnswers(when(mock.entrySet()))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).entrySet());
    }

    @Test
    public void testKeySet() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(Collections.emptySet())
            .recordsInvocationAndAnswers(when(mock.keySet()))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).keySet());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testContainsKey(boolean expectedResult) {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(expectedResult)
            .recordsInvocationAndAnswers(when(mock.containsKey(eq(this))))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).containsKey(this));
    }

    @Test
    public void testGetOrElse() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(new Object())
            .recordsInvocationAndAnswers(when(mock.getOrDefault(eq(this), eq(this))))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).getOrElse(this, this));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testContainsKey(int expectedResult) {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        new DelegationChecker<>()
            .setAnswerFromMockPersistentCollection(expectedResult)
            .recordsInvocationAndAnswers(when(mock.size()))
            .assertDelegatesAndAnswersCorrectly(new PCollectionsHashMapWrapper<>(mock).size());
    }

    @Test
    public void testHashCode() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        assertEquals(mock.hashCode(), new PCollectionsHashMapWrapper<>(mock).hashCode());
        final HashPMap<Object, Object> someOtherMock = mock(HashPMap.class);
        assertNotEquals(mock.hashCode(), new PCollectionsHashMapWrapper<>(someOtherMock).hashCode());
    }

    @Test
    public void testEquals() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        assertEquals(new PCollectionsHashMapWrapper<>(mock), new PCollectionsHashMapWrapper<>(mock));
        final HashPMap<Object, Object> someOtherMock = mock(HashPMap.class);
        assertNotEquals(new PCollectionsHashMapWrapper<>(mock), new PCollectionsHashMapWrapper<>(someOtherMock));
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    public void testToString(String underlyingToStringResult) {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        when(mock.toString()).thenReturn(underlyingToStringResult);
        assertEquals("PCollectionsHashMapWrapper{underlying=" + underlyingToStringResult + "}",
            new PCollectionsHashMapWrapper<>(mock).toString());
    }
}