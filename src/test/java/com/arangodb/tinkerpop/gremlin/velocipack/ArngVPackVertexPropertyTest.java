package com.arangodb.tinkerpop.gremlin.velocipack;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ArngVPackVertexPropertyTest {

    @org.junit.jupiter.api.Test
    void singleValuePropertyHasOneBaseValue() {
        final String key = "firstName";
        final String value = "John";

        VertexProperty vertexProperty = vertexPropertyMock(key, value);

        VPackVertexProperty underTest = new ArngVPackVertexProperty(VertexProperty.Cardinality.single, vertexProperty);
        assertEquals(key, underTest.key());
        assertEquals(value, underTest.baseValue());
    }


    @org.junit.jupiter.api.Test
    void singValuePropertyWithMultipleValueReturnsOneValue() {
        final String key = "hobbies";
        final String value1 = "swimming";
        final String value2 = "biking";
        final String value3 = "programming";

        VertexProperty vertexProperty1 = vertexPropertyMock(key, value1);
        VertexProperty vertexProperty2 = vertexPropertyMock(key, value2);
        VertexProperty vertexProperty3 = vertexPropertyMock(key, value3);

        VPackVertexProperty underTest = new ArngVPackVertexProperty(VertexProperty.Cardinality.single, vertexProperty1);
        underTest.addVertexProperty(vertexProperty2);
        underTest.addVertexProperty(vertexProperty3);

        final Set<String> expectedValue = new HashSet<>();
        expectedValue.add(value1);
        expectedValue.add(value2);
        expectedValue.add(value3);

        assertEquals(key, underTest.key());
        assertNotEquals(expectedValue, underTest.baseValue());
        assertTrue(expectedValue.contains(underTest.baseValue()));
    }

    @org.junit.jupiter.api.Test
    void setValuePropertyHasMultipleBaseValue() {
        final String key = "hobbies";
        final String value1 = "swimming";
        final String value2 = "biking";
        final String value3 = "programming";

        VertexProperty vertexProperty1 = vertexPropertyMock(key, value1);
        VertexProperty vertexProperty2 = vertexPropertyMock(key, value2);
        VertexProperty vertexProperty3 = vertexPropertyMock(key, value3);

        VPackVertexProperty underTest = new ArngVPackVertexProperty(VertexProperty.Cardinality.set, vertexProperty1);
        underTest = underTest.addVertexProperty(vertexProperty2);
        underTest = underTest.addVertexProperty(vertexProperty3);

        final Set<String> expectedValue = new HashSet<>();
        expectedValue.add(value1);
        expectedValue.add(value2);
        expectedValue.add(value3);

        assertEquals(key, underTest.key());
        assertTrue(equals(expectedValue, underTest.baseValue()));
    }

    @org.junit.jupiter.api.Test
    void listValuePropertyHasMultipleBaseValue() {
        final String key = "hobbies";
        final String value1 = "swimming";
        final String value2 = "biking";
        final String value3 = "programming";

        VertexProperty vertexProperty1 = vertexPropertyMock(key, value1);
        VertexProperty vertexProperty2 = vertexPropertyMock(key, value2);
        VertexProperty vertexProperty3 = vertexPropertyMock(key, value3);

        VPackVertexProperty underTest = new ArngVPackVertexProperty(VertexProperty.Cardinality.list, vertexProperty1);
        underTest = underTest.addVertexProperty(vertexProperty2);
        underTest = underTest.addVertexProperty(vertexProperty3);

        final Set<String> expectedValue = new HashSet<>();
        expectedValue.add(value1);
        expectedValue.add(value2);
        expectedValue.add(value3);

        assertEquals(key, underTest.key());
        assertTrue(equalsOrdered(expectedValue, underTest.baseValue()));
    }

    @org.junit.jupiter.api.Test
    void addVertexPropertyWithDifferntKeyThrowsException() {
        final String key1 = "hobbies";
        final String value1 = "swimming";
        final String key2 = "hobbyes";

        VertexProperty vertexProperty1 = vertexPropertyMock(key1, value1);
        VertexProperty vertexProperty2 = mock(VertexProperty.class);
        when(vertexProperty2.key()).thenReturn(key2);

        VPackVertexProperty underTest = new ArngVPackVertexProperty(VertexProperty.Cardinality.list, vertexProperty1);
        try {
            underTest.addVertexProperty(vertexProperty2);
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(String.format("Only properties with the same key can be added. %s != %s",
                        key1, key2)
                    , e.getMessage());
        }
    }


    private VertexProperty vertexPropertyMock(
            String key,
            String value) {
        VertexProperty vertexProperty = mock(VertexProperty.class);
        when(vertexProperty.key()).thenReturn(key);
        when(vertexProperty.value()).thenReturn(value);
        when(vertexProperty.properties()).thenReturn(Collections.emptyIterator());
        return vertexProperty;
    }

    private boolean equals(Collection<?> collection1, Object other){
        if (other == null) {
            return false;
        }
        if (collection1 == other) {
            return true;
        }
        if (!(other instanceof Collection)) {
            return false;
        }
        Collection<?> collection2 = (Collection)other;
        if (collection1.size() != collection2.size()){
            return false;
        }
        return collection1.containsAll(collection2);
    }


    private boolean equalsOrdered(Collection<?> collection1, Object other){
        if (other == null) {
            return false;
        }
        if (collection1 == other) {
            return true;
        }
        if (!(other instanceof Collection)) {
            return false;
        }
        Collection<?> collection2 = (Collection)other;
        if (collection1.size() != collection2.size()){
            return false;
        }
        Iterator<?> it1 = collection1.iterator();
        Iterator<?> it2 = collection2.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            if (!it1.next().equals(it2.next())) {
                return false;
            }
        }
        return true;
    }

}