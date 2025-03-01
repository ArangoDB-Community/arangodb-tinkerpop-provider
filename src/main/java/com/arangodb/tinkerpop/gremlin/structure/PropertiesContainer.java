package com.arangodb.tinkerpop.gremlin.structure;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Stream;

interface PropertiesContainer {
    Map<String, ArangoDBPropertyData> getProperties();

    default Stream<Map.Entry<String, Object>> properties() {
        return getProperties().entrySet().stream()
                .map(it -> new AbstractMap.SimpleEntry<>(it.getKey(), it.getValue().getValue()));
    }

    default boolean hasProperty(String key) {
        return getProperties().containsKey(key);
    }

    default void removeProperty(String key) {
        getProperties().remove(key);
    }

    default void setProperty(String key, Object value) {
        getProperties().put(key, new ArangoDBPropertyData(value));
    }

    default Object getProperty(String key) {
        return getProperties().get(key).getValue();
    }
}
