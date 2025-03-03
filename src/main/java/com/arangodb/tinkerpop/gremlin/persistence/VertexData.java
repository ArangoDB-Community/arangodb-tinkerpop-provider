/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arangodb.tinkerpop.gremlin.persistence;

import com.arangodb.serde.InternalKey;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;
import java.util.stream.Stream;

public class VertexData implements PropertyData<VertexPropertyData>, PersistentData {

    @JsonProperty
    private String label;

    @InternalKey
    private String key;

    @JsonProperty
    private final Map<String, Set<VertexPropertyData>> properties = new HashMap<>();

    public VertexData() {
    }

    public VertexData(String label, String key) {
        ElementHelper.validateLabel(label);
        if (key != null && key.isEmpty()) throw new IllegalArgumentException("empty key");
        this.label = label;
        this.key = key;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public Stream<Map.Entry<String, VertexPropertyData>> entries() {
        return properties.entrySet().stream().flatMap(e -> e.getValue().stream()
                .map(v -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), v)));
    }

    @Override
    public void add(String key, VertexPropertyData value) {
        properties.computeIfAbsent(key, k -> new HashSet<>()).add(value);
    }

    public void remove(String key, VertexPropertyData value) {
        Set<VertexPropertyData> props = properties.getOrDefault(key, Collections.emptySet());
        props.remove(value);
        if (props.isEmpty()) {
            properties.remove(key);
        }
    }

    @Override
    public String toString() {
        return "VertexData{" +
                "key='" + key + '\'' +
                ", label='" + label + '\'' +
                ", properties=" + properties +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VertexData)) return false;
        VertexData that = (VertexData) o;
        return Objects.equals(label, that.label) && Objects.equals(key, that.key) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, key, properties);
    }
}
