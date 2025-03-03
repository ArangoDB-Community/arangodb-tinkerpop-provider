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

import com.arangodb.shaded.fasterxml.jackson.annotation.JsonCreator;
import com.arangodb.shaded.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class VertexPropertyData extends AdbValue implements PropertyData<AdbValue> {

    private final String id;
    private final Map<String, AdbValue> properties;

    @JsonCreator
    VertexPropertyData(
            @JsonProperty("id") String id,
            @JsonProperty("value") Object value,
            @JsonProperty("valueType") String valueType,
            @JsonProperty("properties") Map<String, AdbValue> properties) {
        super(value, valueType);
        this.id = id;
        this.properties = properties;
    }

    public VertexPropertyData(String id, Object value) {
        super(value);
        this.id = id;
        this.properties = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    @Override
    public Map<String, AdbValue> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "VertexPropertyData{" +
                "id='" + id + '\'' +
                ", properties=" + properties +
                ", super=" + super.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VertexPropertyData)) return false;
        if (!super.equals(o)) return false;
        VertexPropertyData that = (VertexPropertyData) o;
        return Objects.equals(id, that.id) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id, properties);
    }
}
