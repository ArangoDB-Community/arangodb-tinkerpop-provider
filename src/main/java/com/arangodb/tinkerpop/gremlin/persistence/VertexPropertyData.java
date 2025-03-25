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
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

import java.util.Objects;

public class VertexPropertyData extends SimplePropertyData {

    private final String id;
    private final Object value;
    private final String valueType;

    @JsonCreator
    VertexPropertyData(
            @JsonProperty("id") String id,
            @JsonProperty("value") Object value,
            @JsonProperty("valueType") String valueType
    ) {
        this.id = id;
        this.value = value;
        this.valueType = valueType;
    }

    public static VertexPropertyData of(String id, Object value) {
        return new VertexPropertyData(id, value, (value != null ? value.getClass() : Void.class).getCanonicalName());
    }

    public String getId() {
        return id;
    }

    public Object getValue() {
        return ArangoDBUtil.getCorretctPrimitive(value, valueType);
    }

    public String getValueType() {
        return valueType;
    }

    @Override
    public String toString() {
        return "VertexPropertyData{" +
                "id='" + id + '\'' +
                ", value=" + value +
                ", valueType='" + valueType + '\'' +
                ", super=" + super.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VertexPropertyData)) return false;
        if (!super.equals(o)) return false;
        VertexPropertyData that = (VertexPropertyData) o;
        return Objects.equals(id, that.id) && Objects.equals(value, that.value) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id, value, valueType);
    }
}
