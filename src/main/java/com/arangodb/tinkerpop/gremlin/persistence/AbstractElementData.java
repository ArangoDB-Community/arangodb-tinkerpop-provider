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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractElementData<T> implements ElementData<T> {

    private String label;
    @InternalKey
    private String key;
    private final Map<String, T> properties = new HashMap<>();

    public AbstractElementData() {
    }

    public AbstractElementData(String label, String key) {
        ElementHelper.validateLabel(label);
        if (key != null && key.isEmpty()) throw new IllegalArgumentException("empty key");

        this.label = label;
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public Map<String, T> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "AbstractElementData{" +
                "key='" + key + '\'' +
                ", label='" + label + '\'' +
                ", properties=" + properties +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AbstractElementData<?> that = (AbstractElementData<?>) o;
        return Objects.equals(key, that.key) && Objects.equals(label, that.label) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, label, properties);
    }
}

