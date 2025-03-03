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

import java.util.Objects;

public abstract class PersistentData<V> implements PropertyData<V> {

    private String label;
    @InternalKey
    private String key;

    public PersistentData() {
    }

    public PersistentData(String label, String key) {
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
    public String toString() {
        return "PersistentData{" +
                "key='" + key + '\'' +
                ", label='" + label + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PersistentData)) return false;
        PersistentData<?> that = (PersistentData<?>) o;
        return Objects.equals(label, that.label) && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, key);
    }
}

