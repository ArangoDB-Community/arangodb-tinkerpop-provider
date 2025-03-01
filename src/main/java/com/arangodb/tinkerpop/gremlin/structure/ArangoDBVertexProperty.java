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

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.persistence.AdbValue;
import com.arangodb.tinkerpop.gremlin.persistence.VertexPropertyData;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class ArangoDBVertexProperty<P> extends ArangoDBElement<AdbValue, VertexPropertyData> implements VertexProperty<P> {

    private final String key;
    private final ArangoDBVertex vertex;

    public ArangoDBVertexProperty(String key, VertexPropertyData data, ArangoDBVertex vertex) {
        super(vertex.graph(), data);
        this.key = key;
        this.vertex = vertex;
    }

    @Override
    protected boolean removed() {
        return super.removed() || vertex.removed();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <V> Property<V> createProperty(String key, Object value) {
        return new ArangoDBProperty<>(this, key, (V) value);
    }

    @Override
    protected <V> Stream<Property<V>> toProperties(String key, AdbValue value) {
        return Stream.of(createProperty(key, value.getValue()));
    }

    @Override
    protected AdbValue toData(Object value) {
        return new AdbValue(value);
    }

    @Override
    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    @Override
    public P value() throws NoSuchElementException {
        return (P) data.getValue();
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public ArangoDBVertex element() {
        return vertex;
    }

    @Override
    public Object id() {
        return data.getId();
    }

    @Override
    protected void update() {
        vertex.update();
    }

    @Override
    protected void doRemove() {
        vertex.removeVertexProperty(this);
        vertex.update();
    }

    @Override
    protected void insert() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public String stringify() {
        return StringFactory.propertyString(this);
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return IteratorUtils.cast(super.properties(propertyKeys));
    }
}

