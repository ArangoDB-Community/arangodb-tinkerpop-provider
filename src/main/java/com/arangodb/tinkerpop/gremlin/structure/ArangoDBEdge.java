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
import com.arangodb.tinkerpop.gremlin.persistence.EdgeData;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Stream;

import static com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil.*;


public class ArangoDBEdge extends ArangoDBEntityElement<AdbValue, EdgeData> implements Edge {

    public static ArangoDBEdge of(final String id, final String label, final String outVertexId, final String inVertexId, ArangoDBGraph graph) {
        return new ArangoDBEdge(graph, new EdgeData(extractLabel(id, label).orElse(DEFAULT_LABEL), extractKey(id), outVertexId, inVertexId));
    }

    public ArangoDBEdge(ArangoDBGraph graph, EdgeData data) {
        super(graph, data);
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
    protected void doRemove() {
        graph.getClient().deleteEdge(this);
    }

    @Override
    protected void update() {
        graph.getClient().updateEdge(this);
    }

    public void insert() {
        graph.getClient().insertEdge(this);
    }

    @Override
    protected String stringify() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Vertex outVertex() {
        return new ArangoDBVertex(graph, graph.getClient().readVertex(data.getFrom()));
    }

    @Override
    public Vertex inVertex() {
        return new ArangoDBVertex(graph, graph.getClient().readVertex(data.getTo()));
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (removed()) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex());
            case IN:
                return IteratorUtils.of(this.inVertex());
            default:
                return IteratorUtils.of(this.outVertex(), this.inVertex());
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return IteratorUtils.cast(super.properties(propertyKeys));
    }
}
