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

import com.arangodb.tinkerpop.gremlin.client.ArangoDBIterator;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil.elementAlreadyRemoved;


public class ArangoDBEdge implements Edge {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDBEdge.class);

    private final ArangoDBGraph graph;
    private final ArangoDBEdgeDocument data;
    private boolean removed;

    public ArangoDBEdge(ArangoDBGraph graph, ArangoDBEdgeDocument data) {
        this.graph = graph;
        this.data = data;
        this.removed = false;
    }

    public ArangoDBEdge(final String id, final String label, final String outVertexId, final String inVertexId, ArangoDBGraph graph) {
        this.graph = graph;
        String inferredLabel, key;
        if (id != null) {
            int separator = id.indexOf('/');
            if (separator > 0) {
                inferredLabel = id.substring(0, separator);
                key = id.substring(separator + 1);
            } else {
                inferredLabel = label != null ? label : DEFAULT_LABEL;
                key = id;
            }
        } else {
            inferredLabel = label != null ? label : DEFAULT_LABEL;
            key = null;
        }

        if (inferredLabel.isEmpty()) {
            throw new IllegalArgumentException("empty label");
        }

        if (key != null && key.isEmpty()) {
            throw new IllegalArgumentException("empty key");
        }

        data = new ArangoDBEdgeDocument(inferredLabel, key, outVertexId, inVertexId);
        removed = false;
    }

    @Override
    public String id() {
        String key = data.getKey();
        if (key == null) {
            return null;
        }
        return graph.getPrefixedCollectioName(label()) + "/" + key;
    }

    @Override
    public String label() {
        return data.getLabel();
    }

    @Override
    public ArangoDBGraph graph() {
        return graph;
    }

    public void insert() {
        graph.getClient().insertEdge(data);
    }

    public void update() {
        graph.getClient().updateEdge(data);
    }

    public void removeProperty(String key) {
        if (removed) throw elementAlreadyRemoved(Edge.class, id());
        if (data.hasProperty(key)) {
            data.removeProperty(key);
            update();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return data.getProperties()
                .entrySet()
                .stream()
                .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                .map(entry -> (Property<V>) new ArangoDBProperty<>(this, entry.getKey(), entry.getValue().getValue()))
                .collect(Collectors.toList()).iterator();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        if (removed) throw elementAlreadyRemoved(Edge.class, id());
        LOGGER.info("set property {} = {}", key, value);
        ElementHelper.validateProperty(key, value);
        data.setProperty(key, value);
        update();
        return new ArangoDBProperty<>(this, key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Property<V> property(final String key) {
        if (data.hasProperty(key)) {
            Object value = data.getProperty(key);
            return new ArangoDBProperty<>(this, key, (V) value);
        }
        return Property.empty();
    }

    @Override
    public Set<String> keys() {
        return data.getProperties().keySet();
    }

    @Override
    public void remove() {
        LOGGER.info("removing {} from graph {}.", id(), graph.name());
        graph.getClient().deleteEdge(data);
        this.removed = true;
    }

    @Override
    public <V> Iterator<V> values(String... propertyKeys) {
        return Edge.super.values(propertyKeys);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        List<String> ids = new ArrayList<>();
        switch (direction) {
            case BOTH:
                ids.add(data.getFrom());
                ids.add(data.getTo());
                break;
            case IN:
                ids.add(data.getTo());
                break;
            case OUT:
                ids.add(data.getFrom());
                break;
        }
        return new ArangoDBIterator<>(graph, graph.getClient().getGraphVertices(ids, Collections.emptyList()));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

}
