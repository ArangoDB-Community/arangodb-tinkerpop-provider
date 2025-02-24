/// ///////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
/// ///////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

import static com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil.elementAlreadyRemoved;


/**
 * The ArangoDB vertex class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBVertex implements Vertex {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDBVertex.class);

    private final ArangoDBGraph graph;
    private final ArangoDBVertexData data;
    private boolean removed;

    public ArangoDBVertex(ArangoDBGraph graph, ArangoDBVertexData data) {
        this.graph = graph;
        this.data = data;
        this.removed = false;
    }

    public ArangoDBVertex(final String id, final String label, ArangoDBGraph graph) {
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

        data = new ArangoDBVertexData(inferredLabel, key);
        removed = false;
    }

    public boolean isRemoved() {
        return removed;
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

    @Override
    public void remove() {
        if (removed) return;
        LOGGER.info("removing {} from graph {}.", id(), graph.name());
        edges(Direction.BOTH).forEachRemaining(Edge::remove);
        graph.getClient().deleteVertex(data);
        this.removed = true;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        LOGGER.info("addEdge in collection {} to vertex {}", label, inVertex == null ? "?" : inVertex.id());
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateLabel(label);
        if (!graph.edgeCollections().contains(label)) {
            throw new IllegalArgumentException(String.format("Edge label (%s)not in graph (%s) edge collections.", label, graph.name()));
        }
        if (inVertex == null) {
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        }
        Object id;
        ArangoDBEdge edge = null;
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            id = ElementHelper.getIdValue(keyValues).get();
            if (graph.features().edge().willAllowId(id)) {
                if (id.toString().contains("/")) {
                    String fullId = id.toString();
                    String[] parts = fullId.split("/");
                    // The collection name is the last part of the full name
                    String[] collectionParts = parts[0].split("_");
                    String collectionName = collectionParts[collectionParts.length - 1];
                    if (collectionName.contains(label)) {
                        id = parts[1];

                    }
                }
                Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String) id);
                if (m.matches()) {
                    edge = new ArangoDBEdge(id.toString(), label, (String) this.id(), (String) inVertex.id(), graph);
                } else {
                    throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
                }
            } else {
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            }
        } else {
            edge = new ArangoDBEdge(null, label, (String) this.id(), (String) inVertex.id(), graph);
        }
        // The vertex needs to exist before we can attach properties
        edge.insert();
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(
            final Cardinality cardinality,
            final String key,
            final V value,
            final Object... keyValues
    ) {
        if (removed) throw elementAlreadyRemoved(Vertex.class, id());
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);

        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        Object[] filteredKeyValues = ArrayUtils.clone(keyValues);
        String idValue = null;
        if (optionalId.isPresent()) {
            if (graph.features().vertex().properties().willAllowId(optionalId.get())) {
                idValue = optionalId.get().toString();
            } else {
                throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            }
            int idIndex = 0;
            for (int i = 0; i < filteredKeyValues.length; i += 2) {
                if (filteredKeyValues[i] == T.id) {
                    idIndex = i;
                    break;
                }
            }
            filteredKeyValues = ArrayUtils.remove(filteredKeyValues, idIndex);
            filteredKeyValues = ArrayUtils.remove(filteredKeyValues, idIndex);
        }

        if (idValue == null) {
            idValue = UUID.randomUUID().toString();
        }

        ArangoDBVertexPropertyData prop = new ArangoDBVertexPropertyData(idValue, value);

        final List<ArangoDBVertexPropertyData> list = data.getProperties().getOrDefault(key, new ArrayList<>());
        list.add(prop);
        data.getProperties().put(key, list);

        ArangoDBVertexProperty<V> vertexProperty = new ArangoDBVertexProperty<>(key, prop, this);
        ElementHelper.attachProperties(vertexProperty, filteredKeyValues);
        update();
        return vertexProperty;
    }


    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        List<String> edgeCollections = getQueryEdgeCollections(edgeLabels);
        // If edgeLabels was not empty but all were discarded, this means that we should
        // return an empty iterator, i.e. no edges for that edgeLabels exist.
        if (edgeCollections.isEmpty()) {
            return Collections.emptyIterator();
        }
        return graph.getClient().getVertexEdges(id(), edgeCollections, direction)
                .stream()
                .map(it -> (Edge) new ArangoDBEdge(graph, it))
                .iterator();
    }


    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        List<String> edgeCollections = getQueryEdgeCollections(edgeLabels);
        // If edgeLabels was not empty but all were discarded, this means that we should
        // return an empty iterator, i.e. no edges for that edgeLabels exist.
        if (edgeCollections.isEmpty()) {
            return Collections.emptyIterator();
        }
        return graph.getClient().getDocumentNeighbors(id(), edgeCollections, direction, ArangoDBPropertyFilter.empty(), ArangoDBVertexData.class).stream()
                .map(it -> (Vertex) new ArangoDBVertex(graph, it))
                .iterator();
    }


    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        LOGGER.debug("Get properties {}", (Object[]) propertyKeys);
        return allProperties()
                .filter(it -> ElementHelper.keyExists(it.key(), propertyKeys))
                .map(it -> (VertexProperty<V>) it)
                .iterator();
    }


    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    private Stream<ArangoDBVertexProperty<?>> allProperties() {
        return data.getProperties().entrySet().stream()
                .flatMap(x -> x.getValue().stream()
                        .map(y -> new ArangoDBVertexProperty<>(x.getKey(), y, this))
                );
    }

    public void insert() {
        if (removed) throw elementAlreadyRemoved(Vertex.class, id());
        graph.getClient().insertVertex(data);
    }


    public void update() {
        if (removed) throw elementAlreadyRemoved(Vertex.class, id());
        graph.getClient().updateVertex(data);
    }

    public void removeProperty(ArangoDBVertexPropertyData prop) {
        if (removed) throw elementAlreadyRemoved(Vertex.class, id());
        for (List<ArangoDBVertexPropertyData> it : data.getProperties().values()) {
            if (it.remove(prop)) return;
        }
    }

    /**
     * Query will raise an exception if the edge_collection name is not in the graph, so we need to filter out
     * edgeLabels not in the graph.
     *
     * @param edgeLabels
     * @return
     */
    private List<String> getQueryEdgeCollections(String... edgeLabels) {
        List<String> vertexCollections;
        if (edgeLabels.length == 0) {
            vertexCollections = graph.edgeCollections().stream().map(graph::getPrefixedCollectioName).collect(Collectors.toList());
        } else {
            vertexCollections = Arrays.stream(edgeLabels)
                    .filter(el -> graph.edgeCollections().contains(el))
                    .map(graph::getPrefixedCollectioName)
                    .collect(Collectors.toList());

        }
        return vertexCollections;
    }

    @Override
    @SuppressWarnings("EqualsDoesntCheckParameterClass")
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

}

