//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The ArangoDB vertex class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertex extends ArangoDBBaseDocument implements Vertex {
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertex() {
		super();
	}

	public ArangoDBVertex(ArangoDBGraph graph, String collection, String key) {
		super(key);
        this.graph = graph;
        this.collection = collection;
	}

	public ArangoDBVertex(ArangoDBGraph graph, String collection) {
		this(graph, collection, null);
	}

    @Override
    public Object id() {
        return _key;
    }

    @Override
    public String label() {
        return collection();
    }
	
	@Override
	public void remove() {
		logger.info("remove {}", this._key());
		graph.getClient().deleteDocument(graph, this);
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
		logger.info("addEdge in collection {} to vertex {}", label, inVertex == null ? "?" :inVertex.id());
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		ElementHelper.validateLabel(label);
		if (!graph.edgeCollections().contains(label)) {
			throw new IllegalArgumentException(String.format("Edge label (%s)not in graph (%s) edge collections.", label, graph.name()));
		}
		if (inVertex == null) {
			Graph.Exceptions.argumentCanNotBeNull("vertex");
		}
		Object id;
		ArangoDBEdge edge = null;
		if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	id = ElementHelper.getIdValue(keyValues).get();
        	if (graph.features().edge().willAllowId(id)) {
        		edge = new ArangoDBEdge(graph, label, this, ((ArangoDBVertex) inVertex), id.toString());
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}
        }
		else {
			edge = new ArangoDBEdge(graph, label, this, ((ArangoDBVertex) inVertex));
		}
        // The vertex needs to exist before we can attach properties
		graph.getClient().insertEdge(graph, edge);
        ElementHelper.attachProperties(edge, keyValues);
		return edge;
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> VertexProperty<V> property(
		Cardinality cardinality,
		String key,
		V value,
		Object... keyValues) {
		logger.debug("property {} = {} ({})", key, value, keyValues);
		ElementHelper.validateProperty(key, value);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		Optional<Object> idValue = ElementHelper.getIdValue(keyValues);
		String id = null;
		if (idValue.isPresent()) {
			if (!graph.features().vertex().willAllowId(idValue.get())) {
				throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
			}
			id = (String) idValue.get();
			int idIndex = 0;
            for (int i = 0; i < keyValues.length; i+=2) {
                if (keyValues[i] == T.id) {
                    idIndex = i;
                    break;
                }
            }
            keyValues = ArrayUtils.remove(keyValues, idIndex);
            keyValues = ArrayUtils.remove(keyValues, idIndex);
		}
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        ArangoDBVertexProperty<V> p = ArangoDBUtil.createArangoDBVertexProperty(id, key, value, this);
        ElementHelper.attachProperties(p, keyValues);
		return p;
	}


	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Edge> edges(
	    Direction direction,
		String... edgeLabels) {
	    List<String> edgeCollections;
        if (edgeLabels.length == 0) {
            edgeCollections = graph.edgeCollections();
        }
        else {
            edgeCollections = Arrays.stream(edgeLabels)
                    .filter(el -> graph.edgeCollections().contains(el))
                    .collect(Collectors.toList());
            if (edgeCollections.isEmpty()) {
                return Collections.emptyIterator();
            }
        }
		ArangoDBQuery query = graph.getClient().getVertexEdges(graph, this, edgeCollections, direction);
		return new ArangoDBIterator<Edge>(graph, query.getCursorResult(ArangoDBEdge.class));
	}


	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Vertex> vertices(Direction direction,
		String... edgeLabels) {
		// Query will raise an exception if the edge_collection name is not in the graph, so we need
		// to filter out edgeLabels not in the graph. 
		List<String> edgeCollections = Arrays.stream(edgeLabels)
				.filter(el -> graph.edgeCollections().contains(el))
				.map(el -> ArangoDBUtil.getCollectioName(graph.name(), el)).collect(Collectors.toList());
		// However, if edgeLabels was not empty but all were discarded, this means that we should
		// return an empty iterator, i.e. no edges for that edgeLabels exist.
		if ((edgeLabels.length > 0) && edgeCollections.isEmpty()) {
			return Collections.emptyIterator();
		}
		ArangoDBQuery query = graph.getClient().getDocumentNeighbors(graph, this, edgeCollections, direction, null);
		return new ArangoDBIterator<Vertex>(graph, query.getCursorResult(ArangoDBVertex.class));
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<String> labels = new ArrayList<>();
        labels.add(ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION);
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("key", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoDBQuery query = graph.getClient().getDocumentNeighbors(graph, this, labels, Direction.OUT, filter);
        return new ArangoDBIterator<VertexProperty<V>>(graph, query.getCursorResult(ArangoDBVertexProperty.class));
    }

	
	@Override
    public String toString() {
    	return StringFactory.vertexString(this);
    }

	public void save() {
		if (paired) {
			graph.getClient().updateDocument(graph, this);
		}
	}

}

