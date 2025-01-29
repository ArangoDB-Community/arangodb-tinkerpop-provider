//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

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

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyIterator;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The ArangoDB vertex class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBVertex extends ArangoDBBaseDocument implements Vertex {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertex() {
		super();
	}

	/**
	 * Instantiates a new ArangoDB vertex with he given key.
	 *
	 * @param key 					the key to assign to the vertex
	 * @param label                	the label of the vertex
	 * @param graph                	the graph that owns the vertex
	 */

	public ArangoDBVertex(String key, String label, ArangoDBGraph graph) {
		super(key, label, graph);
	}

	/**
	 * Instantiates a new ArangoDB vertex.
	 *
	 * @param graph 				the graph
	 * @param collection 			the collection
	 */

	public ArangoDBVertex(ArangoDBGraph graph, String collection) {
		this(null, collection, graph);
	}

    @Override
    public Object id() {
        return _id();
    }

    @Override
    public String label() {
        return label;
    }

	@Override
	public void remove() {
		logger.info("remove {}", this._id());
		if (paired) {
			Map<String, Object> bindVars = new HashMap<>();
			// Delete properties
			properties().forEachRemaining(VertexProperty::remove);
			//Remove vertex
			try {
				graph.getClient().deleteDocument(this);
				// Need to delete incoming edges too
			} catch (ArangoDBGraphException ex) {
				// Pass Removing a property that does not exists should not throw an exception.
			}
		}
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
					String collectionName = collectionParts[collectionParts.length-1];
					if (collectionName.contains(label)) {
	        			id = parts[1];
	        			
	        		}
	        	}
        		Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
        		if (m.matches()) {
        			edge = new ArangoDBEdge(id.toString(), label, this, ((ArangoDBVertex) inVertex), graph);
        		}
        		else {
            		throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
            	}
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}
        }
		else {
			edge = new ArangoDBEdge(graph, label, this, ((ArangoDBVertex) inVertex));
		}
        // The vertex needs to exist before we can attach properties
		graph.getClient().insertEdge(edge);
        ElementHelper.attachProperties(edge, keyValues);
		return edge;
	}


	@Override
	public <V> VertexProperty<V> property(
		Cardinality cardinality,
		String key,
		V value,
		Object... keyValues) {
		logger.debug("setting vertex property {} = {} ({})", key, value, keyValues);
		ElementHelper.validateProperty(key, value);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		Optional<Object> idValue = ElementHelper.getIdValue(keyValues);
		String id = null;
		if (idValue.isPresent()) {
			if (graph.features().vertex().willAllowId(idValue.get())) {
				id = idValue.get().toString();
				if (id.toString().contains("/")) {
	        		String fullId = id.toString();
	        		String[] parts = fullId.split("/");
	        		// The collection name is the last part of the full name
	        		String[] collectionParts = parts[0].split("_");
					String collectionName = collectionParts[collectionParts.length-1];
					if (collectionName.contains(ArangoDBGraph.ELEMENT_PROPERTIES_COLLECTION)) {
	        			id = parts[1];
	        			
	        		}
	        	}
		        Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
				if (!m.matches()) {
					throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
		    	}
			}
			else {
				throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
			}
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


	@Override
	public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
		List<String> edgeCollections = getQueryEdgeCollections(edgeLabels);
		// If edgeLabels was not empty but all were discarded, this means that we should
		// return an empty iterator, i.e. no edges for that edgeLabels exist.
		if (edgeCollections.isEmpty()) {
			return Collections.emptyIterator();
		}
		return new ArangoDBIterator<>(graph, graph.getClient().getVertexEdges(this, edgeCollections, direction));
	}


	@Override
	public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
		List<String> edgeCollections = getQueryEdgeCollections(edgeLabels);
		// If edgeLabels was not empty but all were discarded, this means that we should
		// return an empty iterator, i.e. no edges for that edgeLabels exist.
		if (edgeCollections.isEmpty()) {
			return Collections.emptyIterator();
		}
		ArangoCursor<ArangoDBVertex> documentNeighbors = graph.getClient().getDocumentNeighbors(this, edgeCollections, direction, ArangoDBPropertyFilter.empty(), ArangoDBVertex.class);
		return new ArangoDBIterator<>(graph, documentNeighbors);
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		logger.debug("Get properties {}", (Object[])propertyKeys);
        List<String> labels = new ArrayList<>();
        labels.add(graph.getPrefixedCollectioName(ArangoDBGraph.ELEMENT_PROPERTIES_EDGE_COLLECTION));
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("name", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        ArangoCursor<?> query = graph.getClient().getElementProperties(this, labels, filter, ArangoDBVertexProperty.class);
        return new ArangoDBPropertyIterator<>(graph, (ArangoCursor<ArangoDBVertexProperty<V>>) query);
    }


	@Override
    public String toString() {
    	return StringFactory.vertexString(this);
    }

	/**
	 * Save.
	 */
	public void save() {
		if (paired) {
			graph.getClient().updateDocument(this);
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
		}
		else {
			vertexCollections = Arrays.stream(edgeLabels)
					.filter(el -> graph.edgeCollections().contains(el))
					.map(graph::getPrefixedCollectioName)
					.collect(Collectors.toList());

		}
		return vertexCollections;
	}
	
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

}

