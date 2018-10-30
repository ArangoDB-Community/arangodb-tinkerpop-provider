//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
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

import org.apache.commons.lang.ArrayUtils;
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
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQueryBuilder;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The ArangoDB vertex class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertex extends ArangoDBBaseDocument implements Vertex {

	/** The Logger. */

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertex() {
		super();
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param graph the graph
	 * @param collection the collection
	 * @param key the key
	 */
	public ArangoDBVertex(ArangoDBGraph graph, String collection, String key) {
		super(key);
        this.graph = graph;
        this.collection = collection;
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param graph the graph
	 * @param collection the collection
	 */
	public ArangoDBVertex(ArangoDBGraph graph, String collection) {
		this(graph, collection, null);
	}

    @Override
    public Object id() {
        return _id();
    }

    @Override
    public String label() {
        return collection();
    }

	@Override
	public void remove() {
		logger.info("remove {}", this._id());
		Map<String, Object> bindVars = new HashMap<>();
		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
		queryBuilder.iterateGraph(graph.name(), "v", Optional.of("e"), Optional.empty(),
				Optional.of(1), Optional.empty(), ArangoDBQueryBuilder.Direction.OUT,
				this._id(), bindVars)
			.append(String.format("    REMOVE v IN %s_ELEMENT_PROPERTIES\n", graph.name()))
			.append(String.format("    REMOVE e IN %s_ELEMENT_HAS_PROPERTIES\n", graph.name()));
		String query = queryBuilder.toString();
		graph.getClient().executeAqlQuery(query , bindVars, null, this.getClass());
		queryBuilder = new ArangoDBQueryBuilder()
			.append(String.format("REMOVE Document(@startVertex) IN %s", ArangoDBUtil.getCollectioName(graph.name(), label())));

		query = queryBuilder.toString();
		graph.getClient().executeAqlQuery(query , bindVars, null, this.getClass());
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
        	if (graph.features().edge().willAllowId(id)) {
        		Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
        		if (m.matches()) {
        			edge = new ArangoDBEdge(graph, label, this, ((ArangoDBVertex) inVertex), id.toString());
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
			id = (String) idValue.get();
			if (id.toString().contains("/")) {
        		String fullId = id.toString();
        		String[] parts = fullId.split("/");
        		// The collection name is the last part of the full name
        		String[] collectionParts = parts[0].split("_");
				String collectionName = collectionParts[collectionParts.length-1];
				if (collectionName.contains(ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION)) {
        			id = parts[1];
        			
        		}
        	}
			if (graph.features().vertex().willAllowId(idValue.get())) {
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
		return new ArangoDBIterator<Edge>(graph, graph.getClient().getVertexEdges(graph.name(), this, edgeCollections, direction));
	}


	@Override
	public Iterator<Vertex> vertices(Direction direction,
		String... edgeLabels) {
		// Query will raise an exception if the edge_collection name is not in the graph, so we need
		// to filter out edgeLabels not in the graph.
		List<String> edgeCollections;
		if (edgeLabels.length == 0) {
			edgeCollections = graph.edgeCollections();
		}
		else {
			edgeCollections = Arrays.stream(edgeLabels)
				.filter(el -> graph.edgeCollections().contains(el))
				.collect(Collectors.toList());
			// If edgeLabels was not empty but all were discarded, this means that we should
			// return an empty iterator, i.e. no edges for that edgeLabels exist.
			if (edgeCollections.isEmpty()) {
				return Collections.emptyIterator();
			}
		}
		ArangoCursor<ArangoDBVertex> documentNeighbors = graph.getClient().getDocumentNeighbors(graph.name(), this, edgeCollections, direction, ArangoDBPropertyFilter.empty(), ArangoDBVertex.class);
		return new ArangoDBIterator<Vertex>(graph, documentNeighbors);
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		logger.debug("Get properties {}", (Object[])propertyKeys);
        List<String> labels = new ArrayList<>();
        labels.add(ArangoDBUtil.ELEMENT_PROPERTIES_EDGE);
        ArangoDBPropertyFilter filter = new ArangoDBPropertyFilter();
        for (String pk : propertyKeys) {
            filter.has("key", pk, ArangoDBPropertyFilter.Compare.EQUAL);
        }
        logger.debug("Creating ArangoDB query");
        ArangoCursor<?> query = graph.getClient().getElementProperties(graph.name(), this, labels, filter, ArangoDBVertexProperty.class);
        return (Iterator<VertexProperty<V>>) new ArangoDBPropertyIterator<V, VertexProperty<V>>(graph, (ArangoCursor<ArangoDBVertexProperty<V>>) query);
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
	
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

}

