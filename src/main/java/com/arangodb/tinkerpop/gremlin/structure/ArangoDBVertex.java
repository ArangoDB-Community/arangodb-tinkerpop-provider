//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;
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
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The ArangoDB vertex class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBVertex extends ArangoDBBaseDocument implements Vertex, ArangoDBElement {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

	/** All property access is delegated to the property manager */

	protected ArangoDBPropertyManager pManager;

	/**
	 * Constructor used for ArabgoDB JavaBeans serialisation.
	 */

	public ArangoDBVertex() {
		super();
		pManager = new ArangoDBPropertyManager(this);
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param collection the collection
	 * @param graph the graph
	 */
	public ArangoDBVertex(
		String collection,
		ArangoDBGraph graph) {
		this(null, collection, graph);
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param key the key
	 * @param collection the collection
	 * @param graph the graph
	 */
	public ArangoDBVertex(
		String key,
		String collection,
		ArangoDBGraph graph) {
		super(key, collection, graph);
		pManager = new ArangoDBPropertyManager(this);
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
			Graph.Exceptions.argumentCanNotBeNull("vertex");
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
			edge = new ArangoDBEdge(label, this, ((ArangoDBVertex) inVertex), graph);
		}
        // The vertex needs to exist before we can attach properties
		graph.getClient().insertEdge(edge);
        ElementHelper.attachProperties(edge, keyValues);
		return edge;
	}

	@Override
	public <V> VertexProperty<V> property(final String key) {
		return pManager.vertexProperty(key);
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
		VertexProperty<V> p = null;
		if (cardinality.equals(VertexProperty.Cardinality.single)) {
			p = pManager.vertexProperty(key, value);
			addNestedProperties(p, keyValues);
			ElementHelper.attachProperties(p, keyValues);
		}
		// FIXME This assumes Cardinality is not changed from set to list (and viceversa)
		else {
			p = pManager.vertexProperty(key, value, cardinality);
			Collection<VertexProperty<V>> matches = pManager.propertiesForValue(key, value);
			if (matches.isEmpty()) {
				ElementHelper.attachProperties(p, keyValues);
			}
			else {
				for (VertexProperty<V> m : matches) {
					p = m;
					ElementHelper.attachProperties(m, keyValues);
				}
			}
		}
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
		logger.debug("Get Properties {}", (Object[])propertyKeys);
		return pManager.vertexProperties(propertyKeys);
	}

	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		logger.debug("Get Values {}", (Object[])propertyKeys);
		return pManager.values(propertyKeys);
	}

	@Override
	public Set<String> keys() {
		return pManager.keys();
	}

	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		this.pManager.removeProperty(property);
	}

	/**
	 * This method is intended for rapid deserialization
	 * @return
	 */
	public void attachProperties(String key, Collection<ArangoDBVertexProperty> properties) {
		this.pManager.attachVertexProperties(key, properties);
	}

	/**
	 * Add the nested vertexProperties to the vertex property
	 * @param p             the VertexProperty
	 * @param keyValues     the pairs of nested key:value to add
	 */
	private void addNestedProperties(VertexProperty<?> p, Object[] keyValues) {
		for (int i = 0; i < keyValues.length; i = i + 2) {
			if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label)) {
				p.property((String)keyValues[i], keyValues[i + 1]);
			}
		}
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
	public String toString() {
		return StringFactory.vertexString(this);
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

