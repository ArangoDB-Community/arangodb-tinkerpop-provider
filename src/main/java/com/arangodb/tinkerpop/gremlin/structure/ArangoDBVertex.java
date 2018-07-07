//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDBException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertexProperty.ArangoDBVertexPropertyOwner;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import com.arangodb.velocypack.annotations.Expose;


/**
 * The ArangoDB vertex class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertex extends AbstractArangoDBElement implements ArangoDBElement, Vertex {
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);
	
	/**  Map to store the element properties */
	protected Map<String, ArangoDBVertexPropertyOwner<?>> propertyDesc = new HashMap<>(4, 0.75f);
	
	@Expose(serialize = false, deserialize = false)
	protected Set<VertexProperty<?>> properties = new HashSet<>(4);
	
	
	public ArangoDBVertex() {
		super();
	}

	public ArangoDBVertex(ArangoDBGraph graph, String collection, String key) {
		super(graph, collection, key);
	}

	public ArangoDBVertex(ArangoDBGraph graph, String collection) {
		super(graph, collection);
	}

	
	@Override
	public void remove() {
		logger.info("remove {}", this._key());
		try {
			graph.getClient().deleteVertex(graph, this);
		} catch (ArangoDBGraphException e) {
			logger.error("Unable to remove vertex in DB", e);
			throw new IllegalStateException("Unable to remove vertex in DB", e);
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
        		edge = new ArangoDBEdge(graph, label, id.toString(), this, ((ArangoDBVertex) inVertex));
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}
        }
		else {
			edge = new ArangoDBEdge(graph, label, this, ((ArangoDBVertex) inVertex));
		}
		ElementHelper.attachProperties(edge, keyValues);
		try {
			graph.getClient().insertEdge(graph, edge);
		} catch (ArangoDBGraphException e) {
			throw ArangoDBGraph.Exceptions.getArangoDBException((ArangoDBException) e.getCause());
		}
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
		loadProperties();
		ElementHelper.validateProperty(key, value);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		Optional<Object> id = ElementHelper.getIdValue(keyValues);
		if (id.isPresent()) {
			if (!graph.features().vertex().willAllowId(id.get())) {
				VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
			}
		}
		ArangoDBVertexProperty<V> result;
		ArangoDBVertexPropertyOwner<V> desc = (ArangoDBVertexPropertyOwner<V>) propertyDesc.get(key);
		if (desc == null) {
			desc = new ArangoDBVertexPropertyOwner<V>(this, key, cardinality);
			propertyDesc.put(key, desc);
		}
		result = desc.addProperty(key, value);
		properties.add(result);
		ElementHelper.attachProperties(result, keyValues);
		if (desc.wasModified()) {
			save();
		}
		return result;
	}


	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Edge> edges(Direction direction,
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
		ArangoDBQuery query = graph.getClient().getVertexEdges(graph, this, edgeCollections, direction);
		try {
			return new ArangoDBIterator<Edge>(graph, query.getCursorResult(ArangoDBEdge.class));
		} catch (ArangoDBGraphException e) {
			// TODO Auto-generated catch block
			// return Collections.emptyIterator()?
			return null;
		}
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
		ArangoDBQuery query = graph.getClient().getVertexNeighbors(graph, this, edgeCollections, direction);
		try {
			return new ArangoDBIterator<Vertex>(graph, query.getCursorResult(ArangoDBVertex.class));
		} catch (ArangoDBGraphException e) {
			return null;
		}	
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		loadProperties();
		Set<String> allProperties = new HashSet<>(propertyDesc.keySet());
		if (propertyKeys.length > 1) {
			allProperties.retainAll(Arrays.asList(propertyKeys));
		}
		return properties.stream()
				.filter(e -> allProperties.contains(e.key()))
				.map(p -> (VertexProperty<V>)p)
				.iterator();
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> VertexProperty<V> property(String key) {
		ArangoDBVertexPropertyOwner<?> desc = propertyDesc.get(key);
		if (desc == null) {
			return VertexProperty.empty();
		}
		Collection<?> props = desc.properties();
		if (props.size() > 1) {
			throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
		}
		return (VertexProperty<V>) props.iterator().next();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> V value(String key) throws NoSuchElementException {
		try {
			return (V) property(key).value();
		}
		catch (IllegalStateException ex) {
			throw new NoSuchElementException();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		loadProperties();
		Set<String> allProperties = new HashSet<>(propertyDesc.keySet());
		if (propertyKeys.length > 1) {
			allProperties.retainAll(Arrays.asList(propertyKeys));
		}
		return properties.stream()
				.filter(e -> allProperties.contains(e.key()))
				.map(p -> (V)p.value())
				.iterator();
	}
	
	@Override
    public String toString() {
    	return StringFactory.vertexString(this);
    }

	@Override
	public void save() {
		if (paired) {
			try {
				graph.getClient().updateVertex(graph, this);
			} catch (ArangoDBGraphException e) {
				logger.error("Unable to update vertex in DB", e);
				throw new IllegalStateException("Unable to update vertex in DB", e);
			}
		}
		
	}

	@Override
	public Set<String> propertiesKeys() {
		return Collections.unmodifiableSet(propertyDesc.keySet());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		properties.remove(property);
	}
	
	public void remove(ArangoDBVertexPropertyOwner<?> arangoDBVertexPropertyOwner) {
		propertyDesc.remove(arangoDBVertexPropertyOwner.key());
		save();
	}

	
	/**
	 * Since properties are not stored in the DB, this method makes sure that they are populated
	 * after deserializing.
	 */
	private void loadProperties() {
		if (properties.isEmpty() && !propertyDesc.isEmpty()) {
			Iterator<Entry<String, ArangoDBVertexPropertyOwner<?>>> it = propertyDesc.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, ArangoDBVertexPropertyOwner<?>> e = it.next();
				properties.addAll(e.getValue().properties());
			}
		}
	}


}

