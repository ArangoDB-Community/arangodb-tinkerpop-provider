//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.arangodb.ArangoException;
import com.arangodb.blueprints.client.ArangoDBConfiguration;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBIndex;
import com.arangodb.blueprints.client.ArangoDBSimpleGraph;
import com.arangodb.blueprints.client.ArangoDBSimpleGraphClient;
import com.arangodb.blueprints.utils.ArangoDBUtil;
import com.arangodb.entity.EdgeDefinitionEntity;
import com.arangodb.entity.GraphEntity;
import com.arangodb.entity.IndexType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * The ArangoDB graph class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBGraph implements Graph, MetaGraph<ArangoDBSimpleGraph>, KeyIndexableGraph {

	private static final Features FEATURES = new Features();

	static {
		FEATURES.supportsDuplicateEdges = true;
		FEATURES.supportsSelfLoops = true;
		FEATURES.isPersistent = true;

		FEATURES.supportsVertexIteration = true;
		FEATURES.supportsEdgeIteration = true;
		FEATURES.supportsVertexIndex = false;
		FEATURES.supportsEdgeIndex = false;
		FEATURES.ignoresSuppliedIds = false;

		FEATURES.supportsTransactions = false;
		FEATURES.supportsEdgeKeyIndex = true;
		FEATURES.supportsVertexKeyIndex = true;
		FEATURES.supportsKeyIndices = true;
		FEATURES.isWrapper = true;
		FEATURES.supportsIndices = false;
		FEATURES.supportsEdgeRetrieval = true;
		FEATURES.supportsVertexProperties = true;
		FEATURES.supportsEdgeProperties = true;

		// For more information on supported types, please see:
		// http://code.google.com/p/orient/wiki/Types
		FEATURES.supportsSerializableObjectProperty = true;
		FEATURES.supportsBooleanProperty = true;
		FEATURES.supportsDoubleProperty = true;
		FEATURES.supportsFloatProperty = true;
		FEATURES.supportsIntegerProperty = true;
		FEATURES.supportsPrimitiveArrayProperty = true;
		FEATURES.supportsUniformListProperty = true;
		FEATURES.supportsMixedListProperty = true;
		FEATURES.supportsLongProperty = true;
		FEATURES.supportsMapProperty = true;
		FEATURES.supportsStringProperty = true;
		FEATURES.supportsThreadedTransactions = false;
		FEATURES.supportsThreadIsolatedTransactions = false;
	}

	/**
	 * ArangoDBSimpleGraph
	 */
	private ArangoDBSimpleGraph simpleGraph = null;

	/**
	 * A ArangoDBSimpleGraphClient to handle the connection to the Database
	 */
	private ArangoDBSimpleGraphClient client = null;

	/**
	 * Creates a Graph (simple configuration)
	 * 
	 * @param host
	 *            the ArangoDB host name
	 * @param port
	 *            the ArangoDB port
	 * @param name
	 *            the name of the graph
	 * @param verticesCollectionName
	 *            the name of the vertices collection
	 * @param edgesCollectionName
	 *            the name of the edges collection
	 * 
	 * @throws ArangoDBGraphException
	 *             if the graph could not be created
	 */

	public ArangoDBGraph(String host, int port, String name, String verticesCollectionName, String edgesCollectionName)
			throws ArangoDBGraphException {
		this(new ArangoDBConfiguration(host, port), name, verticesCollectionName, edgesCollectionName);
	}

	/**
	 * Creates a Graph
	 * 
	 * @param configuration
	 *            an ArangoDB configuration object
	 * @param name
	 *            the name of the graph
	 * @param verticesCollectionName
	 *            the name of the vertices collection
	 * @param edgesCollectionName
	 *            the name of the edges collection
	 * 
	 * @throws ArangoDBGraphException
	 *             if the graph could not be created
	 */

	public ArangoDBGraph(ArangoDBConfiguration configuration, String name, String verticesCollectionName,
		String edgesCollectionName) throws ArangoDBGraphException {

		if (StringUtils.isBlank(name)) {
			throw new ArangoDBGraphException("graph name must not be null.");
		}

		if (StringUtils.isBlank(verticesCollectionName)) {
			throw new ArangoDBGraphException("vertex collection name must not be null.");
		}

		if (StringUtils.isBlank(edgesCollectionName)) {
			throw new ArangoDBGraphException("edge collection name must not be null.");
		}

		client = new ArangoDBSimpleGraphClient(configuration);
		try {
			GraphEntity graph = client.getGraph(name);
			if (graph != null) {
				boolean error = false;

				List<EdgeDefinitionEntity> edgeDefinitions = graph.getEdgeDefinitions();

				if (edgeDefinitions.size() != 1 || CollectionUtils.isNotEmpty(graph.getOrphanCollections())) {
					error = true;
				} else {
					EdgeDefinitionEntity edgeDefinitionEntity = edgeDefinitions.get(0);
					if (!edgesCollectionName.equals(edgeDefinitionEntity.getCollection())
							|| edgeDefinitionEntity.getFrom().size() != 1 || edgeDefinitionEntity.getTo().size() != 1
							|| !verticesCollectionName.equals(edgeDefinitionEntity.getFrom().get(0))
							|| !verticesCollectionName.equals(edgeDefinitionEntity.getTo().get(0))) {
						error = true;
					}
				}
				if (error) {
					throw new ArangoDBGraphException("Graph with that name already exists but with other settings");
				}
				simpleGraph = new ArangoDBSimpleGraph(graph, verticesCollectionName, edgesCollectionName);
			}
		} catch (ArangoException e1) {
		}
		if (simpleGraph == null) {
			try {
				simpleGraph = this.client.createGraph(name, verticesCollectionName, edgesCollectionName);
			} catch (ArangoException e2) {
				throw new ArangoDBGraphException(e2);
			}
		}
	}

	public Features getFeatures() {
		return FEATURES;
	}

	public void shutdown() {
		client.shutdown();
	}

	public Vertex addVertex(Object id) {
		return ArangoDBVertex.create(this, id);
	}

	public Vertex getVertex(Object id) {
		return ArangoDBVertex.load(this, id);
	}

	public void removeVertex(Vertex vertex) {
		if (vertex.getClass().equals(ArangoDBVertex.class)) {
			ArangoDBVertex v = (ArangoDBVertex) vertex;
			v.remove();
		}
	}

	public Iterable<Vertex> getVertices() {
		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
		return q.vertices();
	}

	public Iterable<Vertex> getVertices(String key, Object value) {
		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
		q.has(key, value);
		return q.vertices();
	}

	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {

		if (label == null) {
			throw ExceptionFactory.edgeLabelCanNotBeNull();
		}

		return ArangoDBEdge.create(this, id, outVertex, inVertex, label);
	}

	public Edge getEdge(Object id) {
		return ArangoDBEdge.load(this, id);
	}

	public void removeEdge(Edge edge) {
		if (edge.getClass().equals(ArangoDBEdge.class)) {
			ArangoDBEdge e = (ArangoDBEdge) edge;
			e.remove();
		}
	}

	public Iterable<Edge> getEdges() {
		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
		return q.edges();
	}

	public Iterable<Edge> getEdges(String key, Object value) {
		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
		q.has(key, value);
		return q.edges();
	}

	public ArangoDBSimpleGraph getRawGraph() {
		return simpleGraph;
	}

	public String toString() {
		return StringFactory.graphString(this, this.simpleGraph.toString());
	}

	public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {

		List<ArangoDBIndex> indices = null;
		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				indices = client.getVertexIndices(simpleGraph);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				indices = client.getEdgeIndices(simpleGraph);
			}
		} catch (ArangoDBException e) {
		}

		String n = ArangoDBUtil.normalizeKey(key);

		if (indices != null) {
			for (ArangoDBIndex i : indices) {
				if (i.getFields().size() == 1) {
					String field = i.getFields().get(0);

					if (field.equals(n)) {
						try {
							client.deleteIndex(i.getId());
						} catch (ArangoDBException e) {
						}
					}
				}
			}
		}

	}

	@SuppressWarnings("rawtypes")
	public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {

		IndexType type = IndexType.SKIPLIST;
		boolean unique = false;
		Vector<String> fields = new Vector<String>();

		String n = ArangoDBUtil.normalizeKey(key);
		fields.add(n);

		for (Parameter p : indexParameters) {
			if (p.getKey().equals("type")) {
				type = object2IndexType(p.getValue());
			}
			if (p.getKey().equals("unique")) {
				unique = (Boolean) p.getValue();
			}
		}

		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				getClient().createVertexIndex(simpleGraph, type, unique, fields);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				getClient().createEdgeIndex(simpleGraph, type, unique, fields);
			}
		} catch (ArangoDBException e) {
		}
	}

	private IndexType object2IndexType(Object obj) {
		if (obj instanceof IndexType) {
			return (IndexType) obj;
		}

		if (obj != null) {
			String str = obj.toString();
			for (IndexType indexType : IndexType.values()) {
				if (indexType.toString().equalsIgnoreCase(str)) {
					return indexType;
				}
			}
		}

		return IndexType.SKIPLIST;
	}

	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		HashSet<String> result = new HashSet<String>();
		List<ArangoDBIndex> indices = null;
		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				indices = client.getVertexIndices(simpleGraph);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				indices = client.getEdgeIndices(simpleGraph);
			}

			for (ArangoDBIndex i : indices) {
				if (i.getFields().size() == 1) {
					String key = i.getFields().get(0);

					// ignore system index
					if (key.charAt(0) != '_') {
						result.add(ArangoDBUtil.denormalizeKey(key));
					}
				}
			}

		} catch (ArangoDBException e) {
		}

		return result;
	}

	public GraphQuery query() {
		return new ArangoDBGraphQuery(this);
	}

	/**
	 * Returns the ArangoDBSimpleGraphClient object
	 * 
	 * @return the ArangoDBSimpleGraphClient object
	 */
	public ArangoDBSimpleGraphClient getClient() {
		return client;
	}

	/**
	 * Returns the identifier of the graph
	 * 
	 * @return the identifier of the graph
	 */
	public String getId() {
		return simpleGraph.getGraphEntity().getDocumentKey();
	}
}
