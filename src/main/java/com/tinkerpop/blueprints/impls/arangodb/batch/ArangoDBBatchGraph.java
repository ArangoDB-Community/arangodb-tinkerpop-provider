//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraphException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBConfiguration;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBIndex;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleEdge;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleGraph;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleGraphClient;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBSimpleVertex;
import com.tinkerpop.blueprints.impls.arangodb.utils.ArangoDBUtil;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * A Blueprints implementation of the ArangoDB batch inserter for bulk loading
 * data into a ArangoDB graph. This is a single threaded, non-transactional bulk
 * loader and should not be used for any other reason than for massive initial
 * data loads.
 * <p/>
 * ArangoDBBatchGraph is <b>not</b> a completely faithful Blueprints
 * implementation. Many methods throw UnsupportedOperationExceptions and take
 * unique arguments. Be sure to review each method's JavaDoc.
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBBatchGraph implements Graph, MetaGraph<ArangoDBSimpleGraph>, KeyIndexableGraph {

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
	}

	/**
	 * ArangoDBSimpleGraph
	 */

	private ArangoDBSimpleGraph rawGraph = null;

	/**
	 * A ArangoDBSimpleGraphClient to handle the connection to the Database
	 */

	public ArangoDBSimpleGraphClient client = null;

	/**
	 * A cache for all created vertices
	 */

	public HashMap<String, ArangoDBBatchVertex> vertexCache = null;

	/**
	 * A cache for all created edges
	 */

	public HashMap<String, ArangoDBBatchEdge> edgeCache = null;

	/**
	 * Maximum number of changed and not saved elements
	 */

	private int maxChangedElements = 3000;

	/**
	 * Set of added vertices
	 */

	private HashSet<ArangoDBBatchVertex> addedVertices = null;

	/**
	 * Set of added edges
	 */

	private HashSet<ArangoDBBatchEdge> addedEdges = null;

	/**
	 * counter for created identifiers
	 */

	private Long idCounter = 0l;

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

	public ArangoDBBatchGraph(String host, int port, String name, String verticesCollectionName,
			String edgesCollectionName) throws ArangoDBGraphException {
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

	public ArangoDBBatchGraph(ArangoDBConfiguration configuration, String name, String verticesCollectionName,
			String edgesCollectionName) throws ArangoDBGraphException {

		vertexCache = new HashMap<String, ArangoDBBatchVertex>();
		edgeCache = new HashMap<String, ArangoDBBatchEdge>();
		client = new ArangoDBSimpleGraphClient(configuration);

		addedVertices = new HashSet<ArangoDBBatchVertex>();
		addedEdges = new HashSet<ArangoDBBatchEdge>();

		try {
			rawGraph = this.client.getGraph(name);
			if (verticesCollectionName != null && edgesCollectionName != null) {
				// check names
				if (!rawGraph.getVertexCollection().equals(verticesCollectionName)) {
					throw new ArangoDBGraphException("Graph with that name already exists with other vertex collection");
				}
				if (!rawGraph.getEdgeCollection().equals(edgesCollectionName)) {
					throw new ArangoDBGraphException("Graph with that name already exists with other edge collection");
				}
			}
		} catch (ArangoDBException e_gal) {
			try {
				rawGraph = this.client.createGraph(name, verticesCollectionName, edgesCollectionName);
			} catch (ArangoDBException e) {
				throw new ArangoDBGraphException(e.getMessage());
			}
		}
	}

	public Features getFeatures() {
		return FEATURES;
	}

	public void shutdown() {
		saveVertices();
		saveEdges();
	}

	public Vertex addVertex(Object id) {
		return ArangoDBBatchVertex.create(this, id);
	}

	public Vertex getVertex(Object id) {
		return ArangoDBBatchVertex.load(this, id);
	}

	/**
	 * @throws UnsupportedOperationException
	 */
	public void removeVertex(Vertex vertex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException
	 */
	public Iterable<Vertex> getVertices() {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException
	 */
	public Iterable<Vertex> getVertices(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
		return ArangoDBBatchEdge.create(this, id, outVertex, inVertex, label);
	}

	public Edge getEdge(Object id) {
		return ArangoDBBatchEdge.load(this, id);
	}

	/**
	 * @throws UnsupportedOperationException
	 */
	public void removeEdge(Edge edge) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException
	 */
	public Iterable<Edge> getEdges() {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException
	 */
	public Iterable<Edge> getEdges(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	public Object getProperty(String key) {
		return this.rawGraph.getProperty(key);
	}

	public ArangoDBSimpleGraph getRawGraph() {
		return rawGraph;
	}

	public String toString() {
		return StringFactory.graphString(this, this.rawGraph.toString());
	}

	public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {

		List<ArangoDBIndex> indices = null;
		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				indices = client.getVertexIndices(rawGraph);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				indices = client.getEdgeIndices(rawGraph);
			}
		} catch (ArangoDBException e) {
		}

		String n = ArangoDBUtil.normalizeKey(key);

		if (indices != null) {
			for (ArangoDBIndex i : indices) {
				if (i.getFields().size() == 1) {
					String field = i.getFields().elementAt(0);

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

	public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {

		String type = "skiplist";
		boolean unique = false;
		Vector<String> fields = new Vector<String>();

		String n = ArangoDBUtil.normalizeKey(key);
		fields.add(n);

		for (Parameter p : indexParameters) {
			if (p.getKey().equals("type")) {
				type = p.getValue().toString();
			}
			if (p.getKey().equals("unique")) {
				unique = (Boolean) p.getValue();
			}
		}

		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				client.createVertexIndex(rawGraph, type, unique, fields);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				client.createEdgeIndex(rawGraph, type, unique, fields);
			}
		} catch (ArangoDBException e) {
		}
	}

	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		HashSet<String> result = new HashSet<String>();
		List<ArangoDBIndex> indices = null;
		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				indices = client.getVertexIndices(rawGraph);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				indices = client.getEdgeIndices(rawGraph);
			}

			for (ArangoDBIndex i : indices) {
				if (i.getFields().size() == 1) {
					String key = i.getFields().elementAt(0);

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

	/**
	 * Save changed vertices. This functions has to be called in the shutdown
	 * function.
	 * 
	 * @throws ArangoDBException
	 */

	private void saveVertices() {
		List<ArangoDBSimpleVertex> vertices = new ArrayList<ArangoDBSimpleVertex>();

		for (ArangoDBBatchVertex element : addedVertices) {
			vertices.add(element.getRawVertex());
			element.created = true;
		}

		try {
			client.createVertices(rawGraph, vertices, false);
		} catch (ArangoDBException e) {
			e.printStackTrace();
		}

		addedVertices.clear();
	}

	/**
	 * Save changed edges. This functions has to be called in the shutdown
	 * function.
	 * 
	 * @throws ArangoDBException
	 */

	private void saveEdges() {
		List<ArangoDBSimpleEdge> edges = new ArrayList<ArangoDBSimpleEdge>();

		for (ArangoDBBatchEdge element : addedEdges) {
			edges.add(element.getRawEdge());
			element.created = true;
		}

		try {
			client.createEdges(rawGraph, edges, false);
		} catch (ArangoDBException e) {
			e.printStackTrace();
		}

		addedEdges.clear();
	}

	/**
	 * Add a new vertex
	 * 
	 * @param element
	 *            the added Element
	 * 
	 * @throws ArangoDBException
	 */

	public void addCreatedVertex(ArangoDBBatchVertex element) throws ArangoDBException {
		if (addedVertices.size() > maxChangedElements) {
			saveVertices();
		}
		addedVertices.add(element);
	}

	/**
	 * Add a new edge
	 * 
	 * @param element
	 *            the added Element
	 * 
	 * @throws ArangoDBException
	 */

	public void addCreatedEdge(ArangoDBBatchEdge element) throws ArangoDBException {
		if (addedEdges.size() > maxChangedElements) {
			saveVertices();
			saveEdges();
		}
		addedEdges.add(element);
	}

	/**
	 * @throws UnsupportedOperationException
	 */
	public GraphQuery query() {
		throw new UnsupportedOperationException();
	}

	synchronized public Long getNewId() {
		return ++idCounter;
	}

}
