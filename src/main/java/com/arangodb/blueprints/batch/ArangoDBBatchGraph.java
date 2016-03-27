//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.arangodb.ArangoException;
import com.arangodb.blueprints.ArangoDBGraphException;
import com.arangodb.blueprints.client.ArangoDBConfiguration;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBIndex;
import com.arangodb.blueprints.client.ArangoDBSimpleEdge;
import com.arangodb.blueprints.client.ArangoDBSimpleGraph;
import com.arangodb.blueprints.client.ArangoDBSimpleGraphClient;
import com.arangodb.blueprints.client.ArangoDBSimpleVertex;
import com.arangodb.blueprints.utils.ArangoDBUtil;
import com.arangodb.entity.EdgeDefinitionEntity;
import com.arangodb.entity.GraphEntity;
import com.arangodb.entity.IndexType;
import com.arangodb.util.CollectionUtils;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * <p>
 * A Blueprints implementation of the ArangoDB batch inserter for bulk loading
 * data into a ArangoDB graph. This is a single threaded, non-transactional bulk
 * loader and should not be used for any other reason than for massive initial
 * data loads.
 * </p>
 * ArangoDBBatchGraph is <b>not</b> a completely faithful Blueprints
 * implementation. Many methods throw UnsupportedOperationExceptions and take
 * unique arguments. Be sure to review each method's JavaDoc.
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBBatchGraph implements Graph, MetaGraph<ArangoDBSimpleGraph>, KeyIndexableGraph {

	/**
	 * the logger
	 */
	private static final Logger logger = Logger.getLogger(ArangoDBBatchGraph.class);

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

	private ArangoDBSimpleGraph rawGraph;

	/**
	 * A ArangoDBSimpleGraphClient to handle the connection to the Database
	 */

	public final ArangoDBSimpleGraphClient client;

	/**
	 * A cache for all created vertices
	 */

	public final Map<String, ArangoDBBatchVertex> vertexCache;

	/**
	 * A cache for all created edges
	 */

	public final Map<String, ArangoDBBatchEdge> edgeCache;

	/**
	 * Set of added vertices
	 */

	private Set<ArangoDBBatchVertex> addedVertices;

	/**
	 * Set of added edges
	 */

	private Set<ArangoDBBatchEdge> addedEdges = null;

	/**
	 * counter for created identifiers
	 */

	private Long idCounter = 0L;

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
			GraphEntity graph = client.getGraph(name);
			if (graph != null) {
				boolean error = graphHasError(verticesCollectionName, edgesCollectionName, graph);
				if (error) {
					throw new ArangoDBGraphException("Graph with that name already exists but with other settings");
				}
				rawGraph = new ArangoDBSimpleGraph(graph, verticesCollectionName, edgesCollectionName);
			}
		} catch (ArangoException e1) {
			logger.debug("could not read graph", e1);
		}
		if (rawGraph == null) {
			try {
				rawGraph = this.client.createGraph(name, verticesCollectionName, edgesCollectionName);
			} catch (ArangoException e) {
				logger.warn("could not create graph", e);
				throw new ArangoDBGraphException(e.getMessage());
			}
		}
	}

	private boolean graphHasError(String verticesCollectionName, String edgesCollectionName, GraphEntity graph) {
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
		return error;
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public void shutdown() {
		saveVertices();
		saveEdges();
	}

	@Override
	public Vertex addVertex(Object id) {
		return ArangoDBBatchVertex.create(this, id);
	}

	@Override
	public Vertex getVertex(Object id) {
		return ArangoDBBatchVertex.load(this, id);
	}

	/**
	 * not supported in batch mode
	 */
	@Override
	public void removeVertex(Vertex vertex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * not supported in batch mode
	 */
	@Override
	public Iterable<Vertex> getVertices() {
		throw new UnsupportedOperationException();
	}

	/**
	 * not supported in batch mode
	 */
	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
		return ArangoDBBatchEdge.create(this, id, outVertex, inVertex, label);
	}

	@Override
	public Edge getEdge(Object id) {
		return ArangoDBBatchEdge.load(this, id);
	}

	/**
	 * not supported in batch mode
	 */
	@Override
	public void removeEdge(Edge edge) {
		throw new UnsupportedOperationException();
	}

	/**
	 * not supported in batch mode
	 */
	@Override
	public Iterable<Edge> getEdges() {
		throw new UnsupportedOperationException();
	}

	/**
	 * not supported in batch mode
	 */
	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ArangoDBSimpleGraph getRawGraph() {
		return rawGraph;
	}

	@Override
	public String toString() {
		return StringFactory.graphString(this, this.rawGraph.toString());
	}

	@Override
	public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {

		List<ArangoDBIndex> indices = null;
		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				indices = client.getVertexIndices(rawGraph);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				indices = client.getEdgeIndices(rawGraph);
			}
		} catch (ArangoDBException e) {
			logger.warn("could not read indices", e);
		}

		String normalizedKey = ArangoDBUtil.normalizeKey(key);

		if (indices != null) {
			for (ArangoDBIndex index : indices) {
				if (index.getFields().size() == 1) {
					deleteIndexByKey(normalizedKey, index);
				}
			}
		}

	}

	private void deleteIndexByKey(String key, ArangoDBIndex index) {
		String field = index.getFields().get(0);
		if (field.equals(key)) {
			try {
				client.deleteIndex(index.getId());
			} catch (ArangoDBException e) {
				logger.warn("could not delete indice", e);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {

		IndexType type = IndexType.SKIPLIST;
		boolean unique = false;
		List<String> fields = new ArrayList<String>();

		String n = ArangoDBUtil.normalizeKey(key);
		fields.add(n);

		for (Parameter p : indexParameters) {
			if ("type".equals(p.getKey())) {
				type = object2IndexType(p.getValue());
			}
			if ("unique".equals(p.getKey())) {
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
			logger.error("could not create index", e);
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

	@Override
	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		HashSet<String> result = new HashSet<String>();
		List<ArangoDBIndex> indices = null;
		try {
			if (elementClass.isAssignableFrom(Vertex.class)) {
				indices = client.getVertexIndices(rawGraph);
			} else if (elementClass.isAssignableFrom(Edge.class)) {
				indices = client.getEdgeIndices(rawGraph);
			}

			for (ArangoDBIndex index : indices) {
				if (index.getFields().size() == 1) {
					addIndex(result, index);
				}
			}

		} catch (ArangoDBException e) {
			logger.error("could not get index keys", e);
		}

		return result;
	}

	private void addIndex(HashSet<String> result, ArangoDBIndex index) {
		String key = index.getFields().get(0);

		// ignore system index
		if (key.charAt(0) != '_') {
			result.add(ArangoDBUtil.denormalizeKey(key));
		}
	}

	/**
	 * Save changed vertices. This functions has to be called in the shutdown
	 * function.
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
			logger.error("could not create vertices", e);
		}

		addedVertices.clear();
	}

	/**
	 * Save changed edges. This functions has to be called in the shutdown
	 * function.
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
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
			logger.error("could not create edges", e);
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
	 *             if an error occurs
	 */

	public void addCreatedVertex(ArangoDBBatchVertex element) throws ArangoDBException {
		if (addedVertices.size() > client.getConfiguration().getBatchSize()) {
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
	 *             if an error occurs
	 */

	public void addCreatedEdge(ArangoDBBatchEdge element) throws ArangoDBException {
		if (addedEdges.size() > client.getConfiguration().getBatchSize()) {
			saveVertices();
			saveEdges();
		}
		addedEdges.add(element);
	}

	@Override
	public GraphQuery query() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Internal function to create a new element identifier
	 * 
	 * @return a new identifier number
	 */
	public synchronized Long getNewId() {
		return ++idCounter;
	}

	public String getId() {
		return rawGraph.getGraphEntity().getDocumentKey();
	}

}
