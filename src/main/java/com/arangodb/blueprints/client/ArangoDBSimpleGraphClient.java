//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.CursorResult;
import com.arangodb.ErrorNums;
import com.arangodb.blueprints.client.ArangoDBBaseQuery.Direction;
import com.arangodb.blueprints.client.ArangoDBBaseQuery.QueryType;
import com.arangodb.entity.DeletedEntity;
import com.arangodb.entity.EdgeDefinitionEntity;
import com.arangodb.entity.EdgeEntity;
import com.arangodb.entity.GraphEntity;
import com.arangodb.entity.ImportResultEntity;
import com.arangodb.entity.IndexEntity;
import com.arangodb.entity.IndexType;
import com.arangodb.entity.IndexesEntity;
import com.arangodb.entity.marker.VertexEntity;
import com.arangodb.util.AqlQueryOptions;

/**
 * The arangodb graph client class (handles the HTTP connection to arangodb)
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBSimpleGraphClient {

	/**
	 * the configuration (contains connection parameters etc.)
	 */

	private ArangoDBConfiguration configuration;

	/**
	 * the ArangoDB driver (call shutdown!)
	 */

	private ArangoDriver driver;

	/**
	 * Create a simple graph client
	 * 
	 * @param configuration
	 *            the ArangoDB configuration
	 */

	public ArangoDBSimpleGraphClient(ArangoDBConfiguration configuration) {
		this.configuration = configuration;
		configuration.init();

		driver = new ArangoDriver(configuration);
	}

	/**
	 * Shutdown the client and free resources
	 */

	public void shutdown() {
		// TODO driver.shutdown();
	}

	/**
	 * Request the version of ArangoDB
	 * 
	 * @return the Version number
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */

	public String getVersion() throws ArangoDBException {
		try {
			return driver.getVersion().getVersion();
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}
	}

	/**
	 * Create a new graph
	 * 
	 * @param name
	 *            the name of the new graph
	 * @param vertexCollectionName
	 *            the name of the vertex collection
	 * @param edgeCollectionName
	 *            the name of the edge collection
	 * 
	 * @return the graph
	 * 
	 * @throws ArangoException
	 *             if the graph could not be created
	 */
	public ArangoDBSimpleGraph createGraph(String name, String vertexCollectionName, String edgeCollectionName)
			throws ArangoException {

		EdgeDefinitionEntity ed = new EdgeDefinitionEntity();
		ed.setCollection(edgeCollectionName);
		ed.getFrom().add(vertexCollectionName);
		ed.getTo().add(vertexCollectionName);
		List<EdgeDefinitionEntity> edgeDefinitions = new ArrayList<EdgeDefinitionEntity>();
		edgeDefinitions.add(ed);

		GraphEntity graphEntity = driver.createGraph(name, edgeDefinitions, null, true);
		return new ArangoDBSimpleGraph(graphEntity, vertexCollectionName, edgeCollectionName);
	}

	/**
	 * Get a graph by name
	 * 
	 * @param name
	 *            the name of the new graph
	 * 
	 * @return the graph or null if the graph was not found
	 * 
	 * @throws ArangoException
	 *             if the graph could not be created
	 */

	public GraphEntity getGraph(String name) throws ArangoException {
		try {
			return driver.getGraph(name);
		} catch (ArangoException e) {
			if (e.getErrorNumber() == ErrorNums.ERROR_GRAPH_NOT_FOUND) {
				return null;
			}
			throw e;
		}
	}

	/**
	 * Delete a graph by name
	 * 
	 * @param graph
	 *            the graph
	 * 
	 * @return true if the graph was deleted
	 * @throws ArangoException
	 *             if the graph could be deleted
	 */

	public boolean deleteGraph(GraphEntity graph) throws ArangoException {
		DeletedEntity deletedEntity = driver.deleteGraph(graph.getName());
		return deletedEntity.getDeleted();
	}

	/**
	 * Creates a ArangoDBSimpleVertex object
	 * 
	 * @param graph
	 *            the simple graph of the query
	 * @param id
	 *            the id (key) of the vertex
	 * @param properties
	 *            the vertex properties
	 * @return ArangoDBSimpleVertex the created object
	 * @throws ArangoDBException
	 */
	public ArangoDBSimpleVertex createVertex(ArangoDBSimpleGraph graph, String id, Map<String, Object> properties)
			throws ArangoDBException {
		if (properties == null) {
			properties = new HashMap<String, Object>();
		}

		if (id != null) {
			properties.put(ArangoDBSimpleVertex._KEY, id);
		} else if (properties.containsKey(ArangoDBSimpleVertex._KEY)) {
			properties.remove(ArangoDBSimpleVertex._KEY);
		}

		try {
			VertexEntity<Map<String, Object>> vertexEntity = driver.graphCreateVertex(graph.getName(),
				graph.getVertexCollection(), properties, false);
			properties.put(ArangoDBSimpleVertex._KEY, vertexEntity.getDocumentKey());
			properties.put(ArangoDBSimpleVertex._ID, vertexEntity.getDocumentHandle());
			properties.put(ArangoDBSimpleVertex._REV, vertexEntity.getDocumentRevision());
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		return new ArangoDBSimpleVertex(properties);
	}

	/**
	 * Get a vertex
	 * 
	 * @param graph
	 *            the simple graph of the new vertex
	 * @param id
	 *            the id (key) of the vertex
	 * 
	 * @return the vertex
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */
	public ArangoDBSimpleVertex getVertex(ArangoDBSimpleGraph graph, String id) throws ArangoDBException {

		@SuppressWarnings("rawtypes")
		VertexEntity<Map> vertexEntity;
		try {
			vertexEntity = driver.graphGetVertex(graph.getName(), graph.getVertexCollection(), id, Map.class);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> entity = vertexEntity.getEntity();

		return new ArangoDBSimpleVertex(entity);
	}

	/**
	 * Save a vertex
	 * 
	 * @param graph
	 *            the simple graph of the vertex
	 * @param vertex
	 *            the vertex to save
	 * 
	 * @return the vertex
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleVertex saveVertex(ArangoDBSimpleGraph graph, ArangoDBSimpleVertex vertex)
			throws ArangoDBException {

		VertexEntity<Map<String, Object>> vertexEntity;
		try {
			vertexEntity = driver.graphReplaceVertex(graph.getName(), graph.getVertexCollection(),
				vertex.getDocumentKey(), vertex.getProperties());
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		vertex.getProperties().put(ArangoDBSimpleVertex._ID, vertexEntity.getDocumentHandle());
		vertex.getProperties().put(ArangoDBSimpleVertex._KEY, vertexEntity.getDocumentKey());
		vertex.getProperties().put(ArangoDBSimpleVertex._REV, vertexEntity.getDocumentRevision());
		return vertex;
	}

	/**
	 * Delete a vertex by name
	 * 
	 * @param graph
	 *            the simple graph of the vertex
	 * @param vertex
	 *            the vertex to save
	 * 
	 * @return true if the vertex was deleted
	 * 
	 * @throws ArangoDBException
	 *             if deletion failed
	 */

	public boolean deleteVertex(ArangoDBSimpleGraph graph, ArangoDBSimpleVertex vertex) throws ArangoDBException {
		DeletedEntity graphDeleteVertex;
		try {
			graphDeleteVertex = driver.graphDeleteVertex(graph.getName(), graph.getVertexCollection(),
				vertex.getDocumentKey());
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		Boolean deleted = graphDeleteVertex.getDeleted();

		if (deleted) {
			vertex.setDeleted();
		}

		return deleted;
	}

	/**
	 * Create a new edge
	 * 
	 * @param graph
	 *            the simple graph
	 * @param id
	 *            the id (key) of the new edge
	 * @param label
	 *            the label of the new edge
	 * @param from
	 *            the start vertex
	 * @param to
	 *            the end vertex
	 * @param properties
	 *            the predefined properties of the edge
	 * 
	 * @return the edge
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */
	public ArangoDBSimpleEdge createEdge(
		ArangoDBSimpleGraph graph,
		String id,
		String label,
		ArangoDBSimpleVertex from,
		ArangoDBSimpleVertex to,
		Map<String, Object> properties) throws ArangoDBException {
		if (properties == null) {
			properties = new HashMap<String, Object>();
		}

		if (id != null) {
			properties.put(ArangoDBSimpleEdge._KEY, id);
		} else if (properties.containsKey(ArangoDBSimpleEdge._KEY)) {
			properties.remove(ArangoDBSimpleEdge._KEY);
		}
		if (label != null) {
			properties.put(ArangoDBSimpleEdge._LABEL, label);
		} else if (properties.containsKey(ArangoDBSimpleEdge._LABEL)) {
			properties.remove(ArangoDBSimpleEdge._LABEL);
		}

		properties.put(ArangoDBSimpleEdge._FROM, from.getDocumentId());
		properties.put(ArangoDBSimpleEdge._TO, to.getDocumentId());

		EdgeEntity<Map<String, Object>> edgeEntity;
		try {
			edgeEntity = driver.graphCreateEdge(graph.getName(), graph.getEdgeCollection(), id, from.getDocumentId(),
				to.getDocumentId(), properties, false);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		properties.put(ArangoDBSimpleEdge._ID, edgeEntity.getDocumentHandle());
		properties.put(ArangoDBSimpleEdge._KEY, edgeEntity.getDocumentKey());
		properties.put(ArangoDBSimpleEdge._REV, edgeEntity.getDocumentRevision());

		return new ArangoDBSimpleEdge(properties);
	}

	/**
	 * Get an edge
	 * 
	 * @param graph
	 *            the simple graph
	 * @param id
	 *            the id (key) of the edge
	 * 
	 * @return the edge
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ArangoDBSimpleEdge getEdge(ArangoDBSimpleGraph graph, String id) throws ArangoDBException {

		EdgeEntity<Map> edgeEntity;
		try {
			edgeEntity = driver.graphGetEdge(graph.getName(), graph.getEdgeCollection(), id, Map.class);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}
		Map<String, Object> properties = edgeEntity.getEntity();

		return new ArangoDBSimpleEdge(properties);
	}

	/**
	 * Save an edge
	 * 
	 * @param graph
	 *            the simple graph
	 * @param edge
	 *            the edge
	 * 
	 * @return the edge
	 * 
	 * @throws ArangoDBException
	 *             if saving failed
	 */

	public ArangoDBSimpleEdge saveEdge(ArangoDBSimpleGraph graph, ArangoDBSimpleEdge edge) throws ArangoDBException {

		EdgeEntity<Map<String, Object>> edgeEntity;
		try {
			edgeEntity = driver.graphReplaceEdge(graph.getName(), graph.getEdgeCollection(), edge.getDocumentKey(),
				edge.getProperties());
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}
		edge.getProperties().put(ArangoDBSimpleEdge._REV, edgeEntity.getDocumentRevision());

		return edge;
	}

	/**
	 * Delete an edge
	 * 
	 * @param graph
	 *            the simple graph
	 * @param edge
	 *            the edge
	 * 
	 * @return true if the edge was deleted
	 * 
	 * @throws ArangoDBException
	 *             if deletion failed
	 */

	public boolean deleteEdge(ArangoDBSimpleGraph graph, ArangoDBSimpleEdge edge) throws ArangoDBException {

		DeletedEntity deleteEntity;
		try {
			deleteEntity = driver.graphDeleteEdge(graph.getName(), graph.getEdgeCollection(), edge.getDocumentKey());
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		Boolean deleted = deleteEntity.getDeleted();

		if (deleted) {
			edge.setDeleted();
		}

		return deleted;
	}

	/**
	 * Creates vertices (bulk import)
	 * 
	 * @param graph
	 *            The graph
	 * @param vertices
	 *            The list of new vertices
	 * @param details
	 *            True, for details
	 * 
	 * @return a ImportResultEntity object
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public ImportResultEntity createVertices(
		ArangoDBSimpleGraph graph,
		List<ArangoDBSimpleVertex> vertices,
		boolean details) throws ArangoDBException {

		List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();

		for (ArangoDBSimpleVertex v : vertices) {
			values.add(v.getProperties());
		}

		try {
			return driver.importDocuments(graph.getVertexCollection(), true, values);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}
	}

	/**
	 * Creates edges (bulk import)
	 * 
	 * @param graph
	 *            The graph
	 * @param edges
	 *            The list of new edges
	 * @param details
	 *            True, for details
	 * 
	 * @return a ImportResultEntity object
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public ImportResultEntity createEdges(ArangoDBSimpleGraph graph, List<ArangoDBSimpleEdge> edges, boolean details)
			throws ArangoDBException {

		List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();

		for (ArangoDBSimpleEdge e : edges) {
			values.add(e.getProperties());
		}

		try {
			return driver.importDocuments(graph.getEdgeCollection(), true, values);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}
	}

	/**
	 * Get the next values from the cursor
	 * 
	 * @param id
	 *            The cursor id
	 * 
	 * @return JSONObject The result
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	// public JSONObject getNextCursorValues(String id) throws ArangoDBException
	// {
	// return putRequest(configuration.requestDbPrefix() + "_api/cursor/" +
	// urlEncode(id), null);
	// }

	/**
	 * Dispose the cursor
	 * 
	 * @param id
	 *            The cursor id
	 * @return true, if the cursor was deleted
	 */

	// public boolean deleteCursor(String id) {
	// try {
	// deleteRequest(configuration.requestDbPrefix() + "_api/cursor/" +
	// urlEncode(id));
	// } catch (ArangoDBException e) {
	// }
	// return true;
	// }

	/**
	 * Create a query to get all vertices of a graph
	 * 
	 * @param graph
	 *            the simple graph
	 * @param propertyFilter
	 *            a property filter
	 * @param limit
	 *            limit the number of results
	 * @param count
	 *            query total number of results
	 * 
	 * @return ArangoDBBaseQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBBaseQuery getGraphVertices(
		ArangoDBSimpleGraph graph,
		ArangoDBPropertyFilter propertyFilter,
		Long limit,
		boolean count) throws ArangoDBException {

		return new ArangoDBBaseQuery(graph, this, QueryType.GRAPH_VERTICES).setCount(count).setLimit(limit)
				.setPropertyFilter(propertyFilter);
	}

	/**
	 * Create a query to get all edges of a graph
	 * 
	 * @param graph
	 *            the simple graph
	 * @param propertyFilter
	 *            a property filter
	 * @param labelsFilter
	 *            a labels filter
	 * @param limit
	 *            maximum number of results
	 * @param count
	 *            query total number of results
	 * 
	 * @return ArangoDBBaseQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBBaseQuery getGraphEdges(
		ArangoDBSimpleGraph graph,
		ArangoDBPropertyFilter propertyFilter,
		List<String> labelsFilter,
		Long limit,
		boolean count) throws ArangoDBException {

		return new ArangoDBBaseQuery(graph, this, QueryType.GRAPH_EDGES).setCount(count).setLimit(limit)
				.setLabelsFilter(labelsFilter).setPropertyFilter(propertyFilter);
	}

	@SuppressWarnings("rawtypes")
	public CursorResult<Map> executeAqlQuery(String query, Map<String, Object> bindVars, AqlQueryOptions aqlQueryOptions)
			throws ArangoDBException {
		try {
			return driver.executeAqlQuery(query, bindVars, aqlQueryOptions, Map.class);

		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

	}

	/**
	 * Create a query to get all neighbors of a vertex
	 * 
	 * @param graph
	 *            the simple graph
	 * @param vertex
	 *            the vertex
	 * @param propertyFilter
	 *            a property filter
	 * @param labelsFilter
	 *            a list of labels
	 * @param direction
	 *            a direction
	 * @param limit
	 *            the maximum number of results
	 * @param count
	 *            query total number of results
	 * 
	 * @return ArangoDBBaseQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBBaseQuery getVertexNeighbors(
		ArangoDBSimpleGraph graph,
		ArangoDBSimpleVertex vertex,
		ArangoDBPropertyFilter propertyFilter,
		List<String> labelsFilter,
		Direction direction,
		Long limit,
		boolean count) throws ArangoDBException {

		return new ArangoDBBaseQuery(graph, this, QueryType.GRAPH_NEIGHBORS).setCount(count).setLimit(limit)
				.setDirection(direction).setStartVertex(vertex).setLabelsFilter(labelsFilter)
				.setPropertyFilter(propertyFilter);
	}

	/**
	 * Create a query to get all edges of a vertex
	 * 
	 * @param graph
	 *            the simple graph
	 * @param vertex
	 *            the vertex
	 * @param propertyFilter
	 *            a property filter
	 * @param labelsFilter
	 *            a list of labels
	 * @param direction
	 *            a direction
	 * @param limit
	 *            the maximum number of results
	 * @param count
	 *            query total number of results
	 * 
	 * @return ArangoDBBaseQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBBaseQuery getVertexEdges(
		ArangoDBSimpleGraph graph,
		ArangoDBSimpleVertex vertex,
		ArangoDBPropertyFilter propertyFilter,
		List<String> labelsFilter,
		Direction direction,
		Long limit,
		boolean count) throws ArangoDBException {

		return new ArangoDBBaseQuery(graph, this, QueryType.GRAPH_EDGES).setCount(count).setLimit(limit)
				.setStartVertex(vertex).setLabelsFilter(labelsFilter).setPropertyFilter(propertyFilter)
				.setDirection(direction);
	}

	/**
	 * Create an index on collection keys
	 * 
	 * @param graph
	 *            the simple graph
	 * @param type
	 *            the index type ("cap", "geo", "hash", "skiplist")
	 * @param unique
	 *            true for a unique key
	 * @param fields
	 *            a list of key fields
	 * 
	 * @return ArangoDBIndex the index
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBIndex createVertexIndex(
		ArangoDBSimpleGraph graph,
		IndexType type,
		boolean unique,
		Vector<String> fields) throws ArangoDBException {
		return createIndex(graph.getVertexCollection(), type, unique, fields);
	}

	/**
	 * Create an index on collection keys
	 * 
	 * @param graph
	 *            the simple graph
	 * @param type
	 *            the index type ("cap", "geo", "hash", "skiplist")
	 * @param unique
	 *            true for a unique key
	 * @param fields
	 *            a list of key fields
	 * 
	 * @return ArangoDBIndex the index
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBIndex createEdgeIndex(
		ArangoDBSimpleGraph graph,
		IndexType type,
		boolean unique,
		Vector<String> fields) throws ArangoDBException {
		return createIndex(graph.getEdgeCollection(), type, unique, fields);
	}

	/**
	 * Get an index
	 * 
	 * @param id
	 *            id of the index
	 * 
	 * @return ArangoDBIndex the index, or null if the index was not found
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBIndex getIndex(String id) throws ArangoDBException {
		IndexEntity index;
		try {
			index = driver.getIndex(id);
		} catch (ArangoException e) {

			if (e.getErrorNumber() == ErrorNums.ERROR_ARANGO_INDEX_NOT_FOUND) {
				return null;
			}

			throw new ArangoDBException(e);
		}
		return new ArangoDBIndex(index);
	}

	/**
	 * Returns the indices of the vertex collection
	 * 
	 * @param graph
	 *            The graph
	 * 
	 * @return List of indices
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public List<ArangoDBIndex> getVertexIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
		return getIndices(graph.getVertexCollection());
	}

	/**
	 * Returns the indices of the edge collection
	 * 
	 * @param graph
	 *            The graph
	 * 
	 * @return List of indices
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public List<ArangoDBIndex> getEdgeIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
		return getIndices(graph.getEdgeCollection());
	}

	/**
	 * Deletes an index
	 * 
	 * @param id
	 *            The identifier of the index
	 * 
	 * @return true, if the index was deleted
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public boolean deleteIndex(String id) throws ArangoDBException {
		try {
			driver.deleteIndex(id);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		return true;
	}

	/**
	 * Create an index on collection keys
	 * 
	 * @param collectionName
	 *            the collection name
	 * @param type
	 *            the index type ("cap", "geo", "hash", "skiplist")
	 * @param unique
	 *            true for a unique key
	 * @param fields
	 *            a list of key fields
	 * 
	 * @return ArangoDBIndex the index
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	private ArangoDBIndex createIndex(String collectionName, IndexType type, boolean unique, List<String> fields)
			throws ArangoDBException {

		IndexEntity indexEntity;
		try {
			indexEntity = driver.createIndex(collectionName, type, unique, fields.toArray(new String[0]));
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		return new ArangoDBIndex(indexEntity);
	}

	/**
	 * Get the List of indices of a collection
	 * 
	 * @param collectionName
	 *            the collection name
	 * 
	 * @return Vector<ArangoDBIndex> List of indices
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	private Vector<ArangoDBIndex> getIndices(String collectionName) throws ArangoDBException {
		Vector<ArangoDBIndex> indices = new Vector<ArangoDBIndex>();

		IndexesEntity indexes;
		try {
			indexes = driver.getIndexes(collectionName);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		for (IndexEntity indexEntity : indexes.getIndexes()) {
			indices.add(new ArangoDBIndex(indexEntity));
		}

		return indices;
	}

	/**
	 * Returns the current connection configuration
	 * 
	 * @return the configuration
	 */
	public ArangoDBConfiguration getConfiguration() {
		return configuration;
	}

	/**
	 * Truncates a collection
	 * 
	 * @param collectionName
	 */
	public void truncateCollection(String collectionName) throws ArangoDBException {
		try {
			driver.truncateCollection(collectionName);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}
	}

	public ArangoDriver getDriver() {
		return driver;
	}

}
