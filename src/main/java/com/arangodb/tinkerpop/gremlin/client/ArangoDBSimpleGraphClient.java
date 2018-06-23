//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.DocumentField;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.EdgeEntity;
import com.arangodb.entity.GraphEntity;
import com.arangodb.entity.VertexEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBPropertyFilter.Compare;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery.QueryType;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The arangodb graph client class handles the HTTP connection to arangodb and performs database
 * operations on the ArangoDatabase.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
public class ArangoDBSimpleGraphClient {
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBSimpleGraphClient.class);

	/** the ArangoDB driver. */
	private ArangoDB driver;
	
	/** the ArangoDB database. */
	private ArangoDatabase db;

	private int batchSize;
	
	/**
	 * Create a simple graph client and connect to the provided db. If the DB does not exist
	 * one will be created
	 *
	 * @param properties the properties
	 * @param dbname the dbname
	 * @throws ArangoDBGraphException If the db does not exist and cannot be created
	 */

	public ArangoDBSimpleGraphClient(Properties properties, String dbname, int batchSize) throws ArangoDBGraphException {
		//this.configuration = configuration;
		InputStream targetStream = new ByteArrayInputStream(properties.toString().getBytes());
		driver = new ArangoDB.Builder().loadProperties(targetStream).build();
		db = driver.db(dbname);
		if (db == null) {
			if (!driver.createDatabase(dbname)) {
				throw new ArangoDBGraphException("Unable to crate the database " + dbname);
			}
		}
		this.batchSize = batchSize;
	}

	/**
	 * Shutdown the client and free resources.
	 */
	public void shutdown() {
		db.clearQueryCache();
		driver.shutdown();
	}

	/**
	 * Request the version of ArangoDB.
	 *
	 * @return the Version number
	 * @throws ArangoDBGraphException the arango DB graph exception
	 * @throws ArangoDBException             if an error occurs
	 */
	public String getVersion() throws ArangoDBGraphException {
		try {
			return driver.getVersion().getVersion();
		} catch (ArangoDBException e) {
			throw new ArangoDBGraphException(e);
		}
	}
	
	
	/**
	 * Batch size.
	 *
	 * @return the integer
	 */
	public Integer batchSize() {
		return batchSize;
	}
	
	/**
	 * Get a vertex from the database.
	 *
	 * @param graph            		the graph
	 * @param id            		the id (key) of the vertex
	 * @param collection 			the collection from which the vertex is retrieved
	 * @return the vertex
	 * @throws ArangoDBException    if retrieval failed
	 */
	public ArangoDBVertex getVertex(ArangoDBGraph graph, String id, String collection) throws ArangoDBException {

		ArangoDBVertex result = db.graph(graph.name()).vertexCollection(collection).getVertex(id, ArangoDBVertex.class);
		result.collection(collection);
		result.graph(graph);
		return result;
	}
	
	/**
	 * Insert a ArangoDBVertex in the graph. The vertex is updated with the id, rev and key (if not
	 * present) 
	 *
	 * @param graph            	the graph
	 * @param vertex 			the vertex
	 */
	public void insertVertex(final ArangoDBGraph graph, ArangoDBVertex vertex)
			throws ArangoDBException {

		VertexEntity vertexEntity = db.graph(graph.name()).vertexCollection(vertex.collection()).insertVertex(vertex);
		vertex._id(vertexEntity.getId());
		vertex._rev(vertexEntity.getRev());
		if (vertex._key() == null) {
			vertex._key(vertexEntity.getKey());
		}
	}

	/**
	 * Delete a vertex from the graph.
	 *
	 * @param graph             	the graph
	 * @param vertex            	the vertex to delete
	 * @throws ArangoDBException    if deletion failed
	 */
	public void deleteVertex(ArangoDBGraph graph, ArangoDBVertex vertex) throws ArangoDBException {
		
		db.graph(graph.name()).vertexCollection(vertex.collection()).deleteVertex(vertex._key());
	}
	
	/**
	 * Update the vertex in the graph.
	 *
	 * @param graph 				the graph
	 * @param vertex 				the vertex
	 * @throws ArangoDBException 	if update failed
	 */
	public void updateVertex(ArangoDBGraph graph, ArangoDBVertex vertex) throws ArangoDBException {
		
		db.graph(graph.name()).vertexCollection(vertex.collection()).updateVertex(vertex._key(), vertex);
	}
	
	/**
	 * Get an edge from the graph
	 *
	 * @param graph            		the graph
	 * @param id            		the id (key) of the edge
	 * @return the edge
	 * @throws ArangoDBException    if creation failed
	 */
	public ArangoDBEdge getEdge(ArangoDBGraph graph, String id, String collection) throws ArangoDBException {

		ArangoDBEdge result = db.graph(graph.name()).edgeCollection(collection).getEdge(id, ArangoDBEdge.class);
		result.collection(collection);
		result.graph(graph);
		return result;
	}

	/**
	 * Insert an edge in the graph .The edge is updated with the id, rev and key (if not
	 * present) 
	 *
	 * @param graph            		the graph
	 * @param edge            		the edge
	 * @throws ArangoDBException    if saving failed
	 */

	public void insertEdge(ArangoDBGraph graph, ArangoDBEdge edge) throws ArangoDBException {

		EdgeEntity edgeEntity = db.graph(graph.name()).edgeCollection(edge.collection()).insertEdge(edge);
		edge._id(edgeEntity.getId());
		edge._rev(edgeEntity.getRev());
		if (edge._key() == null) {
			edge._key(edgeEntity.getKey());
		}
	}

	/**
	 * Delete an edge from the graph.
	 *
	 * @param graph           		the graph
	 * @param edge            		the edge
	 * @throws ArangoDBException    if deletion failed
	 */

	public void deleteEdge(ArangoDBGraph graph, ArangoDBEdge edge) throws ArangoDBException {
		
		db.graph(graph.name()).edgeCollection(edge.collection()).deleteEdge(edge._key());
	}
	
	/**
	 * Update the edge in the graph.
	 *
	 * @param graph 				the graph
	 * @param vertex 				the vertex
	 * @throws ArangoDBException 	if update failed
	 */
	public void updateEdge(ArangoDBGraph graph, ArangoDBEdge edge) throws ArangoDBException {
		
		db.graph(graph.name()).edgeCollection(edge.collection()).updateEdge(edge._key(), edge);
	}
	
	/**
	 * Create a query to get edges of a vertex. 
	 * 
	 * Retrieve all Edges from the edgeLabels collections and keep those that match the vertex
	 * as source, target or both, depending on the direction 
	 *
	 * @param graph          		the graph
	 * @param vertex            	the vertex
	 * @param edgeLabels        	a list of edge labels to follow, empty if all type of edges
	 * @param direction         	the direction of the edges
	 * @param limit            		the maximum number of results
	 * @param count            		query total number of results
	 * @return ArangoDBBaseQuery	the query object
	 * @throws ArangoDBException    if creation failed
	 */

	public ArangoDBQuery getVertexEdges(ArangoDBGraph graph, ArangoDBVertex vertex,
		List<String> edgeLabels, Direction direction) throws ArangoDBException {
		ArangoDBPropertyFilter propertyFilter = new ArangoDBPropertyFilter();	
		ArangoDBQuery.Direction arangoDirection = null;
		switch(direction) {
		case BOTH:
			arangoDirection = ArangoDBQuery.Direction.ALL;
			propertyFilter.has(DocumentField.Type.TO.getSerializeName(), vertex._id(), Compare.EQUAL);
			propertyFilter.has(DocumentField.Type.FROM.getSerializeName(), vertex._id(), Compare.EQUAL);
			break;
		case IN:
			arangoDirection = ArangoDBQuery.Direction.IN;
			propertyFilter.has(DocumentField.Type.TO.getSerializeName(), vertex._id(), Compare.EQUAL);
			break;
		case OUT:
			arangoDirection = ArangoDBQuery.Direction.OUT;
			propertyFilter.has(DocumentField.Type.FROM.getSerializeName(), vertex._id(), Compare.EQUAL);
			break;
		}
		
		return new ArangoDBQuery(graph, this, QueryType.GRAPH_EDGES).setStartVertex(vertex)
				.setLabelsFilter(edgeLabels).setDirection(arangoDirection).setPropertyFilter(propertyFilter);
	}
	
	/**
	 * Create a query to get all neighbors of a vertex.
	 *
	 * @param graph            the simple graph
	 * @param vertex            the vertex
	 * @param propertyFilter            a property filter
	 * @param labelsFilter            a list of labels
	 * @param direction            a direction
	 * @param limit            the maximum number of results
	 * @param count            query total number of results
	 * @return ArangoDBBaseQuery the query object
	 * @throws ArangoDBException             if creation failed
	 */

	public ArangoDBQuery getVertexNeighbors(
		ArangoDBGraph graph,
		ArangoDBVertex vertex,
		List<String> labelsFilter,
		Direction direction) throws ArangoDBException {
		
		ArangoDBPropertyFilter propertyFilter = new ArangoDBPropertyFilter();	
		ArangoDBQuery.Direction arangoDirection = null;
		switch(direction) {
		case BOTH:
			arangoDirection = ArangoDBQuery.Direction.ALL;
			break;
		case IN:
			arangoDirection = ArangoDBQuery.Direction.IN;
			break;
		case OUT:
			arangoDirection = ArangoDBQuery.Direction.OUT;
			break;
		}
		return new ArangoDBQuery(graph, this, QueryType.GRAPH_NEIGHBORS)
				.setDirection(arangoDirection).setStartVertex(vertex).setLabelsFilter(labelsFilter)
				.setPropertyFilter(propertyFilter);
	}
	
	
	/**
	 * Create a query to get all vertices of a graph.
	 *
	 * @param graph            the simple graph
	 * @param propertyFilter            a property filter
	 * @param limit            limit the number of results
	 * @param count            query total number of results
	 * @return ArangoDBBaseQuery the query object
	 * @throws ArangoDBException             if creation failed
	 */

	public ArangoDBQuery getGraphVertices(
		ArangoDBGraph graph,
		List<String> ids) throws ArangoDBException {
		
		ArangoDBPropertyFilter propertyFilter = new ArangoDBPropertyFilter();
		for(String id : ids) {
			propertyFilter.has(DocumentField.Type.ID.getSerializeName(), id, Compare.EQUAL);
		}
		return new ArangoDBQuery(graph, this, QueryType.GRAPH_VERTICES)
				.setPropertyFilter(propertyFilter);
	}

	
	/**
	 * Execute aql query.
	 *
	 * @param query the query
	 * @param bindVars the bind vars
	 * @param aqlQueryOptions the aql query options
	 * @param type
	 *            The type of the result (POJO class, VPackSlice, String for Json, or Collection/List/Map)
	 * @return the cursor result
	 * @throws ArangoDBException the arango DB exception
	 */
	@SuppressWarnings("rawtypes")
	public <T> ArangoCursor<T> executeAqlQuery(String query, Map<String, Object> bindVars, AqlQueryOptions aqlQueryOptions,
			final Class<T> type)
				throws ArangoDBException {

		return db.query(query, bindVars, aqlQueryOptions, type);

	}


	
	/**
	 * ********   DIRTY *************.
	 *
	 * @param name the name
	 * @param verticesCollectionNames the vertices collection names
	 * @param edgesCollectionNames the edges collection names
	 * @param relations the relations
	 * @throws ArangoDBException the arango DB exception
	 * @throws ArangoDBGraphException the arango DB graph exception
	 */
	
	
	
	
	
	
	
	/**
	 * Create a new graph.
	 *
	 * @param name            			the name of the new graph
	 * @param vertexCollectionName      the names of the vertex collections
	 * @param edgeCollectionName        the names of the edge collections
	 * @param relations					the relations, if any
	 * @throws ArangoDBException        If there is an error creating the graph
	 * @throws ArangoDBGraphException 	if one of the relations cannot be created
	 */
	public void createGraph(String name,
		List<String> verticesCollectionNames,
		List<String> edgesCollectionNames,
		List<String> relations)
		throws ArangoDBException, ArangoDBGraphException {
		
		final Collection<EdgeDefinition> edgeDefinitions = new ArrayList<EdgeDefinition>();
		if (relations.isEmpty()) {
			EdgeDefinition ed = new EdgeDefinition()
					.collection(edgesCollectionNames.get(0))
					.from(verticesCollectionNames.get(0))
					.to(verticesCollectionNames.get(0));
			edgeDefinitions.add(ed);
		} else {
			for (String value : relations) {
				EdgeDefinition ed = ArangoDBUtil.relationPropertyToEdgeDefinition(value);
				edgeDefinitions.add(ed);
			}
		}
		db.createGraph(name, edgeDefinitions);
	}

	/**
	 * Get a graph by name.
	 *
	 * @param name            the name of the new graph
	 * @return the graph or null if the graph was not found
	 */
	public ArangoGraph getGraph(String name) {
		return db.graph(name);
	}

	/**
	 * Delete a graph by name.
	 *
	 * @param graph            the graph
	 * @return true if the graph was deleted
	 * @throws ArangoException             if the graph could be deleted
	 */

	public boolean deleteGraph(GraphEntity graph) throws ArangoException {
		DeletedEntity deletedEntity = driver.deleteGraph(graph.getName());
		return deletedEntity.getDeleted();
	}

	
	

	/**
	 * Create a new edge.
	 *
	 * @param graph            the simple graph
	 * @param id            the id (key) of the new edge
	 * @param label            the label of the new edge
	 * @param from            the start vertex
	 * @param to            the end vertex
	 * @param properties            the predefined properties of the edge
	 * @return the edge
	 * @throws ArangoDBException             if creation failed
	 */
	public ArangoDBSimpleEdge createEdge(
		ArangoDBSimpleGraph graph,
		String id,
		String label,
		ArangoDBSimpleVertex from,
		ArangoDBSimpleVertex to,
		Map<String, Object> properties) throws ArangoDBException {

		Map<String, Object> tmpProperties = properties;
		if (tmpProperties == null) {
			tmpProperties = new HashMap<String, Object>();
		}

		if (id != null) {
			tmpProperties.put(ArangoDBSimpleEdge._KEY, id);
		} else if (tmpProperties.containsKey(ArangoDBSimpleEdge._KEY)) {
			tmpProperties.remove(ArangoDBSimpleEdge._KEY);
		}
		if (label != null) {
			tmpProperties.put(StringFactory.LABEL, label);
		} else if (tmpProperties.containsKey(StringFactory.LABEL)) {
			tmpProperties.remove(StringFactory.LABEL);
		}

		tmpProperties.put(ArangoDBSimpleEdge._FROM, from.getDocumentId());
		tmpProperties.put(ArangoDBSimpleEdge._TO, to.getDocumentId());

		EdgeEntity<Map<String, Object>> edgeEntity;
		try {
			edgeEntity = driver.graphCreateEdge(graph.getName(), graph.getEdgeCollection(), id, from.getDocumentId(),
				to.getDocumentId(), tmpProperties, false);
		} catch (ArangoException e) {
			throw new ArangoDBException(e);
		}

		tmpProperties.put(ArangoDBSimpleEdge._ID, edgeEntity.getDocumentHandle());
		tmpProperties.put(ArangoDBSimpleEdge._KEY, edgeEntity._key());
		Long l = edgeEntity.getDocumentRevision();
		tmpProperties.put(ArangoDBSimpleVertex._REV, l.toString());

		return new ArangoDBSimpleEdge(tmpProperties);
	}

	
	/**
	 * Creates vertices (bulk import).
	 *
	 * @param graph            The graph
	 * @param vertices            The list of new vertices
	 * @param details            True, for details
	 * @return a ImportResultEntity object
	 * @throws ArangoDBException             if an error occurs
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
	 * Creates edges (bulk import).
	 *
	 * @param graph            The graph
	 * @param edges            The list of new edges
	 * @param details            True, for details
	 * @return a ImportResultEntity object
	 * @throws ArangoDBException             if an error occurs
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
	 * Create a query to get all edges of a graph.
	 *
	 * @param graph            the simple graph
	 * @param propertyFilter            a property filter
	 * @param labelsFilter            a labels filter
	 * @param limit            maximum number of results
	 * @param count            query total number of results
	 * @return ArangoDBBaseQuery the query object
	 * @throws ArangoDBException             if creation failed
	 */

	public ArangoDBQuery getGraphEdges(
		ArangoDBSimpleGraph graph,
		ArangoDBPropertyFilter propertyFilter,
		List<String> labelsFilter,
		Long limit,
		boolean count) throws ArangoDBException {

		return new ArangoDBQuery(graph, this, QueryType.GRAPH_EDGES).setCount(count).setLimit(limit)
				.setLabelsFilter(labelsFilter).setPropertyFilter(propertyFilter);
	}

	

	
	
	/**
	 * Create an index on collection keys.
	 *
	 * @param graph            the simple graph
	 * @param type            the index type ("cap", "geo", "hash", "skiplist")
	 * @param unique            true for a unique key
	 * @param fields            a list of key fields
	 * @return ArangoDBIndex the index
	 * @throws ArangoDBException             if creation failed
	 */

	public ArangoDBIndex createVertexIndex(
		ArangoDBSimpleGraph graph,
		IndexType type,
		boolean unique,
		List<String> fields) throws ArangoDBException {
		return createIndex(graph.getVertexCollection(), type, unique, fields);
	}

	/**
	 * Create an index on collection keys.
	 *
	 * @param graph            the simple graph
	 * @param type            the index type ("cap", "geo", "hash", "skiplist")
	 * @param unique            true for a unique key
	 * @param fields            a list of key fields
	 * @return ArangoDBIndex the index
	 * @throws ArangoDBException             if creation failed
	 */

	public ArangoDBIndex createEdgeIndex(ArangoDBSimpleGraph graph, IndexType type, boolean unique, List<String> fields)
			throws ArangoDBException {
		return createIndex(graph.getEdgeCollection(), type, unique, fields);
	}

	/**
	 * Get an index.
	 *
	 * @param id            id of the index
	 * @return ArangoDBIndex the index, or null if the index was not found
	 * @throws ArangoDBException             if creation failed
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
	 * Returns the indices of the vertex collection.
	 *
	 * @param graph            The graph
	 * @return List of indices
	 * @throws ArangoDBException             if an error occurs
	 */
	public List<ArangoDBIndex> getVertexIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
		return getIndices(graph.getVertexCollection());
	}

	/**
	 * Returns the indices of the edge collection.
	 *
	 * @param graph            The graph
	 * @return List of indices
	 * @throws ArangoDBException             if an error occurs
	 */
	public List<ArangoDBIndex> getEdgeIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
		return getIndices(graph.getEdgeCollection());
	}

	/**
	 * Deletes an index.
	 *
	 * @param id            The identifier of the index
	 * @return true, if the index was deleted
	 * @throws ArangoDBException             if an error occurs
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
	 * Create an index on collection keys.
	 *
	 * @param collectionName            the collection name
	 * @param type            the index type ("cap", "geo", "hash", "skiplist")
	 * @param unique            true for a unique key
	 * @param fields            a list of key fields
	 * @return ArangoDBIndex the index
	 * @throws ArangoDBException             if creation failed
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
	 * Get the List of indices of a collection.
	 *
	 * @param collectionName            the collection name
	 * @return Vector<ArangoDBIndex> List of indices
	 * @throws ArangoDBException             if creation failed
	 */

	private List<ArangoDBIndex> getIndices(String collectionName) throws ArangoDBException {
		List<ArangoDBIndex> indices = new ArrayList<ArangoDBIndex>();

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
	 * Returns the current connection configuration.
	 *
	 * @param collectionName the collection name
	 * @return the configuration
	 * @throws ArangoDBException the arango DB exception
	 */
//	public ArangoDBConfiguration getConfiguration() {
//		return configuration;
//	}

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

	/**
	 * Gets the driver.
	 *
	 * @return the driver
	 */
	public ArangoDriver getDriver() {
		return driver;
	}



}