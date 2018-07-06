//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.DocumentField;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.EdgeEntity;
import com.arangodb.entity.EdgeUpdateEntity;
import com.arangodb.entity.VertexEntity;
import com.arangodb.entity.VertexUpdateEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.GraphCreateOptions;
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
	
	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBSimpleGraphClient.class);

	/** the ArangoDB driver. */
	private ArangoDB driver;
	
	/** the ArangoDB database. */
	private ArangoDatabase db;

	/** The batch size. */
	private int batchSize;
	
	/**
	 * Create a simple graph client and connect to the provided db. If the DB does not exist, the driver
	 * will try to create one
	 *
	 * @param properties 				the ArangoDB configuration properties
	 * @param dbname 					the ArangoDB name to connect to or create
	 * @param batchSize					the size of the batch mode chunks
	 * @throws ArangoDBGraphException 	If the db does not exist and cannot be created
	 */

	public ArangoDBSimpleGraphClient(
		Properties properties,
		String dbname,
		int batchSize)
		throws ArangoDBGraphException {
		this(properties, dbname, batchSize, true);
	}
	
	/**
	 * Create a simple graph client and connect to the provided db. The create flag controls what is the
	 * behaviour if the db is not found
	 *
	 * @param properties 				the ArangoDB configuration properties
	 * @param dbname 					the ArangoDB name to connect to or create
	 * @param batchSize					the size of the batch mode chunks
	 * @param create					if true, the driver will attempt to crate the DB if it does not exist
	 * @throws ArangoDBGraphException 	If the db does not exist and cannot be created
	 */
	
	public ArangoDBSimpleGraphClient(
		Properties properties, 
		String dbname, 
		int batchSize,
		boolean create) 
		throws ArangoDBGraphException {	
		logger.debug("Initiating the ArangoDb Client");
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			properties.store(os, null);
			InputStream targetStream = new ByteArrayInputStream(os.toByteArray());
			driver = new ArangoDB.Builder().loadProperties(targetStream)
					.registerModule(new ArangoDBGraphModule()).build();
		} catch (IOException e) {
			throw new ArangoDBGraphException("Unable to read properties", e);
		}
		db = driver.db(dbname);
		if (create) {
			if (!db.exists()) {
				logger.info("DB not found, attemtping to create it.");
				try {
					if (!driver.createDatabase(dbname)) {
						throw new ArangoDBGraphException("Unable to crate the database " + dbname);
					}
				}
				catch (ArangoDBException ex) {
					throw new ArangoDBGraphException("Unable to crate the database " + dbname, ex);
				}
			}
		}
		else {
			if (!db.exists()) {
				logger.error("Database does not exist, or the user has no access");
				throw new ArangoDBGraphException(String.format("DB not found or user has no access: {}@{}",
						properties.getProperty("arangodb.user"), dbname));
			}
		}
		this.batchSize = batchSize;
	}

	/**
	 * Shutdown the client and free resources.
	 */
	
	public void shutdown() {
		logger.debug("Shutdown");
		if (db != null) {
			if (db.exists()) {
				db.clearQueryCache();
			}
		}
		if (driver != null) driver.shutdown();
		db = null;
		driver = null;
	}
	
	/**
	 * Drop the graph and its related collections.
	 *
	 * @param graph 					the graph to clear
	 * @throws ArangoDBGraphException	if there was an error dropping the graph and its collections
	 */
	
	public void clear(ArangoDBGraph graph) throws ArangoDBGraphException {
		logger.info("Clear {}", graph.name());
		deleteGraph(graph.name());
	}

	/**
	 * Request the version of ArangoDB.
	 *
	 * @return the Version number
	 * @throws ArangoDBGraphException if user has no access to the db
	 */
	
	public String getVersion() throws ArangoDBGraphException {
		try {
			return db.getVersion().getVersion();
		} catch (ArangoDBException e) {
			throw new ArangoDBGraphException(e);
		}
	}
	
	
	/**
	 * Batch size.
	 *
	 * @return the batchsize
	 */
	
	public Integer batchSize() {
		return batchSize;
	}
	
	/**
	 * Gets the driver.
	 *
	 * @return the driver
	 */
	
	public ArangoDB getDriver() {
		return driver;
	}
	
	/**
	 * Gets the database.
	 *
	 * @return the ArangoDB
	 */
	
	public ArangoDatabase getDB() {
		return db;
	}

	/**
	 * Test if the db exists.
	 *
	 * @return true if the db exists
	 */
	
	public boolean dbExists() {
		return db == null ? false: db.exists();
	}
	
	/**
	 * Delete the current database accessed by the driver.
	 *
	 * @throws ArangoDBGraphException if there was an error
	 */
	
	public void deleteDb() throws ArangoDBGraphException {
		logger.info("Delete current db");
		if (db !=null) {
			try {
				db.drop();
			} catch (ArangoDBException e) {
				throw new ArangoDBGraphException("Error deleting the database", e);
			}
		}
	}
	
	/**
	 * Get a vertex from the database.
	 *
	 * @param graph            			the graph
	 * @param id            			the id (key) of the vertex
	 * @param collection 				the collection from which the vertex is retrieved
	 * @return the vertex
	 * @throws ArangoDBGraphException	if the retrieval failed
	 */
	
	public ArangoDBVertex<?> getVertex(
		ArangoDBGraph graph,
		String id,
		String collection)
		throws ArangoDBGraphException {
		logger.debug("Get vertex {} from {}:{}", id, graph.name(), collection);
		ArangoDBVertex<?> result = null;
		try {
			result = db.graph(graph.name()).vertexCollection(collection).getVertex(id, ArangoDBVertex.class);
		} catch (ArangoDBException e) {
			logger.error("Failed to retrieve vertex: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to retrieve vertex.", e);
		}
		result.collection(collection);
		result.graph(graph);
		return result;
	}
	
	/**
	 * Insert a ArangoDBVertex in the graph. The vertex is updated with the id, rev and key (if not
	 * present) 
	 *
	 * @param graph            			the graph
	 * @param vertex 					the vertex
	 * @throws ArangoDBGraphException 	if the insertion failed
	 */
	
	public void insertVertex(
		final ArangoDBGraph graph,
		ArangoDBVertex<?> vertex)
		throws ArangoDBGraphException {
		logger.debug("Insert vertex {} in {}", vertex, graph.name());
		VertexEntity vertexEntity;
		try {
			vertexEntity = db.graph(graph.name())
					.vertexCollection(ArangoDBUtil.getCollectioName(graph.name(), vertex.collection()))
					.insertVertex(vertex);
		} catch (ArangoDBException e) {
			// Think is better to let this bubble to the graph.
			String message = e.getMessage();
			Matcher m = ArangoDBGraph.Exceptions.ERROR_CODE.matcher(message);
			if (m.matches()) {
				String errorCode = m.group(1);
				if (errorCode.equals("1210")) {		// 1210 - ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED
					throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
				}
			}
			logger.error("Failed to insert vertex: {}", message);
			throw new ArangoDBGraphException("Failed to insert vertex.", e);
		}
		vertex._id(vertexEntity.getId());
		vertex._rev(vertexEntity.getRev());
		if (vertex._key() == null) {
			vertex._key(vertexEntity.getKey());
		}
		vertex.setPaired(true);
	}

	/**
	 * Delete a vertex from the graph.
	 *
	 * @param graph             		the graph
	 * @param vertex            		the vertex to delete
	 * @throws ArangoDBGraphException	if the deletion failed
	 */
	
	public void deleteVertex(
		ArangoDBGraph graph,
		ArangoDBVertex<?> vertex)
		throws ArangoDBGraphException {
		logger.debug("Delete vertex {} in {}", vertex, graph.name());
		try {
			db.graph(graph.name())
			.vertexCollection(ArangoDBUtil.getCollectioName(graph.name(), vertex.collection()))
			.deleteVertex(vertex._key());
		} catch (ArangoDBException e) {
			logger.error("Failed to delete vertex: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to delete vertex.", e);
		}
		vertex.setPaired(false);
	}
	
	/**
	 * Update the vertex in the graph.
	 *
	 * @param graph 					the graph
	 * @param vertex 					the vertex
	 * @throws ArangoDBGraphException	if the update failed
	 */
	
	public void updateVertex(
		ArangoDBGraph graph,
		ArangoDBVertex<?> vertex)
		throws ArangoDBGraphException {
		logger.debug("Update vertex {} in {}", vertex, graph.name());
		VertexUpdateEntity vertexEntity;
		try {
			vertexEntity = db.graph(graph.name())
					.vertexCollection(ArangoDBUtil.getCollectioName(graph.name(), vertex.collection()))
					.updateVertex(vertex._key(), vertex);
		} catch (ArangoDBException e) {
			logger.error("Failed to update vertex: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to update vertex.", e);
		}
		logger.info("Vertex updated, new rev {}", vertexEntity.getRev());
		vertex._rev(vertexEntity.getRev());
	}
	
	/**
	 * Get an edge from the graph.
	 *
	 * @param graph            			the graph
	 * @param id            			the id (key) of the edge
	 * @param collection 				the collection from which the edge is retrieved
	 * @return the edge
	 * @throws ArangoDBGraphException 	if the retrieval failed
	 */
	
	public ArangoDBEdge<?> getEdge(
		ArangoDBGraph graph,
		String id,
		String collection)
		throws ArangoDBGraphException {
		logger.debug("Get edge {} from {}:{}", id, graph.name(), collection);
		ArangoDBEdge<?> result = null;
		try {
			result = db.graph(graph.name())
					.edgeCollection(ArangoDBUtil.getCollectioName(graph.name(), collection))
					.getEdge(id, ArangoDBEdge.class);
		} catch (ArangoDBException e) {
			logger.error("Failed to retrieve edge: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to retrieve edge.", e);
		}
		result.collection(collection);
		result.graph(graph);
		return result;
	}

	/**
	 * Insert an edge in the graph .The edge is updated with the id, rev and key (if not
	 * present) 
	 *
	 * @param graph            			the graph
	 * @param edge            			the edge
	 * @throws ArangoDBGraphException  	if the insertion failed
	 */

	public void insertEdge(
		ArangoDBGraph graph,
		ArangoDBEdge<?> edge)
		throws ArangoDBGraphException {
		logger.debug("Insert edge {} in {}", edge, graph.name());
		EdgeEntity edgeEntity;
		try {
			edgeEntity = db.graph(graph.name())
					.edgeCollection(ArangoDBUtil.getCollectioName(graph.name(), edge.collection()))
					.insertEdge(edge);
		} catch (ArangoDBException e) {
			logger.error("Failed to insert edge: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to insert edge.", e);
		}
		edge._id(edgeEntity.getId());
		edge._rev(edgeEntity.getRev());
		if (edge._key() == null) {
			edge._key(edgeEntity.getKey());
		}
		edge.setPaired(true);
	}

	/**
	 * Delete an edge from the graph.
	 *
	 * @param graph           			the graph
	 * @param edge            			the edge
	 * @throws ArangoDBGraphException   if the deletion failed
	 */

	public void deleteEdge(
		ArangoDBGraph graph,
		ArangoDBEdge<?> edge)
		throws ArangoDBGraphException {
		logger.debug("Delete edge {} in {}", edge, graph.name());
		try {
			db.graph(graph.name())
			.edgeCollection(ArangoDBUtil.getCollectioName(graph.name(), edge.collection()))
			.deleteEdge(edge._key());
		} catch (ArangoDBException e) {
			logger.error("Failed to delete vertex: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to delete vertex.", e);
		}
		edge.setPaired(false);
	}
	
	/**
	 * Update the edge in the graph.
	 *
	 * @param graph 					the graph
	 * @param edge 						the edge
	 * @throws ArangoDBGraphException 	if the update failed
	 */
	
	public void updateEdge(
		ArangoDBGraph graph,
		ArangoDBEdge<?> edge)
		throws ArangoDBGraphException {
		logger.debug("Update edge {} in {}", edge, graph.name());
		EdgeUpdateEntity edgeEntity;
		try {
			edgeEntity = db.graph(graph.name())
					.edgeCollection(ArangoDBUtil.getCollectioName(graph.name(), edge.collection()))
					.updateEdge(edge._key(), edge);
		} catch (ArangoDBException e) {
			logger.error("Failed to update vertex: {}", e.getErrorMessage());
			throw new ArangoDBGraphException("Failed to delete vertex.", e);
		}
		logger.info("Edge updated, new rev {}", edgeEntity.getRev());
		edge._rev(edgeEntity.getRev());
	}
	
	/**
	 * Create a query to get edges of a vertex. 
	 * 
	 * Retrieve all Edges from the edgeLabels collections and keep those that match the vertex
	 * as source, target or both, depending on the direction 
	 *
	 * @param graph          			the graph
	 * @param vertex            		the vertex
	 * @param edgeLabels        		a list of edge labels to follow, empty if all type of edges
	 * @param direction         		the direction of the edges
	 * @return ArangoDBBaseQuery		the query object
	 */

	public ArangoDBQuery getVertexEdges(
		ArangoDBGraph graph,
		ArangoDBVertex<?> vertex,
		List<String> edgeLabels,
		Direction direction)
		throws ArangoDBException {
		logger.debug("Get Vertex's {}:{} Edges, in {}, from collections {}", vertex, direction, graph.name(), edgeLabels);
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
		logger.debug("Creating query");
		return new ArangoDBQuery(graph, this, QueryType.GRAPH_EDGES).setStartVertex(vertex)
				.setLabelsFilter(edgeLabels).setDirection(arangoDirection);
	}
	
	/**
	 * Create a query to get all neighbours of a vertex.
	 *
	 * @param graph            		the simple graph
	 * @param vertex            	the vertex
	 * @param labelsFilter          a list of vertex types to retrieve
	 * @param direction            	a direction
	 * @return ArangoDBBaseQuery	the query object
	 */

	public ArangoDBQuery getVertexNeighbors(
		ArangoDBGraph graph,
		ArangoDBVertex<?> vertex,
		List<String> labelsFilter,
		Direction direction) {
		logger.debug("Get Vertex's {}:{} Neighbors, in {}, from collections {}", vertex, direction, graph.name(), labelsFilter);
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
	 * @param graph            		the simple graph
	 * @param keys 					the keys (Tinkerpop ids) to filter the results
	 * @return ArangoDBBaseQuery 	the query object
	 */

	public ArangoDBQuery getGraphVertices(
		ArangoDBGraph graph,
		List<String> keys) {
		logger.debug("Get all {} graph vertices, filterd by ids: {}", graph.name(), keys);
		return new ArangoDBQuery(graph, this, QueryType.GRAPH_VERTICES)
				.setKeysFilter(keys);
	}
	
	public ArangoDBQuery getGraphEdges(
		ArangoDBGraph graph,
		List<String> ids) {
		logger.debug("Get all {} graph edges, filterd by ids: {}", graph.name(), ids);
		return new ArangoDBQuery(graph, this, QueryType.GRAPH_EDGES)
				.setKeysFilter(ids);
	}


	
	/**
	 * Execute AQL query.
	 *
	 * @param <T> 						the generic type
	 * @param query 					the query
	 * @param bindVars 					the bind vars
	 * @param aqlQueryOptions 			the aql query options
	 * @param type            			The type of the result (POJO class, VPackSlice, String for Json, or Collection/List/Map)
	 * @return the cursor result
	 * @throws ArangoDBGraphException	if executing the query raised an exception
	 */

	public <T> ArangoCursor<T> executeAqlQuery(
		String query,
		Map<String, Object> bindVars,
		AqlQueryOptions aqlQueryOptions,
		final Class<T> type)
		throws ArangoDBGraphException {
		logger.debug("Executing AQL query ({}) against db, with bind vars: {}", query, bindVars);
		try {
			return db.query(query, bindVars, aqlQueryOptions, type);
		} catch (ArangoDBException ex) {
			logger.error("Error executing query", ex);
			throw new ArangoDBGraphException("Error executing query", ex);
		}
	}
	
	/**
	 * Delete a graph from the db, and all its collections
	 * @param name the name of the graph to delete
	 * @return true, if the graph was deleted
	 */
	public boolean deleteGraph(String name) {
		return deleteGraph(name, true);
	}
	
	/**
	 * Delete a graph from the db. If dropCollection is true, then all the graph collections are also 
	 * dropped
	 *
	 * @param name 				the name
	 * @param dropCollections 	true to drop the graph collections too
	 * @return true if the graph was deleted
	 */
	
	public boolean deleteGraph(
		String name,
		boolean dropCollections) {
		if (db != null) {
			ArangoGraph graph = db.graph(name);
			if (graph.exists()) {
				Collection<String> edgeDefinitions = dropCollections ? graph.getEdgeDefinitions() : Collections.emptyList();
				Collection<String> vertexCollections = dropCollections ? graph.getVertexCollections(): Collections.emptyList();;
				// Drop graph first because it will break if the graph collections do not exist
				graph.drop();
				for (String definitionName : edgeDefinitions) {
					String collectioName = definitionName;
					if (db.collection(collectioName).exists()) {
						db.collection(collectioName).drop();
					}
				}
				for (String vc : vertexCollections) {
					String collectioName = vc;
					if (db.collection(collectioName).exists()) {
						db.collection(collectioName).drop();
					}
				}
				return true;
			} else {
				try {
					graph.drop();
				} catch (ArangoDBException ex) {
					
				}
			}
		}
		return false;
	}

	/**
	 * Delete collection.
	 *
	 * @param name the name
	 * @return true, if successful
	 */
	
	public boolean deleteCollection(String name) {
		ArangoCollection collection = db.collection(name);
		if (collection.exists()) {
			collection.drop();
			return collection.exists();
		}
		return false;
	}
	
	/**
	 * Create a new graph.
	 *
	 * @param name            			the name of the new graph
	 * @param verticesCollectionNames the vertices collection names
	 * @param edgesCollectionNames the edges collection names
	 * @param relations 				the relations, if any
	 * @throws ArangoDBGraphException 	If the graph can not be created
	 */
	
	public void createGraph(String name,
		List<String> verticesCollectionNames,
		List<String> edgesCollectionNames,
		List<String> relations)
		throws ArangoDBGraphException {
		this.createGraph(name, verticesCollectionNames, edgesCollectionNames, relations, null);
	}
	
	public void createGraph(String name,
		List<String> verticesCollectionNames,
		List<String> edgesCollectionNames,
		List<String> relations,
		GraphCreateOptions options)
		throws ArangoDBGraphException {
		logger.info("Creating graph {}", name);
		final Collection<EdgeDefinition> edgeDefinitions;
		if (relations.isEmpty()) {
			logger.info("No relations, creating default one.");
			edgeDefinitions = ArangoDBUtil.createDefaultEdgeDefinitions(name, verticesCollectionNames, edgesCollectionNames);
		} else {
			edgeDefinitions = new ArrayList<EdgeDefinition>();
			for (String value : relations) {
				EdgeDefinition ed = ArangoDBUtil.relationPropertyToEdgeDefinition(name, value);
				edgeDefinitions.add(ed);
			}
		}
		try {
			logger.info("Creating graph in database.");
			db.createGraph(name, edgeDefinitions, options);
		} catch (ArangoDBException e) {
			throw new ArangoDBGraphException("Error creating graph.", e);
		}
		
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

	@SuppressWarnings("unchecked")
	public void updateDocument(String collectionName, String key, Object value) {
		JSONObject obj = new JSONObject();
		obj.put("_key", key);
		obj.put("value", value);
		db.collection(collectionName).updateDocument(key, obj);
		
	}

	public void deleteDocument(String collectionName, String key) {
		db.collection(collectionName).deleteDocument(key);
		
	}

	@SuppressWarnings("unchecked")
	public void createDocument(String collectionName, String key, Object value) {
		JSONObject obj = new JSONObject();
		obj.put("_key", key);
		obj.put("value", value);
		db.collection(collectionName).insertDocument(value);
	}
	
	/**
	 * ********   DIRTY *************.
	 *
	 */

	
//
//	/**
//	 * Create a new edge.
//	 *
//	 * @param graph            the simple graph
//	 * @param id            the id (key) of the new edge
//	 * @param label            the label of the new edge
//	 * @param from            the start vertex
//	 * @param to            the end vertex
//	 * @param properties            the predefined properties of the edge
//	 * @return the edge
//	 * @throws ArangoDBException             if creation failed
//	 */
//	public ArangoDBSimpleEdge createEdge(
//		ArangoDBSimpleGraph graph,
//		String id,
//		String label,
//		ArangoDBSimpleVertex from,
//		ArangoDBSimpleVertex to,
//		Map<String, Object> properties) throws ArangoDBException {
//
//		Map<String, Object> tmpProperties = properties;
//		if (tmpProperties == null) {
//			tmpProperties = new HashMap<String, Object>();
//		}
//
//		if (id != null) {
//			tmpProperties.put(ArangoDBSimpleEdge._KEY, id);
//		} else if (tmpProperties.containsKey(ArangoDBSimpleEdge._KEY)) {
//			tmpProperties.remove(ArangoDBSimpleEdge._KEY);
//		}
//		if (label != null) {
//			tmpProperties.put(StringFactory.LABEL, label);
//		} else if (tmpProperties.containsKey(StringFactory.LABEL)) {
//			tmpProperties.remove(StringFactory.LABEL);
//		}
//
//		tmpProperties.put(ArangoDBSimpleEdge._FROM, from.getDocumentId());
//		tmpProperties.put(ArangoDBSimpleEdge._TO, to.getDocumentId());
//
//		EdgeEntity<Map<String, Object>> edgeEntity;
//		try {
//			edgeEntity = driver.graphCreateEdge(graph.getName(), graph.getEdgeCollection(), id, from.getDocumentId(),
//				to.getDocumentId(), tmpProperties, false);
//		} catch (ArangoException e) {
//			throw new ArangoDBException(e);
//		}
//
//		tmpProperties.put(ArangoDBSimpleEdge._ID, edgeEntity.getDocumentHandle());
//		tmpProperties.put(ArangoDBSimpleEdge._KEY, edgeEntity._key());
//		Long l = edgeEntity.getDocumentRevision();
//		tmpProperties.put(ArangoDBSimpleVertex._REV, l.toString());
//
//		return new ArangoDBSimpleEdge(tmpProperties);
//	}
//
//	
//	/**
//	 * Creates vertices (bulk import).
//	 *
//	 * @param graph            The graph
//	 * @param vertices            The list of new vertices
//	 * @param details            True, for details
//	 * @return a ImportResultEntity object
//	 * @throws ArangoDBException             if an error occurs
//	 */
//	public ImportResultEntity createVertices(
//		ArangoDBSimpleGraph graph,
//		List<ArangoDBSimpleVertex> vertices,
//		boolean details) throws ArangoDBException {
//
//		List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
//
//		for (ArangoDBSimpleVertex v : vertices) {
//			values.add(v.getProperties());
//		}
//
//		try {
//			return driver.importDocuments(graph.getVertexCollection(), true, values);
//		} catch (ArangoException e) {
//			throw new ArangoDBException(e);
//		}
//	}
//
//	/**
//	 * Creates edges (bulk import).
//	 *
//	 * @param graph            The graph
//	 * @param edges            The list of new edges
//	 * @param details            True, for details
//	 * @return a ImportResultEntity object
//	 * @throws ArangoDBException             if an error occurs
//	 */
//	public ImportResultEntity createEdges(ArangoDBSimpleGraph graph, List<ArangoDBSimpleEdge> edges, boolean details)
//			throws ArangoDBException {
//
//		List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
//
//		for (ArangoDBSimpleEdge e : edges) {
//			values.add(e.getProperties());
//		}
//
//		try {
//			return driver.importDocuments(graph.getEdgeCollection(), true, values);
//		} catch (ArangoException e) {
//			throw new ArangoDBException(e);
//		}
//	}
//
//	
//
//	/**
//	 * Create a query to get all edges of a graph.
//	 *
//	 * @param graph            the simple graph
//	 * @param propertyFilter            a property filter
//	 * @param labelsFilter            a labels filter
//	 * @param limit            maximum number of results
//	 * @param count            query total number of results
//	 * @return ArangoDBBaseQuery the query object
//	 * @throws ArangoDBException             if creation failed
//	 */
//
//	public ArangoDBQuery getGraphEdges(
//		ArangoDBSimpleGraph graph,
//		ArangoDBPropertyFilter propertyFilter,
//		List<String> labelsFilter,
//		Long limit,
//		boolean count) throws ArangoDBException {
//
//		return new ArangoDBQuery(graph, this, QueryType.GRAPH_EDGES).setCount(count).setLimit(limit)
//				.setLabelsFilter(labelsFilter).setPropertyFilter(propertyFilter);
//	}
//
//	
//
//	
//	
//	/**
//	 * Create an index on collection keys.
//	 *
//	 * @param graph            the simple graph
//	 * @param type            the index type ("cap", "geo", "hash", "skiplist")
//	 * @param unique            true for a unique key
//	 * @param fields            a list of key fields
//	 * @return ArangoDBIndex the index
//	 * @throws ArangoDBException             if creation failed
//	 */
//
//	public ArangoDBIndex createVertexIndex(
//		ArangoDBSimpleGraph graph,
//		IndexType type,
//		boolean unique,
//		List<String> fields) throws ArangoDBException {
//		return createIndex(graph.getVertexCollection(), type, unique, fields);
//	}
//
//	/**
//	 * Create an index on collection keys.
//	 *
//	 * @param graph            the simple graph
//	 * @param type            the index type ("cap", "geo", "hash", "skiplist")
//	 * @param unique            true for a unique key
//	 * @param fields            a list of key fields
//	 * @return ArangoDBIndex the index
//	 * @throws ArangoDBException             if creation failed
//	 */
//
//	public ArangoDBIndex createEdgeIndex(ArangoDBSimpleGraph graph, IndexType type, boolean unique, List<String> fields)
//			throws ArangoDBException {
//		return createIndex(graph.getEdgeCollection(), type, unique, fields);
//	}
//
//	/**
//	 * Get an index.
//	 *
//	 * @param id            id of the index
//	 * @return ArangoDBIndex the index, or null if the index was not found
//	 * @throws ArangoDBException             if creation failed
//	 */
//
//	public ArangoDBIndex getIndex(String id) throws ArangoDBException {
//		IndexEntity index;
//		try {
//			index = driver.getIndex(id);
//		} catch (ArangoException e) {
//
//			if (e.getErrorNumber() == ErrorNums.ERROR_ARANGO_INDEX_NOT_FOUND) {
//				return null;
//			}
//
//			throw new ArangoDBException(e);
//		}
//		return new ArangoDBIndex(index);
//	}
//
//	/**
//	 * Returns the indices of the vertex collection.
//	 *
//	 * @param graph            The graph
//	 * @return List of indices
//	 * @throws ArangoDBException             if an error occurs
//	 */
//	public List<ArangoDBIndex> getVertexIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
//		return getIndices(graph.getVertexCollection());
//	}
//
//	/**
//	 * Returns the indices of the edge collection.
//	 *
//	 * @param graph            The graph
//	 * @return List of indices
//	 * @throws ArangoDBException             if an error occurs
//	 */
//	public List<ArangoDBIndex> getEdgeIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
//		return getIndices(graph.getEdgeCollection());
//	}
//
//	/**
//	 * Deletes an index.
//	 *
//	 * @param id            The identifier of the index
//	 * @return true, if the index was deleted
//	 * @throws ArangoDBException             if an error occurs
//	 */
//	public boolean deleteIndex(String id) throws ArangoDBException {
//		try {
//			driver.deleteIndex(id);
//		} catch (ArangoException e) {
//			throw new ArangoDBException(e);
//		}
//
//		return true;
//	}
//
//	/**
//	 * Create an index on collection keys.
//	 *
//	 * @param collectionName            the collection name
//	 * @param type            the index type ("cap", "geo", "hash", "skiplist")
//	 * @param unique            true for a unique key
//	 * @param fields            a list of key fields
//	 * @return ArangoDBIndex the index
//	 * @throws ArangoDBException             if creation failed
//	 */
//
//	private ArangoDBIndex createIndex(String collectionName, IndexType type, boolean unique, List<String> fields)
//			throws ArangoDBException {
//
//		IndexEntity indexEntity;
//		try {
//			indexEntity = driver.createIndex(collectionName, type, unique, fields.toArray(new String[0]));
//		} catch (ArangoException e) {
//			throw new ArangoDBException(e);
//		}
//
//		return new ArangoDBIndex(indexEntity);
//	}
//
//	/**
//	 * Get the List of indices of a collection.
//	 *
//	 * @param collectionName            the collection name
//	 * @return Vector<ArangoDBIndex> List of indices
//	 * @throws ArangoDBException             if creation failed
//	 */
//
//	private List<ArangoDBIndex> getIndices(String collectionName) throws ArangoDBException {
//		List<ArangoDBIndex> indices = new ArrayList<ArangoDBIndex>();
//
//		IndexesEntity indexes;
//		try {
//			indexes = driver.getIndexes(collectionName);
//		} catch (ArangoException e) {
//			throw new ArangoDBException(e);
//		}
//
//		for (IndexEntity indexEntity : indexes.getIndexes()) {
//			indices.add(new ArangoDBIndex(indexEntity));
//		}
//
//		return indices;
//	}
//
//	/**
//	 * Returns the current connection configuration.
//	 *
//	 * @param collectionName the collection name
//	 * @return the configuration
//	 * @throws ArangoDBException the arango DB exception
//	 */
////	public ArangoDBConfiguration getConfiguration() {
////		return configuration;
////	}
//
//	/**
//	 * Truncates a collection
//	 * 
//	 * @param collectionName
//	 */
//	public void truncateCollection(String collectionName) throws ArangoDBException {
//		try {
//			driver.truncateCollection(collectionName);
//		} catch (ArangoException e) {
//			throw new ArangoDBException(e);
//		}
//	}

}