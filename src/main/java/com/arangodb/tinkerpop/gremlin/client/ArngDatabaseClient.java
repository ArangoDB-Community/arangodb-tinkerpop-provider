//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.*;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.GraphCreateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The arangodb graph client class handles the HTTP connection to arangodb and performs database
 * operations on the DatabaseClient.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public class ArngDatabaseClient implements DatabaseClient {

	private static final Logger logger = LoggerFactory.getLogger(ArngDatabaseClient.class);

	private final ArangoDatabase db;

	public ArngDatabaseClient(ArangoDatabase db) {
		this.db = db;
	}

	@Override
	public void close() throws Exception {
		logger.debug("Shutdown");
		if (db != null) {
			if (db.exists()) {
				db.clearQueryCache();
			}
		}
	}

	@Override
	public String getVersion() throws ArangoDBGraphException {
		try {
			return db.getVersion().getVersion();
		} catch (ArangoDBException ex) {
			throw ArangoDBExceptions.getArangoDBException(ex);
		}
	}

	@Override
	public boolean exists() {
		if (db == null) {
			throw new ArangoDBGraphException("DatabaseClient is not connected to a DB");
		}
		return db.exists();
	}

	@Override
	public void delete() throws ArangoDBGraphException {
		logger.info("Delete current db");
		if (db != null) {
			try {
				db.drop();
			} catch (ArangoDBException e) {
				throw ArangoDBExceptions.getArangoDBException(e);
			}
		}
	}

	@Override
	public <T> ArangoCursor<T> executeAqlQuery(
		String query,
		Map<String, Object> bindVars,
		AqlQueryOptions aqlQueryOptions,
		final Class<T> type)
		throws ArangoDBGraphException {
		logger.debug("Executing AQL query ({}) against db, with bind vars: {}", query, bindVars);
		try {
			return db.query(query, bindVars, aqlQueryOptions, type);
		} catch (ArangoDBException e) {
			logger.error("Error executing query", e);
			throw ArangoDBExceptions.getArangoDBException(e);
		}
	}

	@Override
	public ArangoGraph graph(String name) {
		return db.graph(name);
	}

	@Override
	public ArangoGraph createGraph(
		String graphName,
		List<EdgeDefinition> edgeDefinitions,
		GraphCreateOptions options) throws GraphCreationException {
		logger.info("Creating graph {}", graphName);
		try {
			db.createGraph(graphName, edgeDefinitions, options);
		} catch (ArangoDBException e) {
            logger.info("Error creating graph in database.");
            throw ArangoDBExceptions.getArangoDBException(e);
        }
		final ArangoGraph g = graph(graphName);
		if (!g.exists()) {
			throw new GraphCreationException("The graph craetion failed");
		}
		return g;
	}

	//	/**
//	 * Create a new graph.
//	 *
//	 * @param name            			the name of the new graph
//	 * @param edgeDefinitions 			the edge definitions for the graph
//	 * @throws ArangoDBGraphException 	If the graph can not be created
//	 */
//
//	public void createGraph(String name, List<EdgeDefinition> edgeDefinitions)
//		throws ArangoDBGraphException {
//		this.createGraph(name, edgeDefinitions, null);
//	}
//
//	/**
//	 * Create a new graph.
//	 *
//	 * @param name 						the name of the new graph
//	 * @param edgeDefinitions			the edge definitions for the graph
//	 * @param options 					additional graph options
//	 * @return the arango graph
//	 * @throws ArangoDBGraphException 	If the graph can not be created
//	 */
//
//	public GraphClient createGraph(String name,
//        List<EdgeDefinition> edgeDefinitions,
//		GraphCreateOptions options)
//		throws ArangoDBGraphException {
//		logger.info("Creating graph {}", name);
//		try {
//			logger.info("Creating graph in database.");
//			db.createGraph(name, edgeDefinitions, options);
//		} catch (ArangoDBException e) {
//            logger.info("Error creating graph in database.", e);
//            throw ArangoDBExceptions.getArangoDBException(e);
//        }
//		GraphClient g = db.graph(name);
//		return g;
//	}


	/**
	 //	 * Translate Tinkerpop Edge directions to ArangoDB edge directions.
	 //	 * @param direction				The Tinkerpop direction
	 //	 * @return
	 //	 */
//	private ArangoDBQueryBuilder.Direction getDirection(Direction direction) {
//		ArangoDBQueryBuilder.Direction arangoDirection;
//		switch (direction) {
//			case IN:
//				arangoDirection = ArangoDBQueryBuilder.Direction.IN;
//				break;
//			case OUT:
//				arangoDirection = ArangoDBQueryBuilder.Direction.OUT;
//				break;
//			case BOTH:
//			default:
//				arangoDirection = ArangoDBQueryBuilder.Direction.ALL;
//				break;
//		}
//		return arangoDirection;
//	}

//	/**
//	 * Drop the graph and its related collections.
//	 *
//	 * @param graph 					the graph to clear
//	 * @throws ArangoDBGraphException	if there was an error dropping the graph and its collections
//	 */
//
//	@Override
//	public void clear(ArangoDBGraph graph) throws ArangoDBGraphException {
//		logger.info("Clear {}", graph.name());
//		deleteGraph(graph.name());
//	}
//
//	@Override
//	public ArngDatabaseClient load() {
//		if (driver == null) {
//			ByteArrayInputStream targetStream = null;
//			try {
//				ByteArrayOutputStream os = new ByteArrayOutputStream();
//				properties.store(os, null);
//				targetStream = new ByteArrayInputStream(os.toByteArray());
//			} catch (IOException e) {
//				// Ignore exception as the ByteArrayOutputStream is always writable.
//			}
//			ArangoDBVertexVPack vertexVpack = new ArangoDBVertexVPack();
//			ArangoDBEdgeVPack edgeVPack = new ArangoDBEdgeVPack();
//			ArangoDB driver = new ArangoDB.Builder().loadProperties(targetStream)
//					.registerDeserializer(ArangoDBVertex.class, vertexVpack)
//					.registerSerializer(ArangoDBVertex.class, vertexVpack)
//					.registerDeserializer(ArangoDBEdge.class, edgeVPack)
//					.registerSerializer(ArangoDBEdge.class, edgeVPack)
//					.build();
//			return new ArngDatabaseClient(properties, graph, driver);
//		}
//		else {
//			return this;
//		}
//	}

//	@Override
//	public ArngDatabaseClient connectTo(String dbname, boolean createDatabase) {
//		if (db != null) {
//			if (db.exists()) {
//				db.clearQueryCache();
//			}
//		}
//		DatabaseClient db = driver.db(dbname);
//		if (createDatabase) {
//			if (!db.exists()) {
//				logger.info("DB not found, attemtping to create it.");
//				try {
//					if (!driver.createDatabase(dbname)) {
//						throw new ArangoDBGraphException("Unable to crate the database " + dbname);
//					}
//				}
//				catch (ArangoDBException ex) {
//					throw ArangoDBExceptions.getArangoDBException(ex);
//				}
//			}
//		}
//		else {
//			boolean exists = false;
//			try {
//				exists = db.exists();
//			} catch (ArangoDBException ex) {
//				// Pass
//			}
//			finally {
//				if (!exists) {
//					logger.error("DatabaseClient does not exist, or the user has no access");
//					throw new ArangoDBGraphException(String.format("DB not found or user has no access: {}@{}. If you " +
//									"want to force craetion set the 'graph.db.create' flag to true in the " +
//									"configuration.",
//							properties.getProperty("arangodb.user"), dbname));
//				}
//			}
//		}
//		return new ArngDatabaseClient(properties, graph, driver, db);
//	}


	//------------------

	/** PURE DOCUMENT OPERATIONS BYPASS THE GRAPH CONTRACT; SHOULD BE AVOIDED? */
//	/**
//	 * Get a document from the database. The method is generic so we it can be used to retrieve
//	 * vertices, properties or variables.
//	 *
//	 * @param <V> 					the value type
//	 * @param id            		the id of the document (should be a valid ArangoDB _id)
//	 * @param docClass 				the returned document class
//	 * @return the document
//	 * @throws ArangoDBGraphException 	If there was an error retrieving the document
//	 */
//
//	public <V extends ArangoDBBaseDocument> V getDocument(
//		String id,
//		Class<V> docClass) {
//		logger.debug("Get document with id {}", id);
//		V result;
//		try {
//			result = db.getDocument(id, docClass);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to retrieve vertex: {}", e.getErrorMessage());
//			throw new ArangoDBGraphException("Failed to retrieve vertex.", e);
//		}
//		result.collection(result._id().split("/")[1]);
//		result.graph(graph);
//		return result;
//	}
//
//	/**
//	 * Insert a ArangoDBBaseDocument in the graph. The document is updated with the id, rev and key (if not present).
//	 * @param document 				the document
//	 * @throws ArangoDBGraphException 	If there was an error inserting the document
//	 */
//
//	public void insertDocument(ArangoDBBaseDocument document) {
//		logger.debug("Insert document {} in {}", document, graph.name());
//		if (document.isPaired()) {
//			throw new ArangoDBGraphException("Paired docuemnts can not be inserted, only updated");
//		}
//		DocumentEntity documentEntity;
//		try {
//			documentEntity = db.collection(document.collection())
//					.insertDocument(document);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to insert document: {}", e.getMessage());
//			ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
//			if (arangoDBException.getErrorCode() == 1210) {
//				throw Graph.Exceptions.vertexWithIdAlreadyExists(document._key);
//			}
//			throw arangoDBException;
//		}
//		document._id(documentEntity.getId());
//		document._rev(documentEntity.getRev());
//		if (document._key() == null) {
//			document._key(documentEntity.getKey());
//		}
//		document.setPaired(true);
//	}
//
//	/**
//	 * Delete a document from the graph.
//	 * @param document            	the document to delete
//	 * @throws ArangoDBGraphException 	If there was an error deleting the document
//	 */
//
//	public void deleteDocument(ArangoDBBaseDocument document) {
//		logger.debug("Delete document {} in {}", document, graph.name());
//		try {
//			db.collection(document.collection()).deleteDocument(document._key());
//		} catch (ArangoDBException e) {
//			logger.error("Failed to delete document: {}", e.getErrorMessage());
//            throw ArangoDBExceptions.getArangoDBException(e);
//		}
//		document.setPaired(false);
//	}
//
//	/**
//	 * Update the document in the graph.
//	 * @param document 				the document
//	 *
//	 * @throws ArangoDBGraphException 	If there was an error updating the document
//	 */
//
//	public void updateDocument(ArangoDBBaseDocument document) {
//		logger.debug("Update document {} in {}", document, graph.name());
//		DocumentUpdateEntity documentUpdateEntity;
//		try {
//			documentUpdateEntity = db.collection(document.collection())
//					.updateDocument(document._key(), document);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to update document: {}", e.getErrorMessage());
//            throw ArangoDBExceptions.getArangoDBException(e);
//		}
//		logger.info("Document updated, new rev {}", documentUpdateEntity.getRev());
//		document._rev(documentUpdateEntity.getRev());
//	}

//
//	public <E extends ArangoDBBaseDocument> E getElement(String key, String collection, Class<E> eType)  {
//		logger.debug("Get element with id {} from {}:{}", key, graph.name(), collection);
//		E result;
//		try {
//			ArangoCollection dbcol = db.collection(collection);
//			result = dbcol.getDocument(key, eType);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to retrieve element: {}", e.getErrorMessage());
//			throw new ArangoDBGraphException("Failed to retrieve element.", e);
//		}
//		result.collection(collection);
//		result.graph(graph);
//		return result;
//	}
//
//	/**
//	 * Get an ArangoDBVertex from the database.
//	 *
//	 * @param key                   the key of the document
//	 * @param collection            the collection from which the document is retrieved
//	 * @return 						the ArangoDBVertex
//	 * @throws ArangoDBGraphException 	If there was an error retrieving the document
//	 */
//
//	public ArangoDBVertex getVertex(String key, String collection) {
//		logger.debug("Get vertex with id {} from {}:{}", key, graph.name(), collection);
//		ArangoDBVertex result;
//		try {
//			result = db.graph(graph.name())
//					.vertexCollection(collection)
//					.getVertex(key, ArangoDBVertex.class);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to retrieve vertex: {}", e.getErrorMessage());
//			throw new ArangoDBGraphException("Failed to retrieve vertex.", e);
//		}
//		result.collection(collection);
//		result.graph(graph);
//		return result;
//	}
//
//	/**
//	 * Insert an ArangoDBVertex in the graph. The ArangoDBVertex is updated with the id, rev and key (if not present)
//	 * @param vertex              the ArangoDBVertex to insert
//	 * @throws ArangoDBGraphException 	If there was an error inserting the vertex
//	 */
//
//	public void insertVertex(ArangoDBVertex vertex) {
//		insertDocument(vertex);
//	}
//
//	public void updateVertex(ArangoDBVertex vertex) {
//		updateDocument(vertex);
//	}
//
//	/**
//	 * Get an edge from the graph.
//	 *
//	 * @param <V> 					the value type
//	 * @param id            		the id (name) of the edge
//	 * @param collection 			the collection from which the edge is retrieved
//	 * @param edgeClass 			the edge's specialised class
//	 * @return the edge
//	 * @throws ArangoDBGraphException 	If there was an error retrieving the edge
//	 */
//
//	public <V extends ArangoDBBaseEdge> V getEdge(
//		String id,
//		String collection,
//        Class<V> edgeClass) {
//		logger.debug("Get edge {} from {}:{}", id, graph.name(), graph.getPrefixedCollectioName(collection));
//		V result;
//		try {
//			result = db.graph(graph.name())
//					.edgeCollection(graph.getPrefixedCollectioName(collection))
//					.getEdge(id, edgeClass);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to retrieve edge: {}", e.getErrorMessage());
//            throw ArangoDBExceptions.getArangoDBException(e);
//		}
//		result.collection(collection);
//		result.graph(graph);
//		return result;
//	}
//
//	/**
//	 * Insert an edge in the graph. The edge is updated with the id, rev and name (if not
//	 * present)
//	 * @param edge            		the edge
//	 * @throws ArangoDBGraphException 	If there was an error inserting the edge
//	 */
//
//	public void insertEdge(ArangoDBBaseEdge edge) {
//		validateElementGraph(edge);
//		logger.debug("Insert edge {} in {} ", edge, graph.name());
//		try {
//			db.graph(graph.name())
//					.edgeCollection(edge.collection())
//					.insertEdge(edge);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to insert edge: {}", e.getErrorMessage());
//			throw ArangoDBExceptions.getArangoDBException(e);
//		}
//		edge.setPaired(true);
//	}
//
//	/**
//	 * Delete an edge from the graph.
//	 * @param edge            		the edge
//	 * @throws ArangoDBGraphException 	If there was an error deleting the edge
//	 */
//
//	public void deleteEdge(ArangoDBBaseEdge edge) {
//		validateElementGraph(edge);
//		logger.debug("Delete edge {} in {}", edge, graph.name());
//		try {
//			db.graph(graph.name())
//			.edgeCollection(edge.collection())
//			.deleteEdge(edge._key());
//		} catch (ArangoDBException e) {
//			logger.error("Failed to delete vertex: {}", e.getErrorMessage());
//            throw ArangoDBExceptions.getArangoDBException(e);
//		}
//		edge.setPaired(false);
//	}
//
//	/**
//	 * Update the edge in the graph.
//	 * @param edge 					the edge
//	 * @throws ArangoDBGraphException 	If there was an error updating the edge
//	 */
//
//	public void updateEdge(ArangoDBBaseEdge edge) {
//		validateElementGraph(edge);
//		logger.debug("Update edge {} in {}", edge, graph.name());
//		EdgeUpdateEntity edgeEntity;
//		try {
//			edgeEntity = db.graph(graph.name())
//					.edgeCollection(edge.collection())
//					.updateEdge(edge._key(), edge);
//		} catch (ArangoDBException e) {
//			logger.error("Failed to update vertex: {}", e.getErrorMessage());
//            throw ArangoDBExceptions.getArangoDBException(e);
//		}
//		logger.info("Edge updated, new rev {}", edgeEntity.getRev());
//		edge._rev(edgeEntity.getRev());
//	}
//
//
//
//	/**
//	 * Create a query to get all the edges of a vertex.
//	 *
//	 * @param vertex            	the vertex
//	 * @param edgeLabels        	a list of edge labels to follow, empty if all type of edges
//	 * @param direction         	the direction of the edges
//	 * @return ArangoDBBaseQuery the query object
//	 * @throws ArangoDBException if there is an error executing the query
//	 */
//
//	public ArangoCursor<ArangoDBEdge> getVertexEdges(
//		ArangoDBVertex vertex,
//		List<String> edgeLabels,
//		Direction direction)
//		throws ArangoDBException {
//		logger.debug("Get Vertex's {}:{} Edges, in {}, from collections {}", vertex, direction, graph.name(), edgeLabels);
//		Map<String, Object> bindVars = new HashMap<>();
//		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
//		ArangoDBQueryBuilder.Direction arangoDirection = getDirection(direction);
//		logger.debug("Creating query");
//		queryBuilder.iterateGraph(graph.name(), "v", Optional.of("e"),
//				Optional.empty(), Optional.empty(), Optional.empty(),
//				arangoDirection, vertex._id(), bindVars)
//			.graphOptions(Optional.of(UniqueVertices.NONE), Optional.empty(), true)
//			.filterSameCollections("e", edgeLabels, bindVars)
//			.ret("e");
//
//		String query = queryBuilder.toString();
//		return executeAqlQuery(query, bindVars, null, ArangoDBEdge.class);
//	}
//
//	/**
//	 * Get all neighbours of a document.
//	 *
//	 * @param <T> 					the document type
//	 * @param document              the document
//	 * @param edgeLabelsFilter      a list of edge types to follow
//	 * @param direction             a direction
//	 * @param propertyFilter 		filter the neighbours on the given property:value values
//	 * @param resultType 			the result type
//	 * @return ArangoDBBaseQuery	the query object
//	 */
//
//	public <T> ArangoCursor<T> getDocumentNeighbors(
//        ArangoDBBaseDocument document,
//        List<String> edgeLabelsFilter,
//        Direction direction,
//        ArangoDBPropertyFilter propertyFilter,
//        Class<T> resultType) {
//		logger.debug("Get Document's {}:{} Neighbors, in {}, from collections {}", document, direction, graph.name(), edgeLabelsFilter);
//		Map<String, Object> bindVars = new HashMap<>();
//		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
//		ArangoDBQueryBuilder.Direction arangoDirection = getDirection(direction);
//		logger.debug("Creating query");
//		queryBuilder.iterateGraph(graph.name(), "v", Optional.of("e"),
//				Optional.empty(), Optional.empty(), Optional.empty(),
//				arangoDirection, document._id(), bindVars)
//			.graphOptions(Optional.of(UniqueVertices.GLOBAL), Optional.empty(), true)
//			.filterSameCollections("e", edgeLabelsFilter, bindVars)
//			.filterProperties(propertyFilter, "v", bindVars)
//			.ret("v");
//
//		String query = queryBuilder.toString();
//		return executeAqlQuery(query, bindVars, null, resultType);
//	}
//
//	/**
//	 * Gets the element properties.
//	 *
//	 * @param <T> 					the generic type
//	 * @param document              the document
//	 * @param edgeLabelsFilter      a list of edge types to follow
//	 * @param propertyFilter 		Filter the neighbours on the given property:value values
//	 * @param propertyType 			the property type
//	 * @return ArangoDBBaseQuery	the query object
//	 */
//
//	public <T> ArangoCursor<T> getElementProperties(
//        ArangoDBBaseDocument document,
//        List<String> edgeLabelsFilter,
//        ArangoDBPropertyFilter propertyFilter,
//        Class<T> propertyType) {
//		logger.debug("Get Vertex's {}:{} Neighbors, in {}, from collections {}", document, graph.name(), edgeLabelsFilter);
//		Map<String, Object> bindVars = new HashMap<>();
//		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
//		logger.debug("Creating query");
//        queryBuilder.iterateGraph(graph.name(), "v", Optional.of("e"),
//				Optional.empty(), Optional.empty(), Optional.empty(),
//				ArangoDBQueryBuilder.Direction.OUT, document._id(), bindVars)
//			.graphOptions(Optional.of(UniqueVertices.GLOBAL), Optional.empty(), true)
//			.filterSameCollections("e", edgeLabelsFilter, bindVars)
//			.filterProperties(propertyFilter, "v", bindVars)
//			.ret("v");
//
//		String query = queryBuilder.toString();
//		logger.debug("AQL {}", query);
//		return executeAqlQuery(query, bindVars, null, propertyType);
//	}
//
//
//	/**
//	 * Get vertices of a graph. If no ids are provided, get all vertices.
//	 *
//	 * @param ids 					the ids to match
//	 * @param collections 			the collections to search within
//	 * @return ArangoDBBaseQuery 	the query object
//	 */
//
//	public ArangoCursor<ArangoDBVertex> getGraphVertices(
//		final Collection<String> ids,
//		final Collection<String> collections) {
//		logger.debug("Get all {} graph vertices, filtered by ids: {}", graph.name(), ids);
//		Map<String, Object> bindVars = new HashMap<>();
//		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
//        List<String> prefixedColNames = graph.vertexCollections().stream().map(graph::getPrefixedCollectioName).collect(Collectors.toList());
//        if (ids.isEmpty()) {
//			if (prefixedColNames.size() > 1) {
//				queryBuilder.union(prefixedColNames, "v", bindVars);
//			} else {
//				queryBuilder.iterateCollection("v", prefixedColNames.get(0), bindVars);
//			}
//		}
//		else {
//			if (!collections.isEmpty()) {
//                prefixedColNames = collections.stream().map(graph::getPrefixedCollectioName).collect(Collectors.toList());
//			}
//			queryBuilder.with(prefixedColNames, bindVars)
//					.documentsById(ids, "v", bindVars);
//
//		}
//		queryBuilder.ret("v");
//		String query = queryBuilder.toString();
//		logger.debug("AQL {}", query);
//		return executeAqlQuery(query, bindVars, null, ArangoDBVertex.class);
//	}
//
//	/**
//	 * Get edges of a graph. If no ids are provided, get all edges.
//	 *
//	 * @param ids 					the ids to match
//	 * @param collections 			the collections to search within
//	 * @return ArangoDBBaseQuery	the query object
//	 */
//
//	public ArangoCursor<ArangoDBEdge> getGraphEdges(
//		List<String> ids,
//		List<String> collections) {
//		logger.debug("Get all {} graph edges, filtered by ids: {}", graph.name(), ids);
//		Map<String, Object> bindVars = new HashMap<>();
//		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
//        List<String> prefixedColNames = graph.edgeCollections().stream().map(graph::getPrefixedCollectioName).collect(Collectors.toList());
//		if (ids.isEmpty()) {
//			if (prefixedColNames.size() > 1) {
//				queryBuilder.union(prefixedColNames, "e", bindVars);
//			} else {
//				queryBuilder.iterateCollection("e", prefixedColNames.get(0), bindVars);
//			}
//		}
//		else {
//            if (!collections.isEmpty()) {
//                prefixedColNames = collections.stream().map(graph::getPrefixedCollectioName).collect(Collectors.toList());
//            }
//		    queryBuilder.with(prefixedColNames, bindVars)
//				    .documentsById(ids, "e", bindVars);
//
//		}
//		queryBuilder.ret("e");
//		String query = queryBuilder.toString();
//		logger.debug("AQL {}", query);
//		return executeAqlQuery(query, bindVars, null, ArangoDBEdge.class);
//	}
//
//	/**
//	 * Gets the edge vertices.
//	 *
//	 * @param edgeId 				the edge id
//	 * @param edgeCollection 		the edge collection
//	 * @param from 					if true, get incoming vertex
//	 * @param to 					if true, get outgoing edge
//	 * @return the edge vertices
//	 */
//
//	public ArangoCursor<ArangoDBVertex> getEdgeVertices(
//		String edgeId,
//		String edgeCollection,
//		boolean from, boolean to) {
//		logger.debug("Get edge {} vertices [{}, {}]", edgeId, from, to);
//		Map<String, Object> bindVars = new HashMap<>();
//		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
//		List<String> edgeCollections = new ArrayList<>();
//		List<String> vertices = new ArrayList<>();
//		edgeCollections.add(graph.getPrefixedCollectioName(edgeCollection));
//		if (from) {
//			vertices.add("_from");
//		}
//		if (to) {
//			vertices.add("_to");
//		}
//		queryBuilder.with(edgeCollections, bindVars)
//			.documentById(edgeId, "e", bindVars)
//			.append("FOR v IN ")
//			.append(vertices.stream().map(v->String.format("e.%s", v))
//					.collect(Collectors.joining(",", "[", "]\n")))
//			.ret("Document(v)");
//		String query = queryBuilder.toString();
//		logger.debug("AQL {}", query);
//		return executeAqlQuery(query, bindVars, null, ArangoDBVertex.class);
//	}
//
//	/**
//	 * Delete a graph from the db, and all its collections.
//	 *
//	 * @param name 					the name of the graph to delete
//	 * @return true, if the graph was deleted
//	 */
//
//	public boolean deleteGraph(String name) {
//		return deleteGraph(name, true);
//	}
//
//	/**
//	 * Delete a graph from the db. If dropCollection is true, then all the graph collections are also
//	 * dropped
//	 *
//	 * @param name 				the name
//	 * @param dropCollections 	true to drop the graph collections too
//	 * @return true if the graph was deleted
//	 */
//
//	public boolean deleteGraph(String name, boolean dropCollections) {
//		if (db != null) {
//			GraphClient graph = db.graph(name);
//			if (graph.exists()) {
//				try {
//					Collection<String> edgeDefinitions = dropCollections ? graph.getEdgeDefinitions() : Collections.emptyList();
//					Collection<String> vertexCollections = dropCollections ? graph.getVertexCollections(): Collections.emptyList();
//					// Drop graph first because it will break if the graph collections do not exist
//					graph.drop();
//					for (String definitionName : edgeDefinitions) {
//						String collectioName = definitionName;
//						if (db.collection(collectioName).exists()) {
//							db.collection(collectioName).drop();
//						}
//					}
//					for (String vc : vertexCollections) {
//						String collectioName = vc;
//						if (db.collection(collectioName).exists()) {
//							db.collection(collectioName).drop();
//						}
//					}
//					// Delete the graph variables
//					db.collection(ArangoDBGraph.GRAPH_VARIABLES_COLLECTION).deleteDocument(name);
//				} catch (ArangoDBException e) {
//					System.out.println(e);
//				}
//				return true;
//			} else {
//				try {
//					graph.drop();
//				} catch (ArangoDBException e) {
//                    //throw ArangoDBExceptions.getArangoDBException(e);
//				}
//			}
//		}
//		return false;
//	}
//
//	/**
//	 * Delete collection.
//	 *
//	 * @param name the name
//	 * @return true, if successful
//	 */
//
//	public boolean deleteCollection(String name) {
//		ArangoCollection collection = db.collection(name);
//		if (collection.exists()) {
//			collection.drop();
//			return collection.exists();
//		}
//		return false;
//	}
//

//
//
//
//	/**
//	 * Validate that the document is in the same graph.
//	 * @param document
//	 */
//	private void validateElementGraph(ArangoDBBaseDocument document) {
//		ArangoDBGraph documentGraph = document.graph();
//		if ((documentGraph != null) && (!documentGraph.equals(graph))) {
//			throw new ArangoDBGraphException("Document belongs to another graph.");
//		}
//	}




    // TODO Decide what of these methods should be restored.
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

//	/**
//	 * Create an index on collection keys.
//	 *
//	 * @param graph            the simple graph
//	 * @param type            the index type ("cap", "geo", "hash", "skiplist")
//	 * @param unique            true for a unique name
//	 * @param fields            a list of name fields
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
//	 * @param unique            true for a unique name
//	 * @param fields            a list of name fields
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
//	 * @param unique            true for a unique name
//	 * @param fields            a list of name fields
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
////	public PlainArangoDBConfiguration getConfiguration() {
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