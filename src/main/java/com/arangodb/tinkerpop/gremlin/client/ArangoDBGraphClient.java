/// ///////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
/// ///////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.arangodb.config.ArangoConfigProperties;
import com.arangodb.entity.*;
import com.arangodb.model.*;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.persistence.EdgeData;
import com.arangodb.tinkerpop.gremlin.persistence.VertexData;
import com.arangodb.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

import static com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil.getArangoDirectionFromGremlinDirection;

/**
 * The arangodb graph client class handles the HTTP connection to arangodb and performs database
 * operations on the ArangoDatabase.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBGraphClient {

    /**
     * Common exceptions to use with an ArangoDB. This class is intended to translate ArangoDB error codes into
     * meaningful exceptions with standard messages. ArangoDBException exception is a RuntimeException intended to
     * break execution.
     */

    public static class ArangoDBExceptions {

        /** The error code regex. Matches response messages from the ArangoDB client */

        public static Pattern ERROR_CODE = Pattern.compile("^Response:\\s\\d+,\\sError:\\s(\\d+)\\s-\\s([a-z\\s]+).+");

        /**
         * Instantiation happens via factory method
         */

        private ArangoDBExceptions() {
        }

        /**
         * Translate ArangoDB Error code into exception (@see <a href="https://docs.arangodb.com/latest/Manual/Appendix/ErrorCodes.html">Error codes</a>)
         *
         * @param ex the ex
         * @return The ArangoDBClientException
         */

        public static ArangoDBGraphException getArangoDBException(ArangoDBException ex) {
            String errorMessage = ex.getMessage();
            Matcher m = ERROR_CODE.matcher(errorMessage);
            if (m.matches()) {
                int code = Integer.parseInt(m.group(1));
                String msg = m.group(2);
                switch ((int) code / 100) {
                    case 10:    // Internal ArangoDB storage errors
                        return new ArangoDBGraphException(code, String.format("Internal ArangoDB storage error (%s): %s", code, msg), ex);
                    case 11:
                        return new ArangoDBGraphException(code, String.format("External ArangoDB storage error (%s): %s", code, msg), ex);
                    case 12:
                        return new ArangoDBGraphException(code, String.format("General ArangoDB storage error (%s): %s", code, msg), ex);
                    case 13:
                        return new ArangoDBGraphException(code, String.format("Checked ArangoDB storage error (%s): %s", code, msg), ex);
                    case 14:
                        return new ArangoDBGraphException(code, String.format("ArangoDB replication/cluster error (%s): %s", code, msg), ex);
                    case 15:
                        return new ArangoDBGraphException(code, String.format("ArangoDB query error (%s): %s", code, msg), ex);
                    case 19:
                        return new ArangoDBGraphException(code, String.format("Graph / traversal errors (%s): %s", code, msg), ex);
                }
            }
            return new ArangoDBGraphException("General ArangoDB error (unkown error code)", ex);
        }

        /** "name to long" Message. */

        public static String NAME_TO_LONG = "Name is too long: {} bytes (max 64 bytes for labels, 256 for keys)";

        /**
         * Gets the naming convention error.
         *
         * @param cause the cause
         * @param details the details
         * @return the naming convention error
         */

        public static ArangoDBGraphException getNamingConventionError(String cause, String details) {
            return new ArangoDBGraphException("The provided label or name name does not satisfy the naming conventions." +
                    String.format(cause, details));
        }

    }

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBGraphClient.class);

    private final ArangoDB driver;

    private final ArangoDatabase db;

    private final int batchSize;

    private final ArangoDBGraph graph;

    /**
     * Create a simple graph client and connect to the provided db. If the DB does not exist, the driver will try to
     * create one.
     *
     * @param graph                    the ArangoDB graph that uses this client
     * @param properties            the ArangoDB configuration properties
     * @param dbname                the ArangoDB name to connect to or create
     * @param batchSize             the size of the batch mode chunks
     * @throws ArangoDBGraphException    If the db does not exist and cannot be created
     */

    public ArangoDBGraphClient(
            ArangoDBGraph graph,
            Properties properties,
            String dbname,
            int batchSize)
            throws ArangoDBGraphException {
        this(graph, properties, dbname, batchSize, false);
    }

    /**
     * Create a simple graph client and connect to the provided db. The create flag controls what is the
     * behaviour if the db is not found
     *
     * @param graph                    the ArangoDB graph that uses this client
     * @param properties            the ArangoDB configuration properties
     * @param dbname                the ArangoDB name to connect to or create
     * @param batchSize             the size of the batch mode chunks
     * @param createDatabase        if true, the driver will attempt to crate the DB if it does not exist
     * @throws ArangoDBGraphException    If the db does not exist and cannot be created
     */

    public ArangoDBGraphClient(
            ArangoDBGraph graph,
            Properties properties,
            String dbname,
            int batchSize,
            boolean createDatabase)
            throws ArangoDBGraphException {
        logger.debug("Initiating the ArangoDb Client");
        this.graph = graph;
        driver = new ArangoDB.Builder()
                .loadProperties(ArangoConfigProperties.fromProperties(properties))
                .build();
        db = driver.db(dbname);
        if (createDatabase) {
            if (!db.exists()) {
                logger.debug("DB not found, attemtping to create it.");
                try {
                    if (!driver.createDatabase(dbname)) {
                        throw new ArangoDBGraphException("Unable to crate the database " + dbname);
                    }
                } catch (ArangoDBException ex) {
                    throw ArangoDBExceptions.getArangoDBException(ex);
                }
            }
        } else {
            boolean exists = false;
            try {
                exists = db.exists();
            } catch (ArangoDBException ex) {
                // Pass
            }
            if (!exists) {
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
    }

    /**
     * Drop the graph and its related collections.
     *
     * @param graph                    the graph to clear
     * @throws ArangoDBGraphException    if there was an error dropping the graph and its collections
     */

    public void clear(ArangoDBGraph graph) throws ArangoDBGraphException {
        logger.debug("Clear {}", graph.name());
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
        } catch (ArangoDBException ex) {
            throw ArangoDBExceptions.getArangoDBException(ex);
        }
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
        return db == null ? false : db.exists();
    }

    /**
     * Delete the current database accessed by the driver.
     *
     * @throws ArangoDBGraphException if there was an error
     */

    public void deleteDb() throws ArangoDBGraphException {
        logger.debug("Delete current db");
        if (db != null) {
            try {
                db.drop();
            } catch (ArangoDBException e) {
                throw ArangoDBExceptions.getArangoDBException(e);
            }
        }
    }

    public ArangoDBGraphVariables getGraphVariables() {
        logger.debug("Get graph variables");
        ArangoDBGraphVariables result;
        try {
            result = db
                    .collection(ArangoDBGraph.GRAPH_VARIABLES_COLLECTION)
                    .getDocument(graph.name(), ArangoDBGraphVariables.class);
        } catch (ArangoDBException e) {
            logger.error("Failed to retrieve vertex: {}", e.getErrorMessage());
            throw new ArangoDBGraphException("Failed to retrieve vertex.", e);
        }
        result.collection(result.label);
        return result;
    }

    /**
     * Insert a ArangoDBBaseDocument in the graph. The document is updated with the id, rev and name
     * (if not * present)
     * @param document                the document
     * @throws ArangoDBGraphException    If there was an error inserting the document
     */

    public void insertGraphVariables(ArangoDBGraphVariables document) {
        logger.debug("Insert graph variables {} in {}", document, graph.name());
        if (document.isPaired()) {
            throw new ArangoDBGraphException("Paired docuements can not be inserted, only updated");
        }
        ArangoCollection gVars = db.collection(document.collection());
        if (!gVars.exists()) {
            CollectionEntity ce = db.createCollection(document.collection());
            System.out.println(ce.getStatus());
        }
        DocumentCreateEntity<?> vertexEntity;
        try {
            vertexEntity = gVars.insertDocument(document);
        } catch (ArangoDBException e) {
            logger.error("Failed to insert document: {}", e.getMessage());
            ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
            if (arangoDBException.getErrorCode() == 1210) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(document._key);
            }
            throw arangoDBException;
        }
        document._id(vertexEntity.getId());
        document._rev(vertexEntity.getRev());
        if (document._key() == null) {
            document._key(vertexEntity.getKey());
        }
        document.setPaired(true);
    }

    /**
     * Delete a document from the graph.
     * @param document                the document to delete
     * @throws ArangoDBGraphException    If there was an error deleting the document
     */

    public void deleteGraphVariables(ArangoDBGraphVariables document) {
        logger.debug("Delete variables {} in {}", document, graph.name());
        try {
            db.collection(document.collection())
                    .deleteDocument(document._key());
        } catch (ArangoDBException e) {
            logger.error("Failed to delete document: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        document.setPaired(false);
    }

    /**
     * Update the document in the graph.
     * @param document                the document
     *
     * @throws ArangoDBGraphException    If there was an error updating the document
     */

    public void updateGraphVariables(ArangoDBGraphVariables document) {
        logger.debug("Update variables {} in {}", document, graph.name());
        DocumentUpdateEntity updateEntity;
        try {
            updateEntity = db.collection(document.collection())
                    .updateDocument(document._key(), document);
        } catch (ArangoDBException e) {
            logger.error("Failed to update document: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        logger.debug("Document updated, new rev {}", updateEntity.getRev());
        document._rev(updateEntity.getRev());
    }

    /**
     * Get vertices of a graph. If no ids are provided, get all vertices.
     *
     * @param ids                    the ids to match
     * @return ArangoDBBaseQuery    the query object
     */

    public ArangoCursor<VertexData> getGraphVertices(final List<String> ids) {
        logger.debug("Get all {} graph vertices, filtered by ids: {}", graph.name(), ids);
        List<String> prefixedColNames = graph.vertexCollections().stream().map(graph::getPrefixedCollectioName).collect(Collectors.toList());
        return getGraphDocuments(ids, prefixedColNames, VertexData.class);
    }

    /**
     * Get edges of a graph. If no ids are provided, get all edges.
     *
     * @param ids                    the ids to match
     * @return ArangoDBBaseQuery    the query object
     */
    public ArangoCursor<EdgeData> getGraphEdges(List<String> ids) {
        logger.debug("Get all {} graph edges, filtered by ids: {}", graph.name(), ids);
        List<String> prefixedColNames = graph.edgeCollections().stream().map(graph::getPrefixedCollectioName).collect(Collectors.toList());
        return getGraphDocuments(ids, prefixedColNames, EdgeData.class);
    }

    private <V> ArangoCursor<V> getGraphDocuments(List<String> ids, List<String> prefixedColNames, Class<V> clazz) {
        Map<String, Object> bindVars = new HashMap<>();
        ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder();
        if (ids.isEmpty()) {
            if (prefixedColNames.size() > 1) {
                queryBuilder.union(prefixedColNames, "d", bindVars);
            } else {
                queryBuilder.iterateCollection("d", prefixedColNames.get(0), bindVars);
            }
        } else {
            queryBuilder.with(prefixedColNames, bindVars).documentsById(ids, "d", bindVars);
        }
        queryBuilder.ret("d");
        String query = queryBuilder.toString();
        logger.debug("AQL {}", query);
        return executeAqlQuery(query, bindVars, null, clazz);
    }

    /**
     * Delete a graph from the db, and all its collections.
     *
     * @param name                    the name of the graph to delete
     * @return true, if the graph was deleted
     */

    public boolean deleteGraph(String name) {
        return deleteGraph(name, true);
    }

    /**
     * Delete a graph from the db. If dropCollection is true, then all the graph collections are also
     * dropped
     *
     * @param name                the name
     * @param dropCollections    true to drop the graph collections too
     * @return true if the graph was deleted
     */

    public boolean deleteGraph(String name, boolean dropCollections) {
        if (db != null) {
            ArangoGraph graph = db.graph(name);
            if (graph.exists()) {
                try {
                    Collection<String> edgeDefinitions = dropCollections ? graph.getEdgeDefinitions() : Collections.emptyList();
                    Collection<String> vertexCollections = dropCollections ? graph.getVertexCollections() : Collections.emptyList();
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
                    // Delete the graph variables
                    db.collection(ArangoDBGraph.GRAPH_VARIABLES_COLLECTION).deleteDocument(name);
                } catch (ArangoDBException e) {
                    System.out.println(e);
                }
                return true;
            } else {
                try {
                    graph.drop();
                } catch (ArangoDBException e) {
                    //throw ArangoDBExceptions.getArangoDBException(e);
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
     * @param name                        the name of the new graph
     * @param edgeDefinitions            the edge definitions for the graph
     * @throws ArangoDBGraphException    If the graph can not be created
     */

    public void createGraph(String name, List<EdgeDefinition> edgeDefinitions)
            throws ArangoDBGraphException {
        this.createGraph(name, edgeDefinitions, null);
    }

    /**
     * Create a new graph.
     *
     * @param name                        the name of the new graph
     * @param edgeDefinitions            the edge definitions for the graph
     * @param options                    additional graph options
     * @return the arango graph
     * @throws ArangoDBGraphException    If the graph can not be created
     */

    public ArangoGraph createGraph(String name,
                                   List<EdgeDefinition> edgeDefinitions,
                                   GraphCreateOptions options)
            throws ArangoDBGraphException {
        logger.debug("Creating graph {}", name);
        try {
            logger.debug("Creating graph in database.");
            db.createGraph(name, edgeDefinitions, options);
        } catch (ArangoDBException e) {
            logger.debug("Error creating graph in database.", e);
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        return db.graph(name);
    }


    /**
     * Get the ArangoGraph that is linked to the client's graph
     *
     * @return the graph or null if the graph was not found
     */

    public ArangoGraph getArangoGraph() {
        return db.graph(graph.name());
    }

    /**
     * Execute AQL query.
     *
     * @param <V> 						the generic type of the returned values
     * @param query                    the query string
     * @param bindVars                    the value of the bind parameters
     * @param aqlQueryOptions            the aql query options
     * @param type                        the type of the result (POJO class, VPackSlice, String for Json, or Collection/List/Map)
     * @return the cursor result
     * @throws ArangoDBGraphException    if executing the query raised an exception
     */

    public <V> ArangoCursor<V> executeAqlQuery(
            String query,
            Map<String, Object> bindVars,
            AqlQueryOptions aqlQueryOptions,
            final Class<V> type)
            throws ArangoDBGraphException {
        logger.debug("Executing AQL query ({}) against db, with bind vars: {}", query, bindVars);
        try {
            return db.query(query, type, bindVars, aqlQueryOptions);
        } catch (ArangoDBException e) {
            logger.error("Error executing query", e);
            throw ArangoDBExceptions.getArangoDBException(e);
        }
    }

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
    public void insertEdge(ArangoDBEdge edge) {
        logger.debug("Insert edge {} in {} ", edge, graph.name());
        EdgeEntity insertEntity;
        try {
            insertEntity = db.graph(graph.name())
                    .edgeCollection(edge.collection())
                    .insertEdge(edge.data());
        } catch (ArangoDBException e) {
            logger.error("Failed to insert edge: {}", e.getErrorMessage());
            ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
            if (arangoDBException.getErrorCode() == 1210) {
                throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.collection() + "/" + edge.key());
            }
            throw arangoDBException;
        }
        edge.key(insertEntity.getKey());
    }

    public void deleteEdge(ArangoDBEdge edge) {
        logger.debug("Delete edge {} in {}", edge, graph.name());
        try {
            db.graph(graph.name())
                    .edgeCollection(edge.collection())
                    .deleteEdge(edge.key());
        } catch (ArangoDBException e) {
            if (e.getErrorNum() == 1202) { // document not found
                return;
            }
            logger.error("Failed to delete vertex: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
    }

    public void updateEdge(ArangoDBEdge edge) {
        logger.debug("Update edge {} in {}", edge, graph.name());
        EdgeUpdateEntity updateEntity;
        try {
            updateEntity = db.graph(graph.name())
                    .edgeCollection(edge.collection())
                    .replaceEdge(edge.key(), edge.data());
        } catch (ArangoDBException e) {
            logger.error("Failed to update edge: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        logger.debug("Edge updated, new rev {}", updateEntity.getRev());
        edge.key(updateEntity.getKey());
    }

    public VertexData readVertex(String id) {
        logger.debug("Read vertex {} in {}", id, graph.name());
        String col = ArangoDBUtil.extractCollection(id);
        String key = ArangoDBUtil.extractKey(id);
        try {
            return db.graph(graph.name())
                    .vertexCollection(col)
                    .getVertex(key, VertexData.class);
        } catch (ArangoDBException e) {
            logger.error("Failed to read vertex: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
    }

    public void insertVertex(ArangoDBVertex vertex) {
        logger.debug("Insert vertex {} in {}", vertex, graph.name());
        VertexEntity vertexEntity;
        try {
            vertexEntity = db.graph(graph.name())
                    .vertexCollection(vertex.collection())
                    .insertVertex(vertex.data());
        } catch (ArangoDBException e) {
            logger.error("Failed to insert document: {}", e.getMessage());
            ArangoDBGraphException arangoDBException = ArangoDBExceptions.getArangoDBException(e);
            if (arangoDBException.getErrorCode() == 1210) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.key());
            }
            throw arangoDBException;
        }
        vertex.key(vertexEntity.getKey());
    }

    public void deleteVertex(ArangoDBVertex vertex) {
        logger.debug("Delete vertex {} in {}", vertex, graph.name());
        try {
            db.graph(graph.name())
                    .vertexCollection(vertex.collection())
                    .deleteVertex(vertex.key());
        } catch (ArangoDBException e) {
            if (e.getErrorNum() == 1202) { // document not found
                return;
            }
            logger.error("Failed to delete vertex: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
    }

    public void updateVertex(ArangoDBVertex vertex) {
        logger.debug("Update document {} in {}", vertex, graph.name());
        VertexUpdateEntity vertexEntity;
        try {
            vertexEntity = db.graph(graph.name())
                    .vertexCollection(vertex.collection())
                    .replaceVertex(vertex.key(), vertex.data());
        } catch (ArangoDBException e) {
            logger.error("Failed to update document: {}", e.getErrorMessage());
            throw ArangoDBExceptions.getArangoDBException(e);
        }
        logger.debug("Document updated, new rev {}", vertexEntity.getRev());
    }

    public Iterator<VertexData> getVertexNeighbors(String vertexId, List<String> edgeCollections, Direction direction) {
        logger.debug("Get vertex {}:{} Neighbors, in {}, from collections {}", vertexId, direction, graph.name(), edgeCollections);
        Map<String, Object> bindVars = new HashMap<>();
        bindVars.put("start", vertexId);
        bindVars.put("graph", graph.name());
        bindVars.put("edgeCollections", edgeCollections);
        String query = "FOR v IN 1..1 " + getArangoDirectionFromGremlinDirection(direction).getAqlName() +
                " @start GRAPH @graph OPTIONS { edgeCollections: @edgeCollections, uniqueVertices: 'global', order: 'bfs' } RETURN v";
        return executeAqlQuery(query, bindVars, null, VertexData.class);
    }

    public Iterator<EdgeData> getVertexEdges(String vertexId, List<String> edgeCollections, Direction direction) {
        logger.debug("Get vertex {}:{} Edges, in {}, from collections {}", vertexId, direction, graph.name(), edgeCollections);
        Map<String, Object> bindVars = new HashMap<>();
        bindVars.put("start", vertexId);
        bindVars.put("graph", graph.name());
        bindVars.put("edgeCollections", edgeCollections);
        String query = "FOR v, e IN 1..1 " + getArangoDirectionFromGremlinDirection(direction).getAqlName() +
                " @start GRAPH @graph OPTIONS { edgeCollections: @edgeCollections } RETURN e";
        return executeAqlQuery(query, bindVars, null, EdgeData.class);
    }
}