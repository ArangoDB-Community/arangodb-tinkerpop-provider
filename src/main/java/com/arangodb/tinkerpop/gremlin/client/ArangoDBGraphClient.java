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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
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

        /**
         * The error code regex. Matches response messages from the ArangoDB client
         */

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
        // FIXME: match errors on code and error num instead of pattern matching on message string
        public static ArangoDBGraphException getArangoDBException(ArangoDBException ex) {
            if (ex.getCause() instanceof InterruptedException) {
                TraversalInterruptedException ie = new TraversalInterruptedException();
                ie.initCause(ex);
                throw ie;
            }

            String errorMessage = ex.getMessage();
            Matcher m = ERROR_CODE.matcher(errorMessage);
            if (m.matches()) {
                int code = Integer.parseInt(m.group(1));
                String msg = m.group(2);
                switch (code / 100) {
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

        /**
         * "name to long" Message.
         */

        public static String NAME_TO_LONG = "Name is too long: {} bytes (max 64 bytes for labels, 256 for keys)";

        /**
         * Gets the naming convention error.
         *
         * @param cause   the cause
         * @param details the details
         * @return the naming convention error
         */

        public static ArangoDBGraphException getNamingConventionError(String cause, String details) {
            return new ArangoDBGraphException("The provided label or name name does not satisfy the naming conventions." +
                    String.format(cause, details));
        }

    }

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBGraphClient.class);

    protected final ArangoDatabase db;

    private final ArangoDBGraph graph;

    /**
     * Create a simple graph client and connect to the provided db.
     *
     * @param graph      the ArangoDB graph that uses this client
     * @param properties the ArangoDB configuration properties
     * @param dbname     the ArangoDB name to connect to or create
     * @throws ArangoDBGraphException If the db does not exist and cannot be created
     */
    public ArangoDBGraphClient(
            ArangoDBGraph graph,
            Properties properties,
            String dbname)
            throws ArangoDBGraphException {
        logger.debug("Initiating the ArangoDb Client");
        this.graph = graph;
        db = new ArangoDB.Builder()
                .loadProperties(ArangoConfigProperties.fromProperties(properties))
                .build()
                .db(dbname);
    }

    /**
     * Shutdown the client and free resources.
     */

    public void shutdown() {
        logger.debug("Shutdown");
        db.arango().shutdown();
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
     *
     * @param document the document
     * @throws ArangoDBGraphException If there was an error inserting the document
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
     *
     * @param document the document to delete
     * @throws ArangoDBGraphException If there was an error deleting the document
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
     *
     * @param document the document
     * @throws ArangoDBGraphException If there was an error updating the document
     */

    public void updateGraphVariables(ArangoDBGraphVariables document) {
        logger.debug("Update variables {} in {}", document, graph.name());
        DocumentUpdateEntity<?> updateEntity;
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
     * @param ids the ids to match
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
     * @param ids the ids to match
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
     * Create a new graph.
     *
     * @param name            the name of the new graph
     * @param edgeDefinitions the edge definitions for the graph
     * @param options         additional graph options
     * @return the arango graph
     * @throws ArangoDBGraphException If the graph can not be created
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
     * @param <V>             the generic type of the returned values
     * @param query           the query string
     * @param bindVars        the value of the bind parameters
     * @param aqlQueryOptions the aql query options
     * @param type            the type of the result (POJO class, VPackSlice, String for Json, or Collection/List/Map)
     * @return the cursor result
     * @throws ArangoDBGraphException if executing the query raised an exception
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
                " @start GRAPH @graph OPTIONS { edgeCollections: @edgeCollections, order: 'bfs' } RETURN v";
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