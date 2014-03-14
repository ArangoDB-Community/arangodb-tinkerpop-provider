//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Vector;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBBaseQuery.Direction;

public class ArangoDBSimpleGraphClient {

	/**
	 * the logger
	 */

	private static Logger LOG = Logger.getLogger(ArangoDBSimpleGraphClient.class);

	/**
	 * the configuration (contains connection parameters etc.)
	 */

	private ArangoDBConfiguration configuration;

	/**
	 * a connection manager (call shutdown!)
	 */

	private static ClientConnectionManager connectionManager;

	/**
	 * a http client
	 */

	private DefaultHttpClient httpClient;

	/**
	 * a request type
	 */

	public static enum RequestType {
		GET, POST, PUT, DELETE
	}

	/**
	 * Create a simple graph client
	 * 
	 * @param configuration
	 *            the ArangoDB configuration
	 */

	public ArangoDBSimpleGraphClient(ArangoDBConfiguration configuration) {
		this.configuration = configuration;

		if (connectionManager == null) {
			connectionManager = this.configuration.createClientConnectionManager();
		}

		int connectionTimeout = 3000;
		int socketTimeout = 10000;
		final long keepAliveTimeout = 90;
		boolean useExpectContinue = false;
		boolean staleConnectionCheck = false;

		HttpParams params = new BasicHttpParams();
		params.setParameter(HttpConnectionParams.STALE_CONNECTION_CHECK, staleConnectionCheck);
		params.setParameter(HttpProtocolParams.USE_EXPECT_CONTINUE, useExpectContinue);
		params.setParameter(HttpConnectionParams.CONNECTION_TIMEOUT, connectionTimeout);
		params.setParameter(HttpConnectionParams.SO_TIMEOUT, socketTimeout);
		params.setParameter(HttpConnectionParams.TCP_NODELAY, true);
		params.setParameter(HttpConnectionParams.SO_KEEPALIVE, false); // keep-alive
																		// on
																		// TCP
																		// level

		ConnectionKeepAliveStrategy customKeepAliveStrategy = new ConnectionKeepAliveStrategy() {
			public long getKeepAliveDuration(org.apache.http.HttpResponse response,
					org.apache.http.protocol.HttpContext context) {
				return keepAliveTimeout * 1000;
			}
		};

		this.httpClient = new DefaultHttpClient(connectionManager, params);
		this.httpClient.setKeepAliveStrategy(customKeepAliveStrategy);
		// this.httpClient = new DefaultHttpClient();
	}

	/**
	 * Shutdown the client and free resources
	 */

	public void shutdown() {
		connectionManager.shutdown();
	}

	/**
	 * Request the version of ArangoDB
	 * 
	 * @return the Version number
	 * 
	 * @throws ArangoDBException
	 *             on
	 */

	public String getVersion() throws ArangoDBException {
		JSONObject obj = getRequest("_api/version");

		try {
			if (obj.has("version") && obj.has("server") && obj.get("server").toString().equals("arango")) {
				return obj.get("version").toString();
			}
		} catch (JSONException e) {
			throw new ArangoDBException("Error in response: " + e.getMessage());
		}

		throw new ArangoDBException("Could not get a version number");
	}

	/**
	 * Create a new graph
	 * 
	 * @param name
	 *            the name of the new graph
	 * @param vertexCollectionName
	 *            the name of the vertex collection
	 * @param EdgeCollectionName
	 *            the name of the edge collection
	 * 
	 * @return the graph
	 * 
	 * @throws ArangoDBException
	 *             if the graph could not be created
	 */

	public ArangoDBSimpleGraph createGraph(String name, String vertexCollectionName, String EdgeCollectionName)
			throws ArangoDBException {
		JSONObject body = new JSONObject();
		try {
			body.put(ArangoDBSimpleGraph._KEY, name);
			body.put(ArangoDBSimpleGraph._VERTICES, vertexCollectionName);
			body.put(ArangoDBSimpleGraph._EDGES, EdgeCollectionName);
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		JSONObject result = postRequest("_api/graph", body);
		JSONObject graph = getResultJsonObject(result, "graph");
		return new ArangoDBSimpleGraph(graph);
	}

	/**
	 * Get a graph by name
	 * 
	 * @param name
	 *            the name of the new graph
	 * 
	 * @return the graph
	 * 
	 * @throws ArangoDBException
	 *             if the graph could not be created
	 */

	public ArangoDBSimpleGraph getGraph(String name) throws ArangoDBException {
		JSONObject result = getRequest("_api/graph/" + urlEncode(name));
		JSONObject graph = getResultJsonObject(result, "graph");
		return new ArangoDBSimpleGraph(graph);
	}

	/**
	 * Delete a graph by name
	 * 
	 * @param graph
	 *            the graph
	 * 
	 * @return true if the graph was deleted
	 * @throws ArangoDBException
	 *             if the graph could be deleted
	 */

	public boolean deleteGraph(ArangoDBSimpleGraph graph) throws ArangoDBException {
		JSONObject result = deleteRequest("_api/graph/" + urlEncode(graph.getDocumentKey()));
		try {
			if (result.has("deleted")) {
				boolean b = result.getBoolean("deleted");

				if (b) {
					graph.setDeleted();
				}

				return b;
			}
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		return false;
	}

	/**
	 * Create a new vertex
	 * 
	 * @param graph
	 *            the simple graph of the new vertex
	 * @param name
	 *            the name of the new vertex
	 * @param properties
	 *            the pre defined properties of the vertex
	 * 
	 * @return the vertex
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleVertex createVertex(ArangoDBSimpleGraph graph, String name, JSONObject properties)
			throws ArangoDBException {
		if (properties == null) {
			properties = new JSONObject();
		}

		try {
			if (name != null) {
				properties.put(ArangoDBSimpleVertex._KEY, name);
			} else if (properties.has(ArangoDBSimpleVertex._KEY)) {
				properties.remove(ArangoDBSimpleVertex._KEY);
			}
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		JSONObject result = postRequest("_api/graph/" + urlEncode(graph.getDocumentKey()) + "/vertex", properties);
		JSONObject vertex = getResultJsonObject(result, "vertex");
		return new ArangoDBSimpleVertex(vertex);
	}

	/**
	 * Get a vertex
	 * 
	 * @param graph
	 *            the simple graph of the new vertex
	 * @param name
	 *            the name of the vertex
	 * 
	 * @return the vertex
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleVertex getVertex(ArangoDBSimpleGraph graph, String name) throws ArangoDBException {
		JSONObject result = getRequest("_api/graph/" + urlEncode(graph.getDocumentKey()) + "/vertex/" + urlEncode(name));
		JSONObject vertex = getResultJsonObject(result, "vertex");
		return new ArangoDBSimpleVertex(vertex);
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
		JSONObject result = putRequest("_api/graph/" + urlEncode(graph.getDocumentKey()) + "/vertex/"
				+ urlEncode(vertex.getDocumentKey()), vertex.getProperties());
		JSONObject newProperties = getResultJsonObject(result, "vertex");
		vertex.setProperties(newProperties);
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
	 * @throws ArangoDBException
	 *             if deletion failed
	 */

	public boolean deleteVertex(ArangoDBSimpleGraph graph, ArangoDBSimpleVertex vertex) throws ArangoDBException {
		JSONObject result = deleteRequest("_api/graph/" + urlEncode(graph.getDocumentKey()) + "/vertex/"
				+ urlEncode(vertex.getDocumentKey()));
		try {
			if (result.has("deleted")) {
				boolean b = result.getBoolean("deleted");

				if (b) {
					vertex.setDeleted();
				}

				return b;
			}
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		return false;
	}

	/**
	 * Create a new edge
	 * 
	 * @param graph
	 *            the simple graph
	 * @param name
	 *            the name of the new edge
	 * @param label
	 *            the label of the new edge
	 * @param from
	 *            the start vertex
	 * @param to
	 *            the end vertex
	 * @param properties
	 *            the pre defined properties of the edge
	 * 
	 * @return the edge
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleEdge createEdge(ArangoDBSimpleGraph graph, String name, String label,
			ArangoDBSimpleVertex from, ArangoDBSimpleVertex to, JSONObject properties) throws ArangoDBException {
		if (properties == null) {
			properties = new JSONObject();
		}

		try {
			if (name != null) {
				properties.put(ArangoDBSimpleEdge._KEY, name);
			} else if (properties.has(ArangoDBSimpleEdge._KEY)) {
				properties.remove(ArangoDBSimpleEdge._KEY);
			}
			if (label != null) {
				properties.put(ArangoDBSimpleEdge._LABEL, label);
			} else if (properties.has(ArangoDBSimpleEdge._LABEL)) {
				properties.remove(ArangoDBSimpleEdge._LABEL);
			}

			properties.put(ArangoDBSimpleEdge._FROM, from.getDocumentKey());
			properties.put(ArangoDBSimpleEdge._TO, to.getDocumentKey());
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		JSONObject result = postRequest("_api/graph/" + urlEncode(graph.getDocumentKey()) + "/edge", properties);
		JSONObject edge = getResultJsonObject(result, "edge");
		return new ArangoDBSimpleEdge(edge);
	}

	/**
	 * Get an edge
	 * 
	 * @param graph
	 *            the simple graph
	 * @param name
	 *            the name of the edge
	 * 
	 * @return the edge
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleEdge getEdge(ArangoDBSimpleGraph graph, String name) throws ArangoDBException {
		JSONObject result = getRequest("_api/graph/" + urlEncode(graph.getDocumentKey()) + "/edge/" + urlEncode(name));
		JSONObject edge = getResultJsonObject(result, "edge");
		return new ArangoDBSimpleEdge(edge);
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
	 *             if creation failed
	 */

	public ArangoDBSimpleEdge saveEdge(ArangoDBSimpleGraph graph, ArangoDBSimpleEdge edge) throws ArangoDBException {
		JSONObject result = putRequest(
				"_api/graph/" + urlEncode(graph.getDocumentKey()) + "/edge/" + urlEncode(edge.getDocumentKey()),
				edge.getProperties());
		JSONObject properties = getResultJsonObject(result, "edge");
		edge.setProperties(properties);
		return edge;
	}

	/**
	 * Detete an edge
	 * 
	 * @param graph
	 *            the simple graph
	 * @param edge
	 *            the edge
	 * 
	 * @return true if the edge was deleted
	 * @throws ArangoDBException
	 *             if deletion failed
	 */

	public boolean deleteEdge(ArangoDBSimpleGraph graph, ArangoDBSimpleEdge edge) throws ArangoDBException {
		JSONObject result = deleteRequest("_api/graph/" + urlEncode(graph.getDocumentKey()) + "/edge/"
				+ urlEncode(edge.getDocumentKey()));
		try {
			if (result.has("deleted")) {
				boolean b = result.getBoolean("deleted");

				if (b) {
					edge.setDeleted();
				}

				return b;
			}
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		return false;
	}

	public JSONObject createVertices(ArangoDBSimpleGraph graph, List<ArangoDBSimpleVertex> vertices, boolean details)
			throws ArangoDBException {

		StringBuilder sb = new StringBuilder(4096);

		for (ArangoDBSimpleVertex v : vertices) {
			JSONObject j = v.getProperties();
			sb.append(j.toString()).append("\n");
		}

		JSONObject result = postRequest("_api/import?collection=" + urlEncode(graph.getVertexCollection())
				+ "&type=documents&details=" + (details ? "true" : "false"), sb.toString());

		return result;
	}

	public JSONObject createEdges(ArangoDBSimpleGraph graph, List<ArangoDBSimpleEdge> edges, boolean details)
			throws ArangoDBException {

		StringBuilder sb = new StringBuilder(4096);

		for (ArangoDBSimpleEdge e : edges) {
			sb.append(e.getProperties().toString()).append("\n");
		}

		JSONObject result = postRequest("_api/import?collection=" + urlEncode(graph.getEdgeCollection())
				+ "&type=documents&details=" + (details ? "true" : "false"), sb.toString());

		return result;
	}

	/**
	 * Delete cursor
	 * 
	 * @param cursorId
	 *            The cursor id
	 * 
	 * @return JSONObject The result
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public JSONObject getNextCursorValues(String id) throws ArangoDBException {
		return putRequest("_api/cursor/" + urlEncode(id), null);
	}

	/**
	 * Load next values
	 * 
	 * @param cursorId
	 *            The cursor id
	 */

	public boolean deleteCursor(String id) {
		try {
			deleteRequest("_api/cursor/" + urlEncode(id));
		} catch (ArangoDBException e) {
		}
		return true;
	}

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
	 * @return ArangoDBSimpleVertexQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleVertexQuery getGraphVertices(ArangoDBSimpleGraph graph, ArangoDBPropertyFilter propertyFilter,
			Long limit, boolean count) throws ArangoDBException {
		String path = "_api/graph/" + urlEncode(graph.getDocumentKey()) + "/vertices";

		return new ArangoDBSimpleVertexQuery(this, path, propertyFilter, null, null, limit, count);
	}

	/**
	 * Create a query to get all edges of a graph
	 * 
	 * @param graph
	 *            the simple graph
	 * @param propertyFilter
	 *            a property filter
	 * @param count
	 *            query total number of results
	 * 
	 * @return ArangoDBSimpleVertexQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleEdgeQuery getGraphEdges(ArangoDBSimpleGraph graph, ArangoDBPropertyFilter propertyFilter,
			List<String> labelsFilter, Long limit, boolean count) throws ArangoDBException {
		String path = "_api/graph/" + urlEncode(graph.getDocumentKey()) + "/edges";

		return new ArangoDBSimpleEdgeQuery(this, path, propertyFilter, labelsFilter, null, limit, count);
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
	 * @return ArangoDBSimpleVertexQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleVertexQuery getVertexNeighbors(ArangoDBSimpleGraph graph, ArangoDBSimpleVertex vertex,
			ArangoDBPropertyFilter propertyFilter, List<String> labelsFilter, Direction direction, Long limit,
			boolean count) throws ArangoDBException {
		String path = "_api/graph/" + urlEncode(graph.getDocumentKey()) + "/vertices/"
				+ urlEncode(vertex.getDocumentKey());

		return new ArangoDBSimpleVertexQuery(this, path, propertyFilter, labelsFilter, direction, limit, count);
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
	 * @return ArangoDBSimpleVertexQuery the query object
	 * 
	 * @throws ArangoDBException
	 *             if creation failed
	 */

	public ArangoDBSimpleEdgeQuery getVertexEdges(ArangoDBSimpleGraph graph, ArangoDBSimpleVertex vertex,
			ArangoDBPropertyFilter propertyFilter, List<String> labelsFilter, Direction direction, Long limit,
			boolean count) throws ArangoDBException {
		String path = "_api/graph/" + urlEncode(graph.getDocumentKey()) + "/edges/"
				+ urlEncode(vertex.getDocumentKey());

		return new ArangoDBSimpleEdgeQuery(this, path, propertyFilter, labelsFilter, direction, limit, count);
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

	public ArangoDBIndex createVertexIndex(ArangoDBSimpleGraph graph, String type, boolean unique, Vector<String> fields)
			throws ArangoDBException {
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

	public ArangoDBIndex createEdgeIndex(ArangoDBSimpleGraph graph, String type, boolean unique, Vector<String> fields)
			throws ArangoDBException {
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
		String path = "_api/index/" + id;

		JSONObject result = null;

		try {
			result = getRequest(path);
		} catch (ArangoDBException e) {
			if (e.errorNumber().equals(1212)) {
				// index not found
				return null;
			}
			throw e;
		}

		return new ArangoDBIndex(result);
	}

	public List<ArangoDBIndex> getVertexIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
		return getIndices(graph.getVertexCollection());
	}

	public List<ArangoDBIndex> getEdgeIndices(ArangoDBSimpleGraph graph) throws ArangoDBException {
		return getIndices(graph.getEdgeCollection());
	}

	public boolean deleteIndex(String id) throws ArangoDBException {
		String path = "_api/index/" + id;
		deleteRequest(path);

		return true;
	}

	/**
	 * #########################################################################
	 * ######
	 */

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

	private ArangoDBIndex createIndex(String collectionName, String type, boolean unique, Vector<String> fields)
			throws ArangoDBException {
		JSONObject o = new JSONObject();
		JSONArray a = new JSONArray(fields);

		try {
			o.put("type", type);
			o.put("unique", unique);
			o.put("fields", a);
		} catch (JSONException e) {
			e.printStackTrace();
			throw new ArangoDBException("Error in JSON: " + e.getMessage());
		}

		String path = "_api/index?collection=" + urlEncode(collectionName);

		JSONObject result = postRequest(path, o);

		return new ArangoDBIndex(result);
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

		String path = "_api/index?collection=" + urlEncode(collectionName);
		JSONObject result = getRequest(path);

		if (result == null) {
			throw new ArangoDBException("JSON data for index list is empty.");
		}

		try {
			if (result.has("indexes")) {
				JSONArray a = result.getJSONArray("indexes");
				for (int i = 0; i < a.length(); i++) {
					indices.add(new ArangoDBIndex(a.getJSONObject(i)));
				}
			} else {
				throw new ArangoDBException("JSON data for index list has no 'indexes' attribute.");
			}

		} catch (JSONException e) {
			throw new ArangoDBException("Error in JSON data. " + e.getMessage());
		}

		return indices;
	}

	/**
	 * Execute POST request
	 * 
	 * @param path
	 *            the request path
	 * @param body
	 *            an optional request body
	 * @return JSONObject the request result
	 * @throws ArangoDBException
	 */

	public JSONObject postRequest(String path, JSONObject body) throws ArangoDBException {
		return request(RequestType.POST, path, body == null ? null : body.toString());
	}

	public JSONObject postRequest(String path, String body) throws ArangoDBException {
		return request(RequestType.POST, path, body);
	}

	/**
	 * Execute PUT request
	 * 
	 * @param path
	 *            the request path
	 * @param body
	 *            an optional request body
	 * @return JSONObject the request result
	 * @throws ArangoDBException
	 */

	public JSONObject putRequest(String path, JSONObject body) throws ArangoDBException {
		return request(RequestType.PUT, path, body == null ? null : body.toString());
	}

	/**
	 * Execute GET request
	 * 
	 * @param path
	 *            the request path
	 * @return JSONObject the request result
	 * @throws ArangoDBException
	 */

	public JSONObject getRequest(String path) throws ArangoDBException {
		return request(RequestType.GET, path, null);
	}

	/**
	 * Execute DELETE request
	 * 
	 * @param path
	 *            the request path
	 * @return JSONObject the request result
	 * @throws ArangoDBException
	 */

	public JSONObject deleteRequest(String path) throws ArangoDBException {
		return request(RequestType.DELETE, path, null);
	}

	/**
	 * private methods
	 */

	/**
	 * Execute request and return result as a JSON object.
	 * 
	 * @param type
	 *            the request type (one of "GET", "POST", "PUT", "DELETE")
	 * @param path
	 *            the request path
	 * @param body
	 *            an optional request body (only for "POST" and "PUT")
	 * @return JSONObject the request result
	 * @throws ArangoDBException
	 */

	private JSONObject request(RequestType type, String path, String body) throws ArangoDBException {

		HttpResponse response = null;
		try {

			path = configuration.getBaseUrl() + "/" + path;
			LOG.debug("-----------------------------------");
			HttpRequestBase request = null;
			switch (type) {
			case GET:
				LOG.debug("Request: GET " + path);
				request = new HttpGet(path);
				break;
			case POST:
				LOG.debug("Request: POST " + path);
				HttpPost post = new HttpPost(path);
				if (body != null) {
					LOG.debug("Request-body: " + body);
					post.setEntity(new StringEntity(body, "application/json", "utf-8"));
				}
				request = post;
				break;
			case PUT:
				LOG.debug("Request: PUT " + path);
				HttpPut put = new HttpPut(path);
				if (body != null) {
					LOG.debug("Request-body: " + body);
					put.setEntity(new StringEntity(body, "application/json", "utf-8"));
				}
				request = put;
				break;
			case DELETE:
				LOG.debug("Request: DELETE " + path);
				request = new HttpDelete(path);
				break;
			}

			request.setHeader("User-Agent", "Mozilla/5.0 (compatible; ArangoDB-Simple-Graph-Client 1.0)");

			response = httpClient.execute(request);
			if (response == null) {
				return null;
			}

			// http status
			StatusLine status = response.getStatusLine();
			LOG.debug("Result: " + status.getStatusCode() + " " + status.getReasonPhrase());

			HttpEntity entity = response.getEntity();
			if (entity != null) {
				Reader tempStreamReader = new InputStreamReader(entity.getContent());
				StringBuilder tempStringBuilder = new StringBuilder();

				int data = tempStreamReader.read();
				while (data != -1) {
					char tempChar = (char) data;
					tempStringBuilder.append(tempChar);
					data = tempStreamReader.read();
				}

				String tempString = tempStringBuilder.toString();

				LOG.debug("Result-body:  " + tempString);

				JSONObject o = new JSONObject(tempString);

				if (status.getStatusCode() > 299) {
					// LOG.error("got code " + status.getStatusCode() +
					// " after: " + path);
					if (o.has("errorNum") && o.has("errorMessage")) {
						throw new ArangoDBException(o.getString("errorMessage"), o.getInt("errorNum"));
					}
				}

				return o;
			}

			LOG.error("no result after: " + path);
			throw new ArangoDBException("no result");
		} catch (ClientProtocolException e) {
			LOG.error("ClientProtocolException after: " + path, e);
			e.printStackTrace();
			throw new ArangoDBException("URL request error: " + e.getMessage());
		} catch (IOException e) {
			LOG.error("IOException after: " + path, e);
			e.printStackTrace();
			throw new ArangoDBException("URL request error: " + e.getMessage());
		} catch (JSONException e) {
			LOG.error("JSONException after: " + path, e);
			e.printStackTrace();
			throw new ArangoDBException("Error in request result: " + e.getMessage());
		}
	}

	private String urlEncode(String value) {
		try {
			return URLEncoder.encode(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return value;
		}
	}

	private JSONObject getResultJsonObject(JSONObject requestResult, String attributeName) throws ArangoDBException {
		// first test for error result
		boolean hasError = true;
		try {
			if (requestResult.has("error")) {
				hasError = requestResult.getBoolean("error");
			}
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		if (hasError) {
			// found error
			try {
				if (requestResult.has("errorMessage")) {
					throw new ArangoDBException(requestResult.getString("errorMessage"));
				}
				throw new ArangoDBException("Error in request result");

			} catch (JSONException e) {
				throw new ArangoDBException("JSON error: " + e.getMessage());
			}
		}

		try {
			if (requestResult.has(attributeName)) {
				return requestResult.getJSONObject(attributeName);
			}
		} catch (JSONException e) {
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}

		throw new ArangoDBException("Attribute '" + attributeName + "' not found in request result.");
	}

	public ArangoDBConfiguration getConfiguration() {
		return configuration;
	}
}
