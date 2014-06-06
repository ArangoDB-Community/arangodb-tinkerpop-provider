//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import java.util.List;

import org.codehaus.jettison.json.JSONObject;

/**
 * The arangodb edge query class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBSimpleEdgeQuery extends ArangoDBBaseQuery {

	/**
	 * Creates an edge query
	 * 
	 * @param client
	 *            The graph client
	 * @param requestPath
	 *            The request path
	 * @param propertyFilter
	 *            The property filter
	 * @param labelsFilter
	 *            The labels filter
	 * @param direction
	 *            The direction
	 * @param limit
	 *            The maximum number of results
	 * @param count
	 *            True, if the database should calculate the number of total
	 *            results
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public ArangoDBSimpleEdgeQuery(ArangoDBSimpleGraphClient client, String requestPath,
			ArangoDBPropertyFilter propertyFilter, List<String> labelsFilter, Direction direction, Long limit,
			boolean count) throws ArangoDBException {
		super(client, requestPath, propertyFilter, labelsFilter, direction, limit, count);
	}

	public ArangoDBSimpleEdgeCursor getResult() throws ArangoDBException {

		JSONObject json = client.postRequest(requestPath, queryBody);

		return new ArangoDBSimpleEdgeCursor(client, json);
	}

}
