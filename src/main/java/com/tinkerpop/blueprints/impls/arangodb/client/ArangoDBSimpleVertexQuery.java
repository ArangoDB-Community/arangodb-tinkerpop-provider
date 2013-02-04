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

public class ArangoDBSimpleVertexQuery extends ArangoDBBaseQuery {
	
	public ArangoDBSimpleVertexQuery(ArangoDBSimpleGraphClient client,
			String requestPath, ArangoDBPropertyFilter propertyFilter,
			List<String> labelsFilter, Direction direction, Long limit,
			boolean count) throws ArangoDBException {
		super(client, requestPath, propertyFilter, labelsFilter, direction, limit,
				count);
		// TODO Auto-generated constructor stub
	}

	public ArangoDBSimpleVertexCursor getResult() throws ArangoDBException {
		
		JSONObject json = client.postRequest(requestPath, queryBody);
		
		return new ArangoDBSimpleVertexCursor(client, json);
	}
	
}
