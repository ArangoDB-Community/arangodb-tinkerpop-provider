//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONObject;

public class ArangoDBSimpleVertexCursor extends ArangoDBBaseCursor {

	public ArangoDBSimpleVertexCursor(ArangoDBSimpleGraphClient client,
			JSONObject json) {
		super(client, json);
	}

	public ArangoDBSimpleVertex next () {
		Object o = super.next();
		
		if (o == null) {
			return null;
		}
		
		if (o instanceof JSONObject) {
			try {
				return new ArangoDBSimpleVertex((JSONObject) o);
			} catch (ArangoDBException e) {
			}
		}
		
		return null;		
	}	
	
}
