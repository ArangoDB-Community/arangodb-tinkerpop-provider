//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONObject;

public class ArangoDBSimpleEdgeCursor extends ArangoDBBaseCursor {

	public ArangoDBSimpleEdgeCursor(ArangoDBSimpleGraphClient client,
			JSONObject json) {
		super(client, json);
	}

	public ArangoDBSimpleEdge next () {
		Object o = super.next();
		
		if (o instanceof JSONObject) {
			try {
				return new ArangoDBSimpleEdge((JSONObject) o);
			} catch (ArangoDBException e) {
			}
		}
		
		return null;		
	}	
	
}
