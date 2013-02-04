//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONObject;

public class ArangoDBSimpleVertex extends ArangoDBBaseDocument {

    public ArangoDBSimpleVertex (JSONObject properties) throws ArangoDBException {
    	this.properties = properties;
    	checkStdProperties();
    }
	
}
