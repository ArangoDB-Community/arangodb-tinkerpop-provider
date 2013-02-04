//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONObject;

public class ArangoDBSimpleGraph extends ArangoDBBaseDocument {

	public static final String _EDGES = "edges";
	public static final String _VERTICES = "vertices";
	
    public ArangoDBSimpleGraph (JSONObject properties) throws ArangoDBException {
    	this.properties = properties;
    	checkStdProperties();
    	checkHasProperty(_EDGES);
    	checkHasProperty(_VERTICES);
    }
	
	public String getName() {
		return getDocumentKey();
	}

	public String getEdgeCollection() {
		return getStringProperty(_EDGES);
	}

	public String getVertexCollection() {
		return getStringProperty(_VERTICES);
	}	
	
}
