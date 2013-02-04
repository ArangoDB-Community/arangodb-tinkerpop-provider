//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONObject;

public class ArangoDBSimpleEdge extends ArangoDBBaseDocument {

	public static final String _TO = "_to";
	public static final String _FROM = "_from";
	public static final String _LABEL = "$label";
	
    public ArangoDBSimpleEdge (JSONObject properties) throws ArangoDBException {
    	this.properties = properties;
    	checkStdProperties();
    	checkHasProperty(_TO);
    	checkHasProperty(_FROM);
    }
	
	public String getName() {
		return getDocumentKey();
	}

	public String getToVertexId() {
		return getStringProperty(_TO);
	}

	public String getFromVertexId() {
		return getStringProperty(_FROM);
	}	

	public String getLabel() {
		return getStringProperty(_LABEL);
	}	
	
}
