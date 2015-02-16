//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONObject;

import com.arangodb.entity.EdgeDefinitionEntity;
import com.arangodb.entity.GraphEntity;

public class ArangoDBSimpleGraph extends ArangoDBBaseDocument {

//	public static final String _EDGES = "edges";
//	public static final String _VERTICES = "vertices";
	
	private GraphEntity graphEntity;
	private String edgeCollectionName;
	private String vertexCollectionName;
	
//    public ArangoDBSimpleGraph (JSONObject properties) throws ArangoDBException {
//    	this.properties = properties;
//    	checkStdProperties();
//    	checkHasProperty(_EDGES);
//    	checkHasProperty(_VERTICES);
//    }
    
    public ArangoDBSimpleGraph (GraphEntity graphEntity) {
    	this.graphEntity = graphEntity;
    	// may only have 1 edge definition entity
    	EdgeDefinitionEntity edgeDefinitionEntity = graphEntity.getEdgeDefinitions().get(0);
    	this.edgeCollectionName = edgeDefinitionEntity.getCollection();
    	// may only have one vertex collection
    	this.vertexCollectionName = edgeDefinitionEntity.getFrom().get(0);
    }
	
	public String getName() {
		return this.graphEntity.getName();
//		return getDocumentKey();
	}

	public String getEdgeCollection() {
		return this.edgeCollectionName;
//		return getStringProperty(_EDGES);
	}

	public String getVertexCollection() {
		return this.vertexCollectionName;
//		return getStringProperty(_VERTICES);
	}	
	
}
