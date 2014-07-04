//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

/**
 * The arangodb graph class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBSimpleGraph extends ArangoDBBaseDocument {

	/**
	 * the name of the "edges" attribute (this attribute contains the name of
	 * the edge collection)
	 */
	public static final String _EDGES = "edges";

	/**
	 * the name of the "vertices" attribute (this attribute contains the name of
	 * the edge collection)
	 */
	public static final String _VERTICES = "vertices";

	/**
	 * Creates a graph by a initial JSON document
	 * 
	 * @param properties
	 *            The JSON document
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public ArangoDBSimpleGraph(JSONObject properties) throws ArangoDBException {
		this.properties = properties;
		handleNewGraphAPI();
		checkStdProperties();
		checkHasProperty(_EDGES);
		checkHasProperty(_VERTICES);
	}

	/**
	 * Returns the name of the graph
	 * 
	 * @return the name of the graph
	 */
	public String getName() {
		return getDocumentKey();
	}

	/**
	 * Returns the name of the edge collection
	 * 
	 * @return the name of the edge collection
	 */
	public String getEdgeCollection() {
		return getStringProperty(_EDGES);
	}

	/**
	 * Returns the name of the vertex collection
	 * 
	 * @return the name of the vertex collection
	 */

	public String getVertexCollection() {
		return getStringProperty(_VERTICES);
	}

	/**
	 * handle new graph document format.
	 */
	private void handleNewGraphAPI() {
		if (properties.has("edgeDefinitions")) {
			try {
				JSONArray jsonArray = properties.getJSONArray("edgeDefinitions");
				if (jsonArray.length() > 0) {
					JSONObject jsonObject = jsonArray.getJSONObject(0);
					if (jsonObject.has("collection")) {
						properties.put(_EDGES, jsonObject.getString("collection"));
					}
					if (jsonObject.has("from")) {
						JSONArray jsonArray2 = jsonObject.getJSONArray("from");
						if (jsonArray2.length() > 0) {
							properties.put(_VERTICES, jsonArray2.getString(0));
						}
					}

				}
			} catch (Exception e) {
			}
			properties.remove("edgeDefinitions");
		}
	}

}
