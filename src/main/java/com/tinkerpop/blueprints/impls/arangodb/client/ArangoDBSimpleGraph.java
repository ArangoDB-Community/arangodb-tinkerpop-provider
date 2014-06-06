//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

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

}
