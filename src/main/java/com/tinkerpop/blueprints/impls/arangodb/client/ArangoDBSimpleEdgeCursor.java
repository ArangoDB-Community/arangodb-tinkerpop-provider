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
 * The arangodb database edge cursor class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBSimpleEdgeCursor extends ArangoDBBaseCursor {

	/**
	 * Creates an edge cursor
	 * 
	 * @param client
	 *            The graph client
	 * @param json
	 *            The initial JSON document
	 */
	public ArangoDBSimpleEdgeCursor(ArangoDBSimpleGraphClient client, JSONObject json) {
		super(client, json);
	}

	public ArangoDBSimpleEdge next() {
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
