//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.Map;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphException;


/**
 * The ArangoDB simple edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
@Deprecated
public class ArangoDBSimpleEdge extends ArangoDBBaseDocument {


	
	private String label;

	/**
	 * Creates a new edge by a JSON document
	 * 
	 * @param properties
	 *            The JSON document
	 * 
	 * @throws ArangoDBGraphException
	 *             if an error occurs
	 */
	public ArangoDBSimpleEdge(final String label, final String fromVertexId, final String toVertexId, Map<String, Object> properties) {
		super(properties);
		this._arango_from = fromVertexId;
		this._arango_to = toVertexId;
		this.label = label;
	}

	/**
	 * Returns the identifier of the "to" vertex
	 * 
	 * @return the identifier of the "to" vertex
	 */
	public String getToVertexId() {
		return _arango_to;
	}

	/**
	 * Returns the identifier of the "from" vertex
	 * 
	 * @return the identifier of the "from" vertex
	 */
	public String getFromVertexId() {
		return _arango_to;
	}

	/**
	 * Returns the edge label
	 * 
	 * @return the edge label
	 */
	public String getLabel() {
		return label;
	}

}
