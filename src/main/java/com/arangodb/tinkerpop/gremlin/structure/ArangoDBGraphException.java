//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.io.IOException;

import com.arangodb.ArangoDBException;

/**
 * The ArangoDB graph exceptions class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBGraphException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4664554827966045400L;

	/**
	 * Creates a graph exception with a message
	 * 
	 * @param message
	 *            the message of the exception
	 */
	public ArangoDBGraphException(String message) {
		super(message);
	}

	/**
	 * Creates a graph exception with a message
	 * 
	 * @param cause
	 *            the cause of the exception
	 */
	public ArangoDBGraphException(Exception cause) {
		super(cause);
	}


	public ArangoDBGraphException(String message, Exception cause) {
		super(message, cause);
	}


}
