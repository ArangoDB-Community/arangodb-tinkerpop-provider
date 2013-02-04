//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb;

/**
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 */

public class ArangoDBGraphException extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4664554827966045400L;

	/**
     */

    public ArangoDBGraphException(String message) {
        super(message);
    }
	
}
