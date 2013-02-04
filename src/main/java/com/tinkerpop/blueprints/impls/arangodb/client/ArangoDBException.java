//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

public class ArangoDBException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Integer errorNum;
	
	/**
     */

    public ArangoDBException(String message) {
        super(message);
        this.errorNum = 0;
    }

	/**
     */

    public ArangoDBException(String message, Integer errorNum) {
        super(message);
        this.errorNum = errorNum;
    }
    
    public Integer errorNumber () {
    	return this.errorNum;
    }
    
}
