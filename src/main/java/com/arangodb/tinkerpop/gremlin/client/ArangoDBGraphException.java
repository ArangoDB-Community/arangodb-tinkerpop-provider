//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

/**
 * The ArangoDBGraphException is used to signal all exceptions related to the graph operations.
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBGraphException extends RuntimeException {

    /** The Constant serialVersionUID. */
	
    private static final long serialVersionUID = -8478050406116402002L;
    
    private int errorCode;
    
    /**
     * Instantiates a new Arango DB graph exception.
     *
     * @param errorCode             the ArangoDB error errorCode
     * @param message 	            the exception message
     * @param cause 	            the exception cause
     */
    
    public ArangoDBGraphException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Instantiates a new Arango DB graph exception.
     *
     * @param message the message
     * @param cause the cause
     */
    
    public ArangoDBGraphException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Instantiates a new Arango DB graph exception.
     *
     * @param errorCode 		    the ArangoDB error errorCode
     * @param message 	            the exception message
     */
    
    public ArangoDBGraphException(int errorCode, String message) {
        super(message);
    }

    /**
     * Instantiates a new Arango DB graph exception.
     *
     * @param message the message
     */
    
    public ArangoDBGraphException(String message) {
        super(message);
    }

    /**
     * Instantiates a new Arango DB graph exception.
     *
     * @param cause the cause
     */
    
    public ArangoDBGraphException(Throwable cause) {
        super(cause);
    }

	public int getErrorCode() {
		return errorCode;
	}

}
