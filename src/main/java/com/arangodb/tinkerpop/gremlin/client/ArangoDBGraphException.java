//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

/**
 * The ArangoDBGraphException is used to signal all exceptions related to the graph operations.
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBGraphException extends RuntimeException {

    /** The Constant serialVersionUID. */
	
    private static final long serialVersionUID = -8478050406116402002L;
    
    private int code;
    
    /**
     * Instantiates a new Arango DB graph exception.
     *
     * @param code 		the ArangoDB error code
     * @param message 	the message
     * @param cause 	the cause
     */
    
    public ArangoDBGraphException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
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
     * @param code 		the ArangoDB error code
     * @param message 	the message
     */
    
    public ArangoDBGraphException(int code, String message) {
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

	public int getCode() {
		return code;
	}

}
