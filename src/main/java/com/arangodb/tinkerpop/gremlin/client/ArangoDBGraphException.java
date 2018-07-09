package com.arangodb.tinkerpop.gremlin.client;

public class ArangoDBGraphException extends RuntimeException {

    private static final long serialVersionUID = -8478050406116402002L;

    public ArangoDBGraphException(String message, Throwable cause) {
        super(message, cause);
    }

    public ArangoDBGraphException(String message) {
        super(message);
    }

    public ArangoDBGraphException(Throwable cause) {
        super(cause);
    }

}
