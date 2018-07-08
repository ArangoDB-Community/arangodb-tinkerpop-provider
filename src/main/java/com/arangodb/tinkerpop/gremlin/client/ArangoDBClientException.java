package com.arangodb.tinkerpop.gremlin.client;

public class ArangoDBClientException extends RuntimeException {

    private static final long serialVersionUID = -8478050406116402002L;

    public ArangoDBClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ArangoDBClientException(String message) {
        super(message);
    }

    public ArangoDBClientException(Throwable cause) {
        super(cause);
    }

}
