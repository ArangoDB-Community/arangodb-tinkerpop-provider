package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;

public class ArngServerClient implements ServerClient {

    private final ArangoDB delegate;


    public ArngServerClient(ArangoDB delegate) {
        this.delegate = delegate;
    }

    @Override
    public ArangoDatabase getDatabase(String name) {
        return delegate.db(name);
    }

    @Override
    public ArangoDatabase createDatabase(String name) throws DatabaseCreationException {
        try {
            if (!delegate.createDatabase(name)) {
                throw new DatabaseCreationException("Unable to crate the database " + name);
            }
        }
        catch (ArangoDBException ex) {
            throw ArangoDBExceptions.getArangoDBException(ex);
        }
        return getDatabase(name);
    }
}
