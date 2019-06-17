package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDatabase;

/**
 * This interface defines the API to retrieve information from an ArangDB server, in particular, Databases.
 */
public interface ServerClient {

    class DatabaseCreationException extends Exception {
        public DatabaseCreationException(String message) {
            super(message);
        }
    }

    /**
     * Get the ArangoDB database with the given name
     * @param name                  the database name
     * @return
     */
    ArangoDatabase getDatabase(String name);

    /**
     * Create the ArangoDB with the given name
     * @param name
     * @return
     * @throws DatabaseCreationException
     */
    ArangoDatabase createDatabase(String name) throws DatabaseCreationException;
}
