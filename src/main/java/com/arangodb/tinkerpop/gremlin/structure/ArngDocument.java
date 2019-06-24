package com.arangodb.tinkerpop.gremlin.structure;

/**
 * An Arango Elements represents the basic ArangoDB document
 */
public interface ArngDocument {

    class ElementNotPairedException extends Exception {
        public ElementNotPairedException(String message) {
            super(message);
        }
    }

    /**
     * Retreive the document's handle. If the document is not paired an ElementNotPairedException is thrown.
     * @return  the documents¿s handle
     * @throws ElementNotPairedException    if the document is not paired
     */
    String handle() throws ElementNotPairedException;

    /**
     * Retreive the document's primary key.
     * @return the primary key
     */
    String primaryKey();

    /**
     * Retreive the document's revision. If the document is not paired an ElementNotPairedException is thrown.
     * @return  the documents¿s revision
     * @throws ElementNotPairedException    if the document is not paired
     */
    String revision() throws ElementNotPairedException;

    /**
     * Returns true if the document is paired. A document is paired if it has been persistend or retreived from the DB.
     * @return true if he document is paired.
     */
    boolean isPaired();
}
