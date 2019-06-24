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
     * Retrieve the document's handle. If the document is not paired an ElementNotPairedException is thrown.
     * @return  the documents¿s handle
     * @throws ElementNotPairedException    if the document is not paired
     */

    String handle() throws ElementNotPairedException;

    /**
     * Retrieve the document's primary key.
     * @return the primary key
     */

    String primaryKey();

    /**
     * Retrieve the document's revision. If the document is not paired an ElementNotPairedException is thrown.
     * @return  the documents¿s revision
     * @throws ElementNotPairedException    if the document is not paired
     */

    String revision() throws ElementNotPairedException;

    /**
     * Every time a document is updated a new revision is provided. Since the Tinkerpop API is not geared towards
     * immutability, we must be able to change the revision number without creating a new element.
     * @param newRev
     */

    void revision(String newRev);

    /**
     * Retrieve the documents label.
     * @return
     */
    String label();

    /**
     * Returns true if the document is paired. A document is paired if it has been persistend or retreived from the DB.
     * @return true if he document is paired.
     */
    boolean isPaired();
}
