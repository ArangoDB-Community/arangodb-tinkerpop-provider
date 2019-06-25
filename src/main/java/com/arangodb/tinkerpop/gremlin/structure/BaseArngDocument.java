//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.entity.DocumentField;
import com.arangodb.velocypack.annotations.Expose;

/**
 * The ArangoDB BaseBaseDocument provides the internal fields required for the driver to correctly
 * serialize and deserialize vertices and edges.
 *
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public abstract class BaseArngDocument implements ArngDocument {

    /** ArangoDB internal handle. */

    protected final String _id;

    /** ArangoDB internal revision. */

    protected String _rev;

    /** ArangoDB internal primaryKey - mapped to TinkerPop's ID. */

    protected final String _key;

    /** The label in which the element is stored. */

    protected final String label;

    /**  Flag to indicate if the element is paired to a document in the DB. */

    protected final boolean paired;

    public BaseArngDocument(
        String id,
        String key,
        String rev,
        String label) {
        this._id = id;
        this._key = key;
        this._rev = rev;
        this.label = label;
        this.paired = (id != null) && (rev != null);
    }

    @Override
    public final String handle() throws ElementNotPairedException {
        if (!paired) {
            throw new ElementNotPairedException("Id of an unpaired element can't be accessed");
        }
        return _id;
    }

    @Override
    public final String primaryKey() {
        return _key;
    }

    @Override
    public final String revision() throws ElementNotPairedException {
        if (!paired) {
            throw new ElementNotPairedException("Id of an unpaired element can't be accessed");
        }
        return _rev;
    }

    @Override
    public void revision(String newRev) {
        this._rev = newRev;
    }

    /**
     * Get the document's label.
     *
     * @return the label
     */

    @Override
    public final String label() {
        return label;
    }

    /**
     * Checks if the document is paired.
     *
     * @return true, if is paired
     */

    @Override
    public final boolean isPaired() {
        return paired;
    }

    
}
