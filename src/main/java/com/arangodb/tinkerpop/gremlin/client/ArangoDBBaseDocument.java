package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.entity.DocumentField;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.velocypack.annotations.Expose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ArangoDBBaseDocument {

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBBaseDocument.class);

    /** ArangoDB internal id. */

    @DocumentField(DocumentField.Type.ID)
    private String _id;

    /** ArangoDB internal revision. */

    @DocumentField(DocumentField.Type.REV)
    private String _rev;

    /** ArangoDB internal key - mapped to Tinkerpop's ID. */

    @DocumentField(DocumentField.Type.KEY)
    protected String _key;

    /** The collection in which the element is placed. */

    @Expose(serialize = false, deserialize = false)
    protected String collection;

    /** the graph of the document. */

    @Expose(serialize = false, deserialize = false)
    protected ArangoDBGraph graph;

    /**  Flag to indicate if the element is paired to a document in the DB. */

    @Expose(serialize = false, deserialize = false)
    protected boolean paired = false;

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */
    public ArangoDBBaseDocument() {
        super();
    }

    public ArangoDBBaseDocument(String _key) {
        this._key = _key;
    }

    /**
     * Get the Element's ArangoDB Id.
     *
     * @return the id
     */

    public String _id() {
        return _id;
    }

    /**
     * Set the Element's ArangoDB Id.
     *
     * @param id the id
     */

    public void _id(String id) {
        this._id = id;
    }

    /**
     * Get the Element's ArangoDB Key.
     *
     * @return the key
     */

    public String _key() {
        return _key;
    }

    /**
     * Set the Element's ArangoDB Key.
     *
     * @param key the key
     */

    public void _key(String key) {
        this._key = key;
    }

    /**
     * Get the Element's ArangoDB Revision.
     *
     * @return the revision
     */

    public String _rev() {
        return _rev;
    }

    /**
     * Set the Element's ArangoDB Revision.
     *
     * @param rev the revision
     */

    public void _rev(String rev) {
        this._rev = rev;
    }

    /**
     * Collection. If the collection is null (i.e from DB deserialization) the value is recomputed
     * from the element's id.
     *
     * @return the collection
     */

    public String collection() {
        if (collection == null) {
            if (_id != null) {
                logger.debug("Extracting collection name form id.");
                collection = _id.split("/")[0];
                int graphLoc = collection.indexOf('_');
                collection = collection.substring(graphLoc+1);
            }
        }
        return collection;
    }

    /**
     * Collection.
     *
     * @param collection the collection
     */

    public void collection(String collection) {
        this.collection = collection;
    }


    public ArangoDBGraph graph() {
    return graph;
}

    /**
     * Graph.
     *
     * @param graph the graph
     */

    public void graph(ArangoDBGraph graph) {
        this.graph = graph;
    }


    /**
     * Checks if is paired.
     *
     * @return true, if is paired
     */

    public boolean isPaired() {
        return paired;
    }

    /**
     * Sets the paired.
     *
     * @param paired the new paired
     */

    public void setPaired(boolean paired) {
        this.paired = paired;
    }

    public int hashCode() {
        return _id.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        if (null == object)
            return false;

        if (this == object)
            return true;
        if (!(object instanceof ArangoDBBaseDocument))
            return false;
        return this._id().equals(((ArangoDBBaseDocument)object)._id());
    }
}
