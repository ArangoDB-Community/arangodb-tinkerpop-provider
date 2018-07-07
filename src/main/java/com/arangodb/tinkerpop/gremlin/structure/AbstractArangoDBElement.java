package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;
import com.arangodb.velocypack.annotations.Expose;


/**
 * The ArangoDB base element class (used by edges and vertices). 
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public abstract class AbstractArangoDBElement implements ArangoDBElement {
	
	/** The Constant logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractArangoDBElement.class);
	
	/** ArangoDB internal id. */
	
	@DocumentField(Type.ID)
	private String _id;
	
	/** ArangoDB internal key - mapped to Tinkerpop's ID. */

	@DocumentField(Type.KEY)
	private String _key;

	/** ArangoDB internal revision. */
	
	@DocumentField(Type.REV)
	private String _rev;
	
	/** The collection in which the element is placed. */
	
	@Expose(serialize = false, deserialize = false)
	private String collection;

	/** the graph of the document. */

	@Expose(serialize = false, deserialize = false)
	protected ArangoDBGraph graph;
	
	/**  Flag to indicate if the element is paired to a document in the DB. */
	
	@Expose(serialize = false, deserialize = false)
	protected boolean paired = false;
	
	/**
	 * Constructor used for ArabgoDB JavaBeans serialisation.
	 */
	public AbstractArangoDBElement() {
	}
	
	/**
	 * Instantiates a new ArangoDB element.
	 *
	 * @param graph the graph that owns the collection
	 * @param collection the name collection to which the element belongs
	 */
	public AbstractArangoDBElement(ArangoDBGraph graph, String collection) {
		this.graph = graph;
		this.collection = collection;
	}
	
	/**
	 * Instantiates a new ArangoDB element.
	 *
	 * @param graph the graph
	 * @param collection the collection
	 * @param key the key
	 */
	public AbstractArangoDBElement(ArangoDBGraph graph, String collection, String key) {
		this(graph, collection);
		this._key = key;
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
	
	@Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

	@Override
	public Graph graph() {
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

	@Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
	

	@Override
	public Object id() {
		return _key;
	}

	/**
	 * Checks if is paired.
	 *
	 * @return true, if is paired
	 */
	
	public boolean isPaired() {
		return paired;
	}

	@Override
	public String label() {
		return collection();
	}
	
	/**
	 * Sets the paired.
	 *
	 * @param paired the new paired
	 */
	
	public void setPaired(boolean paired) {
		this.paired = paired;
	}


}
