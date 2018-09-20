//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * The ArangoDB base element class (used by edges and vertices). 
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public abstract class AbstractArangoDBElement extends ArangoDBBaseDocument implements ArangoDBElement {

	/**
	 * Constructor used for ArabgoDB JavaBeans serialisation.
	 */
	public AbstractArangoDBElement() {
	    super();
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


	@Override
	public Object id() {
		return _key;
	}

	@Override
	public String label() {
		return collection();
	}

	@Override
	public boolean equals(final Object object) {
		return ElementHelper.areEqual(this, object);
	}

	@Override
	public int hashCode() {
		return ElementHelper.hashCode(this);
	}
}
