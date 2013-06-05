package com.tinkerpop.blueprints.impls.arangodb;

import java.util.HashSet;
import java.util.Set;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBBaseDocument;
import com.tinkerpop.blueprints.impls.arangodb.client.ArangoDBException;
import com.tinkerpop.blueprints.impls.arangodb.utils.ArangoDBUtil;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

abstract public class ArangoDBElement implements Element {

	/**
	 * the graph of the document
	 */
	
	protected ArangoDBGraph graph;

	/**
	 * the vertex/edge document
	 */
	
	protected ArangoDBBaseDocument document;
	
	/**
	 * true if the element was changed
	 */
	
	protected boolean changed = false; 
	
	/**
	 * Save the vertex or the edge in ArangoDB 
	 */
	
	abstract public void save () throws ArangoDBException;
	
	/**
	 * @inheritDoc
	 */
	
	public Object getProperty(String key) {
		return document.getProperty(ArangoDBUtil.normalizeKey(key));
	}
	
	/**
	 * Set/Reset the vertex/edge document
	 */
	
	public void setDocument (ArangoDBBaseDocument document) {
		this.document = document;
	}

	public Set<String> getPropertyKeys() {
		Set<String> ps = document.getPropertyKeys();		
		HashSet<String> result = new HashSet<String>(); 
		for (String key: ps) {
			result.add(ArangoDBUtil.denormalizeKey(key));
		}
		return result;
	}

	public void setProperty(String key, Object value) {
		
        if (key == null || key.equals(StringFactory.EMPTY_STRING))
            throw ExceptionFactory.propertyKeyCanNotBeEmpty();
        if (key.equals(StringFactory.ID))
            throw ExceptionFactory.propertyKeyIdIsReserved();
		
		try {
			document.setProperty(ArangoDBUtil.normalizeKey(key), value);	
			graph.addChangedElement(this);
		} catch (ArangoDBException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	public Object removeProperty(String key) {
        if (key == null || key.equals(StringFactory.EMPTY_STRING))
            throw ExceptionFactory.propertyKeyCanNotBeEmpty();
        if (key.equals(StringFactory.ID))
            throw ExceptionFactory.propertyKeyIdIsReserved();
        if (key.equals(StringFactory.LABEL) && this instanceof Edge)
            throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
        
		Object o = null;
		try {
			o = document.removeProperty(ArangoDBUtil.normalizeKey(key));
			graph.addChangedElement(this);
		} catch (ArangoDBException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		return o;
	}

	public Object getId() {
		return document.getDocumentKey();
	}
	
	public ArangoDBBaseDocument getRaw () {
		return document;
	}
	
}
