package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;

public interface ArangoDBElement extends Element {
	
	/**
	 * Persist changes in the element to the DB.
	 */
	void save();
	
	
	Set<String> propertiesKeys();
	
	
    /**
	 *  Remove the key from the element's properties.
	 *
	 * @param property the property
	 */
	void removeProperty(ArangoDBElementProperty<?> property);
	

}
