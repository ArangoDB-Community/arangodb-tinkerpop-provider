package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;

public interface ArangoDBElement extends Element {
	
	/**
	 * Persist changes in the element to the DB.
	 */
	void save();

    /**
	 *  Remove the key from the element's properties.
	 *
	 * @param property the property
	 */
	void removeProperty(ArangoDBElementProperty<?> property);

	/**
	 * Attach all the contained properties by setting their element/owner to this element.
	 * This method should be called after vertex/edge elements have been deserialized.
	 */
	void attachProperties();

    void graph(ArangoDBGraph graph);

    void setPaired(boolean paired);
}
