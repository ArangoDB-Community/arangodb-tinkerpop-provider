//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * The Interface ArangoDBElement.
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
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

    /**
     * Graph.
     *
     * @param graph the graph
     */
    void graph(ArangoDBGraph graph);

    /**
     * Sets the paired.
     *
     * @param paired the new paired
     */
    void setPaired(boolean paired);
}
