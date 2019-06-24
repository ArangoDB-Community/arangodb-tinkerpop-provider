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
 * The Interface ArngElement.
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
public interface ArngElement extends ArngDocument, Element {

    /**
	 *  Remove the primaryKey from the element's vertexProperties.
	 *
	 * @param property the property
	 */
	void removeProperty(ArangoDBElementProperty<?> property);

	/**
	 * Attach all the contained vertexProperties by setting their element/element to this element.
	 * This method should be called after vertex/edge elements have been deserialized.
	 */
//	void attachProperties();


}
