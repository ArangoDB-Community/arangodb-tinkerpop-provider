//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;

/**
 * The Interface ArngElement.
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
public interface ArngElement extends Element {

    /**
	 *  Remove the primaryKey from the element's vertexProperties.
	 *
	 * @param property the property
	 */
	void removeProperty(ArangoDBElementProperty<?> property);

	/**
	 * Attach the properties to the element.
	 * This method should be called after vertex/edge elements have been deserialized.
	 */

	<V> void attachProperties(Iterator<Property<V>> properties);

}
