//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.utils;

import com.arangodb.entity.EdgeDefinition;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphException;

/**
 * This class is used to rename attributes of the vertices and edges to support
 * names starting with a '_' character.
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 */

public class ArangoDBUtil {

	private ArangoDBUtil() {
		// this is a helper class
	}

	/**
	 * Since attributes that start with underscore are considered to be system attributes (), 
	 * rename key "_XXXX" to "«a»XXXX" for storage
	 * 
	 * @see <a href="https://docs.arangodb.com/3.3/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
	 * @param key
	 *            the key to convert
	 * @return String the converted String
	 */
	public static String normalizeKey(String key) {
		if (key.charAt(0) == '_') {
			return "," + key.substring(1);
		}
		return key;
	}

	/**
	 * Since attributes that start with underscore are considered to be system attributes (), 
	 * rename Attribute "«a»XXXX" to "_XXXX" for retreival.
	 * 
	 * @see <a href="https://docs.arangodb.com/3.3/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
	 * @param key
	 *            the key to convert
	 * @return String the converted String
	 */
	public static String denormalizeKey(String key) {
		if (key.startsWith("«a»")) {
			return "_" + key.substring(3);
		}
		return key;
	}
	
	/**
	 * Create an EdgeDefinition from a relation in the Configuration. The format of a relation is:
	 * <pre>
	 * collection:from->to
	 * </pre>
	 * Where collection is the name of the Edge collection, and to and from are comma separated list of
	 * node collection names.
	 * 
	 * @param relation
	 * @return an EdgeDefinition that represents the relation.
	 * @throws ArangoDBGraphException
	 */
	public static EdgeDefinition relationPropertyToEdgeDefinition(String relation) throws ArangoDBGraphException {
		EdgeDefinition result = new EdgeDefinition();
		String[] info = relation.split(":");
		if (info.length != 2) {
			throw new ArangoDBGraphException("Error in configuration. Malformed relation> " + relation);
		}
		result.collection(info[0]);
		info = info[1].split("->");
		if (info.length != 2) {
			throw new ArangoDBGraphException("Error in configuration. Malformed relation> " + relation);
		}
		String[] from = info[0].split(",");
		String[] to = info[1].split(",");
		result.from(from).to(to);
		return result;
	}

}
