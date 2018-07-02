//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.EdgeDefinition;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphException;

/**
 * This class is used to rename attributes of the vertices and edges to support
 * names starting with a '_' character.
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBUtil {
	
	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBUtil.class);

	/**
	 * Instantiates a new ArangoDB Util.
	 */
	private ArangoDBUtil() {
		// this is a helper class
	}

	/**
	 * Since attributes that start with underscore are considered to be system attributes (), 
	 * rename key "_XXXX" to "«a»XXXX" for storage.
	 *
	 * @param key            the key to convert
	 * @return String the converted String
	 * @see <a href="https://docs.arangodb.com/3.3/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
	 */
	public static String normalizeKey(String key) {
		if (key.charAt(0) == '_') {
			return "«a»" + key.substring(1);
		}
		return key;
	}

	/**
	 * Since attributes that start with underscore are considered to be system attributes (), 
	 * rename Attribute "«a»XXXX" to "_XXXX" for retreival.
	 *
	 * @param key            the key to convert
	 * @return String the converted String
	 * @see <a href="https://docs.arangodb.com/3.3/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
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
	 * collection:from-&gt;to
	 * </pre>
	 * Where collection is the name of the Edge collection, and to and from are comma separated list of
	 * node collection names.
	 *
	 * @param relation the relation
	 * @param value 
	 * @return an EdgeDefinition that represents the relation.
	 * @throws ArangoDBGraphException the arango DB graph exception
	 */
	public static EdgeDefinition relationPropertyToEdgeDefinition(String graphName, String relation) throws ArangoDBGraphException {
		logger.info("Creating EdgeRelation from {}", relation);
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
		List<String> trimmed = Arrays.stream(info[0].split(","))
				.map(String::trim)
				.map(c -> getCollectioName(graphName, c))
				.collect(Collectors.toList());
		String[] from = new String[trimmed.size()];
		from = trimmed.toArray(from);
		
		trimmed = Arrays.stream(info[1].split(","))
				.map(String::trim)
				.map(c -> getCollectioName(graphName, c))
				.collect(Collectors.toList());
		String[] to = new String[trimmed.size()];
		to = trimmed.toArray(to);
		result.from(from).to(to);
		return result;
	}
	

	public static EdgeDefinition createDefaultEdgeDefinition(
			String graphName,
			List<String> verticesCollectionNames,
			List<String> edgesCollectionNames) {
		EdgeDefinition ed = new EdgeDefinition()
				.collection(getCollectioName(graphName, edgesCollectionNames.get(0)))
				.from(getCollectioName(graphName, verticesCollectionNames.get(0)))
				.to(getCollectioName(graphName, verticesCollectionNames.get(0)));
		return ed;
	}

	public static String getCollectioName(String graphName, String collectionName) {
		return String.format("%s_%s", graphName, collectionName);
	}
	

}
