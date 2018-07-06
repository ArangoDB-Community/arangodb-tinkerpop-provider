//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.GraphEntity;
import com.arangodb.model.GraphCreateOptions;
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
		result.collection(getCollectioName(graphName, info[0]));
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
	

	/**
	 * Creates the default edge definitions. When no relations are provided, the graph schema is
	 * assumed to be fully connected, i.e. there is an EdgeDefintion for each possible combination
	 * of Vertex-Edge-Vertex triplets.
	 *
	 * @param graphName the graph name
	 * @param verticesCollectionNames the vertices collection names
	 * @param edgesCollectionNames the edges collection names
	 * @return the list
	 */
	public static List<EdgeDefinition> createDefaultEdgeDefinitions (
			String graphName,
			List<String> verticesCollectionNames,
			List<String> edgesCollectionNames) {
		List<EdgeDefinition> result = new ArrayList<>();
		for (String e : edgesCollectionNames) {
			for (String from : verticesCollectionNames) {
				for (String to : verticesCollectionNames) {
					EdgeDefinition ed = new EdgeDefinition()
						.collection(getCollectioName(graphName, e))
						.from(getCollectioName(graphName, from))
						.to(getCollectioName(graphName, to));	
					result.add(ed);
				}
			}
		}
		return result;
	}
	

	public static String getCollectioName(String graphName, String collectionName) {
		return String.format("%s_%s", graphName, collectionName);
	}
	
	/**
	 * Validate if an existing graph is correctly configured to handle the desired vertex, edges 
	 * and relations.
	 *
	 * @param verticesCollectionNames The names of collections for nodes
	 * @param edgesCollectionNames The names of collections for edges
	 * @param relations The description of edge definitions
	 * @param graph the graph
	 * @param options The options used to create the graph
	 * @throws ArangoDBGraphException the ArangoDB graph exception
	 */
	public static void checkGraphForErrors(List<String> verticesCollectionNames,
			List<String> edgesCollectionNames,
			List<String> relations,
			ArangoGraph graph, GraphCreateOptions options) throws ArangoDBGraphException {
		
		
		List<String> allVertexCollections = verticesCollectionNames.stream()
				.map(vc -> ArangoDBUtil.getCollectioName(graph.name(), vc))
				.collect(Collectors.toList());
		allVertexCollections.addAll(options.getOrphanCollections());
		if (!allVertexCollections.containsAll(graph.getVertexCollections())) {
			throw new ArangoDBGraphException("Not all declared vertex names appear in the graph.");
		}
		GraphEntity ge = graph.getInfo();
        Collection<EdgeDefinition> graphEdgeDefinitions = ge.getEdgeDefinitions();
        if (CollectionUtils.isEmpty(relations)) {
        	// If no relations are defined, vertices and edges can only have one value
        	if ((verticesCollectionNames.size() != 1) || (edgesCollectionNames.size() != 1)) {
        		throw new ArangoDBGraphException("No relations where specified but more than one vertex/edge where defined.");
        	}
        	if (graphEdgeDefinitions.size() != 1) {
        		throw new ArangoDBGraphException("No relations where specified but the graph has more than one EdgeDefinition.");
    		}
        }
        Map<String, EdgeDefinition> requiredDefinitions;
		if (relations.isEmpty()) {
			List<EdgeDefinition> eds = ArangoDBUtil.createDefaultEdgeDefinitions(graph.name(), verticesCollectionNames, edgesCollectionNames);
			requiredDefinitions = eds.stream().collect(Collectors.toMap(ed -> ed.getCollection(), ed -> ed));
		} else {
			requiredDefinitions = new HashMap<>(relations.size());
			for (Object value : relations) {
				EdgeDefinition ed = ArangoDBUtil.relationPropertyToEdgeDefinition(graph.name(), (String) value);
				requiredDefinitions.put(ed.getCollection(), ed);
			}
		}
		Iterator<EdgeDefinition> it = graphEdgeDefinitions.iterator();
        while (it.hasNext()) {
        	EdgeDefinition existing = it.next();
        	if (requiredDefinitions.containsKey(existing.getCollection())) {
        		EdgeDefinition requiredEdgeDefinition = requiredDefinitions.remove(existing.getCollection());
        		HashSet<String> existingSet = new HashSet<String>(existing.getFrom());
        		HashSet<String> requiredSet = new HashSet<String>(requiredEdgeDefinition.getFrom());
        		if (!existingSet.equals(requiredSet)) {
        			throw new ArangoDBGraphException(String.format("The from collections dont match for edge definition %s", existing.getCollection()));
        		}
        		existingSet.clear();
        		existingSet.addAll(existing.getTo());
        		requiredSet.clear();
        		requiredSet.addAll(requiredEdgeDefinition.getTo());
        		if (!existingSet.equals(requiredSet)) {
        			throw new ArangoDBGraphException(String.format("The to collections dont match for edge definition %s", existing.getCollection()));
        		}
        	} else {
        		throw new ArangoDBGraphException(String.format("The graph has a surplus edge definition %s", edgeDefinitionString(existing)));
        	}
        }
	}
	
	public static String edgeDefinitionString(EdgeDefinition ed) {
		return String.format("[%s]: %s->%s", ed.getCollection(), ed.getFrom(), ed.getTo());
	}
}
