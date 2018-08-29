//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.utils;

import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.GraphEntity;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdgeProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElementProperty.ElementHasProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBPropertyProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertexProperty;

/**
 * This class is used to rename attributes of the vertices and edges to support
 * names starting with a '_' character.
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
//FIXME We should add more util methods to validate attribute names, e.g. scape ".".
public class ArangoDBUtil {
	
	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBUtil.class);

    public static final String GRAPH_VARIABLES_COLLECTION = "GRAPH_VARIABLES";
    public static final String ELEMENT_PROPERTIES_COLLECTION = "ELEMENT_PROPERTIES";
    public static final String ELEMENT_PROPERTIES_EDGE = "ELEMENT_HAS_PROPERTIES";

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
	 * rename Attribute "«a»XXXX" to "_XXXX" for retrieval.
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
	 * The prefix to denote that a collection is a hidden collection.
	 */
	private final static String HIDDEN_PREFIX = "adbt_";
	private static final int HIDDEN_PREFIX_LENGTH = HIDDEN_PREFIX.length();

	/**
	 * Hidden keys, labels, etc. are prefixed in Tinkerpop with  @link Graph.Hidden.HIDDEN_PREFIX). Since in ArangoDB
	 * collection names must always start with a letter, this method normalizes Hidden collections name to valid
	 * ArangoDB names by replacing the "~" with
	 *
	 * @param key the key to convert
	 * @return String the converted String
	 * @see <a href="https://docs.arangodb.com/3.3/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
	 */
	public static String normalizeCollection(String key) {
		String nname = isHidden(key) ? key : HIDDEN_PREFIX.concat(key);
		if (!NamingConventions.COLLECTION.hasValidNameSize(nname)) {
			throw ArangoDBGraphClient.ArangoDBExceptions.getNamingConventionError(ArangoDBGraphClient.ArangoDBExceptions.NAME_TO_LONG, key);
		}
		return nname;
	}

	/**
	 * Since attributes that start with underscore are considered to be system attributes (), 
	 * rename Attribute "«a»XXXX" to "_XXXX" for retreival.
	 *
	 * @param key            the key to convert
	 * @return String the converted String
	 * @see <a href="https://docs.arangodb.com/3.3/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
	 */
	public static String denormalizeCollection(String key) {
		return isHidden(key) ? key.substring(HIDDEN_PREFIX_LENGTH) : key;
	}

	public enum NamingConventions {
		COLLECTION(64), KEY(256);

		private int maxLength;

		NamingConventions(int maxLength) {
			this.maxLength = maxLength;
		}

		public boolean hasValidNameSize(String name) {
			final byte[] utf8Bytes;
			try {
				utf8Bytes = name.getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				return false;
			}
			return utf8Bytes.length <= maxLength;
		}
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
	 * @param graphName the name of the graph
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
		if (!graph.getVertexCollections().containsAll(allVertexCollections)) {
			Set<String> avc = new HashSet<>(allVertexCollections);
			avc.removeAll(graph.getVertexCollections());
			throw new ArangoDBGraphException("Not all declared vertex names appear in the graph. Missing " + avc);
		}
		GraphEntity ge = graph.getInfo();
        Collection<EdgeDefinition> graphEdgeDefinitions = ge.getEdgeDefinitions();
        if (CollectionUtils.isEmpty(relations)) {
        	// If no relations are defined, vertices and edges can only have one value
        	if ((verticesCollectionNames.size() != 1) || (edgesCollectionNames.size() != 1)) {
        		throw new ArangoDBGraphException("No relations where specified but more than one vertex/edge where defined.");
        	}
        	if (graphEdgeDefinitions.size() != 2) {		// There is always a edgeDefinition for ELEMENT_HAS_PROPERTIES
        		throw new ArangoDBGraphException("No relations where specified but the graph has more than one EdgeDefinition.");
    		}
        }
        Map<String, EdgeDefinition> requiredDefinitions;
        final Collection<EdgeDefinition> eds = new ArrayList<>();
        if (relations.isEmpty()) {
			eds.addAll(ArangoDBUtil.createDefaultEdgeDefinitions(graph.name(), verticesCollectionNames, edgesCollectionNames));

		} else {
			for (Object value : relations) {
				EdgeDefinition ed = ArangoDBUtil.relationPropertyToEdgeDefinition(graph.name(), (String) value);
				eds.add(ed);
			}
		}
        eds.add(ArangoDBUtil.createPropertyEdgeDefinitions(graph.name(), verticesCollectionNames, edgesCollectionNames));
        requiredDefinitions = eds.stream().collect(Collectors.toMap(EdgeDefinition::getCollection, ed -> ed));
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

    /**
     * Create the graph private collections. There is a collection for storing graph properties.
     * Both vertices and edges can have properties
     * @param graphName
     * @param vertexCollections
     * @return
     */
    public static EdgeDefinition createPropertyEdgeDefinitions(
    	String graphName,
    	List<String> vertexCollections,
    	List<String> edgeCollections) {
        List<String> from = vertexCollections
        		.stream().map(vc -> ArangoDBUtil.getCollectioName(graphName, vc))
        		.collect(Collectors.toList());
        edgeCollections.forEach(ec -> from.add(ArangoDBUtil.getCollectioName(graphName, ec)));
        String propCollection = ArangoDBUtil.getCollectioName(graphName, ELEMENT_PROPERTIES_COLLECTION);
        from.add(propCollection);
        String[] f = from.toArray(new String[from.size()]);
        EdgeDefinition ed = new EdgeDefinition()
                .collection(ArangoDBUtil.getCollectioName(graphName, ELEMENT_PROPERTIES_EDGE))
                .from(f)
                .to(propCollection);
        return ed;
    }


    public static <U> ArangoDBEdgeProperty<U> createArangoDBEdgeProperty(String key, U value, ArangoDBEdge edge) {
        ArangoDBEdgeProperty<U> p;
        p = new ArangoDBEdgeProperty<>(key, value, edge);
        ArangoDBGraph g = edge.graph();
        ArangoDBGraphClient c = g.getClient();
        c.insertDocument(g.name(), p);
        ElementHasProperty e = p.assignToElement(edge);
        c.insertEdge(g.name(), e);
        return p;
    }

    public static <U> ArangoDBVertexProperty<U> createArangoDBVertexProperty(String key, U value, ArangoDBVertex vertex) {
        ArangoDBVertexProperty<U> p;
        p = new ArangoDBVertexProperty<>(key, value, vertex);
        ArangoDBGraph g = vertex.graph();
        ArangoDBGraphClient c = g.getClient();
        c.insertDocument(g.name(), p);
        ElementHasProperty e = p.assignToElement(vertex);
        c.insertEdge(g.name(), e);
        return p;
    }

    public static <U> ArangoDBVertexProperty<U> createArangoDBVertexProperty(String id, String key, U value, ArangoDBVertex vertex) {
        ArangoDBVertexProperty<U> p;
        p = new ArangoDBVertexProperty<>(id, key, value, vertex);
        ArangoDBGraph g = vertex.graph();
        ArangoDBGraphClient c = g.getClient();
        c.insertDocument(g.name(), p);
        ElementHasProperty e = p.assignToElement(vertex);
        c.insertEdge(g.name(), e);
        return p;
    }

    public static <U> ArangoDBPropertyProperty<U> createArangoDBPropertyProperty(String key, U value, ArangoDBVertexProperty<?> vertexProperty) {
        ArangoDBPropertyProperty<U> p;
        p = new ArangoDBPropertyProperty<>(key, value, vertexProperty);
        ArangoDBGraph g = vertexProperty.graph();
        ArangoDBGraphClient c = g.getClient();
        c.insertDocument(g.name(), p);
        ElementHasProperty e = p.assignToElement(vertexProperty);
        c.insertEdge(g.name(), e);
        return p;
    }

    public static <V> Object getCorretctPrimitive(V value) {
        if (value instanceof Number) {
            if (value instanceof Float) {
                return value;
            }
            else if (value instanceof Double) {
                return value;
            }
            else {
                String numberStr = value.toString();
                BigInteger number = new BigInteger(numberStr);
                if(number.longValue() < Integer.MAX_VALUE && number.longValue() > Integer.MIN_VALUE) {
                    return new Integer(numberStr);
                }
                else if(number.longValueExact() < Long.MAX_VALUE && number.longValue() > Long.MIN_VALUE) {
                    return new Long(numberStr);
                }
                else {
                    return number;
                }
            }
        }
        else {
            return value;
        }
    }


}
