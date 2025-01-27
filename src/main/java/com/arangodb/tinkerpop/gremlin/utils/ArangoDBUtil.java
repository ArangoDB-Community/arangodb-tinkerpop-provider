//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.utils;

import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.GraphEntity;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQueryBuilder;
import com.arangodb.tinkerpop.gremlin.structure.*;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElementProperty.ElementHasProperty;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides utility methods for creating properties and for normalising property and
 * collections names (to satisfy Arango DB naming conventions.
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
//FIXME We should add more util methods to validate attribute names, e.g. scape ".".
public class ArangoDBUtil {
	
	/** The Logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBUtil.class);

	/** Utiliy mapper for conversions. **/

	private static final ObjectMapper mapper = new ObjectMapper();

	/**
	 * The prefix to denote that a collection is a hidden collection.
	 */
	
	private final static String HIDDEN_PREFIX = "adbt_";
	
	/** The Constant HIDDEN_PREFIX_LENGTH. */
	
	private static final int HIDDEN_PREFIX_LENGTH = HIDDEN_PREFIX.length();

	/** The regex to match DOCUMENT_KEY. */
	
	public static final Pattern DOCUMENT_KEY = Pattern.compile("^[A-Za-z0-9_:\\.@()\\+,=;\\$!\\*'%-]*");

	/**
	 * Instantiates a new ArangoDB Util.
	 */
	private ArangoDBUtil() {
		// this is a helper class
	}

	/**
	 * Since attributes that start with underscore are considered to be system attributes (),
	 * rename name "_XXXX" to "«a»XXXX" for storage.
	 *
	 * @param key       	the name to convert
	 * @return String 		the converted String
	 * @see <a href="https://docs.arangodb.com/latest/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
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
	 * @param key           the name to convert
	 * @return String 		the converted String
	 * @see <a href="https://docs.arangodb.com/latest/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
	 */
	
	public static String denormalizeKey(String key) {
		if (key.startsWith("«a»")) {
			return "_" + key.substring(3);
		}
		return key;
	}

	/**
	 * Hidden keys, labels, etc. are prefixed in Tinkerpop with  @link Graph.Hidden.HIDDEN_PREFIX). Since in ArangoDB
	 * collection names must always start with a letter, this method normalises Hidden collections name to valid
	 * ArangoDB names by replacing the "~" with
	 *
	 * @param key 			the name to convert
	 * @return String 		the converted String
	 * @see <a href="https://docs.arangodb.com/latest/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
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
	 * rename Attribute "«a»XXXX" to "_XXXX" for retrieval.
	 *
	 * @param key           the name to convert
	 * @return String 		the converted String
	 * @see <a href="https://docs.arangodb.com/latest/Manual/DataModeling/NamingConventions/AttributeNames.html">Manual</a>
	 */
	
	public static String denormalizeCollection(String key) {
		return isHidden(key) ? key.substring(HIDDEN_PREFIX_LENGTH) : key;
	}

	/**
	 * The Enum NamingConventions.
	 */
	
	public enum NamingConventions {
		
		/** The collection. */
		COLLECTION(64), 
		
		/** The name. */
		KEY(256);

		/** The max length. */
		
		private int maxLength;

		/**
		 * Instantiates a new naming conventions.
		 *
		 * @param maxLength the max length
		 */
		NamingConventions(int maxLength) {
			this.maxLength = maxLength;
		}

		/**
		 * Checks for valid name size.
		 *
		 * @param name the name
		 * @return true, if successful
		 */
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
	 * @param graph            the name of the graph
	 * @param relation                the relation
	 * @return an EdgeDefinition that represents the relation.
	 * @throws ArangoDBGraphException if the relation is malformed
	 */
	
	public static EdgeDefinition relationPropertyToEdgeDefinition(ArangoDBGraph graph, String relation) throws ArangoDBGraphException {
		logger.info("Creating EdgeRelation from {}", relation);
		EdgeDefinition result = new EdgeDefinition();
		String[] info = relation.split(":");
		if (info.length != 2) {
			throw new ArangoDBGraphException("Error in configuration. Malformed relation " + relation);
		}
		result.collection(graph.getPrefixedCollectioName(info[0]));
		info = info[1].split("->");
		if (info.length != 2) {
			throw new ArangoDBGraphException("Error in configuration. Malformed relation> " + relation);
		}
		List<String> trimmed = Arrays.stream(info[0].split(","))
				.map(String::trim)
				.map(c -> graph.getPrefixedCollectioName(c))
				.collect(Collectors.toList());
		String[] from = new String[trimmed.size()];
		from = trimmed.toArray(from);
		
		trimmed = Arrays.stream(info[1].split(","))
				.map(String::trim)
				.map(c -> graph.getPrefixedCollectioName(c))
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
	 * @param verticesCollectionNames    the vertex collection names
	 * @param edgesCollectionNames        the edge collection names
	 * @return the list of edge definitions
	 */
	
	public static List<EdgeDefinition> createDefaultEdgeDefinitions(
		List<String> verticesCollectionNames,
		List<String> edgesCollectionNames) {
		List<EdgeDefinition> result = new ArrayList<>();
		for (String e : edgesCollectionNames) {
			for (String from : verticesCollectionNames) {
				for (String to : verticesCollectionNames) {
					EdgeDefinition ed = new EdgeDefinition()
						.collection(e)
						.from(from)
						.to(to);
					result.add(ed);
				}
			}
		}
		return result;
	}
	

	/**
	 * Gets a collection that is unique for the given graph.
	 *
	 * @param graphName 		the graph name
	 * @param collectionName 	the collection name
	 * @param shouldPrefixWithGraphName flag to indicate if the name should be prefixed
	 * @return 					the unique collection name
	 */
	@Deprecated
	public static String getCollectioName(String graphName, String collectionName, Boolean shouldPrefixWithGraphName) {
		if(shouldPrefixWithGraphName) {
			return String.format("%s_%s", graphName, collectionName);
		}else{
			return collectionName;
		}
	}
	
	/**
	 * Validate if an existing graph is correctly configured to handle the desired vertex, edges 
	 * and relations.
	 *
	 * @param verticesCollectionNames    The names of collections for nodes
	 * @param edgesCollectionNames        The names of collections for edges
	 * @param requiredDefinitions                The description of edge definitions
	 * @param graph                    the graph
	 * @param options                    The options used to create the graph
	 * @throws ArangoDBGraphException 	If the graph settings do not match the configuration information
	 */
	
	public static void checkGraphForErrors(
		List<String> verticesCollectionNames,
		List<String> edgesCollectionNames,
		List<EdgeDefinition> requiredDefinitions,
		ArangoGraph graph,
		GraphCreateOptions options) throws ArangoDBGraphException {

		checkGraphVertexCollections(verticesCollectionNames, graph, options);

		GraphEntity ge = graph.getInfo();
        Collection<EdgeDefinition> graphEdgeDefinitions = ge.getEdgeDefinitions();
        if (CollectionUtils.isEmpty(requiredDefinitions)) {
        	// If no relations are defined, vertices and edges can only have one value
        	if ((verticesCollectionNames.size() != 1) || (edgesCollectionNames.size() != 1)) {
        		throw new ArangoDBGraphException("No relations where specified but more than one vertex/edge where defined.");
        	}
        	if (graphEdgeDefinitions.size() != 2) {		// There is always a edgeDefinition for ELEMENT_HAS_PROPERTIES
        		throw new ArangoDBGraphException("No relations where specified but the graph has more than one EdgeDefinition.");
    		}
        }
		Map<String, EdgeDefinition> eds = requiredDefinitions.stream().collect(Collectors.toMap(EdgeDefinition::getCollection, ed -> ed));

        Iterator<EdgeDefinition> it = graphEdgeDefinitions.iterator();
        while (it.hasNext()) {
        	EdgeDefinition existing = it.next();
        	if (eds.containsKey(existing.getCollection())) {
        		EdgeDefinition requiredEdgeDefinition = eds.remove(existing.getCollection());
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

	private static void checkGraphVertexCollections(List<String> verticesCollectionNames, ArangoGraph graph, GraphCreateOptions options) {
		List<String> allVertexCollections = new ArrayList<>(verticesCollectionNames);
		final Collection<String> orphanCollections = options.getOrphanCollections();
		if (orphanCollections != null) {
			allVertexCollections.addAll(orphanCollections);
		}
		if (!graph.getVertexCollections().containsAll(allVertexCollections)) {
			Set<String> avc = new HashSet<>(allVertexCollections);
			avc.removeAll(graph.getVertexCollections());
			throw new ArangoDBGraphException("Not all declared vertex names appear in the graph. Missing " + avc);
		}
	}

	/**
	 * Get a string representation of the Edge definition that complies with the configuration options.
	 *
	 * @param ed			the Edge definition
	 * @return the string that represents the edge definition	
	 */
	
	public static String edgeDefinitionString(EdgeDefinition ed) {
		return String.format("[%s]: %s->%s", ed.getCollection(), ed.getFrom(), ed.getTo());
	}

    /**
     * Create the EdgeDefinition for the graph properties.
     *
	 * @param graph The graph
     * @param vertexCollections the vertex collections
     * @param edgeCollections the edge collections
     * @return the edge definition
     */
	
    public static EdgeDefinition createPropertyEdgeDefinitions(
		final ArangoDBGraph graph,
		final List<String> vertexCollections,
		final List<String> edgeCollections) {
        final List<String> from = new ArrayList<>(vertexCollections);
        from.addAll(edgeCollections);
        from.add(graph.getPrefixedCollectioName(ArangoDBGraph.ELEMENT_PROPERTIES_COLLECTION));
        String[] f = from.toArray(new String[from.size()]);
        EdgeDefinition ed = new EdgeDefinition()
                .collection(graph.getPrefixedCollectioName(ArangoDBGraph.ELEMENT_PROPERTIES_EDGE_COLLECTION))
                .from(f)
                .to(graph.getPrefixedCollectioName(ArangoDBGraph.ELEMENT_PROPERTIES_COLLECTION));
        return ed;
    }


    /**
     * Creates an Arango DB edge property.
     *
     * @param <U> 			the generic type
     * @param key 			the name
     * @param value 		the value
     * @param edge 			the edge
     * @return the created Arango DB edge property
     */
    
    public static <U> ArangoDBEdgeProperty<U> createArangoDBEdgeProperty(
    	String key,
    	U value,
    	ArangoDBEdge edge) {
        ArangoDBEdgeProperty<U> p = new ArangoDBEdgeProperty<>(key, value, edge);
        insertElementAndProperty(edge, p);
        return p;
    }

    /**
     * Creates an Arango DB vertex property.
     *
     * @param <U> 			the generic type
     * @param propertyName 			the name
     * @param propertyValue 		the value
     * @param vertex 		the vertex
     * @return the created Arango DB vertex property
     */
    
    public static <U> ArangoDBVertexProperty<U> createArangoDBVertexProperty(String propertyName, U propertyValue, ArangoDBVertex vertex) {
        ArangoDBVertexProperty<U> p = new ArangoDBVertexProperty<>(propertyName, propertyValue, vertex);
		insertElementAndProperty(vertex, p);
        return p;
    }

    /**
     * Creates an Arango DB vertex property.
     *
     * @param <U> 			the generic type
     * @param id 			the id
     * @param propertyName 			the name
     * @param propertyValue 		the value
     * @param vertex 		the vertex
     * @return the created Arango DB vertex property
     */
    
    public static <U> ArangoDBVertexProperty<U> createArangoDBVertexProperty(String id, String propertyName, U propertyValue, ArangoDBVertex vertex) {
        ArangoDBVertexProperty<U> p;
        p = new ArangoDBVertexProperty<>(id, propertyName, propertyValue, vertex);
		insertElementAndProperty(vertex, p);
        return p;
    }

    /**
     * Creates an Arango DB property property.
     *
     * @param <U> 				the generic type
     * @param key 				the name
     * @param value 			the value
     * @param vertexProperty	the vertex property
     * @return the created Arango DB property property
     */
    
    public static <U> ArangoDBPropertyProperty<U> createArangoDBPropertyProperty(String key, U value, ArangoDBVertexProperty<?> vertexProperty) {
        ArangoDBPropertyProperty<U> p;
        p = new ArangoDBPropertyProperty<>(key, value, vertexProperty);
		insertElementAndProperty(vertexProperty, p);
        return p;
    }

    /**
     * Gets the correct primitive.
     *
     * @param value		the value
     * @param valueClass the exoected class of the value
     * @param <V> 		the value type
	 * @return the 		correct Java primitive
     */
    
    @SuppressWarnings("unchecked")
	public static <V> Object getCorretctPrimitive(V value, String valueClass) {
    	
		switch(valueClass) {
    		case "java.lang.Float":
	    		{
	    			if (value instanceof Double) {
						return ((Double) value).floatValue();
	    			}
					else if (value instanceof Long) {
						return ((Long) value).floatValue();
					}
					else if (value instanceof Integer) {
						return ((Integer) value).floatValue();
					}
	    			else {
	    				logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
	    			}
	    			break;
	    		}
    		case "java.lang.Double":
    		{
    			if (value instanceof Double) {
    				return value;
    			}
				else if (value instanceof Long) {
					return ((Long) value).doubleValue();
				}
				else if (value instanceof Integer) {
					return ((Integer) value).doubleValue();
				}
    			else {
    				logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
    			}
    			break;
    		}
    		case "java.lang.Long":
    		{
    			if (value instanceof Long) {
    				return value;
    			}
    			else if (value instanceof Double) {
    				return ((Double)value).longValue();
    			}
				else if (value instanceof Integer) {
					return ((Integer)value).longValue();
				}
    			else {
    				logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
    			}
    			break;
    		}
    		case "java.lang.Integer":
    		{
    			if (value instanceof Long) {
					return ((Long) value).intValue();
    			}
    			break;
    		}
    		case "java.lang.String":
    		case "java.lang.Boolean":
    		case "":
    			return value;
    		case "java.util.HashMap":
    			//logger.debug(((Map<?,?>)value).keySet().stream().map(Object::getClass).collect(Collectors.toList()));
    			//logger.debug("Add conversion for map values to " + valueClass);
    			// Maps are handled by ArangoOK, but we have an extra field, remove it
				Map<String, ?> valueMap = (Map<String,?>)value;
				for (String key : valueMap.keySet()) {
	    			if (key.startsWith("_")) {
    					valueMap.remove(key);
    				}
	    			// We might need to check individual values...
    			}
    			break;
    		case "java.util.ArrayList":
    			// Should we save the type per item?
    			List<Object> list = new ArrayList<>();
    			((ArrayList<?>)value).forEach(e -> list.add(getCorretctPrimitive(e, "")));
    			return list;
    		case "boolean[]":
    			List<Object> barray = (List<Object>)value;
    			boolean[] br = new boolean[barray.size()]; 
    			IntStream.range(0, barray.size())
    	         .forEach(i -> br[i] = (boolean) barray.get(i));
				return br;
    		case "double[]":
    			List<Object> darray = (List<Object>)value;
    			double[] dr = new double[darray.size()]; 
    			IntStream.range(0, darray.size())
    	         .forEach(i -> dr[i] = (double) getCorretctPrimitive(darray.get(i), "java.lang.Double"));
				return dr;
    		case "float[]":
    			List<Object> farray = (List<Object>)value;
    			float[] fr = new float[farray.size()]; 
    			IntStream.range(0, farray.size())
    	         .forEach(i -> fr[i] = (float) getCorretctPrimitive(farray.get(i), "java.lang.Float"));
				return fr;
    		case "int[]":
    			List<Object> iarray = (List<Object>)value;
    			int[] ir = new int[iarray.size()]; 
    			IntStream.range(0, iarray.size())
    	         .forEach(i -> ir[i] = (int) getCorretctPrimitive(iarray.get(i), "java.lang.Integer"));
				return ir;
    		case "long[]":
    			List<Object> larray = (List<Object>)value;
    			long[] lr = new long[larray.size()]; 
    			IntStream.range(0, larray.size())
    	         .forEach(i -> lr[i] = (long) getCorretctPrimitive(larray.get(i), "java.lang.Long"));
				return lr;
    		case "java.lang.String[]":
    			List<Object> sarray = (List<Object>)value;
    			String[] sr = new String[sarray.size()]; 
    			IntStream.range(0, sarray.size())
    	         .forEach(i -> sr[i] = (String) sarray.get(i));
				return sr;
    		default:
				Object result;
				try {
					result = mapper.convertValue(value, Class.forName(valueClass));
					return result;
				} catch (IllegalArgumentException | ClassNotFoundException e1) {
					logger.warn("Type not deserializable", e1);
				}
				logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
    	}
    	return value;
    }

	/**
	 * Translate a Gremlin direction to Arango direction
	 * @param direction the direction to translate
	 * @return the ArangoDBQueryBuilder.Direction that represents the gremlin direction
	 */
	public static ArangoDBQueryBuilder.Direction getArangoDirectionFromGremlinDirection(final Direction direction) {
		switch (direction) {
			case BOTH:
				return ArangoDBQueryBuilder.Direction.ALL;
			case IN:
				return ArangoDBQueryBuilder.Direction.IN;
			case OUT:
				return ArangoDBQueryBuilder.Direction.OUT;
		}
		return null;
	}

	private static void insertElementAndProperty(ArangoDBBaseDocument element, ArangoDBElementProperty p) {
		ArangoDBGraph g = element.graph();
		ArangoDBGraphClient c = g.getClient();
		c.insertDocument(p);
		ElementHasProperty e = p.assignToElement(element);
		c.insertEdge(e);
	}
}
