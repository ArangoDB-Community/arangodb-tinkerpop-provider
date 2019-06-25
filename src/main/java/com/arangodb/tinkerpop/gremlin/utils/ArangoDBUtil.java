//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
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
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdgeProperty;
import com.arangodb.tinkerpop.gremlin.structure.properties.ArngElementProperty.ElementHasProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBPropertyProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.properties.ArngVertexProperty;
import com.arangodb.velocypack.VPack;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.exception.VPackParserException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides utility methods for creating elementProperties and for normalising property and
 * collections names (to satisfy Arango DB naming conventions.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */
//FIXME We should add more util methods to validate attribute names, e.g. scape ".".
public class ArangoDBUtil {

	/** The Logger. */

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBUtil.class);

	/** The Constant GRAPH_VARIABLES_COLLECTION. */

	public static final String GRAPH_VARIABLES_COLLECTION = "GRAPH-VARIABLES";

	/** The Constant ELEMENT_PROPERTIES_COLLECTION. */

	public static final String ELEMENT_PROPERTIES_COLLECTION = "ELEMENT-PROPERTIES";

	/** The Constant ELEMENT_PROPERTIES_EDGE. */

	public static final String ELEMENT_PROPERTIES_EDGE = "ELEMENT-HAS-PROPERTIES";

	/**
	 * The prefix to denote that a label is a hidden label.
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
	 * rename primaryKey "_XXXX" to "«a»XXXX" for storage.
	 *
	 * @param key       	the primaryKey to convert
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
	 * @param key           the primaryKey to convert
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
	 * label names must always start with a letter, this method normalises Hidden collections name to valid
	 * ArangoDB names by replacing the "~" with
	 *
	 * @param key 			the primaryKey to convert
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
	 * @param key           the primaryKey to convert
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

		/** The label. */
		COLLECTION(64),

		/** The primaryKey. */
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
	 * Gets a label that is unique for the given graph.
	 *
	 * @param graphName 		the graph name
	 * @param collectionName 	the label name
	 * @return 					the unique label name
	 */

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
	 * @param verticesCollectionNames 	The names of collections for nodes
	 * @param edgesCollectionNames 		The names of collections for edges
	 * @param relations 				The description of edge definitions
	 * @param graph 					the graph
	 * @param options 					The options used to create the graph
	 * @throws ArangoDBGraphException 	If the graph settings do not match the configuration information
	 */

	public static void checkGraphForErrors(List<String> verticesCollectionNames,
			List<String> edgesCollectionNames,
			List<String> relations,
			ArangoGraph graph, GraphCreateOptions options, boolean shouldPrefixCollectionWithGraphName) throws ArangoDBGraphException {


		List<String> allVertexCollections = verticesCollectionNames.stream()
				.map(vc -> ArangoDBUtil.getCollectioName(graph.name(), vc, shouldPrefixCollectionWithGraphName))
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
			// If no relations are defined, vertices and edges can only have one baseValue
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
			eds.addAll(
					ArangoDBUtil.createDefaultEdgeDefinitions(graph.name(), verticesCollectionNames, edgesCollectionNames, shouldPrefixCollectionWithGraphName)
			);

		} else {
			for (Object value : relations) {
				EdgeDefinition ed = ArangoDBUtil.relationPropertyToEdgeDefinition(graph.name(), (String) value, shouldPrefixCollectionWithGraphName);
				eds.add(ed);
			}
		}
		eds.add(ArangoDBUtil.createPropertyEdgeDefinitions(graph.name(), verticesCollectionNames, edgesCollectionNames, shouldPrefixCollectionWithGraphName));
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
	 * Create the graph private collections. There is a label for storing graph elementProperties.
	 * Both vertices and edges can have elementProperties
	 *
	 * @param graphName the graph name
	 * @param vertexCollections the vertex collections
	 * @param edgeCollections the edge collections
	 * @return the edge definition
	 */

	public static EdgeDefinition createPropertyEdgeDefinitions(
			String graphName,
			List<String> vertexCollections,
			List<String> edgeCollections,
			boolean shouldPrefixCollectionWithGraphName) {
		List<String> from = vertexCollections
				.stream().map(vc -> ArangoDBUtil.getCollectioName(graphName, vc, shouldPrefixCollectionWithGraphName))
				.collect(Collectors.toList());
		edgeCollections.forEach(ec -> from.add(ArangoDBUtil.getCollectioName(graphName, ec, shouldPrefixCollectionWithGraphName)));
		String propCollection = ArangoDBUtil.getCollectioName(graphName, ELEMENT_PROPERTIES_COLLECTION, true);
		from.add(propCollection);
		String[] f = from.toArray(new String[from.size()]);
		EdgeDefinition ed = new EdgeDefinition()
				.collection(ArangoDBUtil.getCollectioName(graphName, ELEMENT_PROPERTIES_EDGE, true))
				.from(f)
				.to(propCollection);
		return ed;
	}


	/**
	 * Creates an Arango DB edge property.
	 *
	 * @param <U> 			the generic type
	 * @param key 			the primaryKey
	 * @param value 		the baseValue
	 * @param edge 			the edge
	 * @return the created Arango DB edge property
	 */

	public static <U> ArangoDBEdgeProperty<U> createArangoDBEdgeProperty(
			String key,
			U value,
			ArangoDBEdge edge) {
		ArangoDBEdgeProperty<U> p;
		p = new ArangoDBEdgeProperty<>(key, value, edge);
		ArangoDBGraph g = edge.graph();
		ArangoDBGraphClient c = g.getClient();
		c.insertDocument(p, true);
		ElementHasProperty e = p.assignToElement(edge);
		c.insertEdge(e, true);
		return p;
	}

	/**
	 * Creates an Arango DB vertex property.
	 *
	 * @param <U> 			the generic type
	 * @param key 			the primaryKey
	 * @param value 		the baseValue
	 * @param vertex 		the vertex
	 * @return the created Arango DB vertex property
	 */

	public static <U> ArngVertexProperty<U> createArangoDBVertexProperty(String key, U value, ArangoDBVertex vertex) {
		ArngVertexProperty<U> p;
		p = new ArngVertexProperty<>(key, value, vertex);
		ArangoDBGraph g = vertex.graph();
		ArangoDBGraphClient c = g.getClient();
		c.insertDocument(p, true);
		ElementHasProperty e = p.assignToElement(vertex);
		c.insertEdge(e, true);
		return p;
	}

	/**
	 * Creates an Arango DB vertex property.
	 *
	 * @param <U> 			the generic type
	 * @param id 			the id
	 * @param key 			the primaryKey
	 * @param value 		the baseValue
	 * @param vertex 		the vertex
	 * @return the created Arango DB vertex property
	 */

	public static <U> ArngVertexProperty<U> createArangoDBVertexProperty(String id, String key, U value, ArangoDBVertex vertex) {
		ArngVertexProperty<U> p;
		p = new ArngVertexProperty<>(id, key, value, vertex);
		ArangoDBGraph g = vertex.graph();
		ArangoDBGraphClient c = g.getClient();
		//This is insertion of a property to ELEMENT-PROPERTIES label, which must be always prefixed with graph name
		c.insertDocument(p, true);
		ElementHasProperty e = p.assignToElement(vertex);
		//This is insertion of edge to ELEMENT-HAS-PROPERTIES label, between ArangoBaseDocument and Property
		c.insertEdge(e, true);
		return p;
	}

	/**
	 * Creates an Arango DB property property.
	 *
	 * @param <U> 				the generic type
	 * @param key 				the primaryKey
	 * @param value 			the baseValue
	 * @param vertexProperty	the vertex property
	 * @return the created Arango DB property property
	 */

	public static <U> ArangoDBPropertyProperty<U> createArangoDBPropertyProperty(String key, U value, ArngVertexProperty<?> vertexProperty) {
		ArangoDBPropertyProperty<U> p;
		p = new ArangoDBPropertyProperty<>(key, value, vertexProperty);
		ArangoDBGraph g = vertexProperty.graph();
		ArangoDBGraphClient c = g.getClient();
		c.insertDocument(p, true);
		ElementHasProperty e = p.assignToElement(vertexProperty);
		c.insertEdge(e, true);
		return p;
	}


}
