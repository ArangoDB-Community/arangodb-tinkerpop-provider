//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoGraph;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleGraphClient;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import com.google.gson.JsonObject;

/**
 * The ArangoDB graph class.
 * 
 * NOTE: USE OF THIS API REQUIRES A USER WITH administrate ACCESS IF THE db USED FOR THE GRAPH DO NOT EXIST.
 * As per ArangoDB, creating DB is only allowed for the root user.
 * <p>
 * An ArangoDBGraph is instantiated from an Apache Commons Configuration instance. The configuration must 
 * provide both Tinkerpop and ArangoDB configuration options. The ArangoDB options are described in the
 * ArangoDB Java Driver <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">documentation.</a> 
 * For the Tinkerpop part, the configuration must provide as a minimum the database name and the graph name.
 * If no vertex, edge and relation information is provided, the graph will be considered schema-less. 
 * <p>
 * All settings are prefixed with "gremlin.arangodb.conf". So, for example, to set the value of the ArangDB
 * hosts property, the configuration must read:
 * <pre>
 * 	gremlin.arangodb.conf.arangodb.hosts = 127.0.0.1:8529
 * </pre>
 * while for the db name it will be:
 * <pre>
 * 	gremlin.arangodb.conf.graph.db = myDB
 * </pre>
 * <p>
 * To define the schema, (EdgeCollecitons in ArangoDB world ) three properties can be used: graph.vertex,
 * graph.edge and graph.relation. The graph.vertex and graph.edge properties allow definition of the ArangoDB
 * collections used to store nodes and edges respectively. The relations property is used to describe the
 * allowed edge-node relations. For simple graphs, only one graph.vertex and graph.edge properties are to be 
 * used. In this case edges are allowed to connect to any two nodes. For example
 * <pre>
 * 	gremlin.arangodb.conf.graph.vertex = Place
 *  gremlin.arangodb.conf.graph.edge = Transition
 * </pre>
 * would allow the user to create Vertices that represent Places, and Edges that represent Transitions. A
 * transition can be created between any two Places.
 * <p>
 * For more complex graph structures, the graph.relation property is used to tell the ArangoDB what relations
 * are allowed, e.g.:
 * <ul>
 * <li>On-to-one edges
 * 	<pre>
 *	gremlin.arangodb.conf.graph.vertex = Place
 * 	gremlin.arangodb.conf.graph.vertex = Transition
 *  gremlin.arangodb.conf.graph.edge = PTArc
 *  gremlin.arangodb.conf.graph.edge = TPArc
 *  gremlin.arangodb.conf.graph.relation = PTArc:Place-&gt;Transition
 *  gremlin.arangodb.conf.graph.relation = TPArc:Transition-&gt;Place
 *  </pre>
 * would allow the user to create nodes to represent Places and Transitions, and edges to represent Arcs. 
 * However, in this case, we have two type of arcs: PTArc and TPArc. The relations specify that PTArcs can
 * only go from Place to Transitions and TPArcs can only go from Transitions to Places. A relation can also 
 * specify multiple to/from nodes. In this case, the to/from values is a comma separated list of names.
 * <li>many-to-many edges
 *  <pre>
 * 	gremlin.arangodb.conf.graph.vertex = male
 * 	gremlin.arangodb.conf.graph.vertex = female
 *  gremlin.arangodb.conf.graph.edge = relation
 *  gremlin.arangodb.conf.graph.relation = relation:male,female-&gt;male,female
 *  </pre>
 * </ul> 
 * The list of allowed settings is:
 * <ul>
 *   <li>  graph.db: 		The name of the database
 *   <li>  graph.name: 		The name of the graph
 *   <li>  graph.vertex: 	The name of a vertices collection
 *   <li>  graph.edge: 		The name of an edges collection
 *   <li>  graph.relation: 	The allowed from/to relations for edges
 *   <li>  arangodb.hosts
 *   <li>  arangodb.timeout
 *   <li>  arangodb.user
 *   <li>  arangodb.password
 *   <li>  arangodb.usessl
 *   <li>  arangodb.chunksize
 *   <li>  arangodb.connections.max
 *   <li>  arangodb.protocol
 *   <li>  arangodb.acquireHostList
 *   <li>  arangodb.loadBalancingStrategy
 * </ul>
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 * 
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("com.arangodb.tinkerpop.gremlin.ArangoDBTestSuite")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoCustomTest",
		method = "shouldSupportUUID",
		specific = "graphson-v3",
		reason = "There is a problem with graphson-v3 recreating the edge from a Map.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest",
		method = "shouldReadWriteEdge",
		specific = "graphson-v3",
		reason = "There is a problem with graphson-v3 recreating the edge from a Map.")
// OptOut ALL graph IO out tests. Not possible with ArangoDBedge definitions
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
		method = "shouldReadWriteModernToFileWithHelpers",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
		method = "shouldReadWriteClassic",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
		method = "shouldReadWriteModern",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
		method = "shouldReadWriteClassicToFileWithHelpers",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
		method = "shouldMigrateModernGraph",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
		method = "shouldMigrateClassicGraph",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest",
		method = "shouldReadWriteVertexWithBOTHEdges",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest",
		method = "shouldReadWriteVerticesNoEdgesToGraphSONManual",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest",
		method = "shouldReadWriteVerticesNoEdges",
		reason = "Doubles with 0 decimal values are deserialized as Integers: 1.0 == 1. But the test expects a Double.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoPropertyTest",
		method = "shouldReadWriteVertexPropertyWithMetaProperties",
		reason = "Tests expected LoadGraphWith.GraphData.CREW to be loaded, but another graph is, so property navigation fails.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
		method = "shouldRemoveVertices",
		reason = "Test creates vertices with random labels, which does not work with our schema-based approach.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
		method = "shouldRemoveEdges",
		reason = "Test creates edges with random labels, which does not work with our schema-based approach.")
/* How to pot out inner classes?
Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.IoTest.GraphSONTest",
		method = "shouldReadWriteModernWrappedInJsonObject",
		reason = "Double/Float serialize/deserialize discrepancies.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.GraphSONLegacyTest",
		method = "shouldReadLegacyGraphSON",
		reason = "Double/Float serialize/deserialize discrepancies.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.GraphMLTest",
		method = "shouldReadGraphML",
		reason = "Double/Float serialize/deserialize discrepancies.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.GraphMLTest",
		method = "shouldReadGraphMLWithAllSupportedDataTypes",
		reason = "Double/Float serialize/deserialize discrepancies.")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.io.GraphMLTest",
		method = "shouldTransformGraphMLV2ToV3ViaXSLT",
		reason = "Double/Float serialize/deserialize discrepancies.")
		*/
public class ArangoDBGraph implements Graph {

	/**
     * The Class ArangoDBGraphFeatures.
     * We can not use Java Obejcts as keys, ergo we can not support UUID and Integer ids. However, 
     * the string representation of these is fine for ArangoDB, which makes the test complain 
     * because it expects not even the toString() representation of these to be allowed. We can test
     * to see if a string is accepted for deserialization.
     */
    public class ArangoDBGraphFeatures implements Features {
	    
    	/**
         * The Class ArangoDBGraphEdgeFeatures.
         */
        public class ArangoDBGraphEdgeFeatures extends ArangoDBGraphElementFeatures implements EdgeFeatures {

		    /** The edge property features. */
    		private final EdgePropertyFeatures edgePropertyFeatures = new ArangoDBGraphEdgePropertyFeatures();

            /**
             * Instantiates a new ArangoDB graph edge features.
             */
            ArangoDBGraphEdgeFeatures() { }

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
			
			@Override
			public boolean supportsAnyIds() {
				return false;
			}
			
			@Override
			public boolean supportsCustomIds() {
				return false;
			}
			
			@Override
			public boolean supportsNumericIds() {
				return false;
			}

			@Override
			public boolean supportsUuidIds() {
				return false;
			}
        }
        
        /**
         * The Class ArangoDBGraphEdgePropertyFeatures.
         */
        private class ArangoDBGraphEdgePropertyFeatures implements EdgePropertyFeatures {

		    /**
    		 * Instantiates a new ArangoDB graph edge property features.
    		 */
    		ArangoDBGraphEdgePropertyFeatures() {
            }
		    
        }
        
        /**
         * The Class ArangoDBGraphElementFeatures.
         */
        public class ArangoDBGraphElementFeatures implements ElementFeatures {

            /**
             * Instantiates a new ArangoDB graph element features.
             */
            ArangoDBGraphElementFeatures() {
            }
        }

		/**
         * The Class ArangoDBGraphGraphFeatures.
         */
        public class ArangoDBGraphGraphFeatures implements GraphFeatures {

			/** The variable features. */
			private VariableFeatures variableFeatures = new ArangoDBGraphVariables.ArangoDBGraphVariableFeatures();

			/**
			 * Instantiates a new ArangoDB graph graph features.
			 */
			ArangoDBGraphGraphFeatures () {

			}

			@Override
			public boolean supportsComputer() {
				return false;
			}

			@Override
			public boolean supportsThreadedTransactions() {
				return false;
			}

			@Override
			public boolean supportsTransactions() {
				return false;
			}

			@Override
			public VariableFeatures variables() {
				return variableFeatures;
			}
			
			
			
		}

		/**
         * The Class ArangoDBGraphVertexFeatures.
         */
        public class ArangoDBGraphVertexFeatures extends ArangoDBGraphElementFeatures implements VertexFeatures {

		    /** The vertex property features. */
    		private final VertexPropertyFeatures vertexPropertyFeatures = new ArangoDBGraphVertexPropertyFeatures();

            /**
             * Instantiates a new ArangoDB graph vertex features.
             */
            ArangoDBGraphVertexFeatures () { }


			@Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }
			
			@Override
			public boolean supportsAnyIds() {
				return false;
			}
			
			@Override
			public boolean supportsCustomIds() {
				return false;
			}
			
			@Override
			public boolean supportsNumericIds() {
				return false;
			}

			@Override
			public boolean supportsUuidIds() {
				return false;
			}
        
        }

		/**
         * The Class ArangoDBGraphVertexPropertyFeatures.
         */
        private class ArangoDBGraphVertexPropertyFeatures implements VertexPropertyFeatures {

		    /**
    		 * Instantiates a new ArangoDB graph vertex property features.
    		 */
    		ArangoDBGraphVertexPropertyFeatures() { }

			@Override
			public boolean supportsAnyIds() {
				return false;
			}
			
			@Override
			public boolean supportsCustomIds() {
				return false;
			}
			
			@Override
			public boolean supportsNumericIds() {
				return false;
			}

			@Override
			public boolean supportsUuidIds() {
				return false;
			}

        }

		/** The graph features. */
    	protected GraphFeatures graphFeatures = new ArangoDBGraphGraphFeatures();

        /** The vertex features. */
        protected VertexFeatures vertexFeatures = new ArangoDBGraphVertexFeatures();

        /** The edge features. */
        protected EdgeFeatures edgeFeatures = new ArangoDBGraphEdgeFeatures();

        @Override
		public EdgeFeatures edge() {
			return edgeFeatures;
		}


        @Override
		public GraphFeatures graph() {
			return graphFeatures;
		}

        @Override
		public String toString() {
			return StringFactory.featureString(this);
		}

        @Override
		public VertexFeatures vertex() {
			return vertexFeatures;
		}
    }

	public static class ArangoDBIterator<IType extends Element> implements Iterator<IType> {
		
		private final Iterator<IType> delegate;
		private final ArangoDBGraph graph;
		
		public ArangoDBIterator(ArangoDBGraph graph, Iterator<IType> delegate) {
			super();
			this.delegate = delegate;
			this.graph = graph;
		}

		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@SuppressWarnings("unchecked")
		@Override
		public IType next() {
			ArangoDBElement<?> next = (ArangoDBElement<?>) delegate.next();
			next.graph(graph);
			next.setPaired(true);
			return (IType) next;
		}
		
	}
	
    /** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBGraph.class);
    
    /** The Constant CONFIG_CONF. */
    public static final String ARANGODB_CONFIG_PREFIX = "gremlin.arangodb.conf";
    
    /** The Constant CONFIG_DB. */
    public static final String CONFIG_DB_NAME = "graph.db";
    
    /** The Constant CONFIG_NAME. */
    public static final String CONFIG_GRAPH_NAME = "graph.name";
    
    /** The Constant CONFIG_VERTICES. */
    public static final String CONFIG_VERTICES = "graph.vertex";
    
    /** The Constant CONFIG_EDGES. */
    public static final String CONFIG_EDGES = "graph.edge";
    
    /** The Constant CONFIG_RELATIONS. */
    public static final String CONFIG_RELATIONS = "graph.relation";
    
    /** The Constant DEFAULT_VERTEX_COLLECTION. */
    public static final String DEFAULT_VERTEX_COLLECTION = "vertex";
    
	/** The Constant DEFAULT_VERTEX_COLLECTION. */
    public static final String DEFAULT_EDGE_COLLECTION = "edge";
    
    private static final Pattern DOCUMENT_KEY = Pattern.compile("^[A-Za-z0-9_:\\.@()\\+,=;\\$!\\*'%-]*");
	

    /**
     * Create a new ArangoDBGraph from the provided configuration.
     *
     * @param configuration 	the configuration
     * @return the Arango B graph
     * @throws ArangoDBGraphException if there was an error craeting the graph.
     */
    
    public static ArangoDBGraph open(Configuration configuration) throws ArangoDBGraphException {
		return new ArangoDBGraph(configuration);
	}
	
	/** The features. */
	private final Features FEATURES = new ArangoDBGraphFeatures();
	
	/** A ArangoDBSimpleGraphClient to handle the connection to the Database. */
	private ArangoDBSimpleGraphClient client = null;
	
	/** The name. */
	private String name;
	
	/** The vertex collections. */
	private final List<String> vertexCollections;

	/** The edge collections. */
	private final List<String> edgeCollections;
	
	/** The relations. */
	private final List<String> relations;
	
	/** Flat to indicate that the graph has no schema */
	private boolean schemaless = false;

	private Variables variables;

	private Configuration configuration;
	

	/**
	 * Creates a Graph (simple configuration).
	 *
	 * @param configuration 			the configuration
	 * @throws ArangoDBGraphException   if the graph could not be created
	 */

	public ArangoDBGraph(Configuration configuration)
			throws ArangoDBGraphException {
		
		logger.info("Creating new ArangoDB Graph from configuration");
		Configuration arangoConfig = configuration.subset(ARANGODB_CONFIG_PREFIX);
		vertexCollections = arangoConfig.getList(CONFIG_VERTICES).stream()
				.map(String.class::cast)
				.collect(Collectors.toList());
		edgeCollections = arangoConfig.getList(CONFIG_EDGES).stream()
				.map(String.class::cast)
				.collect(Collectors.toList());
		relations = arangoConfig.getList(CONFIG_RELATIONS).stream()
				.map(String.class::cast)
				.collect(Collectors.toList());
		String graphName = arangoConfig.getString(CONFIG_GRAPH_NAME);
		checkValues(arangoConfig.getString(CONFIG_DB_NAME), graphName,	vertexCollections,
				edgeCollections, relations);
		if (CollectionUtils.isEmpty(vertexCollections)) {
			schemaless = true;
			vertexCollections.add(DEFAULT_VERTEX_COLLECTION);
		}
		if (CollectionUtils.isEmpty(edgeCollections)) {
			edgeCollections.add(DEFAULT_EDGE_COLLECTION);
		}
		Properties arangoProperties = ConfigurationConverter.getProperties(arangoConfig);
		int batchSize = 0;
		client = new ArangoDBSimpleGraphClient(arangoProperties, arangoConfig.getString(CONFIG_DB_NAME), batchSize);
        ArangoGraph graph = client.getGraph(graphName);
        GraphCreateOptions options = new  GraphCreateOptions();
    	options.orphanCollections(ArangoDBUtil.getCollectioName(graphName, ArangoDBGraphVariables.GRAPH_VARIABLES_COLLECTION));
        if (graph.exists()) {
            ArangoDBUtil.checkGraphForErrors(vertexCollections, edgeCollections, relations, graph, options);
            variables = new ArangoDBGraphVariables(this);
            String query = String.format("FOR v IN %s RETURN v", ArangoDBUtil.getCollectioName(graph.name(), ArangoDBGraphVariables.GRAPH_VARIABLES_COLLECTION));
            ArangoCursor<JsonObject> iter = client.executeAqlQuery(query, null, null, JsonObject.class);
            iter.forEach(jo -> this.variables.set(jo.get("_key").getAsString(), jo.get("value")));
        }
        else {
			client.createGraph(graphName, vertexCollections,
            		edgeCollections, relations, options);
			variables = new ArangoDBGraphVariables(this);
		}
		this.name = graph.name();
		this.configuration = configuration;
	}

	
	@Override
	public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object id;
        String collection;
        if (!schemaless) {
        	collection = ElementHelper.getLabelValue(keyValues).orElse(null);
        	ElementHelper.validateLabel(collection);
        }
        else {
        	collection = DEFAULT_VERTEX_COLLECTION;
        }
        if (!vertexCollections().contains(collection)) {
			throw new IllegalArgumentException(String.format("Vertex label (%s) not in graph (%s) vertex collections.", collection, name));
		}
        ArangoDBVertex<Object> vertex = null;
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	id = ElementHelper.getIdValue(keyValues).get();
        	if (this.features().vertex().willAllowId(id)) {
        		Matcher m = DOCUMENT_KEY.matcher((String)id);
        		if (m.matches()) {
        			vertex = new ArangoDBVertex<Object>(this, collection, id.toString());
        		}
        		else {
            		throw new ArangoDBTinkerpopException(String.format("Given id (%s) has unsupported characters.", id));
            	}
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}
        	
        }
        else {
        	vertex = new ArangoDBVertex<Object>(this, collection);
        }
        ElementHelper.attachProperties(vertex, keyValues);
        try {
			client.insertVertex(this, vertex);
		} catch (ArangoDBGraphException e) {
			throw Exceptions.getArangoDBException((ArangoDBException) e.getCause());
		}
        return vertex;
	}

	/**
	 * Check that the configuration values are sound.
	 *
	 * @param db the db
	 * @param name the name
	 * @param vertices the vertices
	 * @param edges the edges
	 * @param relations the relations
	 * @throws ArangoDBGraphException the ArangoDB graph exception
	 */
	private void checkValues(String db, String name, List<String> vertices, List<String> edges, List<String> relations)
			throws ArangoDBGraphException {
		
		if (StringUtils.isBlank(db)) {
			throw new ArangoDBGraphException("db name must not be null.");
		}
		
		if (StringUtils.isBlank(name)) {
			throw new ArangoDBGraphException("graph name must not be null.");
		}
		if (CollectionUtils.isEmpty(edges)) {
			logger.warn("Empty edges collection(s), the default 'edge' collection will be used.");
		}
		if ((vertices.size() > 1) && (edges.size() > 1) && CollectionUtils.isEmpty(relations)) {
			throw new ArangoDBGraphException("If more than one vertex/edge collection is provided, relations must be defined");
		}
	}

	@Override
	public void close() {
		client.shutdown();
	}
	

	@Override
	public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
	}

	@Override
	public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Configuration configuration() {
		return configuration;
	}

	/**
	 * Edge collections.
	 *
	 * @return the list
	 */
	public List<String> edgeCollections() {
		return Collections.unmodifiableList(edgeCollections);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Edge> edges(Object... edgeIds) {
		List<String> ids = Arrays.stream(edgeIds)
        		.map(id -> id instanceof Element ? ((Element)id).id() : id)
        		.map(Object::toString)
        		.collect(Collectors.toList());
		ArangoDBQuery query = getClient().getGraphEdges(this, ids);
		try {
			return new ArangoDBIterator<Edge>(this, query.getCursorResult(ArangoDBEdge.class));
		} catch (ArangoDBGraphException e) {
			// TODO Auto-generated catch block
			return null;
		}
	}

	@Override
	public Features features() {
		return FEATURES;
	}

	/**
	 * Returns the ArangoDBSimpleGraphClient object.
	 *
	 * @return the ArangoDBSimpleGraphClient object
	 */
	public ArangoDBSimpleGraphClient getClient() {
		return client;
	}

	/**
	 * Returns the identifier of the graph.
	 *
	 * @return the identifier of the graph
	 */
	public String getId() {
		ArangoGraph graph = client.getGraph(name);
		return graph.getInfo().getName();
	}

	/**
	 * Name.
	 *
	 * @return the string
	 */
	public String name() {
		return this.name;
	}

	/**
	 * Relations.
	 *
	 * @return the collection
	 */
	private Collection<String> relations() {
		return relations;
	}


	@Override
	public String toString() {
		String vertices = vertexCollections().stream()
				.map(vc -> String.format("\"%s\"", vc))
				.collect(Collectors.joining(", ", "{", "}"));
		String edges = edgeCollections().stream()
				.map(vc -> String.format("\"%s\"", vc))
				.collect(Collectors.joining(", ", "{", "}"));
		String relations = relations().stream()
				.map(vc -> String.format("\"%s\"", vc))
				.collect(Collectors.joining(", ", "{", "}"));
		String internal =  "{"
					+ "\"name\":\"" + name() + "\","
					+ "\"vertices\":" + vertices + ","
					+ "\"edges\":" + edges+ ","
					+ "\"relations\":" + relations
				+"}";
		return StringFactory.graphString(this, internal);
	}

	@Override
	public Transaction tx() {
		throw Graph.Exceptions.transactionsNotSupported();
	}

	@Override
	public Variables variables() {
		return variables;
	}

	/**
	 * Vertex collections.
	 *
	 * @return the list
	 */
	public List<String> vertexCollections() {
		return Collections.unmodifiableList(vertexCollections);
	}

    @SuppressWarnings("unchecked")
	@Override
	public Iterator<Vertex> vertices(Object... vertexIds) {
    	List<String> ids = Arrays.stream(vertexIds)
        		.map(id -> id instanceof Element ? ((Element)id).id() : id)
        		.map(Object::toString)
        		.collect(Collectors.toList());
		ArangoDBQuery query = getClient().getGraphVertices(this, ids);
		try {
			return new ArangoDBIterator<Vertex>(this, query.getCursorResult(ArangoDBVertex.class));
		} catch (ArangoDBGraphException e) {
			// TODO Auto-generated catch block
			return null;
		}
	}
    
    /**
     * Common exceptions to use with an ArangoDB graph. This class is intended to translate ArangoDB
     * error codes into meaningful exceptions with standard messages. ArangoDBException exception
     * is a RuntimeException intended to stop execution.
     */
    public static class Exceptions {
    	
    	public static Pattern ERROR_CODE = Pattern.compile("^Response:\\s\\d+,\\sError:\\s(\\d+)\\s-\\s([a-z\\s]+).+");
    	
        private Exceptions() {
        }
        
        /**
         * Translate ArangoDB Error code into exception (@see <a href="https://docs.arangodb.com/latest/Manual/Appendix/ErrorCodes.html">Error codes</a>)
         * @param errorCode
         * @return
         */
        public static ArangoDBTinkerpopException getArangoDBException(ArangoDBException ex) {
        	String errorMessage = ex.getMessage();
			Matcher m = ERROR_CODE.matcher(errorMessage);
        	if (m.matches()) {
        		int code = Integer.parseInt(m.group(1));
        		String msg = m.group(2);
        		switch ((int)code/100) {
        		case 10:	// Internal ArangoDB storage errors
        			return new ArangoDBTinkerpopException(String.format("Internal ArangoDB storage error (%s): %s", code, msg));
        		case 11:
        			return new ArangoDBTinkerpopException(String.format("External ArangoDB storage error (%s): %s", code, msg));
        		case 12:
        			return new ArangoDBTinkerpopException(String.format("General ArangoDB storage error (%s): %s", code, msg));
        		case 13:
        			return new ArangoDBTinkerpopException(String.format("Checked ArangoDB storage error (%s): %s", code, msg));
        		case 14:
        			return new ArangoDBTinkerpopException(String.format("ArangoDB replication/cluster error (%s): %s", code, msg));
        		case 15:
        			return new ArangoDBTinkerpopException(String.format("ArangoDB query error (%s): %s", code, msg));
        		case 19:
        			return new ArangoDBTinkerpopException(String.format("Graph / traversal errors (%s): %s", code, msg));
        		}
        	}
        	return new ArangoDBTinkerpopException("General ArangoDB error (unkown error)");
        }

    }
    
    public static class ArangoDBTinkerpopException extends RuntimeException {

		private static final long serialVersionUID = -8478050406116402002L;

		public ArangoDBTinkerpopException(String message, Throwable cause) {
			super(message, cause);
		}

		public ArangoDBTinkerpopException(String message) {
			super(message);
		}

		public ArangoDBTinkerpopException(Throwable cause) {
			super(cause);
		}
    	
    }

//
//	@Override
//	public Vertex getVertex(Object id) {
//		return ArangoDBVertex.load(this, id);
//	}
//
//	@Override
//	public void removeVertex(Vertex vertex) {
//		if (vertex.getClass().equals(ArangoDBVertex.class)) {
//			ArangoDBVertex v = (ArangoDBVertex) vertex;
//			v.remove();
//		}
//	}
//
//
//	@Override
//	public Iterable<Vertex> getVertices() {
//		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
//		return q.vertices();
//	}
//
//	@Override
//	public Iterable<Vertex> getVertices(String key, Object value) {
//		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
//		q.has(key, value);
//		return q.vertices();
//	}

//	@Override
//	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
//
//		if (label == null) {
//			throw ExceptionFactory.edgeLabelCanNotBeNull();
//		}
//
//		return ArangoDBEdge.create(this, id, outVertex, inVertex, label);
//	}
//
//	@Override
//	public Edge getEdge(Object id) {
//		return ArangoDBEdge.load(this, id);
//	}
//
//	@Override
//	public void removeEdge(Edge edge) {
//		if (edge.getClass().equals(ArangoDBEdge.class)) {
//			ArangoDBEdge e = (ArangoDBEdge) edge;
//			e.remove();
//		}
//	}
//
//	@Override
//	public Iterable<Edge> getEdges() {
//		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
//		return q.edges();
//	}
//
//	@Override
//	public Iterable<Edge> getEdges(String key, Object value) {
//		ArangoDBGraphQuery q = new ArangoDBGraphQuery(this);
//		q.has(key, value);
//		return q.edges();
//	}
//
//	@Override
//	public ArangoDBSimpleGraph getRawGraph() {
//		return simpleGraph;
//	}

//	@Override
//	public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
//
//		List<ArangoDBIndex> indices = null;
//		try {
//			if (elementClass.isAssignableFrom(Vertex.class)) {
//				indices = client.getVertexIndices(simpleGraph);
//			} else if (elementClass.isAssignableFrom(Edge.class)) {
//				indices = client.getEdgeIndices(simpleGraph);
//			}
//		} catch (ArangoDBException e) {
//			logger.warn("error while reading an index", e);
//		}
//
//		String normalizedKey = ArangoDBUtil.normalizeKey(key);
//
//		if (indices != null) {
//			for (ArangoDBIndex index : indices) {
//				if (index.getFields().size() == 1) {
//					deleteIndexByKey(normalizedKey, index);
//				}
//			}
//		}
//
//	}
//
//	private void deleteIndexByKey(String normalizedKey, ArangoDBIndex index) {
//		String field = index.getFields().get(0);
//
//		if (field.equals(normalizedKey)) {
//			try {
//				client.deleteIndex(index.getId());
//			} catch (ArangoDBException e) {
//				logger.warn("error while deleting an index", e);
//			}
//		}
//	}
//
//	@SuppressWarnings("rawtypes")
//	@Override
//	public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
//
//		IndexType type = IndexType.SKIPLIST;
//		boolean unique = false;
//		List<String> fields = new ArrayList<String>();
//
//		String n = ArangoDBUtil.normalizeKey(key);
//		fields.add(n);
//
//		for (Parameter p : indexParameters) {
//			if ("type".equals(p.getKey())) {
//				type = object2IndexType(p.getValue());
//			}
//			if ("unique".equals(p.getKey())) {
//				unique = (Boolean) p.getValue();
//			}
//		}
//
//		try {
//			if (elementClass.isAssignableFrom(Vertex.class)) {
//				getClient().createVertexIndex(simpleGraph, type, unique, fields);
//			} else if (elementClass.isAssignableFrom(Edge.class)) {
//				getClient().createEdgeIndex(simpleGraph, type, unique, fields);
//			}
//		} catch (ArangoDBException e) {
//			logger.warn("error while creating a vertex index", e);
//		}
//	}

//	private IndexType object2IndexType(Object obj) {
//		if (obj instanceof IndexType) {
//			return (IndexType) obj;
//		}
//
//		if (obj != null) {
//			String str = obj.toString();
//			for (IndexType indexType : IndexType.values()) {
//				if (indexType.toString().equalsIgnoreCase(str)) {
//					return indexType;
//				}
//			}
//		}
//
//		return IndexType.SKIPLIST;
//	}

//	@Override
//	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
//		HashSet<String> result = new HashSet<String>();
//		List<ArangoDBIndex> indices = null;
//		try {
//			if (elementClass.isAssignableFrom(Vertex.class)) {
//				indices = client.getVertexIndices(simpleGraph);
//			} else if (elementClass.isAssignableFrom(Edge.class)) {
//				indices = client.getEdgeIndices(simpleGraph);
//			}
//
//			for (ArangoDBIndex i : indices) {
//				if (i.getFields().size() == 1) {
//					addNotSystemKey(result, i);
//				}
//			}
//
//		} catch (ArangoDBException e) {
//			logger.warn("error while reading index keys", e);
//		}
//
//		return result;
//	}


	//	@Override
//	public GraphQuery query() {
//		return new ArangoDBGraphQuery(this);
//	}
}
