//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.GraphCreateOptions;
import com.arangodb.tinkerpop.gremlin.cache.EdgeLoader;
import com.arangodb.tinkerpop.gremlin.cache.VertexLoader;
import com.arangodb.tinkerpop.gremlin.client.*;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoGraph;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The ArangoDB graph class.
 *
 * NOTE: USE OF THIS API REQUIRES A USER WITH <b>ADMINISTRATOR</b> ACCESS IF THE <b>DB</b> USED FOR
 * THE GRAPH DOES NOT EXIST. As per ArangoDB, creating DB is only allowed for the root user, hence
 * only the root user can be used if the DB does not exist.
 * <p>
 * <b>ArangoDB and TinkerPop Ids.</b>
 * <p>
 * In TinkerPop, graph elements are expected to have a unique Id within the graph; in ArangoDB the
 * Id (document handle) consists of the collection's name and the document name (_key attribute)
 * separated by /, hence the only way to hint at ids is by providing a _key during construction.
 * Hence, ArangoDBGraph elements do not strictly support <i>User Supplied Ids</i>. We allow
 * ids to be supplied during vertex creation: {@code graph.addVertex(id,x)}, but this id actually
 * represents the _key. As a result, posterior search/match by id must prefix the vertex's label
 * (collection) followed by a /.
 * <p>
 * An ArangoDBGraph is instantiated from an Apache Commons Configuration instance. The configuration
 * must provide both TinkerPop and ArangoDB configuration options. The ArangoDB options are
 * described in the ArangoDB Java Driver <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">documentation.</a>
 *
 * For the TinkerPop part, the configuration must provide as a minimum the database name and the
 * graph name. If no vertex, edge and relation information is provided, the graph will be considered
 * schema-less.
 * <p>
 * All settings are prefixed with "gremlin.arangodb.conf". So, for example, to set the value of the
 * Arango DB hosts property (arango db configuration), the configuration must read:
 * <pre>gremlin.arangodb.conf.arangodb.hosts = 127.0.0.1:8529
 * </pre>
 * while for the db name (graph configuration) it will be:
 * <pre>gremlin.arangodb.conf.graph.db = myDB
 * </pre>
 * <p>
 * To define the schema, (EdgeCollections in ArangoDB world) three properties can be used:
 * <tt>graph.vertex</tt>, <tt>graph.edge</tt> and <tt>graph.relation</tt>. The graph.vertex and
 * graph.edge properties allow definition of the ArangoDB collections used to store nodes and edges
 * respectively. The relations property is used to describe the allowed edge-node relations. For
 * simple graphs, only one graph.vertex and graph.edge properties need to be provided. In this case
 * edges are allowed to connect to any two nodes. For example:
 * <pre>gremlin.arangodb.conf.graph.vertex = Place
 *gremlin.arangodb.conf.graph.edge = Transition
 * </pre>
 * would allow the user to create Vertices that represent Places, and Edges that represent
 * Transitions. A transition can be created between any two Places. If additional vertices and edges
 * were added, the resulting graph schema would be fully connected, that is, edges would be allowed
 * between any two pair of vertices.
 * <p>
 * For more complex graph structures, the graph.relation property is used to tell the ArangoDB what
 * relations are allowed, e.g.:
 * <ul>
 * <li>One-to-one edges
 *<pre>gremlin.arangodb.conf.graph.vertex = Place
 *gremlin.arangodb.conf.graph.vertex = Transition
 *gremlin.arangodb.conf.graph.edge = PTArc
 *gremlin.arangodb.conf.graph.edge = TPArc
 *gremlin.arangodb.conf.graph.relation = PTArc:Place-&gt;Transition
 *gremlin.arangodb.conf.graph.relation = TPArc:Transition-&gt;Place
 *</pre>
 * would allow the user to create nodes to represent Places and Transitions, and edges to represent
 * Arcs. However, in this case, we have two type of arcs: PTArc and TPArc. The relations specify
 * that PTArcs can only go from Place to Transitions and TPArcs can only go from Transitions to
 * Places. A relation can also specify multiple to/from nodes. In this case, the to/from values is a
 * comma separated list of names.
 * <li>Many-to-many edges
 *  <pre>gremlin.arangodb.conf.graph.vertex = male
 *gremlin.arangodb.conf.graph.vertex = female
 *gremlin.arangodb.conf.graph.edge = relation
 *gremlin.arangodb.conf.graph.relation = relation:male,female-&gt;male,female
 *  </pre>
 * </ul>
 * <p>
 * In order to allow multiple graphs in the same database, vertex and edge collections can be prefixed with the
 * graph name in order to avoid collection clashes. To enable this function the graph.shouldPrefixCollectionNames
 * property should be set to <code>true</code>. If you have an existing graph/collections and want to reuse those,
 * the flag should be set to <code>false</code>. The default value is <code>true</code>.
 * <p>
 * The list of allowed settings is:
 * <ul>
 *   <li>  graph.db 								// The name of the database
 *   <li>  graph.db.create							// Create the DB if not found
 *   <li>  graph.name 								// The name of the graph
 *   <li>  graph.vertex 							// The name of a vertices collection
 *   <li>  graph.edge 								// The name of an edges collection
 *   <li>  graph.relation 							// The allowed from/to relations for edges
 *   <li>  graph.shouldPrefixCollectionNames 		// Boolean flag, true if Vertex and Edge collections will be prefixed with graph name
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
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
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
/* How to opt-out inner test classes?
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.EdgeTest",
		method = "shouldAutotypeFloatProperties",
		reason = "Arango does not keep strict Number types when serializing/deserializing")
@Graph.OptOut(
		test = "org.apache.tinkerpop.gremlin.structure.EdgeTest",
		method = "shouldAutotypeLongProperties",
		reason = "Arango does not keep strict Number types when serializing/deserializing")
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

	private final ArangoDBConfiguration arangoConfig;

	/**
     * The Class ArangoDBGraphFeatures defines the features supported by the ArangoDBGraph
     */

	public class ArangoDBGraphFeatures implements Features {

    	/**
         * The Class ArangoDBGraphGraphFeatures.
         */

    	private class ArangoDBGraphGraphFeatures implements GraphFeatures {

			/** The variable features. */
			private VariableFeatures variableFeatures = new ArangoDBGraphVariables.ArangoDBGraphVariableFeatures();

			/**
			 * Instantiates a new ArangoDB graph graph features.
			 */

			ArangoDBGraphGraphFeatures () { }

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
         * The Class ArangoDBGraphElementFeatures.
         */

        private class ArangoDBGraphElementFeatures implements ElementFeatures {

            /**
             * Instantiates a new ArangoDB graph element features.
             */

            ArangoDBGraphElementFeatures() { }

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
				/*	We can not use Java Objects as keys, ergo we can not support UUID and Integer
				 *  the string representation of these is fine for ArangoDB, which makes the test
				 *  complain because it expects the actual class to be deserialized. We can test
				 *  to see if a string is accepted for deserialization.
				 *  TODO As with properties, a way to support this is to store the id value class
				 */
				return false;
			}
        }

		/**
         * The Class ArangoDBGraphVertexFeatures.
         */

        private class ArangoDBGraphVertexFeatures extends ArangoDBGraphElementFeatures implements VertexFeatures {

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
        }

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
				/*	We can not use Java Objects as keys, ergo we can not support UUID and Integer
				 *  the string representation of these is fine for ArangoDB, which makes the test
				 *  complain because it expects the actual class to be deserialized. We can test
				 *  to see if a string is accepted for deserialization.
				 *  TODO As with properties, a way to support this is to store the id value class
				 */
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

    		ArangoDBGraphEdgePropertyFeatures() { }
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

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBGraph.class);


	/** The Constant DEFAULT_VERTEX_COLLECTION. */

	public static final String DEFAULT_VERTEX_COLLECTION = "vertex";

	/** The Constant DEFAULT_VERTEX_COLLECTION. */

	public static final String DEFAULT_EDGE_COLLECTION = "edge";

	/** The Constant GRAPH_VARIABLES_COLLECTION. */

	public static final String GRAPH_VARIABLES_COLLECTION = "TINKERPOP-GRAPH-VARIABLES";

	/** The Constant ELEMENT_PROPERTIES_COLLECTION. */

	public static final String ELEMENT_PROPERTIES_COLLECTION = "ELEMENT-PROPERTIES";

	/** The Constant ELEMENT_PROPERTIES_EDGE_COLLECTION. */

	public static final String ELEMENT_PROPERTIES_EDGE_COLLECTION = "ELEMENT-HAS-PROPERTIES";

	public static Set<String> GRAPH_COLLECTIONS = new HashSet<>(Arrays.asList(ELEMENT_PROPERTIES_EDGE_COLLECTION, ELEMENT_PROPERTIES_COLLECTION));

	/** The features. */

	private final Features FEATURES = new ArangoDBGraphFeatures();

	/** A EssentialArangoDatabase to handle the connection to the Database. */

	private final EssentialArangoDatabase client;

	/** The name. */

	private final String name;

	/** The vertex collections. */

	private final Collection<String> vertexCollections;

	/** The edge collections. */

	private final Collection<String> edgeCollections;

	/** The relations. */

	private final Collection<String> relations;

	/**  Flat to indicate that the graph has no schema. */

	private boolean schemaless = false;

	/** If collection names should be prefixed with graph name */
	private final boolean shouldPrefixCollectionNames;

	private boolean ready = false;

	// FIXME Cache time expire should be configurable
	LoadingCache<String, Vertex> vertices;

	LoadingCache<String, Edge> edges;

    /**
     * Create a new ArangoDBGraph from the provided configuration.
     *
     * @param configuration		the Apache Commons configuration
     * @return 					the Arango DB graph
     */

    public static ArangoDBGraph open(Configuration configuration) {
		ArangoDBConfiguration arangoConfig = new PlainArangoDBConfiguration(configuration);
		ByteArrayInputStream targetStream = null;
		try {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			arangoConfig.transformToProperties().store(os, null);
			targetStream = new ByteArrayInputStream(os.toByteArray());
		} catch (IOException e) {
			// Ignore exception as the ByteArrayOutputStream is always writable.
		}
		ArangoDBVertexVPack vertexVpack = new ArangoDBVertexVPack();
		ArangoDBEdgeVPack edgeVPack = new ArangoDBEdgeVPack();
		ArangoDB driver = new ArangoDB.Builder().loadProperties(targetStream)
				.registerDeserializer(ArangoDBVertex.class, vertexVpack)
				.registerSerializer(ArangoDBVertex.class, vertexVpack)
				.registerDeserializer(ArangoDBEdge.class, edgeVPack)
				.registerSerializer(ArangoDBEdge.class, edgeVPack)
			.build();
		String dbname = arangoConfig.databaseName()
				.orElseThrow(() -> new IllegalStateException("Database name property missing from configuration."));
		return new ArangoDBGraph(arangoConfig, getDatabase(driver, dbname, arangoConfig.createDatabase()));
	}

	private static ArangoDatabase getDatabase(ArangoDB driver, String dbname, boolean createDatabase) {
		ArangoDatabase db = driver.db(dbname);
		if (createDatabase) {
			if (!db.exists()) {
				logger.info("DB not found, attemtping to create it.");
				try {
					if (!driver.createDatabase(dbname)) {
						throw new ArangoDBGraphException("Unable to crate the database " + dbname);
					}
				}
				catch (ArangoDBException ex) {
					throw ArangoDBExceptions.getArangoDBException(ex);
				}
			}
		}
		else {
			boolean exists = false;
			try {
				exists = db.exists();
			} catch (ArangoDBException ex) {
				// Pass
			}
			finally {
				if (!exists) {
					logger.error("Database does not exist, or the user has no access");
					throw new ArangoDBGraphException("DB not found or user has no access. If you want to force creation " +
							"set the 'graph.db.create' flag to true in the configuration and make sure the user has " +
							"admin rights over the database.");
				}
			}
		}
		return db;
	}

	/**
	 * Creates a Graph (simple configuration).
	 *
	 * @param configuration 	the Apache Commons configuration
	 */

	public ArangoDBGraph(ArangoDBConfiguration configuration, ArangoDatabase database) {

		logger.info("Creating new ArangoDB Graph from configuration");
		arangoConfig = configuration;
		name = arangoConfig.graphName()
				.orElseThrow(() -> new IllegalStateException("Graph name property missing from configuration."));
		vertexCollections = arangoConfig.vertexCollections();
		edgeCollections = arangoConfig.edgeCollections();
		relations = arangoConfig.relations();
		if ((vertices.size() > 1) && (edges.size() > 1) && relations.isEmpty()) {
			throw new IllegalStateException("If two or more vertex and two or more edge collections are defined, then " +
					"at least one relation must be defined too.");
		}
		if (vertexCollections.isEmpty()) {
			schemaless = true;
			vertexCollections.add(DEFAULT_VERTEX_COLLECTION);
		}
		if (edgeCollections.isEmpty()) {
			schemaless = true;
			edgeCollections.add(DEFAULT_EDGE_COLLECTION);
		}
		shouldPrefixCollectionNames = arangoConfig.shouldPrefixCollectionNames();
		client = new EssentialArangoDatabase(database);
		// FIXME Cache time expire should be configurable
		vertices = CacheBuilder.newBuilder()
				.expireAfterAccess(10, TimeUnit.SECONDS)
				.build(new VertexLoader(this));
		edges = CacheBuilder.newBuilder()
				.expireAfterAccess(10, TimeUnit.SECONDS)
				.build(new EdgeLoader(this));
	}

    @Override
	public Vertex addVertex(Object... keyValues) {
		logger.info("Creating vertex in graph with keyValues: {}", keyValues);
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
        ArangoDBVertex vertex = null;
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	id = ElementHelper.getIdValue(keyValues).get();
        	if (this.features().vertex().willAllowId(id)) {
	        	if (id.toString().contains("/")) {
	        		String fullId = id.toString();
	        		String[] parts = fullId.split("/");
	        		// The collection name is the last part of the full name
	        		String[] collectionParts = parts[0].split("_");
					String collectionName = collectionParts[collectionParts.length-1];
					if (collectionName.contains(collection)) {
	        			id = parts[1];
	        			
	        		}
	        	}
        		Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
        		if (m.matches()) {
        			vertex = new ArangoDBVertex(id.toString(), collection, this);
        		}
        		else {
            		throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
            	}
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}

        }
        else {
        	vertex = new ArangoDBVertex(collection, this);
        }
        // The vertex needs to exist before we can attach properties
        getClient().insertVertex(vertex);
        ElementHelper.attachProperties(vertex, keyValues);
		vertices.put((String) vertex.id(), vertex);
        return vertex;
	}


	@Override
	public void close() {
		getClient().shutdown();
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
		return arangoConfig.configuration();
	}

	@Override
	public Features features() {
		return FEATURES;
	}

	/**
	 * Returns the EssentialArangoDatabase object.
	 *
	 * @return the EssentialArangoDatabase object
	 */

	public EssentialArangoDatabase getClient() {
		if (!client.isReady()) {
			String dbname = arangoConfig.databaseName()
					.orElseThrow(() -> new IllegalStateException("Database name property missing from configuration."));
			client.load().connectTo(dbname,arangoConfig.createDatabase());
		}
		if (!ready) {
			ArangoGraph graph = client.getArangoGraph();
			GraphCreateOptions options = new GraphCreateOptions();
			// FIXME Cant be in orphan collections because it will be deleted with graph?
			// options.orphanCollections(GRAPH_VARIABLES_COLLECTION);
			final List<String> prefVCols = vertexCollections.stream().map(this::getPrefixedCollectioName).collect(Collectors.toList());
			final List<String> prefECols = edgeCollections.stream().map(this::getPrefixedCollectioName).collect(Collectors.toList());
			final List<EdgeDefinition> edgeDefinitions = new ArrayList<>();
			if (relations.isEmpty()) {
				logger.info("No relations, creating default ones.");
				edgeDefinitions.addAll(ArangoDBUtil.createDefaultEdgeDefinitions(prefVCols, prefECols));
			} else {
				for (String value : relations) {
					EdgeDefinition ed = ArangoDBUtil.relationPropertyToEdgeDefinition(this, value);
					edgeDefinitions.add(ed);
				}
			}
			edgeDefinitions.add(ArangoDBUtil.createPropertyEdgeDefinitions(this, prefVCols, prefECols));

			if (graph.exists()) {
				ArangoDBUtil.checkGraphForErrors(prefVCols, prefECols, edgeDefinitions, graph, options);
				ArangoDBGraphVariables iter = client.getGraphVariables();
				if (iter == null) {
					throw new ArangoDBGraphException("Existing graph does not have a Variables collection");
				}
			}
			else {
				graph = client.createGraph(name, edgeDefinitions, options);
				ArangoDBGraphVariables variables = new ArangoDBGraphVariables(name, GRAPH_VARIABLES_COLLECTION, this);
				client.insertGraphVariables(variables);
			}
			ready = true;
		}
		return client;
	}

	/**
	 * Returns the identifier of the graph.
	 *
	 * @return the identifier of the graph
	 */

	public String getId() {
		ArangoGraph graph = getClient().getArangoGraph();
		return graph.getInfo().getName();
	}

	/**
	 * The graph name
	 *
	 * @return the name
	 */

	public String name() {
		return this.name;
	}

	@Override
	public Transaction tx() {
		throw Graph.Exceptions.transactionsNotSupported();
	}

	@Override
	public Variables variables() {
		ArangoDBGraphVariables v = getClient().getGraphVariables();
		if (v != null) {
			v.graph(this);
			return v;
        }
        else {
        	throw new ArangoDBGraphException("Existing graph does not have a Variables collection");
        }
	}

	public void removeVertex(ArangoDBVertex vertex) {
		getClient().deleteVertex(vertex);
		vertices.invalidate(vertex.id());
	}

	public void removeEdge(ArangoDBEdge edge) {
		Iterator<Vertex> verticesIt = edge.vertices(Direction.BOTH);
		getClient().deleteEdge(edge);
		edges.invalidate(edge.id());
	}

	/**
	 * Vertex collections.
	 *
	 * @return the list
	 */
	public Collection<String> vertexCollections() {
		return Collections.unmodifiableCollection(vertexCollections);
	}

	//FIXME Implement an Iterator that exploits the cache, we probably want to do an AQL to get all ids, create the
	// iterator with those and populate the cache... maybe load in chunks of 100 elements or something, the idea
	// is to avoid loading the complete graph into memory

	@Override
	public Iterator<Vertex> vertices(Object... vertexIds) {
    	List<String> vertexCollections = new ArrayList<>();
    	List<String> ids = Arrays.stream(vertexIds)
        		.map(id -> {
					if (id instanceof ArangoDBVertex) {
						ArangoDBVertex vertex = (ArangoDBVertex) id;
						if (vertex.isPaired()) {
							vertexCollections.add(vertex.label());
						}
						else {
							vertexCollections.add(getPrefixedCollectioName(vertex.label()));
						}
						return vertex.id();
        			}
        			else {
        				// We only support String ids
        				return id;
        			}
        			})
        		.map(id -> id == null ? (String)id : id.toString())
        		.collect(Collectors.toList());
		try {
			if (ids.isEmpty()) {

			}
			else {
				return vertices.getAll(ids).values().iterator();
			}
		} catch (ExecutionException e) {
			logger.error("Error computing vertices", e);
			throw new IllegalStateException(e);
		}
		//return new ArangoDBIterator<Vertex>(this, getClient().getGraphVertices(ids, vertexCollections));
	}

	/**
	 * Edge collections.
	 *
	 * @return the list
	 */

	public Collection<String> edgeCollections() {
		return Collections.unmodifiableCollection(edgeCollections);
	}

	@Override
	public Iterator<Edge> edges(Object... edgeIds) {
		List<String> edgeCollections = new ArrayList<>();
		List<String> ids = Arrays.stream(edgeIds)
				.map(id -> {
					if (id instanceof ArangoDBEdge) {
						ArangoDBEdge edge = (ArangoDBEdge) id;
						if (edge.isPaired()) {
							edgeCollections.add(edge.label());
						}
						else {
							edgeCollections.add(getPrefixedCollectioName(edge.label()));
						}
						return edge.id();
					}
					else {
						// We only support String ids
						return id;
					}
				})
				.map(id -> id == null ? (String)id : id.toString())
				.collect(Collectors.toList());
		return new ArangoDBIterator<Edge>(this, getClient().getGraphEdges(ids, edgeCollections));
	}

	/**
	 * Return the collection name correctly prefixed according to the shouldPrefixCollectionNames flag
	 * @param collectionName
	 * @return
	 */
	public String getPrefixedCollectioName(String collectionName) {
		if (GRAPH_VARIABLES_COLLECTION.equals(collectionName)) {
			return collectionName;
		}
		if (GRAPH_COLLECTIONS.contains(collectionName)) {
			return String.format("%s_%s", name, collectionName);
		}
		if(shouldPrefixCollectionNames) {
			return String.format("%s_%s", name, collectionName);
		}else{
			return collectionName;
		}
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

	/**
	 * The graph relations.
	 *
	 * @return the collection of relations
	 */
	private Collection<String> relations() {
		return relations;
	}

   // TODO Decide which of these methods we want to keep

//	@Override
//	public <T extends Element> void dropKeyIndex(String name, Class<T> elementClass) {
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
//		String normalizedKey = ArangoDBUtil.normalizeKey(name);
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
//	public <T extends Element> void createKeyIndex(String name, Class<T> elementClass, Parameter... indexParameters) {
//
//		IndexType type = IndexType.SKIPLIST;
//		boolean unique = false;
//		List<String> fields = new ArrayList<String>();
//
//		String n = ArangoDBUtil.normalizeKey(name);
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

}
