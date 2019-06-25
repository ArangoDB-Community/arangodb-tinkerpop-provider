//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import com.arangodb.ArangoDatabase;
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
 * The ArangoDB graphClient class.
 *
 * NOTE: USE OF THIS API REQUIRES A USER WITH <b>ADMINISTRATOR</b> ACCESS IF THE <b>DB</b> USED FOR
 * THE GRAPH DOES NOT EXIST. As per ArangoDB, creating DB is only allowed for the root user, hence
 * only the root user can be used if the DB does not exist.
 * <p>
 * <b>ArangoDB and TinkerPop Ids.</b>
 * <p>
 * In TinkerPop, graphClient elements are expected to have a unique Id within the graphClient; in ArangoDB the
 * Id (document handle) consists of the label's name and the document name (primaryKey attribute)
 * separated by /, hence the only way to hint at ids is by providing a primaryKey during construction.
 * Hence, ArangoDBGraph elements do not strictly support <i>User Supplied Ids</i>. We allow
 * ids to be supplied during vertex creation: {@code graphClient.addVertex(id,x)}, but this id actually
 * represents the primaryKey. As a result, posterior search/match by id must prefix the vertex's label
 * (label) followed by a /.
 * <p>
 * An ArangoDBGraph is instantiated from an Apache Commons Configuration instance. The configuration
 * must provide both TinkerPop and ArangoDB configuration options. The ArangoDB options are
 * described in the ArangoDB Java ServerClient <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">documentation.</a>
 *
 * For the TinkerPop part, the configuration must provide as a minimum the databaseClient name and the
 * graphClient name. If no vertex, edge and relation information is provided, the graphClient will be considered
 * schema-less.
 * <p>
 * All settings are prefixed with "gremlin.arangodb.conf". So, for example, to set the value of the
 * Arango DB hosts property (arango db configuration), the configuration must read:
 * <pre>gremlin.arangodb.conf.arangodb.hosts = 127.0.0.1:8529
 * </pre>
 * while for the db name (graphClient configuration) it will be:
 * <pre>gremlin.arangodb.conf.graphClient.db = myDB
 * </pre>
 * <p>
 * To define the schema, (EdgeCollections in ArangoDB world) three elementProperties can be used:
 * <tt>graphClient.vertex</tt>, <tt>graphClient.edge</tt> and <tt>graphClient.relation</tt>. The graphClient.vertex and
 * graphClient.edge elementProperties allow definition of the ArangoDB collections used to store nodes and edges
 * respectively. The relations property is used to describe the allowed edge-node relations. For
 * simple graphs, only one graphClient.vertex and graphClient.edge elementProperties need to be provided. In this case
 * edges are allowed to connect to any two nodes. For example:
 * <pre>gremlin.arangodb.conf.graphClient.vertex = Place
 *gremlin.arangodb.conf.graphClient.edge = Transition
 * </pre>
 * would allow the user to create Vertices that represent Places, and Edges that represent
 * Transitions. A transition can be created between any two Places. If additional vertices and edges
 * were added, the resulting graphClient schema would be fully connected, that is, edges would be allowed
 * between any two useClient of vertices.
 * <p>
 * For more complex graphClient structures, the graphClient.relation property is used to tell the ArangoDB what
 * relations are allowed, e.g.:
 * <ul>
 * <li>One-to-one edges
 *<pre>gremlin.arangodb.conf.graphClient.vertex = Place
 *gremlin.arangodb.conf.graphClient.vertex = Transition
 *gremlin.arangodb.conf.graphClient.edge = PTArc
 *gremlin.arangodb.conf.graphClient.edge = TPArc
 *gremlin.arangodb.conf.graphClient.relation = PTArc:Place-&gt;Transition
 *gremlin.arangodb.conf.graphClient.relation = TPArc:Transition-&gt;Place
 *</pre>
 * would allow the user to create nodes to represent Places and Transitions, and edges to represent
 * Arcs. However, in this case, we have two type of arcs: PTArc and TPArc. The relations specify
 * that PTArcs can only go from Place to Transitions and TPArcs can only go from Transitions to
 * Places. A relation can also specify multiple to/from nodes. In this case, the to/from values is a
 * comma separated list of names.
 * <li>Many-to-many edges
 *  <pre>gremlin.arangodb.conf.graphClient.vertex = male
 *gremlin.arangodb.conf.graphClient.vertex = female
 *gremlin.arangodb.conf.graphClient.edge = relation
 *gremlin.arangodb.conf.graphClient.relation = relation:male,female-&gt;male,female
 *  </pre>
 * </ul>
 * <p>
 * In order to allow multiple graphs in the same databaseClient, vertex and edge collections can be prefixed with the
 * graphClient name in order to avoid label clashes. To enable this function the graphClient.shouldPrefixCollectionNames
 * property should be set to <code>true</code>. If you have an existing graphClient/collections and want to reuse those,
 * the flag should be set to <code>false</code>. The default value is <code>true</code>.
 * <p>
 * The list of allowed settings is:
 * <ul>
 *   <li>  graphClient.db 								// The name of the databaseClient
 *   <li>  graphClient.db.create							// Create the DB if not found
 *   <li>  graphClient.name 								// The name of the graphClient
 *   <li>  graphClient.vertex 							// The name of a vertices label
 *   <li>  graphClient.edge 								// The name of an edges label
 *   <li>  graphClient.relation 							// The allowed from/to relations for edges
 *   <li>  graphClient.shouldPrefixCollectionNames 		// Boolean flag, true if Vertex and Edge collections will be prefixed with graphClient name
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
// OptOut ALL graphClient IO out tests. Not possible with ArangoDBedge definitions
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
		reason = "Tests expected LoadGraphWith.GraphData.CREW to be loaded, but another graphClient is, so property navigation fails.")
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
public class ArangoDBGraph implements ArngGraph {

	private final GraphConfiguration arangoConfig;

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
			 * Instantiates a new ArangoDB graphClient graphClient features.
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
             * Instantiates a new ArangoDB graphClient element features.
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
				 *  TODO As with elementProperties, a way to support this is to store the id value class
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
             * Instantiates a new ArangoDB graphClient vertex features.
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
             * Instantiates a new ArangoDB graphClient edge features.
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
    		 * Instantiates a new ArangoDB graphClient vertex property features.
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
				 *  TODO As with elementProperties, a way to support this is to store the id value class
				 */
				return false;
			}
        }

        /**
         * The Class ArangoDBGraphEdgePropertyFeatures.
         */
        private class ArangoDBGraphEdgePropertyFeatures implements EdgePropertyFeatures {

		    /**
    		 * Instantiates a new ArangoDB graphClient edge property features.
    		 */

    		ArangoDBGraphEdgePropertyFeatures() { }
        }

		/** The graphClient features. */

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

	/** The Constant DEFAULT_VERTEX_LABEL. */

	public static final String DEFAULT_VERTEX_LABEL = "vertex";

	/** The Constant DEFAULT_VERTEX_LABEL. */

	public static final String DEFAULT_EDGE_COLLECTION = "edge";

	/** The features. */

	private final Features FEATURES = new ArangoDBGraphFeatures();

	private final GraphClient graphClient;

	private final GraphVariablesClient variablesClient;

	/** The name. */

	private final String name;

	/** The vertex collections. */

	private final Collection<String> vertexCollections;

	/** The edge collections. */

	private final Collection<String> edgeCollections;

	/**  Flat to indicate that the graphClient has no schema. */

	private boolean schemaless = false;

	// FIXME Cache time expire should be configurable
	LoadingCache<String, Vertex> vertices;

	LoadingCache<String, Edge> edges;

    /**
     * Create a new ArangoDBGraph from the provided configuration.
     *
     * @param configuration		the Apache Commons configuration
     * @return 					the Arango DB graphClient
     */
	// FIXME Move this to another class
    public static ArangoDBGraph open(Configuration configuration) {
		final GraphConfiguration arangoConfig = new ArngGraphConfiguration(configuration);
		final String dbname = arangoConfig.databaseName()
				.orElseThrow(() -> new IllegalStateException("DatabaseClient name property missing from configuration."));
		final String graphName = arangoConfig.graphName().orElseThrow(() -> new IllegalStateException("Graph name property missing from configuration."));
		final ServerClient driver = new ArngServerClient(arangoConfig.buildDriver());
		ArangoDatabase database = driver.getDatabase(dbname);
		if (!database.exists()) {
			if (arangoConfig.createDatabase()) {
				try {
					database = driver.createDatabase(dbname);
				} catch (ServerClient.DatabaseCreationException e) {
					throw new ArangoDBGraphException("Unable to crate the databaseClient " + dbname);
				}
			}
			else {
				throw new ArangoDBGraphException("DB not found or user has no access. If you want to force creation " +
						"set the 'graphClient.db.create' flag to true in the configuration and make sure the user has " +
						"admin rights over the databaseClient.");
			}
		}

		DatabaseClient databaseClient = new ArngDatabaseClient(database);
		ArangoGraph databaseGraph = databaseClient.graph(graphName);
		if (databaseGraph.exists()) {
			try {
				arangoConfig.checkGraphForErrors(databaseGraph, new GraphCreateOptions());
			} catch (ArngGraphConfiguration.MalformedRelationException e) {
				throw new IllegalStateException("Existing graph does not match configuration", e);
			}
		}
		else {
			arangoConfig.createGraph(graphName, new GraphCreateOptions());
		}



//		// FIXME GraphClient could only use the configuration...
//
//		GraphClient graphClient = new ArngGraphClient(
//				databaseClient,
//				graphName,
//				arangoConfig.shouldPrefixCollectionNames());
//
//		final List<String> prefECols = arangoConfig.edgeCollections().stream().map(graphClient::getPrefixedCollectioName).collect(Collectors.toList());
//		try {
//			graphClient = graphClient.pairWithDatabaseGraph(
//						prefVCols,
//						new ArngEdgeDefinitions(graphClient)
//								.createEdgeDefinitions(prefVCols, prefECols, arangoConfig.relations()),
//					    new GraphCreateOptions());
//		} catch (DatabaseClient.GraphCreationException e) {
//			throw new ArangoDBGraphException(e);
//		} catch (EdgeDefinitions.MalformedRelationException e) {
//			throw new ArangoDBGraphException(e);
//		}

		return new ArangoDBGraph(arangoConfig, databaseClient, new ArngGraphVariablesClient(databaseClient, graphName));
	}

	/**
	 * Creates a Graph
	 *
	 * @param configuration 		the GraphConfiguration
	 * @param databaseClient		the database client
	 * @param variablesClient		the graph variables client
	 */

	public ArangoDBGraph(
		GraphConfiguration configuration,
		DatabaseClient databaseClient,
		GraphVariablesClient variablesClient) {

		logger.info("Creating new ArangoDB Graph from configuration");
		arangoConfig = configuration;
		name = arangoConfig.graphName()
				.orElseThrow(() -> new IllegalStateException("Graph name property missing from configuration."));
		vertexCollections = arangoConfig.vertexCollections();
		edgeCollections = arangoConfig.edgeCollections();
		if ((vertices.size() > 1) && (edges.size() > 1) && arangoConfig.relations().isEmpty()) {
			throw new IllegalStateException("If two or more vertex and two or more edge collections are defined, then " +
					"at least one relation must be defined too.");
		}
		if (vertexCollections.isEmpty()) {
			schemaless = true;
			vertexCollections.add(DEFAULT_VERTEX_LABEL);
		}
		if (edgeCollections.isEmpty()) {
			schemaless = true;
			edgeCollections.add(DEFAULT_EDGE_COLLECTION);
		}
		this.graphClient = new ArngGraphClient(databaseClient, this);
		this.variablesClient = variablesClient;
		// FIXME Cache time expire should be configurable
		vertices = CacheBuilder.newBuilder()
				.expireAfterAccess(10, TimeUnit.SECONDS)
				.build(new VertexLoader(this));
		edges = CacheBuilder.newBuilder()
				.expireAfterAccess(10, TimeUnit.SECONDS)
				.build(new EdgeLoader(this));
	}

	@Override
	public void close() throws Exception {
		graphClient.close();
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

	@Override
	public Transaction tx() {
		throw Graph.Exceptions.transactionsNotSupported();
	}

	@Override
	public Variables variables() {
		try {
			return variablesClient.getGraphVariables();
		} catch (GraphVariablesClient.GraphVariablesNotFoundException e) {
			return variablesClient.insertGraphVariables();
		}
	}

	@Override
	public Vertex addVertex(Object... keyValues) {
		logger.info("Creating vertex in graphClient with keyValues: {}", keyValues);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		String label;
		if (!schemaless) {
			label = ElementHelper.getLabelValue(keyValues).orElse(null);
			ElementHelper.validateLabel(label);
		}
		else {
			label = DEFAULT_VERTEX_LABEL;
		}
		if (!vertexCollections().contains(label)) {
			throw new IllegalArgumentException(String.format("Vertex label (%s) not in graphClient (%s) vertex collections.", label, name));
		}
		String key = null;
		if (ElementHelper.getIdValue(keyValues).isPresent()) {
			Object id = ElementHelper.getIdValue(keyValues).get();
			if (this.features().vertex().willAllowId(id)) {
				if (id.toString().contains("/")) {
					String fullId = id.toString();
					String[] parts = fullId.split("/");
					// The label name is the last part of the full name
					String[] collectionParts = parts[0].split("_");
					String collectionName = collectionParts[collectionParts.length-1];
					if (collectionName.contains(label)) {
						id = parts[1];

					}
				}
				Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
				if (!m.matches()) {
					throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
				}
			}
			else {
				throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
			}
			key = id.toString();
		}
		ArangoDBVertex vertex = graphClient.insertVertex(key, label, keyValues);
		vertices.put((String) vertex.id(), vertex);
		return vertex;
	}


	//FIXME Implement an Iterator that exploits the cache, we probably want to do an AQL to get all ids, create the
	// iterator with those and populate the cache... maybe load in chunks of 100 elements or something, the idea
	// is to avoid loading the complete graph into memory

	@Override
	public Iterator<Vertex> vertices(Object... vertexIds) {
    	List<String> collections = new ArrayList<>();
    	List<String> ids = Arrays.stream(vertexIds)
        		.map(id -> {
					if (id instanceof ArangoDBVertex) {
						ArangoDBVertex vertex = (ArangoDBVertex) id;
						collections.add(vertex.label());
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
				// We want all vertices
			}
			else {
				return vertices.getAll(ids).values().iterator();
			}
		} catch (ExecutionException e) {
			logger.error("Error computing vertices", e);
			throw new IllegalStateException(e);
		}
		return new ArangoDBIterator<Vertex>(this, getDatabaseClient().getGraphVertices(ids, collections));
	}


	@Override
	public Iterator<Edge> edges(Object... edgeIds) {
		List<String> collections = new ArrayList<>();
		List<String> ids = Arrays.stream(edgeIds)
				.map(id -> {
					if (id instanceof ArangoDBEdge) {
						ArangoDBEdge edge = (ArangoDBEdge) id;
						collections.add(edge.label());
						return edge.id();
					}
					else {
						// We only support String ids
						return id;
					}
				})
				.map(id -> id == null ? (String)id : id.toString())
				.collect(Collectors.toList());
		return new ArangoDBIterator<Edge>(this, getDatabaseClient().getGraphEdges(ids, collections));
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

//	/**
//	 * Returns the identifier of the graphClient.
//	 *
//	 * @return the identifier of the graphClient
//	 */
//	@Deprecated
//	public String getId() {
//		ArangoGraph graph = getDatabaseClient().getArangoGraph();
//		return graph.getInfo().getName();
//	}

	/**
	 * The graph's name
	 *
	 * @return the graph's name
	 */

	@Override
	public String name() {
		return name;
	}

	/**
	 * Vertex collections.
	 *
	 * @return the list
	 */

	@Override
	public Collection<String> vertexCollections() {
		return vertexCollections.stream().map(arangoConfig::getDBCollectionName).collect(Collectors.toList();
	}

	/**
	 * Edge collections.
	 *
	 * @return the list
	 */

	@Override
	public Collection<String> edgeCollections() {
		return edgeCollections.stream().map(arangoConfig::getDBCollectionName).collect(Collectors.toList();
	}

	@Override
	public boolean hasEdgeCollection(String label) {
		return edgeCollections.contains(label);
	}

	@Override
	public String getDBCollectionName(String collectionName) {
		return arangoConfig.getDBCollectionName(collectionName);
	}


	/// ADD THIS METHOS DO INTERFACE




	/**
	 * The graphClient relations.
	 *
	 * @return the label of relations
	 */
	private Collection<String> relations() {
		return arangoConfig.relations();
	}

   // TODO Decide which of these methods we want to keep

//	@Override
//	public <T extends Element> void dropKeyIndex(String name, Class<T> elementClass) {
//
//		List<ArangoDBIndex> indices = null;
//		try {
//			if (elementClass.isAssignableFrom(Vertex.class)) {
//				indices = databaseClient.getVertexIndices(simpleGraph);
//			} else if (elementClass.isAssignableFrom(Edge.class)) {
//				indices = databaseClient.getEdgeIndices(simpleGraph);
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
//				databaseClient.deleteIndex(index.getId());
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
//				getDatabaseClient().createVertexIndex(simpleGraph, type, unique, fields);
//			} else if (elementClass.isAssignableFrom(Edge.class)) {
//				getDatabaseClient().createEdgeIndex(simpleGraph, type, unique, fields);
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
//				indices = databaseClient.getVertexIndices(simpleGraph);
//			} else if (elementClass.isAssignableFrom(Edge.class)) {
//				indices = databaseClient.getEdgeIndices(simpleGraph);
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
