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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.GraphEntity;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBQuery;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleGraphClient;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

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
public class ArangoDBGraph implements Graph {

	/**
     * The Class ArangoDBGraphFeatures.
     */
    public class ArangoDBGraphFeatures implements Features {
	    
    	/**
         * The Class ArangoDBGraphEdgeFeatures.
         */
        public class ArangoDBGraphEdgeFeatures extends ArangoDBGraphElementFeatures implements EdgeFeatures {

		    /** The edge property features. */
    		private final EdgePropertyFeatures edgePropertyFeatures = new ArangoDBGraphEdgePropertyFeatures();

            /**
             * Instantiates a new arango DB graph edge features.
             */
            ArangoDBGraphEdgeFeatures() { }

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
        }
        
        /**
         * The Class ArangoDBGraphEdgePropertyFeatures.
         */
        private class ArangoDBGraphEdgePropertyFeatures implements EdgePropertyFeatures {

		    /**
    		 * Instantiates a new arango DB graph edge property features.
    		 */
    		ArangoDBGraphEdgePropertyFeatures() {
            }
		    
        }
        
        /**
         * The Class ArangoDBGraphElementFeatures.
         */
        public class ArangoDBGraphElementFeatures implements ElementFeatures {

            /**
             * Instantiates a new arango DB graph element features.
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
			 * Instantiates a new arango DB graph graph features.
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
             * Instantiates a new arango DB graph vertex features.
             */
            ArangoDBGraphVertexFeatures () { }
            
            @Override
			public Cardinality getCardinality(String key) {
				return VertexProperty.Cardinality.single;
			}

			@Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }
        
        }

		/**
         * The Class ArangoDBGraphVertexPropertyFeatures.
         */
        private class ArangoDBGraphVertexPropertyFeatures implements VertexPropertyFeatures {

		    /**
    		 * Instantiates a new arango DB graph vertex property features.
    		 */
    		ArangoDBGraphVertexPropertyFeatures() { }

			@Override
			public boolean supportsAnyIds() {
				return false;
			}
			
			@Override
			public boolean supportsNumericIds() {
				return false;
			}

			@Override
			public boolean supportsUserSuppliedIds() {
				return true;
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

	class ArangoDBIterator<IType extends Element> implements Iterator<IType> {
		
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
		checkValues(arangoConfig.getString(CONFIG_DB_NAME), arangoConfig.getString(CONFIG_GRAPH_NAME),	vertexCollections,
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
        ArangoGraph graph = client.getGraph(arangoConfig.getString(CONFIG_GRAPH_NAME));
        if (graph.exists()) {
            graphHasError(vertexCollections, edgeCollections, relations, graph);
        }
        else {
			client.createGraph(arangoConfig.getString(CONFIG_GRAPH_NAME), vertexCollections,
            		edgeCollections,
            		relations);
		}
		this.name = graph.name();
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
			throw new IllegalArgumentException("Vertex label not in graph vertex collections.");
		}
        ArangoDBVertex<Object> vertex = null;
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	id = ElementHelper.getIdValue(keyValues).get();
        	if (this.features().vertex().willAllowId(id)) {
        		vertex = new ArangoDBVertex<Object>(this, collection, id.toString());
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
			// TODO Auto-generated catch block
			return null;
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
	 * @throws ArangoDBGraphException the arango DB graph exception
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
        throw new IllegalArgumentException();
	}

	@Override
	public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Configuration configuration() {
		return null;
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
        		.map(String.class::cast)
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
	 * Validate if an existing graph is correctly configured to handle the desired nodes, edges and relations.
	 *
	 * @param verticesCollectionNames The names of collections for nodes
	 * @param edgesCollectionNames The names of collections for edges
	 * @param relations The description of edge definitions
	 * @param graph the graph
	 * @throws ArangoDBGraphException the arango DB graph exception
	 */
	private void graphHasError(List<String> verticesCollectionNames,
			List<String> edgesCollectionNames,
			List<String> relations,
			ArangoGraph graph) throws ArangoDBGraphException {
		
		
		if (!verticesCollectionNames.containsAll(graph.getVertexCollections())) {
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
        Map<String, EdgeDefinition> requiredDefinitions = new HashMap<>(relations.size());
		if (relations.isEmpty()) {
			EdgeDefinition ed = ArangoDBUtil.createDefaultEdgeDefinition(name, verticesCollectionNames, edgesCollectionNames);
			requiredDefinitions.put(ed.getCollection(), ed);
		} else {
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
        	}
        }
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
		return null;
	}

	@Override
	public Variables variables() {
		return null;
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
        		.map(String.class::cast)
        		.collect(Collectors.toList());
		ArangoDBQuery query = getClient().getGraphVertices(this, ids);
		try {
			return new ArangoDBIterator<Vertex>(this, query.getCursorResult(ArangoDBVertex.class));
		} catch (ArangoDBGraphException e) {
			// TODO Auto-generated catch block
			return null;
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
