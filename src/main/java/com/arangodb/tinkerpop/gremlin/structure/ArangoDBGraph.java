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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
 * The configuration must provide the database name, Graph name and EdgeCollection information. Additionally, it must
 * provide the various DB settings. These settings must use the "gremlin.arangodb.conf" prefix followed by the
 * specific ArangoDB property name, e.g. to set the value of the hosts property, the configuration must read:
 * <pre>
 * 	gremlin.arangodb.conf.arangodb.hosts = 127.0.0.1:8529
 * </pre>
 * To define the ArangoDb graph EdgeColleciton information three properties can be used: graph.vertex, graph.edge and
 * graph.relation. The graph.vertex and graph.edge properties allow definition of the ArangoDB collections used to 
 * store nodes and edges respectively. The relations property is used to describe the allowed relations. For simple
 * graphs, only one graph.vertex and graph.edge properties are to be used. In this case edges are allowed to connect
 * to any two nodes. For example
 * <pre>
 * 	gremlin.arangodb.conf.graph.vertex = Place
 *  gremlin.arangodb.conf.graph.edge = Transition
 * </pre>
 * would allow the user to create Vertices that represent Places, and Edges that represent Transitions. A transition
 * can be created between any two Places.
 * 
 *  For more complex graph structures, the graph.relation property is used to tell the ArangoDB what relations are 
 *  allowed, e.g.:
 * <pre>
 * 	gremlin.arangodb.conf.graph.vertex = Place
 * 	gremlin.arangodb.conf.graph.vertex = Transition
 *  gremlin.arangodb.conf.graph.edge = PTArc
 *  gremlin.arangodb.conf.graph.edge = TPArc
 *  gremlin.arangodb.conf.graph.relation = PTArc:Place->Transition
 *  gremlin.arangodb.conf.graph.relation = TPArc:Transition->Place
 * </pre>
 * would allow the user to create nodes to represent Places and Transitions, and edges to represent Arcs. However, in
 * this case, we have two type of arcs: PTArc and TPArc. The relations specify that PTArcs can only go from Place to
 * Transitions and TPArcs can only go from Transitions to Places. A relation can also specify multiple to/from nodes.
 * In this case, the to/from values is a comma separated list of names. 
 * 
 * The list of ArangoDB properties are:
 * <ul>
 *   <li>  arangodb.graph.db: The name of the database
 *   <li>  arangodb.graph.name: The name of the graph
 *   <li>  arangodb.graph.vertex: The name of a vertices collection
 *   <li>  arangodb.graph.edge: The name of an edges collection
 *   <li>  arangodb.graph.relation: The allowed from/to relations for edges
 *   <li>  arangodb.hosts
 *   <li>  arangodb.host
 *   <li>  arangodb.port
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

public class ArangoDBGraph implements Graph {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBGraph.class);

	private final Features FEATURES = new ArangoDBGraphFeatures();
	
    public static final String CONFIG_CONF = "gremlin.arangodb.conf";
    public static final String CONFIG_DB = "graph.db";
    public static final String CONFIG_NAME = "graph.name";
    public static final String CONFIG_VERTICES = "graph.vertex";
    public static final String CONFIG_EDGES = "graph.edge";
    public static final String CONFIG_RELATIONS = "graph.relation";
    
	/**
	 * A ArangoDBSimpleGraphClient to handle the connection to the Database
	 */
	private ArangoDBSimpleGraphClient client = null;

	private String name;
	
	private final List<String> vertexCollections;
	private final List<String> edgeCollections;
	private final List<String> relations;

	/**
	 * Creates a Graph (simple configuration)
	 * 
	 * @param host
	 *            the ArangoDB host name
	 * @param port
	 *            the ArangoDB port
	 * @param name
	 *            the name of the graph
	 * @param verticesCollectionName
	 *            the name of the vertices collection
	 * @param edgesCollectionName
	 *            the name of the edges collection
	 * 
	 * @throws ArangoDBGraphException
	 *             if the graph could not be created
	 */

	public ArangoDBGraph(Configuration configuration)
			throws ArangoDBGraphException {
		
		// logger.info("Creating new ArangoDB Graph with name {}", name);
		Configuration arangoConfig = configuration.subset(CONFIG_CONF);
		vertexCollections = arangoConfig.getList(CONFIG_VERTICES).stream()
				.map(String.class::cast)
				.collect(Collectors.toList());
		edgeCollections = arangoConfig.getList(CONFIG_EDGES).stream()
				.map(String.class::cast)
				.collect(Collectors.toList());
		relations = arangoConfig.getList(CONFIG_RELATIONS).stream()
				.map(String.class::cast)
				.collect(Collectors.toList());
		checkValues(arangoConfig.getString(CONFIG_DB), arangoConfig.getString(CONFIG_NAME),	vertexCollections,
				edgeCollections, relations);
		Properties arangoProperties = ConfigurationConverter.getProperties(arangoConfig);
		int batchSize = 0;
		client = new ArangoDBSimpleGraphClient(arangoProperties, arangoConfig.getString(CONFIG_DB), batchSize);
        ArangoGraph graph = client.getGraph(arangoConfig.getString(CONFIG_NAME));
        if (graph.exists()) {
            graphHasError(vertexCollections, edgeCollections, relations, graph);
        }
        else {
			client.createGraph(arangoConfig.getString(CONFIG_NAME), vertexCollections,
            		edgeCollections,
            		relations);
		}
		this.name = graph.name();
	}
	
	public String name() {
		return this.name;
	}
	

	public List<String> vertexCollections() {
		return Collections.unmodifiableList(vertexCollections);
	}

	public List<String> edgeCollections() {
		return Collections.unmodifiableList(edgeCollections);
	}

	/**
	 * Check that the configuration values are sound
	 * @param db
	 * @param name
	 * @param vertices
	 * @param edges
	 * @param relations
	 * @throws ArangoDBGraphException
	 */
	private void checkValues(String db, String name, List<String> vertices, List<String> edges, List<String> relations)
			throws ArangoDBGraphException {
		
		if (StringUtils.isBlank(db)) {
			throw new ArangoDBGraphException("db name must not be null.");
		}
		
		if (StringUtils.isBlank(name)) {
			throw new ArangoDBGraphException("graph name must not be null.");
		}

		if (CollectionUtils.isEmpty(vertices)) {
			throw new ArangoDBGraphException("vertex collection name must not be empty.");
		}

		if (CollectionUtils.isEmpty(vertices)) {
			throw new ArangoDBGraphException("edge collection name must not be empty.");
		}
		Collection<String> v = CollectionUtils.emptyIfNull(vertices);
		Collection<String> e = CollectionUtils.emptyIfNull(edges);
		if ((v.size() > 1) && (e.size() > 1) && CollectionUtils.isEmpty(relations)) {
			throw new ArangoDBGraphException("If more than one vertex/edge collection is provided, relations must be defined");
		}	
	}

	/**
	 * Validate if an existing graph is correctly configured to handle the desired nodes, edges and relations
	 * @param verticesCollectionNames The names of collections for nodes
	 * @param edgesCollectionNames	The names of collections for edges
	 * @param relations The description of edge definitions
	 * @param graph
	 * @return
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
			EdgeDefinition ed = new EdgeDefinition()
					.collection(edgesCollectionNames.get(0))
					.from(verticesCollectionNames.get(0))
					.to(verticesCollectionNames.get(0));
			requiredDefinitions.put(ed.getCollection(), ed);
		} else {
			for (Object value : relations) {
				EdgeDefinition ed = ArangoDBUtil.relationPropertyToEdgeDefinition((String) value);
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
        		if (existingSet.equals(requiredSet)) {
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
	
	

	private boolean hasOneFromAndTo(EdgeDefinition edgeDefinitionEntity) {
		return edgeDefinitionEntity.getFrom().size() != 1 || edgeDefinitionEntity.getTo().size() != 1;
	}

	@Override
	public Features features() {
		return FEATURES;
	}

	@Override
	public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        String key;
        final ArangoDBVertex vertex;
        if (!ElementHelper.getLabelValue(keyValues).isPresent()) {
        	Graph.Exceptions.argumentCanNotBeNull(T.label.name());
        }
        String collection = ElementHelper.getLabelValue(keyValues).get();
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	key = ElementHelper.getIdValue(keyValues).get().toString();
        	vertex = new ArangoDBVertex(this, collection, key);
        }
        else {
        	vertex = new ArangoDBVertex(this, collection);
        }
        ElementHelper.attachProperties(vertex, keyValues);
        client.insertVertex(this, vertex);
        return vertex;
	}

	@Override
	public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
		throw new UnsupportedOperationException();
	}

	@Override
	public GraphComputer compute() throws IllegalArgumentException {
        throw new IllegalArgumentException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Vertex> vertices(Object... vertexIds) {
        List<String> ids = Arrays.stream(vertexIds)
        		.map(String.class::cast)
        		.collect(Collectors.toList());
		ArangoDBQuery query = getClient().getGraphVertices(this, ids);
		return query.getCursorResult(Vertex.class);
	}

	@Override
	public Iterator<Edge> edges(Object... edgeIds) {
		return null;
	}

	@Override
	public Transaction tx() {
		return null;
	}

	@Override
	public void close() {
		client.shutdown();
	}

	@Override
	public Variables variables() {
		return null;
	}

	@Override
	public Configuration configuration() {
		return null;
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
		String internal =  "{\"name\":\"" + name() + "\",\"vertices\":\"" + vertices + "\",\"edges\":\""
					+ edges+ "\",\"relations\":\""
							+ relations +"\"}";
		return StringFactory.graphString(this, internal);
	}

	private Collection<String> relations() {
		return relations;
	}

	/**
	 * Returns the ArangoDBSimpleGraphClient object
	 * 
	 * @return the ArangoDBSimpleGraphClient object
	 */
	public ArangoDBSimpleGraphClient getClient() {
		return client;
	}

	/**
	 * Returns the identifier of the graph
	 * 
	 * @return the identifier of the graph
	 */
	public String getId() {
		ArangoGraph graph = client.getGraph(name);
		return graph.getInfo().getName();
	}

    public class ArangoDBGraphFeatures implements Features {
	    protected GraphFeatures graphFeatures = new ArangoDBGraphGraphFeatures();
        protected VertexFeatures vertexFeatures = new ArangoDBGraphVertexFeatures();
        protected EdgeFeatures edgeFeatures = new ArangoDBGraphEdgeFeatures();

		@Override
		public GraphFeatures graph() {
			return graphFeatures;
		}

		@Override
		public VertexFeatures vertex() {
			return vertexFeatures;
		}

		@Override
		public EdgeFeatures edge() {
			return edgeFeatures;
		}

		@Override
		public String toString() {
			return StringFactory.featureString(this);
		}

        public class ArangoDBGraphGraphFeatures implements GraphFeatures {

			private VariableFeatures variableFeatures = new ArangoDBGraphVariables.ArangoDBGraphVariableFeatures();

			ArangoDBGraphGraphFeatures () {

			}

			@Override
			public boolean supportsComputer() {
				return false;
			}

			@Override
			public boolean supportsConcurrentAccess() {
				return false;
			}

			@Override
			public boolean supportsTransactions() {
				return false;
			}

			@Override
			public boolean supportsThreadedTransactions() {
				return false;
			}

			@Override
			public VariableFeatures variables() {
				return variableFeatures;
			}
		}

        public class ArangoDBGraphVertexFeatures extends ArangoDBGraphElementFeatures implements VertexFeatures {

		    private final VertexPropertyFeatures vertexPropertyFeatures = new ArangoDBGraphVertexPropertyFeatures();

            ArangoDBGraphVertexFeatures () { }
            
            @Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }
        }

        public class ArangoDBGraphEdgeFeatures extends ArangoDBGraphElementFeatures implements EdgeFeatures {

		    private final EdgePropertyFeatures edgePropertyFeatures = new ArangoDBGraphEdgePropertyFeatures();

            ArangoDBGraphEdgeFeatures() { }

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
        }


        public class ArangoDBGraphElementFeatures implements ElementFeatures {

            ArangoDBGraphElementFeatures() {
            }
        }

        private class ArangoDBGraphVertexPropertyFeatures implements VertexPropertyFeatures {

		    ArangoDBGraphVertexPropertyFeatures() { }

			@Override
			public boolean supportsUserSuppliedIds() {
				return true;
			}

			@Override
			public boolean supportsNumericIds() {
				return false;
			}

			@Override
			public boolean supportsAnyIds() {
				return false;
			}
		    
        }

        private class ArangoDBGraphEdgePropertyFeatures implements EdgePropertyFeatures {

		    ArangoDBGraphEdgePropertyFeatures() {
            }
		    
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
