//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;


/**
 * The Class ArangoDBConfigurationBuilder provides a convenient method for creating ArangoDB graph
 * configurations.
 * If no parameters are set the default values used are:
 * <ul>
 * <li>dbName: tinkerpop
 * <li>graphName: graph
 * <li>vertices: (empty - use default ArangoDBGraph settings)
 * <li>edges: (empty - use default ArangoDBGraph settings)
 * <li>relations: (empty - use default ArangoDBGraph settigns)
 * <li>user: gremlin
 * <li>password: gremlin
 * <li>other db settings: (default ArangoDB Java driver settings).
 * <li>collectionNames: prefixed with graphName</li>
 * </ul>
 */
public class ArangoDBConfigurationBuilder {
	
	/** The Logger. */
    
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBConfigurationBuilder.class);
	
	private static final String PROPERTY_KEY_HOSTS = "arangodb.hosts";
	private static final String PROPERTY_KEY_TIMEOUT = "arangodb.timeout";
	private static final String PROPERTY_KEY_USER = "arangodb.user";
	private static final String PROPERTY_KEY_PASSWORD = "arangodb.password";
	private static final String PROPERTY_KEY_USE_SSL = "arangodb.usessl";
	private static final String PROPERTY_KEY_V_STREAM_CHUNK_CONTENT_SIZE = "arangodb.chunksize";
	private static final String PROPERTY_KEY_MAX_CONNECTIONS = "arangodb.connections.max";
	private static final String PROPERTY_KEY_CONNECTION_TTL = "arangodb.connections.ttl";
	private static final String PROPERTY_KEY_ACQUIRE_HOST_LIST = "arangodb.acquireHostList";
	private static final String PROPERTY_KEY_LOAD_BALANCING_STRATEGY = "arangodb.loadBalancingStrategy";
	private static final String PROPERTY_KEY_PROTOCOL = "arangodb.protocol";
	private static final String PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES = "arangodb.shouldPrefixCollectionNames";

	/** The db name. */
	private String dbName = "tinkerpop";
	
	/** The graph name. */
	private String graphName = "graph";
	
	/** The user. */
	private String user = "gremlin";
	
	/** The password. */
	private String password = "gremlin";
	
	/** The acquire host list flag. */
	private boolean hostList;
	
	/** The use ssl flag. */
	private boolean useSsl;
	
	/** The max number of connections. */
	private Integer connections;
	
	/** The timeout. */
	private Integer timeout;
	
	/** The VelocyStream chunk size. */
	private Long velocyStreamChunk;
	
	/** The connection ttl. */
	private Long connectionTtl;
	
	/** The protocol. */
	private Protocol protocol;
	
	/** The strategy. */
	private LoadBalancingStrategy strategy;
	
	/** The edges. */
	private Set<String> edges = new HashSet<>();
	
	/** The vertices. */
	private Set<String> vertices = new HashSet<>();
	
	/** The relations. */
	private Set<Triple<String, Set<String>, Set<String>>> relations = new HashSet<>();
	
	/** The hosts. */
	private Set<String> hosts = new HashSet<>();

	/** If Collection Names should be prefixed with Graph name. **/
	private Boolean shouldPrefixCollectionNames = true;

	/**
	 * Instantiates a new arango DB configuration builder.
	 */
	
	public ArangoDBConfigurationBuilder() {
		
	}
	
	/**
	 * Build the configuration.
	 *
	 * @return a configuration that can be used to instantiate a new {@link ArangoDBGraph}.
	 * @see ArangoDBGraph#open(org.apache.commons.configuration.Configuration)
	 */
	
	public BaseConfiguration build() {
		BaseConfiguration config = new BaseConfiguration();
		config.setListDelimiter('/');
		config.addProperty(fullPropertyKey(ArangoDBGraph.PROPERTY_KEY_DB_NAME), dbName);
		config.addProperty(fullPropertyKey(ArangoDBGraph.PROPERTY_KEY_GRAPH_NAME), graphName);
		config.addProperty(fullPropertyKey(ArangoDBGraph.PROPERTY_KEY_VERTICES), vertices);
		config.addProperty(fullPropertyKey(ArangoDBGraph.PROPERTY_KEY_EDGES), edges);
		List<String> rels = new ArrayList<>();
		for (Triple<String, Set<String>, Set<String>> r : relations) {
			// Make sure edge and vertex collections have been added
			StringBuilder rVal = new StringBuilder();
			rVal.append(r.getLeft());
			if (!edges.contains(r.getLeft())) {
				logger.warn("Missing edege collection {} from relations added to edge collections");
				edges.add(r.getLeft());
			}
			rVal.append(":");
			for (String sv : r.getMiddle()) {
				if (!vertices.contains(sv)) {
					logger.warn("Missing vertex collection {} from relations added to vertex collections");
					vertices.add(r.getLeft());
				}
			}
			rVal.append(r.getMiddle().stream().collect(Collectors.joining(",")));
			rVal.append("->");
			for (String sv : r.getRight()) {
				if (!vertices.contains(sv)) {
					logger.warn("Missing vertex collection {} from relations added to vertex collections");
					vertices.add(r.getLeft());
				}
			}
			rVal.append(r.getRight().stream().collect(Collectors.joining(",")));
			rels.add(rVal.toString());
			
		}
		if (!rels.isEmpty()) {
			config.addProperty(fullPropertyKey(ArangoDBGraph.PROPERTY_KEY_RELATIONS), rels.stream().collect(Collectors.joining("/")));
		}
		config.addProperty(fullPropertyKey(PROPERTY_KEY_USER), user);
		config.addProperty(fullPropertyKey(PROPERTY_KEY_PASSWORD), password);
		if (hostList) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_ACQUIRE_HOST_LIST), Boolean.toString(hostList));
		}
		if(useSsl) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_USE_SSL), Boolean.toString(useSsl));
		}
		if (connections != null) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_MAX_CONNECTIONS), connections);
		}
		if (timeout != null) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_TIMEOUT), timeout);
		}
		if (velocyStreamChunk != null) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_V_STREAM_CHUNK_CONTENT_SIZE), velocyStreamChunk);
		}
		if (connectionTtl != null) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_CONNECTION_TTL), connectionTtl);
		}
		if (protocol != null) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_PROTOCOL), protocol.name());
		}
		if (strategy != null) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_LOAD_BALANCING_STRATEGY), strategy.name());
		}
		if (!hosts.isEmpty()) {
			config.addProperty(fullPropertyKey(PROPERTY_KEY_HOSTS), hosts.stream().collect(Collectors.joining(",")));
		}
		if(shouldPrefixCollectionNames != null){
			config.addProperty(fullPropertyKey(PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES), shouldPrefixCollectionNames);
		}

		config.addProperty(Graph.GRAPH, ArangoDBGraph.class.getName());
		return config;
	}
	
	private String fullPropertyKey(String key) {
		return ArangoDBGraph.PROPERTY_KEY_PREFIX + "." + key;
	}
	
	/**
	 * Name of the database to use.
	 *
	 * @param name 				the db name
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder dataBase(String name) {
		dbName = name;
		return this;
	}
	
	/**
	 * Name of the graph to use.
	 *
	 * @param name 			the graph name
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder graph(String name) {
		graphName = name;
		return this;
	}
	
	/**
	 * Add vertex collection.
	 *
	 * @param name 				the vertex collection name
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder withVertexCollection(String name) {
		vertices.add(name);
		return this;
	}
	
	/**
	 * Add edge collection.
	 *
	 * @param name 				the edge collection name
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder withEdgeCollection(String name) {
		edges.add(name);
		return this;
	}
	
	/**
	 * Configure a 1-to-1 edge, i.e. for a given edge collection define the source and target vertex
	 * collections.
	 *
	 * @param edgeCollection 		the edge collection
	 * @param sourceCollection 		the source vertex collection
	 * @param targetCollection 		the target vertex collection
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder configureEdge(
		String edgeCollection,
		String sourceCollection,
		String targetCollection) {
		Set<String> source = new HashSet<>();
		Set<String> target = new HashSet<>();
		source.add(sourceCollection);
		target.add(targetCollection);
		ImmutableTriple<String, Set<String>, Set<String>> triple = new ImmutableTriple<>(edgeCollection, source, target);
		relations.add(triple);
		return this;
	}
	
	/**
	 * Configure a 1-to-many edge, i.e. for a given edge collection define the source and target vertex
	 * collections.
	 *
	 * @param edgeCollection 		the edge collection
	 * @param sourceCollection 		the source vertex collection
	 * @param targetCollections 	the target vertices collections
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder configureEdge(
		String edgeCollection,
		String sourceCollection,
		Set<String> targetCollections) {
		Set<String> source = new HashSet<>();
		source.add(sourceCollection);
		ImmutableTriple<String, Set<String>, Set<String>> triple = new ImmutableTriple<>(edgeCollection, source, Collections.unmodifiableSet(targetCollections));
		relations.add(triple);
		return this;
	}
	
	
	/**
	 * Configure a many-to-1 edge, i.e. for a given edge collection define the source and target vertex
	 * collections.
	 *
	 * @param edgeCollection 		the edge collection
	 * @param sourceCollections 	the source vertices collections
	 * @param targetCollection 		the target vertex collection
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder configureEdge(
		String edgeCollection,
		Set<String> sourceCollections,
		String targetCollection) {
		Set<String> target = new HashSet<>();
		target.add(targetCollection);
		ImmutableTriple<String, Set<String>, Set<String>> triple = new ImmutableTriple<>(edgeCollection, Collections.unmodifiableSet(sourceCollections), target);
		relations.add(triple);
		return this;
	}
	
	
	/**
	 * Configure a many-to-many edge, i.e. for a given edge collection define the source and target vertex
	 * collections.
	 *
	 * @param edgeCollection 		the edge collection
	 * @param sourceCollections 	the source vertices collections
	 * @param targetCollections 	the target vertices collections
	 * @return a reference to this object.
	 */
	
	public ArangoDBConfigurationBuilder configureEdge(
		String edgeCollection,
		Set<String> sourceCollections,
		Set<String> targetCollections) {
		ImmutableTriple<String, Set<String>, Set<String>> triple = new ImmutableTriple<>(edgeCollection, Collections.unmodifiableSet(sourceCollections), Collections.unmodifiableSet(targetCollections));
		relations.add(triple);
		return this;
	}
	
	
	/**
	 * ArangoDB hosts.
	 * <p>
	 * This method can be used multiple times to add multiple hosts (i.e. fallback hosts).
	 *
	 * @param host 					the host in the form url:port
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoHosts(String host) {
		this.hosts.add(host);
		return this;
	}
	
	/**
	 * ArangoDB socket connect timeout(milliseconds).
	 *
	 * @param timeout 					the tiemout
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoTimeout(int timeout) {
		this.timeout = timeout;
		return this;
	}
	
	/**
	 * ArangoDB Basic Authentication User.
	 *
	 * @param user 					the user
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoUser(String user) {
		this.user = user;
		return this;
	}
	
	
	/**
	 * ArangoDB Basic Authentication Password.
	 *
	 * @param password the password
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoPassword(String password) {
		this.password = password;
		return this;
	}
	
	/**
	 * ArangoDB use SSL connection.
	 *
	 * @param useSsl 					true, to use SSL connection.
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoSSL(boolean useSsl) {
		this.useSsl = useSsl;
		return this;
	}
	
	/**
	 * ArangoDB VelocyStream Chunk content-size(bytes).
	 *
	 * @param size 					the size
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoVelocyStreamChunk(long size) {
		this.velocyStreamChunk = size;
		return this;
	}
	
	
	/**
	 * ArangoDB max number of connections.
	 *
	 * @param connections 					the connections
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoMaxConnections(int connections) {
		this.connections = connections;
		return this;
	}
	
	/**
	 * ArangoDB Connection time to live (ms).
	 *
	 * @param time 				the time
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoTTL(long time) {
		this.connectionTtl = time;
		return this;
	}
	
	
	/**
	 * ArangoDB used network protocol.
	 * <p>
	 * <b>Note:</b> If you are using ArangoDB 3.0.x you have to set the protocol to Protocol.HTTP_JSON
	 * because it is the only one supported.
	 *
	 * @param protocol 				the protocol
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoNetworkProtocol(Protocol protocol) {
		this.protocol = protocol;
		return this;
	}
	
	
	/**
	 * ArangoDB load balancing strategy.
	 *
	 * @param strategy 				the strategy
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoNetworkProtocol(LoadBalancingStrategy strategy) {
		this.strategy = strategy;
		return this;
	}
	
	/**
	 * ArangoDB acquire a list of all known hosts in the cluster.
	 *
	 * @param hostList the host list
	 * @return a reference to this object.
	 * @see <a href="https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/Setup.md">ArangoDB Java Driver</a>
	 */
	
	public ArangoDBConfigurationBuilder arangoAcquireHostList(boolean hostList) {
		this.hostList = hostList;
		return this;
	}

	/**
	 * In case of colliding collection names in Graph, these names can be prefixed with Graph Name. <br/>
	 * If set to true collection names are in {@code %s_%s} format (where first %s is graph name, second %s is collection name).
	 * <br/>If set to false, collection names are without any prefix.
	 * <br/>Default set to <b>true</b>.
	 * @param shouldPrefixCollectionNames whether it should prefixed or not.
	 * @return a reference to this object.
	 */
	public ArangoDBConfigurationBuilder shouldPrefixCollectionNamesWithGraphName(boolean shouldPrefixCollectionNames){
		this.shouldPrefixCollectionNames = shouldPrefixCollectionNames;
		return this;
	}

}
