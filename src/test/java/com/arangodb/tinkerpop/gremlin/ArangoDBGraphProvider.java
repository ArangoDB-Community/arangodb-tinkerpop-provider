package com.arangodb.tinkerpop.gremlin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBSimpleGraphClient;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElement;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElement.ArangoDBProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex.ArangoDBVertexProperty;

/**
 * The Class ArangoDBGraphProvider. This provider assumes that there is a local ArangoDB running (i.e.
 * http://127.0.0.1:8529) with a tinkerpop database and a gremlin user that has Administrate permissions
 * on the db.
 */
public class ArangoDBGraphProvider extends AbstractGraphProvider {
	
	/** The Constant IMPLEMENTATIONS. */
	private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(ArangoDBEdge.class);
        add(ArangoDBElement.class);
        add(ArangoDBGraph.class);
        add(ArangoDBGraphVariables.class);
        add(ArangoDBProperty.class);
        add(ArangoDBVertex.class);
        add(ArangoDBVertexProperty.class);
    }};
    
	@Override
	public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
			GraphData loadGraphWith) {
		
		HashMap<String, Object> config = new HashMap<String, Object>();
		config.put(Graph.GRAPH, ArangoDBGraph.class.getName());
		config.put(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + "." + ArangoDBGraph.CONFIG_DB_NAME, "tinkerpop");
		config.put(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + "." + ArangoDBGraph.CONFIG_GRAPH_NAME, graphName);
		config.put(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".arangodb.user", "gremlin");
		config.put(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".arangodb.password", "gremlin");
		switch (testMethodName) {
		case "shouldProcessVerticesEdges":
			config.put(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
		}
		//knows
		return config;
	}

	@Override
	public void clear(Graph graph, Configuration configuration) throws Exception {
		ArangoDBSimpleGraphClient client;
		if (graph ==null) {
			Configuration arangoConfig = configuration.subset(ArangoDBGraph.ARANGODB_CONFIG_PREFIX);
			Properties arangoProperties = ConfigurationConverter.getProperties(arangoConfig);
			client = new ArangoDBSimpleGraphClient(arangoProperties, "tinkerpop", 0);
			client.deleteGraph(arangoConfig.getString(ArangoDBGraph.CONFIG_GRAPH_NAME));
		}
		else {
			ArangoDBGraph agraph = (ArangoDBGraph) graph;
			client = agraph.getClient();
			client.clear(agraph);
			agraph.close();
		}
	}

	@Override
	public Set<Class> getImplementations() {
		return IMPLEMENTATIONS;
	}

}
