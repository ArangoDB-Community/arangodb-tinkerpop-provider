package com.arangodb.tinkerpop.gremlin;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
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
 * 
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
    public Configuration newGraphConfiguration(final String graphName, final Class<?> test,
                                               final String testMethodName,
                                               final Map<String, Object> configurationOverrides,
                                               final LoadGraphWith.GraphData loadGraphWith) {
        final Configuration conf = new BaseConfiguration();
        getBaseConfiguration(graphName, test, testMethodName, loadGraphWith, conf);

        // assign overrides but don't allow gremlin.graph setting to be overridden.  the test suite should
        // not be able to override that.
        configurationOverrides.entrySet().stream()
                .filter(c -> !c.getKey().equals(Graph.GRAPH))
                .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));
        return conf;
    }
    
	public void getBaseConfiguration(
		String graphName,
		Class<?> test,
		String testMethodName,
		GraphData loadGraphWith,
		Configuration conf) {
		conf.addProperty(Graph.GRAPH, ArangoDBGraph.class.getName());
		conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + "." + ArangoDBGraph.CONFIG_DB_NAME, "tinkerpop");
		conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + "." + ArangoDBGraph.CONFIG_GRAPH_NAME, graphName);
		conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".arangodb.user", "gremlin");
		conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".arangodb.password", "gremlin");
		// Perhaps change for startsWith, but then it would be more verbose. Perhaps a set?
		switch (testMethodName) {
		case "shouldNotHaveAConcurrentModificationExceptionWhenIteratingAndRemovingAddingEdges":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "pets");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "walks");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "livesWith");
		case "shouldProcessVerticesEdges":
		case "shouldGenerateSameGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.1)]":
		case "shouldGenerateDifferentGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.1)]":
		case "shouldGenerateSameGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.5)]":
		case "shouldGenerateDifferentGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.5)]":
		case "shouldGenerateSameGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.5)]":
		case "shouldGenerateDifferentGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.5)]":
		case "shouldGenerateSameGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.1)]":
		case "shouldGenerateDifferentGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.1)]":
		case "shouldGenerateSameGraph[test(PowerLawDistribution{gamma=2.3, multiplier=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.25)]":
		case "shouldGenerateDifferentGraph[test(PowerLawDistribution{gamma=2.3, multiplier=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.25)]":
		case "shouldGenerateSameGraph[test(PowerLawDistribution{gamma=2.3, multiplier=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.25)]":
		case "shouldGenerateDifferentGraph[test(PowerLawDistribution{gamma=2.3, multiplier=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.25)]":
		case "shouldProcessEdges":
		case "shouldGenerateSameGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=2.0, mean=0.0})]":
		case "shouldGenerateDifferentGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=2.0, mean=0.0})]":
		case "shouldGenerateSameGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=5.0, mean=0.0})]":
		case "shouldGenerateDifferentGraph[test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=5.0, mean=0.0})]":
		case "shouldGenerateSameGraph[test(PowerLawDistribution{gamma=2.1, multiplier=0.0},PowerLawDistribution{gamma=2.1, multiplier=0.0})]":
		case "shouldGenerateDifferentGraph[test(PowerLawDistribution{gamma=2.1, multiplier=0.0},PowerLawDistribution{gamma=2.1, multiplier=0.0})]":
		case "shouldGenerateSameGraph[test(PowerLawDistribution{gamma=2.9, multiplier=0.0},PowerLawDistribution{gamma=2.9, multiplier=0.0})]":
		case "shouldGenerateDifferentGraph[test(PowerLawDistribution{gamma=2.9, multiplier=0.0},PowerLawDistribution{gamma=2.9, multiplier=0.0})]":
		case "shouldGenerateSameGraph[test(PowerLawDistribution{gamma=3.9, multiplier=0.0},PowerLawDistribution{gamma=3.9, multiplier=0.0})]":
		case "shouldGenerateDifferentGraph[test(PowerLawDistribution{gamma=3.9, multiplier=0.0},PowerLawDistribution{gamma=3.9, multiplier=0.0})]":
		case "shouldGenerateSameGraph[test(PowerLawDistribution{gamma=2.3, multiplier=0.0},PowerLawDistribution{gamma=2.8, multiplier=0.0})]":
		case "shouldGenerateDifferentGraph[test(PowerLawDistribution{gamma=2.3, multiplier=0.0},PowerLawDistribution{gamma=2.8, multiplier=0.0})]":
		case "shouldReturnOutThenInOnVertexIterator":
		case "shouldAutotypeBooleanProperties":
		case "shouldAutotypeFloatProperties":

		case "shouldAutotypeDoubleProperties":
		case "shouldAutotypeStringProperties":
		case "shouldReturnEmptyIteratorIfNoProperties":
		case "shouldAutotypeIntegerProperties":
		case "shouldAutotypeLongProperties":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
			break;
		case "shouldValidateIdEquality":
		case "shouldValidateEquality":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "self");
			break;
		case "shouldGetPropertyKeysOnEdge":
		case "shouldNotGetConcurrentModificationException":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friend");
			break;
		case "shouldHaveStandardStringRepresentation":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friends");
			break;
		default:
			System.out.println("case " + testMethodName);
		}

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

	@Override
	public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
			GraphData loadGraphWith) {
		// TODO Auto-generated method stub
		return null;
	}

}
