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
		case "shouldSupportUUID[graphson-v1-embedded]":
		case "shouldSupportUUID[graphson-v2-embedded]":
		case "shouldSupportUUID[graphson-v3]":
		case "shouldSupportUUID[gryo-v1]":
		case "shouldSupportUUID[gryo-v3]":
		case "shouldReadWriteDetachedEdgeAsReference[graphson-v1]":
		case "shouldReadWriteEdge[graphson-v1]":
		case "shouldReadWriteDetachedEdge[graphson-v1]":
		case "shouldReadWriteDetachedEdgeAsReference[graphson-v1-embedded]":
		case "shouldReadWriteEdge[graphson-v1-embedded]":
		case "shouldReadWriteDetachedEdge[graphson-v1-embedded]":
		case "shouldReadWriteDetachedEdgeAsReference[graphson-v2]":
		case "shouldReadWriteEdge[graphson-v2]":
		case "shouldReadWriteDetachedEdge[graphson-v2]":
		case "shouldReadWriteDetachedEdgeAsReference[graphson-v2-embedded]":
		case "shouldReadWriteEdge[graphson-v2-embedded]":
		case "shouldReadWriteDetachedEdge[graphson-v2-embedded]":
		case "shouldReadWriteDetachedEdgeAsReference[graphson-v3]":
		case "shouldReadWriteEdge[graphson-v3]":
		case "shouldReadWriteDetachedEdge[graphson-v3]":
		case "shouldReadWriteDetachedEdgeAsReference[gryo]":
		case "shouldReadWriteEdge[gryo]":
		case "shouldReadWriteDetachedEdge[gryo]":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friend");
			break;
		case "shouldHaveStandardStringRepresentation":
		case "shouldReadWriteVertexWithBOTHEdges[graphson-v1]":
		case "shouldReadWriteVertexWithINEdges[graphson-v1]":
		case "shouldReadWriteVertexMultiPropsNoEdges[graphson-v1]":
		case "shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v1]":
		case "shouldReadWriteVertexNoEdges[graphson-v1]":
		case "shouldReadWriteVertexWithOUTEdges[graphson-v1]":
		case "shouldReadWriteDetachedVertexNoEdges[graphson-v1]":
		case "shouldReadWriteVertexWithBOTHEdges[graphson-v1-embedded]":
		case "shouldReadWriteVertexWithINEdges[graphson-v1-embedded]":
		case "shouldReadWriteVertexMultiPropsNoEdges[graphson-v1-embedded]":
		case "shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v1-embedded]":
		case "shouldReadWriteVertexNoEdges[graphson-v1-embedded]":
		case "shouldReadWriteVertexWithOUTEdges[graphson-v1-embedded]":
		case "shouldReadWriteDetachedVertexNoEdges[graphson-v1-embedded]":
		case "shouldReadWriteVertexWithBOTHEdges[graphson-v2]":
		case "shouldReadWriteVertexWithINEdges[graphson-v2]":
		case "shouldReadWriteVertexMultiPropsNoEdges[graphson-v2]":
		case "shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v2]":
		case "shouldReadWriteVertexNoEdges[graphson-v2]":
		case "shouldReadWriteVertexWithOUTEdges[graphson-v2]":
		case "shouldReadWriteDetachedVertexNoEdges[graphson-v2]":
		case "shouldReadWriteVertexWithBOTHEdges[graphson-v2-embedded]":
		case "shouldReadWriteVertexWithINEdges[graphson-v2-embedded]":
		case "shouldReadWriteVertexMultiPropsNoEdges[graphson-v2-embedded]":
		case "shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v2-embedded]":
		case "shouldReadWriteVertexNoEdges[graphson-v2-embedded]":
		case "shouldReadWriteVertexWithOUTEdges[graphson-v2-embedded]":
		case "shouldReadWriteDetachedVertexNoEdges[graphson-v2-embedded]":
		case "shouldReadWriteVertexWithBOTHEdges[graphson-v3]":
		case "shouldReadWriteVertexWithINEdges[graphson-v3]":
		case "shouldReadWriteVertexMultiPropsNoEdges[graphson-v3]":
		case "shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v3]":
		case "shouldReadWriteVertexNoEdges[graphson-v3]":
		case "shouldReadWriteVertexWithOUTEdges[graphson-v3]":
		case "shouldReadWriteDetachedVertexNoEdges[graphson-v3]":
		case "shouldReadWriteVertexWithBOTHEdges[gryo]":
		case "shouldReadWriteVertexWithINEdges[gryo]":
		case "shouldReadWriteVertexMultiPropsNoEdges[gryo]":
		case "shouldReadWriteDetachedVertexAsReferenceNoEdges[gryo]":
		case "shouldReadWriteVertexNoEdges[gryo]":
		case "shouldReadWriteVertexWithOUTEdges[gryo]":
		case "shouldReadWriteDetachedVertexNoEdges[gryo]":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friends");
			break;
		case "shouldReadWriteCrew[graphml]":
		case "shouldReadWriteCrew[graphsonv1d0]":
		case "shouldReadWriteCrew[graphsonv2d0]":
		case "shouldReadWriteCrew[graphsonv3d0]":
		case "shouldReadWriteCrew[gryo]":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "software");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "person");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "uses");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "develops");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "traverses");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "uses:person->software");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "develops:person->software");
			break;
		case "shouldReadWriteModernToFileWithHelpers[graphml]":
		case "shouldReadWriteModern[graphml]":
		case "shouldMigrateModernGraph[graphml]":
		case "shouldReadWriteClassic[graphml]":
		case "shouldReadWriteClassicToFileWithHelpers[graphml]":
		case "shouldMigrateClassicGraph[graphml]":
		case "shouldReadWriteModernToFileWithHelpers[graphsonv1d0]":
		case "shouldReadWriteClassic[graphsonv1d0]":
		case "shouldReadWriteModern[graphsonv1d0]":
		case "shouldReadWriteClassicToFileWithHelpers[graphsonv1d0]":
		case "shouldMigrateModernGraph[graphsonv1d0]":
		case "shouldMigrateClassicGraph[graphsonv1d0]":
		case "shouldReadWriteModernToFileWithHelpers[graphsonv2d0]":
		case "shouldReadWriteClassic[graphsonv2d0]":
		case "shouldReadWriteModern[graphsonv2d0]":
		case "shouldReadWriteClassicToFileWithHelpers[graphsonv2d0]":
		case "shouldMigrateModernGraph[graphsonv2d0]":
		case "shouldMigrateClassicGraph[graphsonv2d0]":
		case "shouldReadWriteModernToFileWithHelpers[graphsonv3d0]":
		case "shouldReadWriteClassic[graphsonv3d0]":
		case "shouldReadWriteModern[graphsonv3d0]":
		case "shouldReadWriteClassicToFileWithHelpers[graphsonv3d0]":
		case "shouldMigrateModernGraph[graphsonv3d0]":
		case "shouldMigrateClassicGraph[graphsonv3d0]":
		case "shouldReadWriteModernToFileWithHelpers[gryo]":
		case "shouldReadWriteClassic[gryo]":
		case "shouldReadWriteModern[gryo]":
		case "shouldReadWriteClassicToFileWithHelpers[gryo]":
		case "shouldMigrateModernGraph[gryo]":
		case "shouldMigrateClassicGraph[gryo]":
			//conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "vertex");
			if (graphName == "standard") {
				System.out.println("readGraph needs oher vertices?");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "vertex");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "person");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "software");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "edge");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "created");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "knows:person->person");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "edge:vertex->vertex");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "created:person->software");
			}
			else {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "vertex");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "person");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "software");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "created");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "created:person->software");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "knows:person->person");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "knows:vertex->vertex");
			}
		case "shouldReadWriteVerticesNoEdgesToGryoManual[graphson-v1]":
		case "shouldReadWriteVerticesNoEdgesToGraphSONManual[graphson-v1]":
		case "shouldReadWriteVerticesNoEdges[graphson-v1]":
		case "shouldReadWriteVerticesNoEdgesToGryoManual[graphson-v1-embedded]":
		case "shouldReadWriteVerticesNoEdgesToGraphSONManual[graphson-v1-embedded]":
		case "shouldReadWriteVerticesNoEdges[graphson-v1-embedded]":
		case "shouldReadWriteVerticesNoEdgesToGryoManual[graphson-v2]":
		case "shouldReadWriteVerticesNoEdgesToGraphSONManual[graphson-v2]":
		case "shouldReadWriteVerticesNoEdges[graphson-v2]":
		case "shouldReadWriteVerticesNoEdgesToGryoManual[graphson-v2-embedded]":
		case "shouldReadWriteVerticesNoEdgesToGraphSONManual[graphson-v2-embedded]":
		case "shouldReadWriteVerticesNoEdges[graphson-v2-embedded]":
		case "shouldReadWriteVerticesNoEdgesToGryoManual[graphson-v3]":
		case "shouldReadWriteVerticesNoEdgesToGraphSONManual[graphson-v3]":
		case "shouldReadWriteVerticesNoEdges[graphson-v3]":
		case "shouldReadWriteVerticesNoEdgesToGryoManual[gryo]":
		case "shouldReadWriteVerticesNoEdgesToGraphSONManual[gryo]":
		case "shouldReadWriteVerticesNoEdges[gryo]":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "created");
			break;
		case "shouldSupportUserSuppliedIdsOfTypeUuid":
		case "shouldSupportUserSuppliedIdsOfTypeAny":
		case "shouldSupportUserSuppliedIdsOfTypeNumericLong":
		case "shouldSupportUserSuppliedIdsOfTypeNumericInt":
			conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "test");
			break;
		default:
			System.out.println("case \"" + testMethodName + "\":");
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
