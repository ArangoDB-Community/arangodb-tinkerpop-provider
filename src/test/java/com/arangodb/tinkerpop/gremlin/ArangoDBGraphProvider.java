package com.arangodb.tinkerpop.gremlin;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.AbstractArangoDBElement;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElementProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraphVariables;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertexProperty;

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
        add(AbstractArangoDBElement.class);
        add(ArangoDBGraph.class);
        add(ArangoDBGraphVariables.class);
        add(ArangoDBElementProperty.class);
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
		
		if (loadGraphWith != null) {
			switch(loadGraphWith) {
			case CLASSIC:
				System.out.println("CLASSIC");
				System.out.println("case \"" + testMethodName + "\":");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "created");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "knows:vertex->vertex");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "created:vertex->vertex");
				break;
			case CREW:
				System.out.println("CREW");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "software");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "person");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "uses");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "develops");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "traverses");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "uses:person->software");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "develops:person->software");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "traverses:software->software");
				break;
			case GRATEFUL:
				System.out.println("GRATEFUL");
				break;
			case MODERN:
				System.out.println("MODERN");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "person");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.vertex", "software");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "created");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "knows:person->person");
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.relation", "created:person->software");
				break;
			default:
				System.out.println("default");
				break;
			}
		}
		else {
			if (testMethodName.startsWith("shouldProcessVerticesEdges") ||
					testMethodName.startsWith("shouldGenerate") ||
					testMethodName.startsWith("shouldSetValueOnEdge")) {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
			}
			else if(testMethodName.startsWith("shouldIterateEdgesWithStringIdSupport")) {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "self");
			}
			else if(testMethodName.startsWith("shouldAutotype")) {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
			}
			else if(testMethodName.startsWith("shouldSupportUserSuppliedIds")) {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "test");
			}
			else if(testMethodName.startsWith("shouldSupportUUID")) {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friend");
			}
			else if(testMethodName.startsWith("shouldSupportUUID")) {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friend");
			}
			else if(testMethodName.startsWith("shouldReadWrite")) {
				conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friends");
			}
			else {
				// Perhaps change for startsWith, but then it would be more verbose. Perhaps a set?
				switch (testMethodName) {
				case "shouldGetPropertyKeysOnEdge":
				case "shouldNotGetConcurrentModificationException":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friend");
					break;
				case "shouldTraverseInOutFromVertexWithMultipleEdgeLabelFilter":
				case "shouldTraverseInOutFromVertexWithSingleEdgeLabelFilter":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "hate");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friend");
					break;
				case "shouldPersistDataOnClose":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "collaborator");
					break;
				case "shouldTestTreeConnectivity":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "test1");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "test2");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "test3");
					break;
				case "shouldEvaluateConnectivityPatterns":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "hates");
					break;
				case "shouldRemoveEdgesWithoutConcurrentModificationException":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "link");
					break;
				case "shouldGetValueThatIsNotPresentOnEdge":
				case "shouldHaveStandardStringRepresentationForEdgeProperty":
				case "shouldHaveTruncatedStringRepresentationForEdgeProperty":
				case "shouldValidateIdEquality":
				case "shouldValidateEquality":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "self");
					break;
				case "shouldAllowRemovalFromEdgeWhenAlreadyRemoved":
				case "shouldRespectWhatAreEdgesAndWhatArePropertiesInMultiProperties":
				case "shouldProcessEdges":	
				case "shouldReturnOutThenInOnVertexIterator":
				case "shouldReturnEmptyIteratorIfNoProperties":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
					break;
				case "shouldNotHaveAConcurrentModificationExceptionWhenIteratingAndRemovingAddingEdges":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "pets");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "walks");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "livesWith");
					break;
				case "shouldHaveStandardStringRepresentation":		
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "friends");
					break;
				case "shouldReadWriteSelfLoopingEdges":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "CONTROL");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "SELFLOOP");
					break;
				case "shouldReadGraphML":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "created");
					break;
				case "shouldReadGraphMLUnorderedElements":
				case "shouldTransformGraphMLV2ToV3ViaXSLT":
				case "shouldReadLegacyGraphSON":
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "created");
					conf.addProperty(ArangoDBGraph.ARANGODB_CONFIG_PREFIX + ".graph.edge", "knows");
					break;
				default:
					System.out.println("case \"" + testMethodName + "\":");
				}
			}
		}
	}

	@Override
	public void clear(Graph graph, Configuration configuration) throws Exception {
		ArangoDBGraphClient client;
		if (graph ==null) {
			Configuration arangoConfig = configuration.subset(ArangoDBGraph.ARANGODB_CONFIG_PREFIX);
			Properties arangoProperties = ConfigurationConverter.getProperties(arangoConfig);
			client = new ArangoDBGraphClient(arangoProperties, "tinkerpop", 0);
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

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }
}
