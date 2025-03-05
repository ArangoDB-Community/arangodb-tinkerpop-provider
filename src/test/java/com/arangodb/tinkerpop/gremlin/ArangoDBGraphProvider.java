package com.arangodb.tinkerpop.gremlin;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.arangodb.tinkerpop.gremlin.structure.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;
import org.apache.tinkerpop.gremlin.structure.VertexTest;

public class ArangoDBGraphProvider extends AbstractGraphProvider {

    @Override
    public Configuration newGraphConfiguration(final String graphName, final Class<?> test,
                                               final String testMethodName,
                                               final Map<String, Object> configurationOverrides,
                                               final LoadGraphWith.GraphData loadGraphWith) {
        return getConfiguration(graphName, test, testMethodName, loadGraphWith);
    }

    private Configuration getConfiguration(
            String graphName,
            Class<?> test,
            String testMethodName,
            GraphData loadGraphWith) {
        ArangoDBConfigurationBuilder builder = new ArangoDBConfigurationBuilder()
                .arangoHosts("127.0.0.1:8529")
                .arangoUser("root")
                .arangoPassword("test")
                .graph(graphName);
        if (loadGraphWith != null) {
            switch (loadGraphWith) {
                case CLASSIC:
                    System.out.println("CLASSIC");
                    builder.withEdgeCollection("knows");
                    builder.withEdgeCollection("created");
                    builder.configureEdge("knows", "vertex", "vertex");
                    builder.configureEdge("created", "vertex", "vertex");
                    break;
                case CREW:
                    System.out.println("CREW");
                    builder.withVertexCollection("software");
                    builder.withVertexCollection("person");
                    builder.withEdgeCollection("uses");
                    builder.withEdgeCollection("develops");
                    builder.withEdgeCollection("traverses");
                    builder.configureEdge("uses", "person", "software");
                    builder.configureEdge("develops", "person", "software");
                    builder.configureEdge("traverses", "software", "software");
                    break;
                case GRATEFUL:
                    System.out.println("GRATEFUL");
                    break;
                case MODERN:
                    System.out.println("MODERN");
                    builder.withVertexCollection("dog");
                    builder.withVertexCollection("software");
                    builder.withVertexCollection("person");
                    builder.withEdgeCollection("knows");
                    builder.withEdgeCollection("created");
                    builder.configureEdge("knows", "person", "person");
                    builder.configureEdge("created", "person", "software");
                    break;
                default:
                    System.out.println("default");
                    break;
            }
        } else {
            if (testMethodName.startsWith("shouldProcessVerticesEdges")
                    || testMethodName.startsWith("shouldGenerate")
                    || testMethodName.startsWith("shouldSetValueOnEdge")
                    || testMethodName.startsWith("shouldAutotype")) {
                builder.withEdgeCollection("knows");
            } else if (testMethodName.startsWith("shouldIterateEdgesWithStringIdSupport")) {
                builder.withEdgeCollection("self");
            } else if (testMethodName.startsWith("shouldSupportUserSuppliedIds")) {
                builder.withEdgeCollection("test");
            } else if (testMethodName.startsWith("shouldSupportUUID")) {
                builder.withEdgeCollection("friend");
            } else if (testMethodName.startsWith("shouldReadWriteVertexWithBOTHEdges")) {
                builder.withEdgeCollection("friends");
            } else if (testMethodName.startsWith("shouldReadWriteVertexWithINEdges")) {
                builder.withEdgeCollection("friends");
            } else if (testMethodName.startsWith("shouldReadWriteVertexMultiPropsNoEdges")) {
                builder.withEdgeCollection("friends");
            } else if (testMethodName.startsWith("shouldReadWriteDetachedVertexAsReferenceNoEdges")) {
                builder.withEdgeCollection("friends");
            } else if (testMethodName.startsWith("shouldReadWriteVertexNoEdges")) {
                builder.withEdgeCollection("friends");
            } else if (testMethodName.startsWith("shouldReadWriteVertexWithOUTEdges")) {
                builder.withEdgeCollection("friends");
            } else if (testMethodName.startsWith("shouldReadWriteDetachedVertexNoEdges")) {
                builder.withEdgeCollection("friends");
            } else if (testMethodName.startsWith("shouldReadWriteDetachedEdge")) {
                builder.withVertexCollection("person");
                builder.withEdgeCollection("friend");
            } else if (testMethodName.startsWith("shouldReadWriteDetachedEdgeAsReference")) {
                builder.withVertexCollection("person");
                builder.withEdgeCollection("friend");
            } else if (testMethodName.startsWith("shouldReadWriteEdge")) {
                builder.withVertexCollection("person");
                builder.withEdgeCollection("friend");
            } else if (testMethodName.startsWith("shouldThrowOnGraphEdgeSetPropertyStandard")) {
                builder.withEdgeCollection("self");
            } else if (testMethodName.startsWith("shouldThrowOnGraphAddEdge")) {
                builder.withEdgeCollection("self");
            } else {
                // Perhaps change for startsWith, but then it would be more verbose. Perhaps a set?
                switch (testMethodName) {
                    case "shouldGetPropertyKeysOnEdge":
                    case "shouldNotGetConcurrentModificationException":
                        builder.withEdgeCollection("friend");
                        builder.withEdgeCollection("knows");
                        break;
                    case "shouldTraverseInOutFromVertexWithMultipleEdgeLabelFilter":
                    case "shouldTraverseInOutFromVertexWithSingleEdgeLabelFilter":
                        builder.withEdgeCollection("hate");
                        builder.withEdgeCollection("friend");
                        break;
                    case "shouldPersistDataOnClose":
                        builder.withEdgeCollection("collaborator");
                        break;
                    case "shouldTestTreeConnectivity":
                        builder.withEdgeCollection("test1");
                        builder.withEdgeCollection("test2");
                        builder.withEdgeCollection("test3");
                        break;
                    case "shouldEvaluateConnectivityPatterns":
                        builder.withEdgeCollection("knows");
                        builder.withEdgeCollection("knows");
                        break;
                    case "shouldRemoveEdgesWithoutConcurrentModificationException":
                        builder.withEdgeCollection("link");
                        break;
                    case "shouldGetValueThatIsNotPresentOnEdge":
                    case "shouldHaveStandardStringRepresentationForEdgeProperty":
                    case "shouldHaveTruncatedStringRepresentationForEdgeProperty":
                    case "shouldValidateIdEquality":
                    case "shouldValidateEquality":
                    case "shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge":
                    case "shouldAddEdgeWithUserSuppliedStringId":
                    case "shouldAllowNullAddEdge":
                        builder.withEdgeCollection("self");
                        break;
                    case "shouldAllowRemovalFromEdgeWhenAlreadyRemoved":
                    case "shouldRespectWhatAreEdgesAndWhatArePropertiesInMultiProperties":
                    case "shouldProcessEdges":
                    case "shouldReturnOutThenInOnVertexIterator":
                    case "shouldReturnEmptyIteratorIfNoProperties":
                        builder.withEdgeCollection("knows");
                        break;
                    case "shouldNotHaveAConcurrentModificationExceptionWhenIteratingAndRemovingAddingEdges":
                        builder.withEdgeCollection("knows");
                        builder.withEdgeCollection("pets");
                        builder.withEdgeCollection("walks");
                        builder.withEdgeCollection("livesWith");
                        break;
                    case "shouldHaveStandardStringRepresentation":
                        builder.withEdgeCollection("friends");
                        break;
                    case "shouldReadWriteSelfLoopingEdges":
                        builder.withEdgeCollection("CONTROL");
                        builder.withEdgeCollection("SELFLOOP");
                        break;
                    case "shouldReadGraphML":
                    case "shouldReadGraphMLUnorderedElements":
                    case "shouldTransformGraphMLV2ToV3ViaXSLT":
                    case "shouldReadLegacyGraphSON":
                        builder.withEdgeCollection("knows");
                        builder.withEdgeCollection("created");
                        break;
                    case "shouldAddVertexWithLabel":
                    case "shouldAllowNullAddVertexProperty":
                        builder.withVertexCollection("person");
                        break;
                    case "shouldNotAllowSetProperty":
                    case "shouldHashAndEqualCorrectly":
                    case "shouldNotAllowRemove":
                    case "shouldNotConstructNewWithSomethingAlreadyDetached":
                    case "shouldNotConstructNewWithSomethingAlreadyReferenced":
                        builder.withEdgeCollection("test");
                        break;
                    case "shouldHaveExceptionConsistencyWhenUsingNullVertex":
                        builder.withEdgeCollection("tonothing");
                        break;
                    case "shouldHandleSelfLoops":
                        builder.withVertexCollection("person");
                        builder.withEdgeCollection("self");
                        break;
                    case "shouldAttachWithCreateMethod":
                    case "testAttachableCreateMethod":
                        builder.withVertexCollection("person");
                        builder.withVertexCollection("project");
                        builder.withEdgeCollection("knows");
                        builder.withEdgeCollection("developedBy");
                        builder.configureEdge("knows", "person", "person");
                        builder.configureEdge("developedBy", "project", "person");
                        break;
                    case "shouldConstructReferenceVertex":
                        builder.withVertexCollection("blah");
                        break;
                    case "shouldHaveExceptionConsistencyWhenUsingSystemVertexLabel":
                    case "shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabel":
                    case "shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabelOnOverload":
                    case "shouldHaveExceptionConsistencyWhenUsingSystemVertexLabelOnOverload":
                        if (VertexTest.class.equals(test.getEnclosingClass())) {
                            builder.withVertexCollection("foo");
                        }
                        break;
                    case "shouldHaveExceptionConsistencyWhenUsingNullVertexLabelOnOverload":
                    case "shouldHaveExceptionConsistencyWhenUsingNullVertexLabel":
                        builder.withVertexCollection("foo");
                        break;
                    case "shouldReadGraphMLWithCommonVertexAndEdgePropertyNames":
                        builder.withEdgeCollection("created");
                        builder.withEdgeCollection("knows");
                        break;
                    default:
                        System.out.println("case \"" + testMethodName + "\":");
                }
            }
        }
        return builder.build();
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        Configuration arangoConfig = configuration.subset(ArangoDBGraph.PROPERTY_KEY_PREFIX);
        Properties arangoProperties = ConfigurationConverter.getProperties(arangoConfig);
        TestGraphClient client = new TestGraphClient(arangoProperties, "tinkerpop");
        client.deleteGraph(arangoConfig.getString(ArangoDBGraph.PROPERTY_KEY_GRAPH_NAME));
        if (graph != null) {
            graph.close();
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Set<Class> getImplementations() {
        return Stream.of(
                ArangoDBEdge.class,
                ArangoDBElement.class,
                ArangoDBGraph.class,
                ArangoDBGraphVariables.class,
                ArangoDBPersistentElement.class,
                ArangoDBProperty.class,
                ArangoDBSimpleElement.class,
                ArangoDBVertex.class,
                ArangoDBVertexProperty.class
        ).collect(Collectors.toSet());
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
