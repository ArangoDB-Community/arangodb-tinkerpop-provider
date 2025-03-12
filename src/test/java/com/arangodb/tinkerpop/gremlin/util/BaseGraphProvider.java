package com.arangodb.tinkerpop.gremlin.util;

import com.arangodb.tinkerpop.gremlin.TestGraph;
import com.arangodb.tinkerpop.gremlin.custom.CustomGraph;
import com.arangodb.tinkerpop.gremlin.structure.*;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BaseGraphProvider extends AbstractGraphProvider {

    private final String dbName = getClass().getSimpleName();

    protected abstract void configure(ArangoDBConfigurationBuilder builder, Class<?> test, String testMethodName);

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
            LoadGraphWith.GraphData loadGraphWith) {
        System.out.println("case \"" + test.getCanonicalName() + "." + testMethodName + "\":");
        ArangoDBConfigurationBuilder builder = new ArangoDBConfigurationBuilder()
                .arangoHosts("127.0.0.1:8529")
                .arangoUser("root")
                .arangoPassword("test")
                .dataBase(dbName)
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
                case MODERN:
                    System.out.println("MODERN");
                    builder.withVertexCollection("name");
                    builder.withVertexCollection("vertex");
                    builder.withVertexCollection("animal");
                    builder.withVertexCollection("dog");
                    builder.withVertexCollection("software");
                    builder.withVertexCollection("person");
                    builder.withEdgeCollection("knows");
                    builder.withEdgeCollection("created");
                    builder.withEdgeCollection("createdBy");
                    builder.withEdgeCollection("existsWith");
                    builder.withEdgeCollection("codeveloper");
                    builder.withEdgeCollection("uses");
                    builder.configureEdge("knows", "person", "person");
                    builder.configureEdge("created", "person", "software");
                    builder.configureEdge("createdBy", "software", "person");
                    builder.configureEdge("existsWith", "software", "software");
                    builder.configureEdge("codeveloper", "person", "person");
                    builder.configureEdge("uses", "person", "software");
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
                    builder.withVertexCollection("vertex");
                    builder.withVertexCollection("song");
                    builder.withVertexCollection("artist");
                    builder.withEdgeCollection("followedBy");
                    builder.withEdgeCollection("sungBy");
                    builder.withEdgeCollection("writtenBy");
                    builder.configureEdge("followedBy", "vertex", "vertex");
                    builder.configureEdge("sungBy", "song", "artist");
                    builder.configureEdge("writtenBy", "song", "artist");
                    break;
                case SINK:
                    System.out.println("SINK");
                    builder.withVertexCollection("loops");
                    builder.withVertexCollection("message");
                    builder.withEdgeCollection("link");
                    builder.withEdgeCollection("self");
                    builder.configureEdge("self", "loops", "loops");
                    builder.configureEdge("link", "message", "message");
                    break;
            }
        } else {
            configure(builder, test, testMethodName);
        }
        return builder.build();
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        Configuration arangoConfig = configuration.subset(ArangoDBGraph.PROPERTY_KEY_PREFIX);
        Properties arangoProperties = ConfigurationConverter.getProperties(arangoConfig);
        TestGraphClient client = new TestGraphClient(arangoProperties, dbName);
        client.clear(arangoConfig.getString(ArangoDBGraph.PROPERTY_KEY_GRAPH_NAME));
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
                TestGraph.class,
                CustomGraph.class,
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
                                                    LoadGraphWith.GraphData loadGraphWith) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }

}
