package com.arangodb.tinkerpop.gremlin;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;
import org.apache.commons.configuration.Configuration;

import java.io.File;


public class Issue57 {

    public static void main(String... args) {
        ArangoDBConfigurationBuilder builder = new ArangoDBConfigurationBuilder();
        builder.dataBase("Test02")
                .graph("Test02Graph01")
                .arangoUser("gremlin")
                .arangoPassword("gremlin")
                .arangoHosts("127.0.0.1:8529")
                .withEdgeCollection("testedge")
                .withVertexCollection("testfrom")
                .withVertexCollection("testto")
                .shouldPrefixCollectionNamesWithGraphName(false)
                .configureEdge("testedge", "testfrom", "testto");

        ArangoDBGraph g = ArangoDBGraph.open(builder.build());
    }
}
