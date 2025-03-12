package com.arangodb.tinkerpop.gremlin.custom;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

@Graph.OptIn("com.arangodb.tinkerpop.gremlin.custom.CustomStandardSuite")
public class CustomGraph extends ArangoDBGraph {

    @SuppressWarnings("unused")
    public static CustomGraph open(Configuration configuration) {
        return new CustomGraph(configuration);
    }

    public CustomGraph(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Features features() {
        return new ArangoDBGraph.ArangoDBGraphFeatures() {

            @Override
            public Features.EdgeFeatures edge() {
                return new ArangoDBGraphFeatures.ArangoDBGraphEdgeFeatures() {
                    @Override
                    public boolean supportsNumericIds() {
                        return true;
                    }
                };
            }

            @Override
            public Features.VertexFeatures vertex() {
                return new ArangoDBGraphFeatures.ArangoDBGraphVertexFeatures() {
                    @Override
                    public boolean supportsNumericIds() {
                        return true;
                    }

                    @Override
                    public Features.VertexPropertyFeatures properties() {
                        return new ArangoDBGraphFeatures.ArangoDBGraphVertexPropertyFeatures() {
                            @Override
                            public boolean supportsNumericIds() {
                                return true;
                            }
                        };
                    }
                };
            }
        };
    }
}
