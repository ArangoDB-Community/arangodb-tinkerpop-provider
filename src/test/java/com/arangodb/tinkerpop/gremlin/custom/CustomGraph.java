package com.arangodb.tinkerpop.gremlin.custom;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.commons.configuration2.Configuration;


public class CustomGraph extends ArangoDBGraph {

    public static CustomGraph open(Configuration configuration) {
        return new CustomGraph(configuration);
    }

    class CustomFeatures extends ArangoDBGraph.ArangoDBGraphFeatures {

        @Override
        public EdgeFeatures edge() {
            return new ArangoDBGraphEdgeFeatures() {
                @Override
                public boolean supportsNumericIds() {
                    return true;
                }
            };
        }

        @Override
        public VertexFeatures vertex() {
            return new ArangoDBGraphVertexFeatures() {
                @Override
                public boolean supportsNumericIds() {
                    return true;
                }

                @Override
                public VertexPropertyFeatures properties() {
                    return new ArangoDBGraphVertexPropertyFeatures() {
                        @Override
                        public boolean supportsNumericIds() {
                            return true;
                        }
                    };
                }
            };
        }
    }

    public CustomGraph(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Features features() {
        return new CustomFeatures();
    }
}
