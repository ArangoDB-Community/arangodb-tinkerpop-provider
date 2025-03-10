package com.arangodb.tinkerpop.gremlin.custom;

import com.arangodb.tinkerpop.gremlin.util.BaseGraphProvider;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;
import org.apache.tinkerpop.gremlin.structure.VertexTest;

public class CustomGraphProvider extends BaseGraphProvider {

    @Override
    protected void configure(ArangoDBConfigurationBuilder builder, Class<?> test, String testMethodName) {
        switch (testMethodName) {
            case "g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated":
            case "g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists":
            case "g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated":
            case "g_mergeEXlabel_knows_out_marko_in_vadasX":
            case "g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists":
            case "g_V_mergeEXlabel_self_weight_05X":
            case "g_injectXlabel_knows_out_marko_in_vadasX_mergeE":
            case "g_mergeE_with_outV_inV_options":
                builder.withVertexCollection("person");
                builder.withEdgeCollection("knows");
                builder.withEdgeCollection("self");
                builder.configureEdge("knows", "person", "person");
                builder.configureEdge("self", "person", "person");
                break;
        }
    }
}
