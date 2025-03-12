package com.arangodb.tinkerpop.gremlin.process;

import com.arangodb.tinkerpop.gremlin.TestGraph;
import com.arangodb.tinkerpop.gremlin.util.BaseGraphProvider;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;

public class ProcessGraphProvider extends BaseGraphProvider {

    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration conf = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        conf.setProperty(Graph.GRAPH, TestGraph.class.getName());
        return conf;
    }

    @Override
    protected void configure(ArangoDBConfigurationBuilder builder, Class<?> test, String testMethodName) {
        switch (testMethodName) {
            case "g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX":
                builder.withEdgeCollection("next");
                break;
            case "shouldGenerateDefaultIdOnAddEWithSpecifiedId":
            case "shouldSetIdOnAddEWithNamePropertyKeySpecifiedAndNameSuppliedAsProperty":
            case "shouldSetIdOnAddEWithIdPropertyKeySpecifiedAndNameSuppliedAsProperty":
            case "shouldGenerateDefaultIdOnAddEWithGeneratedId":
            case "shouldTriggerAddEdgePropertyAdded":
            case "shouldReferencePropertyOfEdgeWhenRemoved":
            case "shouldTriggerAddEdge":
            case "shouldTriggerRemoveEdge":
            case "shouldTriggerRemoveEdgeProperty":
            case "shouldReferenceEdgeWhenRemoved":
            case "shouldUseActualEdgeWhenAdded":
            case "shouldDetachEdgeWhenAdded":
            case "shouldUseActualEdgeWhenRemoved":
            case "shouldDetachPropertyOfEdgeWhenNew":
            case "shouldDetachPropertyOfEdgeWhenChanged":
            case "shouldUseActualPropertyOfEdgeWhenChanged":
            case "shouldDetachEdgeWhenRemoved":
            case "shouldTriggerUpdateEdgePropertyAddedViaMergeE":
            case "shouldDetachPropertyOfEdgeWhenRemoved":
            case "shouldUseActualPropertyOfEdgeWhenRemoved":
            case "shouldUseActualPropertyOfEdgeWhenNew":
            case "shouldTriggerEdgePropertyChanged":
            case "shouldTriggerAddEdgeViaMergeE":
            case "shouldReferencePropertyOfEdgeWhenNew":
            case "shouldReferenceEdgeWhenAdded":
            case "shouldReferencePropertyOfEdgeWhenChanged":
            case "shouldTriggerAddEdgeByPath":
            case "shouldWriteToMultiplePartitions":
            case "shouldAppendPartitionToEdge":
            case "shouldThrowExceptionOnEInDifferentPartition":
                builder.withEdgeCollection("self");
                builder.withEdgeCollection("self-but-different");
                builder.withEdgeCollection("aTOa");
                builder.withEdgeCollection("aTOb");
                builder.withEdgeCollection("aTOc");
                builder.withEdgeCollection("bTOc");
                builder.withEdgeCollection("connectsTo");
                builder.withEdgeCollection("knows");
                builder.withEdgeCollection("relatesTo");
                break;
            case "g_io_read_withXreader_graphsonX":
            case "g_io_read_withXreader_gryoX":
            case "g_io_read_withXreader_graphmlX":
            case "g_io_readXjsonX":
            case "g_io_readXkryoX":
            case "g_io_readXxmlX":
                builder.withVertexCollection("person");
                builder.withVertexCollection("software");
                builder.withEdgeCollection("knows");
                builder.withEdgeCollection("created");
                builder.configureEdge("knows", "person", "person");
                builder.configureEdge("created", "person", "software");
                break;
            case "g_addV_propertyXlabel_personX":
            case "g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated":
            case "g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists":
            case "g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated":
            case "g_mergeEXlabel_knows_out_marko_in_vadasX":
            case "g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists":
            case "g_V_mergeEXlabel_self_weight_05X":
            case "g_mergeE_with_outV_inV_options":
            case "g_injectXlabel_knows_out_marko_in_vadasX_mergeE":
                builder.withVertexCollection("person");
                builder.withEdgeCollection("self");
                break;
            case "g_V_hasXname_regexXTinkerXX":
            case "g_V_hasXname_regexXTinkerUnicodeXX":
                builder.withVertexCollection("software");
                break;
            case "shouldDetachVertexWhenAdded":
            case "shouldReferenceVertexWhenAdded":
            case "shouldUseActualVertexWhenAdded":
                builder.withVertexCollection("thing");
                break;
            case "shouldAppendPartitionToAllVertexProperties":
                builder.withVertexCollection("person");
                builder.withVertexCollection("vertex");
                builder.configureEdge("edge", "person", "person");
                break;
            case "shouldPartitionWithAbstractLambdaChildTraversal":
                builder.withVertexCollection("testV");
                builder.withEdgeCollection("self");
                break;
        }
    }
}
