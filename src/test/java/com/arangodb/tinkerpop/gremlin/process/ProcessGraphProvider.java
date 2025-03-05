package com.arangodb.tinkerpop.gremlin.process;

import com.arangodb.tinkerpop.gremlin.util.BaseGraphProvider;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;

public class ProcessGraphProvider extends BaseGraphProvider {

    @Override
    protected void configure(ArangoDBConfigurationBuilder builder, Class<?> test, String testMethodName) {
        switch (testMethodName) {
            case "g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX":
                builder.withEdgeCollection("next");
                break;
            case "g_V_mergeEXlabel_self_weight_05X":
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
            case "shouldPartitionWithAbstractLambdaChildTraversal":
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
                builder.withEdgeCollection("knows");
                builder.withEdgeCollection("created");
                break;
        }
    }
}
