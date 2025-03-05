package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.util.BaseGraphProvider;

import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;
import org.apache.tinkerpop.gremlin.structure.VertexTest;

public class StructureGraphProvider extends BaseGraphProvider {

    @Override
    protected void configure(ArangoDBConfigurationBuilder builder, Class<?> test, String testMethodName) {
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
                    System.out.println("case \"" + test.getCanonicalName() + "." + testMethodName + "\":");
            }
        }
    }
}
