package com.arangodb.tinkerpop.gremlin;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.util.detached.DetachedGraphTest",
        method = "testAttachableCreateMethod",
        reason = "replaced by com.arangodb.tinkerpop.gremlin.custom.structure.util.detached.DetachedGraphTest#testAttachableCreateMethod()")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldAddVertexWithUserSuppliedStringId",
        reason = "FIXME: DE-996")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldRemoveVertices",
        reason = "FIXME: DE-998")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldRemoveEdges",
        reason = "FIXME: DE-998")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldEvaluateConnectivityPatterns",
        reason = "FIXME: DE-996")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest",
        method = "shouldAttachWithCreateMethod",
        reason = "FIXME")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest",
        method = "shouldCopyFromGraphAToGraphB",
        reason = "FIXME")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest",
        method = "shouldEvaluateEquivalentVertexHashCodeWithSuppliedIds",
        reason = "FIXME")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest",
        method = "shouldEvaluateVerticesEquivalentWithSuppliedIdsViaTraversal",
        reason = "FIXME")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest",
        method = "shouldEvaluateVerticesEquivalentWithSuppliedIdsViaIterators",
        reason = "FIXME")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexTest$AddEdgeTest",
        method = "shouldAddEdgeWithUserSuppliedStringId",
        reason = "FIXME")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeTest$Traversals",
        method = "*",
        reason = "replaced by com.arangodb.tinkerpop.gremlin.custom.process.traversal.step.map.MergeEdgeTest"
)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexTest$Traversals",
        method = "g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option",
        reason = "FIXME: DE-995"
)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexTest$Traversals",
        method = "g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option",
        reason = "FIXME: DE-995"
)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest$Traversals",
        method = "*",
        reason = "replaced by com.arangodb.tinkerpop.gremlin.custom.process.traversal.step.OrderabilityTest"
)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals",
        method = "*",
        reason = "FIXME: DE-996"
)
public class TestGraph extends ArangoDBGraph {

    @SuppressWarnings("unused")
    public static TestGraph open(Configuration configuration) {
        return new TestGraph(configuration);
    }

    public TestGraph(Configuration configuration) {
        super(configuration);
    }
}
