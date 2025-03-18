/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arangodb.tinkerpop.gremlin.custom.structure.util.star;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarGraphTest extends AbstractGremlinTest {

    @Ignore("FIXME")
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldCopyFromGraphAToGraphB() throws Exception {
        final List<StarGraph> starGraphs = IteratorUtils.stream(graph.vertices()).map(StarGraph::of).collect(Collectors.toList());

        // via vertices and then edges
        final Configuration g1Configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), null);
        Graph g1 = graphProvider.openTestGraph(g1Configuration);
        starGraphs.stream().map(StarGraph::getStarVertex).forEach(vertex -> vertex.attach(Attachable.Method.getOrCreate(g1)));
        starGraphs.stream().forEach(starGraph -> starGraph.edges().forEachRemaining(edge -> ((Attachable<Edge>) edge).attach(Attachable.Method.getOrCreate(g1))));
        assertEquals(IteratorUtils.count(graph.vertices()), IteratorUtils.count(g1.vertices()));
        assertEquals(IteratorUtils.count(graph.edges()), IteratorUtils.count(g1.edges()));
        graph.vertices().forEachRemaining(vertex -> TestHelper.validateVertexEquality(vertex, g1.vertices(vertex.id()).next(), true));
        graphProvider.clear(g1, g1Configuration);

        // via edges only
        final Configuration g2Configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), null);
        final Graph g2 = graphProvider.openTestGraph(g2Configuration);
        starGraphs.stream().forEach(starGraph -> starGraph.edges().forEachRemaining(edge -> ((Attachable<Edge>) edge).attach(Attachable.Method.getOrCreate(g2))));
        assertEquals(IteratorUtils.count(graph.vertices()), IteratorUtils.count(g2.vertices()));
        assertEquals(IteratorUtils.count(graph.edges()), IteratorUtils.count(g2.edges()));
        // TODO: you can't get adjacent labels -- graph.vertices().forEachRemaining(vertex -> TestHelper.validateVertexEquality(vertex, g1.vertices(vertex.id()).next(), true));
        graphProvider.clear(g2, g2Configuration);
    }

//    @Ignore("FIXME: DE-996")
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    public void shouldAttachWithCreateMethod() {
        final Random random = TestHelper.RANDOM;
        StarGraph starGraph = StarGraph.open();
        Vertex starVertex = starGraph.addVertex(T.id, "person/" + UUID.randomUUID(), T.label, "person", "name", "stephen", "name", "spmallete");
        starVertex.property("acl", true, "timestamp", random.nextLong(), "creator", "marko");
        for (int i = 0; i < 100; i++) {
            starVertex.addEdge(
                    "knows",
                    starGraph.addVertex(T.id, "person/" + UUID.randomUUID(), T.label, "person", "name", new UUID(random.nextLong(), random.nextLong()), "since", random.nextLong()),
                    T.id, "knows/" + UUID.randomUUID()
            );
            starGraph
                    .addVertex(T.id, "project/" + UUID.randomUUID(), T.label, "project")
                    .addEdge("developedBy", starVertex, T.id, "developedBy/" + UUID.randomUUID(), "public", random.nextBoolean());
        }
        final Vertex createdVertex = starGraph.getStarVertex().attach(Attachable.Method.create(graph));
        starGraph.getStarVertex().edges(Direction.BOTH).forEachRemaining(edge -> ((Attachable<Edge>) edge).attach(Attachable.Method.create(random.nextBoolean() ? graph : createdVertex)));
        TestHelper.validateEquality(starVertex, createdVertex);
    }

}
