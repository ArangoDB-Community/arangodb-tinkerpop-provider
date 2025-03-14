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
package com.arangodb.tinkerpop.gremlin.custom.structure.util.detached;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedGraphTest extends AbstractGremlinTest {

    // FIXME: review after DE-996
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
    public void testAttachableCreateMethod() {
        final Random random = TestHelper.RANDOM;
        StarGraph starGraph = StarGraph.open();
        final String label = "person";
        final String id = label + "/" + UUID.randomUUID();
        Vertex starVertex = starGraph.addVertex(T.id, id, T.label, label, "name", "stephen", "name", "spmallete");
        starVertex.property("acl", true, "timestamp", random.nextLong(), "creator", "marko");
        for (int i = 0; i < 100; i++) {
            starVertex.addEdge("knows", starGraph.addVertex("person", "name", new UUID(random.nextLong(), random.nextLong()), "since", random.nextLong()));
            starGraph.addVertex(T.label, "project").addEdge("developedBy", starVertex, "public", random.nextBoolean());
        }
        final DetachedVertex detachedVertex = DetachedFactory.detach(starGraph.getStarVertex(), true);
        final Vertex createdVertex = detachedVertex.attach(Attachable.Method.create(graph));
        TestHelper.validateVertexEquality(detachedVertex, createdVertex, false);
        TestHelper.validateVertexEquality(detachedVertex, starVertex, false);

        starGraph.getStarVertex().edges(Direction.BOTH).forEachRemaining(starEdge -> {
            final DetachedEdge detachedEdge = DetachedFactory.detach(starEdge, true);
            final Edge createdEdge = detachedEdge.attach(Attachable.Method.create(random.nextBoolean() ? graph : createdVertex));
            TestHelper.validateEdgeEquality(detachedEdge, starEdge);
            TestHelper.validateEdgeEquality(detachedEdge, createdEdge);
        });

    }
}
