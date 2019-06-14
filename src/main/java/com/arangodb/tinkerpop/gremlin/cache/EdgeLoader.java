package com.arangodb.tinkerpop.gremlin.cache;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A CacheLoader to load Edges from an ArangoDBGraph
 */
public class EdgeLoader extends CacheLoader<String, Edge> {

		final ArangoDBGraph graph;

		public EdgeLoader(ArangoDBGraph graph) {
			this.graph = graph;
		}

		@Override
		public Edge load(String key) {
			return graph.getDatabaseClient().getDocument(key, ArangoDBEdge.class);
		}

		@Override
		public Map<String, Edge> loadAll(Iterable<? extends String> keys) {
			Map<String, Edge> result = new HashMap<>();
			ArangoDBIterator<Edge> it = new ArangoDBIterator<>(graph, graph.getDatabaseClient().getGraphEdges(Lists.newArrayList(), Collections.emptyList()));
			it.forEachRemaining(e -> result.put((String) e.id(), e));
			return result;
		}

	}