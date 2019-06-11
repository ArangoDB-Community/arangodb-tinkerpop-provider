package com.arangodb.tinkerpop.gremlin.cache;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBIterator;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class VertexLoader extends CacheLoader<String, Vertex> {

	final ArangoDBGraph graph;

	public VertexLoader(ArangoDBGraph graph) {
		this.graph = graph;
	}

	@Override
	public Vertex load(String key) {
		return graph.getClient().getVertex(key, ArangoDBVertex.class);
	}

	@Override
	public Map<String, Vertex> loadAll(Iterable<? extends String> keys) {
		Map<String, Vertex> result = new HashMap<>();
		ArangoDBIterator<Vertex> it = new ArangoDBIterator<>(graph, graph.getClient().getGraphVertices(Lists.newArrayList(), Collections.emptyList()));
		it.forEachRemaining(v -> result.put((String) v.id(), v));
		return result;
	}

}