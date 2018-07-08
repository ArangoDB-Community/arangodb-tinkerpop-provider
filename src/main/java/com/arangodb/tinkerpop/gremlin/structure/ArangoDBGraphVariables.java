package com.arangodb.tinkerpop.gremlin.structure;


import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.velocypack.annotations.Expose;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Variables;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class ArangoDBGraphVariables extends ArangoDBBaseDocument implements Graph.Variables {

    @Expose(serialize = false, deserialize = false)
    private final ArangoDBGraph graph;

    private final Map<String, Object> store = new HashMap<>(4);

    public ArangoDBGraphVariables(ArangoDBGraph graph) {
        super();
        this.graph = graph;
    }

    @Override
    public Set<String> keys() {
        return store.keySet();
    }

    @SuppressWarnings("unchecked")
	@Override
    public <R> Optional<R> get(String key) {
        Object value = store.get(key);
        return Optional.of((R)value);
    }

    @Override
    public void set(String key, Object value) {
    	if (key == null) {
    		throw Variables.Exceptions.variableKeyCanNotBeNull();
    	}
    	if (StringUtils.isEmpty(key)) {
    		throw Variables.Exceptions.variableKeyCanNotBeEmpty();
    	}
    	if (value == null) {
    		throw Variables.Exceptions.variableValueCanNotBeNull();
    	}
    	Object oldValue = this.store.put(key, value);
    	if (oldValue != null) {
    		if (!oldValue.equals(value)) {
    			graph.getClient().updateDocument(graph, this);
    		}
    	}
    	else {
            graph.getClient().updateDocument(graph, this);
    	}
    }

    @Override
    public void remove(String key) {
    	Object oldValue = this.store.remove(key);
    	if (oldValue != null) {
            graph.getClient().updateDocument(graph, this);
    	}
    }


    public static class ArangoDBGraphVariableFeatures implements Graph.Features.VariableFeatures {

    }

}
