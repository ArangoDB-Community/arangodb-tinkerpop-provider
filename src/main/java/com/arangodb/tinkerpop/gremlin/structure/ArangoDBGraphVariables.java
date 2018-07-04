package com.arangodb.tinkerpop.gremlin.structure;


import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Variables;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.swing.tree.VariableHeightLayoutCache;

public class ArangoDBGraphVariables implements Graph.Variables {

	public static final String GRAPH_VARIABLES_COLLECTION = "GRAPH_VARIABLES";
    private final ArangoDBGraph graph;
    private final Map<String, Object> store = new HashMap<>(4);

    public ArangoDBGraphVariables(ArangoDBGraph graph) {
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
    			graph.getClient().updateDocument(ArangoDBUtil.getCollectioName(graph.name(), GRAPH_VARIABLES_COLLECTION), key, value);
    		}
    	}
    	else {
    		graph.getClient().createDocument(ArangoDBUtil.getCollectioName(graph.name(), GRAPH_VARIABLES_COLLECTION), key, value);
    	}
    }

    @Override
    public void remove(String key) {
    	Object oldValue = this.store.remove(key);
    	if (oldValue != null) {
    		graph.getClient().deleteDocument(ArangoDBUtil.getCollectioName(graph.name(), GRAPH_VARIABLES_COLLECTION), key);
    	}
    }

    public static class ArangoDBGraphVariableFeatures implements Graph.Features.VariableFeatures {

    }
}
