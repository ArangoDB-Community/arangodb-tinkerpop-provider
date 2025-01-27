//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;


import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;

/**
 * The Class ArangoDBGraphVariables.
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBGraphVariables extends ArangoDBBaseDocument implements Graph.Variables {


	/**
     * The Class ArangoDBGraphVariableFeatures.
     */
	
    public static class ArangoDBGraphVariableFeatures implements Graph.Features.VariableFeatures {

    }

    /** The key:value store for properties. */

	@JsonProperty
    private final Map<String, Object> store = new HashMap<>(4);
    
    /**
     * Constructor used for ArabgoDB JavaBeans de-/serialisation.
     */
    
    public ArangoDBGraphVariables() {
        super();
    }

    /**
     * Instantiates a new Arango DB graph variables.
     *
	 * @param key 			        the key to assign to the document
	 * @param label                 the document label
	 * @param graph                 the graph that contains the document
	 */
    public ArangoDBGraphVariables(String key, String label, ArangoDBGraph graph) {
        super(key, label, graph);
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
    	GraphVariableHelper.validateVariable(key, value);
    	Object oldValue = this.store.put(key, value);
    	if (oldValue != null) {
    		if (!oldValue.equals(value)) {
    			graph.getClient().updateGraphVariables(this);
    		}
    	}
    	else {
            graph.getClient().updateGraphVariables(this);
    	}
    }

    @Override
    public void remove(String key) {
    	Object oldValue = this.store.remove(key);
    	if (oldValue != null) {
            graph.getClient().deleteGraphVariables(this);
    	}
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((store == null) ? 0 : store.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArangoDBGraphVariables other = (ArangoDBGraphVariables) obj;
		if (store == null) {
			if (other.store != null)
				return false;
		} else if (!store.equals(other.store))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return StringFactory.graphVariablesString(this);
	}
}
