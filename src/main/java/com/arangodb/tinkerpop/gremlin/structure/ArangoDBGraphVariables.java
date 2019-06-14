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

import com.arangodb.tinkerpop.gremlin.client.GraphClient;
import com.arangodb.velocypack.annotations.Expose;
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
    
    private final Map<String, Object> store = new HashMap<>(4);

	@Expose(serialize = false, deserialize = false)
	private GraphClient client;
    
    /**
     * Constructor used for ArabgoDB JavaBeans de-/serialisation.
     */
    
    public ArangoDBGraphVariables() {
        super();
    }

    /**
     * Instantiates a new Arango DB graph variables.
     * @param graphName 			the graph name
	 * @param client				the graph client
	 *
	 */

    public ArangoDBGraphVariables(String graphName, GraphClient client) {
        this(graphName, client, false);
    }

	public ArangoDBGraphVariables(String graphName, GraphClient client, boolean paired) {
		super(graphName, client.GRAPH_VARIABLES_COLLECTION, client.GRAPH_VARIABLES_COLLECTION, paired);
		this.client = client;
	}

	// FIXME Move to interface
    public ArangoDBGraphVariables pair(GraphClient client) {
		return new ArangoDBGraphVariables(_key, client, true);

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
				client.updateGraphVariables(this);
    		}
    	}
    	else {
			client.updateGraphVariables(this);
    	}
    }

    @Override
    public void remove(String key) {
    	Object oldValue = this.store.remove(key);
    	if (oldValue != null) {
			client.deleteGraphVariables(this);
    	}
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((store.isEmpty()) ? 0 : store.hashCode());
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
		if (store.isEmpty()) {
			if (!other.store.isEmpty())
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
