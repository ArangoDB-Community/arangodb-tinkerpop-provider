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

import com.arangodb.tinkerpop.gremlin.client.GraphVariablesClient;
import com.arangodb.velocypack.annotations.Expose;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * The Class ArangoDBGraphVariables.
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBGraphVariables extends BaseArngDocument implements Graph.Variables {


	/**
     * The Class ArangoDBGraphVariableFeatures.
     */
	
    public static class ArangoDBGraphVariableFeatures implements Graph.Features.VariableFeatures {

    }

    /** The primaryKey:baseValue store for elementProperties. */
    
    private final Map<String, Object> store = new HashMap<>(4);

	@Expose(serialize = false, deserialize = false)
	private final GraphVariablesClient client;


    /**
     * Instantiates a new Arango DB graph variables.
     * @param graphName 			the graph name
	 * @param client				the graph client
	 *
	 */

    public ArangoDBGraphVariables(String graphName, GraphVariablesClient client) {
        this(null, graphName, null, client.GRAPH_VARIABLES_COLLECTION, client);
    }


	public ArangoDBGraphVariables(
		String id,
		String key,
		String rev,
		String label,
		GraphVariablesClient client) {
		super(id, key, rev, label);
		this.client = client;
	}

	// FIXME Move to interface
    public ArangoDBGraphVariables useClient(GraphVariablesClient client) {
		return new ArangoDBGraphVariables(_id, _key, _rev, label, client);

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
    	try {
			if (oldValue != null) {
				if (!oldValue.equals(value)) {
					client.updateGraphVariables(this);
				}
			} else {
				client.updateGraphVariables(this);
			}
		}
    	catch (GraphVariablesClient.GraphVariablesNotFoundException ex) {
    		throw new IllegalStateException("Unable to update graph variables information.", ex);
		}
    }

    @Override
    public void remove(String key) {
    	this.store.remove(key);
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((client == null) ? 0 : client.graphName().hashCode());
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
		if (client == null) {
			if (other.client != null) {
				return false;
			}
		}
		else if (!client.graphName().equals(other.client.graphName())) {
			return false;
		}
		if (store.isEmpty()) {
			if (!other.store.isEmpty()) {
				return false;
			}
		}
		else if (!store.equals(other.store)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return StringFactory.graphVariablesString(this);
	}
}
