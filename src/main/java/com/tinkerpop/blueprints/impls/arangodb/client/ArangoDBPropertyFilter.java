//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ArangoDBPropertyFilter {
	
    public enum Compare {EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, HAS, HAS_NOT};
	
    private List<PropertyContainer> propertyContainers = new ArrayList<PropertyContainer>();
    
    public ArangoDBPropertyFilter has(final String key, final Object value, final Compare compare) {
        this.propertyContainers.add(new PropertyContainer(key, value, compare));
        return this;
    }

    public JSONArray getAsJSON () throws ArangoDBException {
    	JSONArray result = new JSONArray();
		try {
    	
    	for (final PropertyContainer container : propertyContainers) {
    		JSONObject o = new JSONObject();
				o.put("key", container.key);
    		o.put("value", container.value); // TODO check different value types
            switch (container.compare) {
            case EQUAL:
            	o.put("compare", "==");
            	break;
            case NOT_EQUAL:
            	o.put("compare", "!=");
            	break;
            case GREATER_THAN:
            	o.put("compare", ">");
            	break;
            case LESS_THAN:
            	o.put("compare", "<");
            	break;
            case GREATER_THAN_EQUAL:
            	o.put("compare", ">=");
            	break;
            case LESS_THAN_EQUAL:
            	o.put("compare", "<=");
            	break;
            case HAS:
            	o.put("compare", "HAS");
            	break;
            case HAS_NOT:
            	o.put("compare", "HAS_NOT");
            	break;
            }
            result.put(o);
    	}
    	
    	return result;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ArangoDBException("JSON error: " + e.getMessage());
		}
    }
    
    private class PropertyContainer {
        public String key;
        public Object value;
        public Compare compare;

        public PropertyContainer(final String key, final Object value, final Compare compare) {
            this.key = key;
            this.value = value;
            this.compare = compare;
        }
    }

}
