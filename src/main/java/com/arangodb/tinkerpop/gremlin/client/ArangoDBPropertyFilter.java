//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ArangoDB property filter class constructs AQL segments for comparing a document property
 * with a given value.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBPropertyFilter {
	
	/**
	 * The Compare Operators.
	 */
	
	public enum Compare {
		
		/** The equal. */
		EQUAL,
		
		/** The not equal. */
		NOT_EQUAL,
		
		/** The greater than. */
		GREATER_THAN,
		
		/** The greater than equal. */
		GREATER_THAN_EQUAL,
		
		/** The less than. */
		LESS_THAN,
		
		/** The less than equal. */
		LESS_THAN_EQUAL,
		
		/** The has. */
		HAS,
		
		/** The has not. */
		HAS_NOT,
		
		/** The in. */
		IN,
		
		/** The not in. */
		NOT_IN
	};

	/** The Constant PROPERTY. */
	
	private static final String PROPERTY = "property";
	
	/** The Constant COMPARE. */
	
	private static final String COMPARE = "compare";
	
	/** The Constant KEY. */
	
	private static final String KEY = "key";
	
	/** The Constant VALUE. */
	
	private static final String VALUE = "value";
	
	/** The Constant logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBPropertyFilter.class);



	/** The property containers. */
	
	private List<PropertyContainer> propertyContainers = new ArrayList<PropertyContainer>();

	/**
	 * Adds a new "has" filter and returns the object. This is a fluent method that allows 
	 * adding multiple property filters to an ArangoDBPropertyFilter.
	 *
	 * @param key           Name of the attribute to compare
	 * @param value         Value of the attribute to compare against
	 * @param compare   	the compare operator
	 * @return return 		the property filter
	 */
	
	public ArangoDBPropertyFilter has(
		final String key,
		final Object value,
		final Compare compare) {
		this.propertyContainers.add(new PropertyContainer(key, value, compare));
		return this;
	}

	/**
	 * Constructs the the AQL segment for each property filter and adds the required key-value
	 * entries to the bind parameters map.
	 *
	 * @param prefix 			the iterator/variable to which the property filter will be applied
	 * @param filterSegments 	the list to populate with the AQL segments
	 * @param bindVars 			the map to populate with the key-value bindings
	 */
	
	public void addAqlSegments(
		String prefix,
		List<String> filterSegments,
		Map<String, Object> bindVars) {
		logger.debug("addAqlSegments");
		int count = 0;
		for (final PropertyContainer container : propertyContainers) {
			String key = escapeKey(container.key);
			switch (container.compare) {
			case EQUAL:
				filterSegments.add(prefix + key + " == @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case NOT_EQUAL:
				filterSegments.add(prefix + key + " != @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case GREATER_THAN:
				filterSegments.add(prefix + key + " > @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case LESS_THAN:
				filterSegments.add(prefix + key + " < @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case GREATER_THAN_EQUAL:
				filterSegments.add(prefix + key + " >= @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case LESS_THAN_EQUAL:
				filterSegments.add(prefix + key + " <= @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case HAS:
				filterSegments.add(prefix + container.key + " != null");
				break;
			case HAS_NOT:
				filterSegments.add(prefix + container.key + " == null");
				break;
			case IN:
				filterSegments.add(
					prefix + container.key + " IN [" + addArray(bindVars, PROPERTY + count, container.value) + "]");
				break;
			case NOT_IN:
				filterSegments.add(
					prefix + container.key + " NOT IN [" + addArray(bindVars, PROPERTY + count, container.value) + "]");
				break;
			default:
				// do nothing
			}
			count++;
		}
	}

	/**
	 * Adds the array.
	 *
	 * @param bindVars the bind vars
	 * @param propertyName the property name
	 * @param value the value
	 * @return the string
	 */
	
	private String addArray(Map<String, Object> bindVars, String propertyName, Object value) {
		int c = 0;
		List<String> elements = new ArrayList<String>();
		if (value instanceof Iterable) {
			Iterable<?> iterable = (Iterable<?>) value;
			Iterator<?> iter = iterable.iterator();
			while (iter.hasNext()) {
				String prop = propertyName + "_" + c++;
				elements.add("@" + prop);
				bindVars.put(prop, iter.next());
			}
		} else {
			elements.add("@" + propertyName);
			bindVars.put(propertyName, value);
		}

		return StringUtils.join(elements, ", ");
	}

	/**
	 * Escape key.
	 *
	 * @param key the key
	 * @return the string
	 */
	private String escapeKey(String key) {
		return "`" + key.replaceAll("`", "") + "`";
	}

	/**
	 * The Class PropertyContainer.
	 */
	
	private class PropertyContainer {
		
		/** The key. */
		
		public final String key;
		
		/** The value. */
		
		public final Object value;
		
		/** The compare. */
		
		public final Compare compare;

		/**
		 * Instantiates a new property container.
		 *
		 * @param key the key
		 * @param value the value
		 * @param compare the compare
		 */
		public PropertyContainer(final String key, final Object value, final Compare compare) {
			this.key = key;
			this.value = value;
			this.compare = compare;
		}
	}

}
