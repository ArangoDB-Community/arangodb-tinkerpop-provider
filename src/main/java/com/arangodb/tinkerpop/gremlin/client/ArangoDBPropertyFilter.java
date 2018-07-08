//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
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
 * The ArangoDB property filter class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBPropertyFilter {

	private static final String PROPERTY = "property";
	private static final String COMPARE = "compare";
	private static final String KEY = "key";
	private static final String VALUE = "value";
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBPropertyFilter.class);

	public enum Compare {
		EQUAL,
		NOT_EQUAL,
		GREATER_THAN,
		GREATER_THAN_EQUAL,
		LESS_THAN,
		LESS_THAN_EQUAL,
		HAS,
		HAS_NOT,
		IN,
		NOT_IN
	};

	private List<PropertyContainer> propertyContainers = new ArrayList<PropertyContainer>();

	/**
	 * Adds a new "has" filter and returns the object
	 * 
	 * @param key
	 *            Name of the attribute
	 * @param value
	 *            Value of the attribute
	 * @param compare
	 *            Compare type
	 * @return return the current object
	 */
	public ArangoDBPropertyFilter has(final String key, final Object value, final Compare compare) {
		this.propertyContainers.add(new PropertyContainer(key, value, compare));
		return this;
	}

	public void addProperties(String prefix, List<String> filter, Map<String, Object> bindVars) {
		logger.info("addProperties");
		int count = 0;
		for (final PropertyContainer container : propertyContainers) {
			String key = escapeKey(container.key);
			switch (container.compare) {
			case EQUAL:
				filter.add(prefix + key + " == @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case NOT_EQUAL:
				filter.add(prefix + key + " != @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case GREATER_THAN:
				filter.add(prefix + key + " > @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case LESS_THAN:
				filter.add(prefix + key + " < @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case GREATER_THAN_EQUAL:
				filter.add(prefix + key + " >= @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case LESS_THAN_EQUAL:
				filter.add(prefix + key + " <= @property" + count);
				bindVars.put(PROPERTY + count, container.value);
				break;
			case HAS:
				filter.add(prefix + container.key + " != null");
				break;
			case HAS_NOT:
				filter.add(prefix + container.key + " == null");
				break;
			case IN:
				filter.add(
					prefix + container.key + " IN [" + addArray(bindVars, PROPERTY + count, container.value) + "]");
				break;
			case NOT_IN:
				filter.add(
					prefix + container.key + " NOT IN [" + addArray(bindVars, PROPERTY + count, container.value) + "]");
				break;
			default:
				// do nothing
			}
			count++;
		}
	}

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

	private String escapeKey(String key) {
		return "`" + key.replaceAll("`", "") + "`";
	}

	private class PropertyContainer {
		public final String key;
		public final Object value;
		public final Compare compare;

		public PropertyContainer(final String key, final Object value, final Compare compare) {
			this.key = key;
			this.value = value;
			this.compare = compare;
		}
	}

}
