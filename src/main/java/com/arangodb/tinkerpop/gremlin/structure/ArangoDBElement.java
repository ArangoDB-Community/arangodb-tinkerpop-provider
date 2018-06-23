package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.collections4.map.AbstractHashedMap;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import com.arangodb.velocypack.annotations.Expose;

/**
 * The ArangoDB base element class (used by edges and vertices). 
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

abstract class ArangoDBElement extends AbstractHashedMap<String, Object> implements Element, ArangoDBDocument {
	
	public class ArangoDBProperty<V> extends HashEntry<String, V> implements Property<V>, Entry<String, V> {
    	
		private ArangoDBElement element;

		protected ArangoDBProperty(final HashEntry<String, V> next, final int hashCode,
				final Object key, final V value, final ArangoDBElement element) {
			super(next, hashCode, key, value);
			this.element = element;
		}

		@Override
		public String key() {
			return getKey();
		}

		@Override
		public V value() throws NoSuchElementException {
			V value = super.getValue();
			if (value == null) {
				throw new NoSuchElementException();
			}
			return value;
		}

		@Override
		public boolean isPresent() {
			return super.getValue() != null;
		}

		@Override
		public Element element() {
			return element;
		}

		@Override
		public void remove() {
			element.remove(getKey());
			
		}
    	
    }
	

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBElement.class);
	
	/**
	 * ArangoDB internal id
	 */
	@DocumentField(Type.ID)
	private String arango_db_id;
	
	/**
	 * ArangoDB internal key - mapped to Tinkerpop's ID
	 */
	@DocumentField(Type.KEY)
	private String arango_db_key;

	/**
	 * ArangoDB internal revision
	 */
	@DocumentField(Type.REV)
	private String arango_db_rev;
	
	/**
	 * The collection in which the element is placed
	 */
	@Expose(serialize = false, deserialize = false)
	private String arango_db_collection;

	/**
	 * the graph of the document
	 */
	@Expose(serialize = false, deserialize = false)
	protected ArangoDBGraph graph;
	
	
	public ArangoDBElement(ArangoDBGraph graph, String collection) {
		this.graph = graph;
		this.arango_db_collection = collection;
	}
	
	public ArangoDBElement(ArangoDBGraph graph, String collection, String key) {
		this.graph = graph;
		this.arango_db_collection = collection;
		this.arango_db_key = key;
	}
	
	/**
	 * Constructor used for ArabgoDB JavaBeans serialisation
	 */
	public ArangoDBElement() {
		super(4, 0.75f);
	}
	

	@Override
	public Object id() {
		return arango_db_id;
	}

	@Override
	public Graph graph() {
		return graph;
	}
	

	@Override
	public String label() {
		return arango_db_collection;
	}
	
	public void graph(ArangoDBGraph graph) {
		this.graph = graph;
	}
	
	@Override
	public void collection(String collection) {
		this.arango_db_collection = collection;
	}

	@Override
	public Set<String> keys() {
		logger.debug("keys");
		final Set<String> keys = new HashSet<>();
		for (final String key : this.keySet()) {
			if (!Graph.Hidden.isHidden(key))
                keys.add(ArangoDBUtil.denormalizeKey(key));
		}
		return Collections.unmodifiableSet(keys);
	}
	
	@Override
	public String _id() {
		return arango_db_id;
	}

	@Override
	public String _rev() {
		return arango_db_rev;
	}

	@Override
	public String _key() {
		return arango_db_key;
	}
	
	@Override
	public void _id(String id) {
		this.arango_db_id = id;
	}

	@Override
	public void _rev(String rev) {
		this.arango_db_rev = rev;
	}

	@Override
	public void _key(String key) {
		this.arango_db_key = key;
	}

	@Override
	public String collection() {
		return arango_db_collection;
	}
	
	/**
     * Creates an entry to store the key-value data.
     *
     * @param next  the next entry in sequence
     * @param hashCode  the hash code to use
     * @param key  the key to store
     * @param value  the value to store
     * @return the newly created entry
     */
    protected HashEntry<String, Object> createEntry(final HashEntry<String, Object> next, final int hashCode,
    		final String key, final Object value) {
    	
        return new ArangoDBProperty<Object>(next, hashCode, convertKey(key), value, this);
    }

	@Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

	
//	/**
//	 * Return the object value associated with the provided string key. If no
//	 * value exists for that key, return null.
//	 * 
//	 * @param key
//	 *            the key of the key/value property
//	 * @return the object value related to the string key
//	 */
//	@SuppressWarnings("unchecked")
//	@Override
//	public <T> T getProperty(String key) {
//		return (T) this.properties.get(ArangoDBUtil.normalizeKey(key));
//	}
//
//	/**
//	 * Set/Reset the vertex/edge document
//	 * 
//	 * @param document
//	 *            the new internal data of the element
//	 */
//	public void setDocument(ArangoDBBaseDocument document) {
//		this.document = document;
//	}
//
//	@Override
//	public Set<String> getPropertyKeys() {
//		Set<String> ps = document.getPropertyKeys();
//		HashSet<String> result = new HashSet<String>();
//
//		if (this instanceof Edge) {
//			// do not return lable property
//			for (String key : ps) {
//				if (!StringFactory.LABEL.equals(key)) {
//					result.add(ArangoDBUtil.denormalizeKey(key));
//				}
//			}
//		} else {
//			for (String key : ps) {
//				result.add(ArangoDBUtil.denormalizeKey(key));
//			}
//		}
//		return result;
//	}
//
//	@Override
//	public void setProperty(String key, Object value) {
//
//		if (StringFactory. ID.equals(key)) {
//			throw ExceptionFactory.propertyKeyIdIsReserved();
//		}
//
//		if (StringFactory.LABEL.equals(key) && this instanceof Edge) {
//			throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
//		}
//
//		if (StringUtils.isBlank(key)) {
//			throw ExceptionFactory.propertyKeyCanNotBeEmpty();
//		}
//
//		try {
//			document.setProperty(ArangoDBUtil.normalizeKey(key), value);
//			save();
//		} catch (ArangoDBException e) {
//			logger.debug("error while setting a property", e);
//			throw new IllegalArgumentException(e.getMessage());
//		}
//	}
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public <T> T removeProperty(String key) {
//
//		if (StringUtils.isBlank(key)) {
//			throw ExceptionFactory.propertyKeyCanNotBeEmpty();
//		}
//
//		if (key.equals(StringFactory.LABEL) && this instanceof Edge) {
//			throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
//		}
//
//		T o = null;
//		try {
//			o = (T) document.removeProperty(ArangoDBUtil.normalizeKey(key));
//			save();
//
//		} catch (ArangoDBException e) {
//			logger.debug("error while removing a property", e);
//			throw new IllegalArgumentException(e.getMessage());
//		}
//		return o;
//	}
//
//	@Override
//	public Object getId() {
//		return document.getDocumentKey();
//	}
//
//	/**
//	 * Returns the internal data of the element
//	 * 
//	 * @return the internal data
//	 */
//	public ArangoDBBaseDocument getRaw() {
//		return document;
//	}
//
//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + ((document == null) ? 0 : document.hashCode());
//		return result;
//	}
//
//	@Override
//	public boolean equals(Object obj) {
//		if (this == obj)
//			return true;
//		if (obj == null)
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		ArangoDBElement other = (ArangoDBElement) obj;
//		if (document == null) {
//			if (other.document != null)
//				return false;
//		} else if (!document.equals(other.document))
//			return false;
//		return true;
//	}

}
