package com.arangodb.tinkerpop.gremlin.structure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.collections4.map.AbstractHashedMap;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The ArangoDB base element class (used by edges and vertices). 
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public abstract class ArangoDBElement<T> extends AbstractHashedMap<String, T> implements Element {
	
	/**
	 * The Class ArangoDBProperty.
	 *
	 * @param <T> the value type
	 */
	
	public static class ArangoDBProperty<V> extends HashEntry<String, V> implements Property<V>, Entry<String, V> {
    	
		/** The element. */
		
		private ArangoDBElement<V> element;

		/**
		 * Instantiates a new ArangoDB property.
		 *
		 * @param next the next
		 * @param hashCode the hash code
		 * @param key the key
		 * @param value the value
		 * @param element the element
		 */
		
		protected ArangoDBProperty(
			final HashEntry<String, V> next,
			final int hashCode,
			final Object key,
			final V value,
			final ArangoDBElement<V> element) {
			super(next, hashCode, key, value);
			this.element = element;
		}

		@Override
		public Element element() {
			return element;
		}

		@Override
		public boolean isPresent() {
			return super.getValue() != null;
		}

		@Override
		public String key() {
			return getKey();
		}

		@Override
		public void remove() {
			element.remove(getKey());
			
		}

		@Override
		public V value() throws NoSuchElementException {
			V value = super.getValue();
			if (value == null) {
				throw new NoSuchElementException();
			}
			return value;
		}
    	
    }
	
	
	public static class ArangoElementPropertyIterator<P extends Property<V>, V> implements Iterator<P> {
		
		/** The parent map */
        private final ArangoDBElement<V> parent;
        /** The current index into the array of buckets */
        private Set<String> keys;
        /** The last returned entry */
        private P last;
        /** The next entry */
        private P next;
        /** The modification count expected */
        private int expectedModCount;
        /** Keys to skip */
		private List<String> filterKeys;
		
    	
        @SuppressWarnings("unchecked")
		protected ArangoElementPropertyIterator(
        	final ArangoDBElement<V> parent,
        	List<String> filterKeys) {
        	this.parent = parent;
        	this.filterKeys = filterKeys;
        	this.keys = new HashSet<>(parent.keySet());
        	if (this.keys.isEmpty()) {
        		this.next = null; 
        	}
        	else {
        		if (filterKeys.isEmpty()) {
        			this.filterKeys = new ArrayList<>(keys);
        		}
        		next = (P)parent.getEntry(this.filterKeys.remove(0));
        	}
        }
        
        protected ArangoElementPropertyIterator(final ArangoDBElement<V> parent) {
        	this(parent, new ArrayList<>(0));            
        }
        
        public boolean hasNext() {
            return next != null;
        }

		@SuppressWarnings("unchecked")
		@Override
		public P next() {
			final P newCurrent = next;
            if (newCurrent == null)  {
                throw new NoSuchElementException(AbstractHashedMap.NO_NEXT_ENTRY);
            }
            if (filterKeys.isEmpty()) {
    			next = null;
    		}
    		else {
    			// The property may have been removed, get the next
    			do {
    				next = (P)parent.getEntry(filterKeys.remove(0));
    			} while (!filterKeys.isEmpty() && (next == null) );
    		}
            last = newCurrent;
            return newCurrent;
		}
		
		protected P currentEntry() {
            return last;
        }
	}
	
	/** The Constant logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBElement.class);
	
	/** ArangoDB internal id. */

	private String arango_db_id;
	
	/** ArangoDB internal key - mapped to Tinkerpop's ID. */

	private String arango_db_key;

	/** ArangoDB internal revision. */

	private String arango_db_rev;
	
	/** The collection in which the element is placed. */

	private String arango_db_collection;

	/** the graph of the document. */

	protected ArangoDBGraph graph;
	
	/**  Flag to indicate if the element is paired to a document in the DB. */
	
	protected boolean paired = false;
	
	
	/**
	 * Constructor used for ArabgoDB JavaBeans serialisation.
	 */
	public ArangoDBElement() {
		super(4, 0.75f);
	}
	
	/**
	 * Instantiates a new ArangoDB element.
	 *
	 * @param graph the graph
	 * @param collection the collection
	 */
	public ArangoDBElement(ArangoDBGraph graph, String collection) {
		super(4, 0.75f);
		this.graph = graph;
		this.arango_db_collection = collection;
		
	}
	
	/**
	 * Instantiates a new ArangoDB element.
	 *
	 * @param graph the graph
	 * @param collection the collection
	 * @param key the key
	 */
	public ArangoDBElement(ArangoDBGraph graph, String collection, String key) {
		super(4, 0.75f);
		this.graph = graph;
		this.arango_db_collection = collection;
		this.arango_db_key = key;
	}
	

	/**
	 * Get the Element's ArangoDB Id.
	 *
	 * @return the id
	 */
	
	public String _id() {
		return arango_db_id;
	}

	/**
	 * Set the Element's ArangoDB Id.
	 *
	 * @param id the id
	 */
	
	public void _id(String id) {
		this.arango_db_id = id;
	}
	
	/**
	 * Get the Element's ArangoDB Key.
	 *
	 * @return the key
	 */
	
	public String _key() {
		return arango_db_key;
	}
	
	/**
	 * Set the Element's ArangoDB Key.
	 *
	 * @param key the key
	 */
	
	public void _key(String key) {
		this.arango_db_key = key;
	}
	
	/**
	 * Get the Element's ArangoDB Revision.
	 *
	 * @return the revision
	 */
	
	public String _rev() {
		return arango_db_rev;
	}

	/**
	 * Set the Element's ArangoDB Revision.
	 *
	 * @param rev the revision
	 */
	
	public void _rev(String rev) {
		this.arango_db_rev = rev;
	}
	
	/**
	 * Collection. When Elements are deserialized from the DB the collection name is recomputed
	 * from the element's id.  
	 *
	 * @return the string
	 */
	
	public String collection() {
		if (arango_db_collection == null) {
			if (arango_db_id != null) {
				arango_db_collection = arango_db_id.split("/")[0];
				int graphLoc = arango_db_collection.indexOf('_');
				arango_db_collection = arango_db_collection.substring(graphLoc+1);
			}
		}
		return arango_db_collection;
	}

	/**
	 * Collection.
	 *
	 * @param collection the collection
	 */

	public void collection(String collection) {
		this.arango_db_collection = collection;
	}
	
	

	/**
	 * Checks if is paired.
	 *
	 * @return true, if is paired
	 */
	
	public boolean isPaired() {
		return paired;
	}

	/**
	 * Sets the paired.
	 *
	 * @param paired the new paired
	 */
	
	public void setPaired(boolean paired) {
		this.paired = paired;
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
    protected HashEntry<String, T> createEntry(
    	final HashEntry<String, T> next,
    	final int hashCode,
    	final String key,
    	final T value) {
    	
        return new ArangoDBProperty<T>(next, hashCode, convertKey(key), value, this);
    }
	
	@Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

	@Override
	public Graph graph() {
		return graph;
	}

	/**
	 * Graph.
	 *
	 * @param graph the graph
	 */
	
	public void graph(ArangoDBGraph graph) {
		this.graph = graph;
	}

	@Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
	

	@Override
	public Object id() {
		return arango_db_id;
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
	public String label() {
		return arango_db_collection;
	}
    
    protected List<String> getValidProperties(String... propertyKeys) {
		Set<String> validProperties = new HashSet<>(Arrays.asList(propertyKeys));
		validProperties.retainAll(keySet());
		return new ArrayList<>(validProperties);
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
