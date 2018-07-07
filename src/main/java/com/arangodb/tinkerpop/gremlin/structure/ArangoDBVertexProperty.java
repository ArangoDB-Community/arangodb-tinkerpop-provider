package com.arangodb.tinkerpop.gremlin.structure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.velocypack.annotations.Expose;


public class ArangoDBVertexProperty<V> implements VertexProperty<V>, ArangoDBElement {
	
	public static class ArangoDBVertexPropertyOwner<T> {
		
		private String key;
		
		private Cardinality cardinality;
		
		@Expose(serialize = false, deserialize = false)
		private ArangoDBVertex owner;

		private Collection<ArangoDBVertexProperty<T>> properties;
		
		private ArangoDBVertexProperty<T> property;
		
		@Expose(serialize = false, deserialize = false)
		private boolean modified;
		
		
		public ArangoDBVertexPropertyOwner(ArangoDBVertex owner, String key, Cardinality cardinality) {
			this.owner = owner;
			this.key = key;
			this.cardinality = cardinality;
		}

		public ArangoDBVertexPropertyOwner() {
		}

		public void save() {
			owner.save();
			modified = false;
		}

		public void removeProperty(ArangoDBVertexProperty<?> arangoDBVertexProperty) {
			if (cardinality != Cardinality.single) {
				properties.remove(arangoDBVertexProperty);
				if (properties.isEmpty()) {
					owner.remove(this);
				}
			}
			else {
				if (arangoDBVertexProperty.equals(property)) {
					owner.remove(this);
				}
			}
		}
		
		public ArangoDBVertexProperty<T> addProperty(String key, T value) {
			modified = false;
			ArangoDBVertexProperty<T> p = new ArangoDBVertexProperty<T>(this, value);
			switch(cardinality) {
			case single:
				ArangoDBVertexProperty<T> existing = (ArangoDBVertexProperty<T>) property;
				if (existing == null) {
					property = p;
					modified = true;
				}
				else {
					T oldValue = existing.value;
					existing.value = value;
					modified = !oldValue.equals(value);
				}
				break;
			case list:
				if (properties == null) {
					properties = new ArrayList<>();
				}
				break;
			case set:
				if (properties == null) {
					properties = new HashSet<>();
				}
				modified = properties.add(p);
				break;
			default:
				modified = false;
			}
			return p;
		}
		
		
		public Collection<ArangoDBVertexProperty<T>> properties() {
			if (property != null) {
				return Collections.singleton(property);
			}
			return properties;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((cardinality == null) ? 0 : cardinality.hashCode());
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ArangoDBVertexPropertyOwner<?> other = (ArangoDBVertexPropertyOwner<?>) obj;
			if (cardinality != other.cardinality)
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}
		
		public boolean wasModified() {
			boolean result = modified;
			modified = false;
			return result;
		}

		public String key() {
			return key;
		}

		public void owner(ArangoDBVertex owner) {
			this.owner = owner;
		}

		public void key(String key) {
			this.key = key;
		}

		public void cardinality(Cardinality cardinality) {
			this.cardinality = cardinality;
		}

		public void properties(Collection<ArangoDBVertexProperty<T>> properties) {
			this.properties = properties;
		}

		public void property(ArangoDBVertexProperty<T> property) {
			this.property = property;
		}
		
		
		
	}
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertexProperty.class);
	
	/**  Map to store the element properties */
	protected Map<String, ArangoDBElementProperty<?>> properties = new HashMap<>(4, 0.75f);
	
	private V value;
	
	@Expose(serialize = false, deserialize = false)
	private ArangoDBVertexPropertyOwner<V> owner;
	
	public ArangoDBVertexProperty() {
		super();
	}

	public ArangoDBVertexProperty(ArangoDBVertexPropertyOwner<V> owner, V value) {
		super();
		this.owner = owner;
		this.value = value;
	}

	@Override
    public String toString() {
    	return StringFactory.propertyString(this);
    }

	@Override
	public Object id() {
		return owner.key;
	}
	
	@Override
    public String label() {
        return owner.key;
    }


	@Override
	public <U> Property<U> property(String key, U value) {
		logger.info("property {} = {}", key, value);
		ElementHelper.validateProperty(key, value);
		@SuppressWarnings("unchecked")
		ArangoDBElementProperty<U> p = (ArangoDBElementProperty<U>) property(key);
		if (p == null) {
			p = new ArangoDBElementProperty<U>(key, value, owner.owner);
		}
		else {
			U oldValue = p.value(value);
			if ((oldValue != null) && !oldValue.equals(value)) {
				save();
			}
		}
		return p;
	}


	@SuppressWarnings("unchecked")
	@Override
	public <U> Iterator<Property<U>> properties(String... propertyKeys) {
		Set<String> allProperties = new HashSet<>(properties.keySet());
		if (propertyKeys.length > 1) {
			allProperties.retainAll(Arrays.asList(propertyKeys));
		}
		return properties.entrySet().stream()
				.filter(e -> allProperties.contains(e.getKey()))
				.map(e -> e.getValue())
				.map(p -> (Property<U>)p)
				.iterator();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <U> Property<U> property(String key) {
		Property<U> property = (Property<U>) properties.get(key);
		if (property == null) {
			property = Property.empty();
		}
		return property;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> U value(String key) throws NoSuchElementException {
		return (U) properties.get(key).value();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Iterator<U> values(String... propertyKeys) {
		// FIXME Is this a filtering operation too?
		return (Iterator<U>) Arrays.stream(propertyKeys).map(this.properties::get).iterator();
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((owner == null) ? 0 : owner.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArangoDBVertexProperty<?> other = (ArangoDBVertexProperty<?>) obj;
		if (owner == null) {
			if (other.owner != null)
				return false;
		} else if (!owner.equals(other.owner))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public void save() {
		this.owner.save();
	}

	@Override
	public Set<String> propertiesKeys() {
		return this.properties.keySet();
	}

	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		ArangoDBElementProperty<?> oldValue = this.properties.remove(property.key());
		if (oldValue != null) {
			save();
		}
		
	}

	@Override
	public String key() {
		return owner.key;
	}

	@Override
	public V value() throws NoSuchElementException {
		return value;
	}

	@Override
	public boolean isPresent() {
		return value != null;
	}

	@Override
	public void remove() {
		owner.removeProperty(this);
		
	}

	@Override
	public Vertex element() {
		return owner.owner;
	}

	public void owner(ArangoDBVertexPropertyOwner<V> owner) {
		this.owner = owner;
	}
	
	
	
	
//	
//	@Override
//	public Object id() {
//		return key;
//	}
//	
//	public Set<ArangoDBVertexProperty.ArangoDBVertexPropertyProperty<Object>> getElementProperties() {
//		return properties;
//	}
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public <VP> Property<VP> property(String key, VP value) {
//		ElementHelper.validateProperty(key, value);
//		if (key == T.id.name()) {
//			if (!element().graph().features().vertex().properties().willAllowId(key)) {
//				VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
//			}
//		}
//		ArangoDBVertexProperty.ArangoDBVertexPropertyProperty<VP> property;
//		Optional<ArangoDBVertexProperty.ArangoDBVertexPropertyProperty<Object>> existing = this.properties.stream().filter(p -> p.key.equals(key)).findFirst(); 
//		if (existing.isPresent()) {
//			property = (ArangoDBVertexProperty.ArangoDBVertexPropertyProperty<VP>) existing.get();
//			property.value = value;
//		} else {
//			property = new ArangoDBVertexProperty.ArangoDBVertexPropertyProperty<VP>(key, value, this);
//			this.properties.add((ArangoDBVertexProperty.ArangoDBVertexPropertyProperty<Object>) property);
//		}
//		owner.save();
//		return property;
//	}
//
//	@Override
//	public Vertex element() {
//		return (Vertex) super.element();
//	}
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public <VP> Iterator<Property<VP>> properties(String... propertyKeys) {
//		if (propertyKeys.length == 0) {		// No filter
//			return properties.stream()
//					.map(v -> (Property<VP>)v)
//					.iterator();
//		}
//		Set<String> filterProperties = new HashSet<>(Arrays.asList(propertyKeys));
//		return this.properties.stream()
//				.filter(p -> filterProperties.contains(p.key()))
//				.map(v -> (Property<VP>)v)
//				.iterator();
//	}
//	


}