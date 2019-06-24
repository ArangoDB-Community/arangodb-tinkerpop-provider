//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.velocypack.annotations.Expose;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;


/**
 * The Class ArangoDBVertexProperty.
 *
 * @param <V> the type of the property value
 * 
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */

public class ArangoDBVertexProperty<V> extends ArangoDBElementProperty<V> implements VertexProperty<V>, ArngElement {

	/** The Logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertexProperty.class);

    /** The cardinality of the property */

    private Cardinality cardinality;

    @Expose(serialize = false, deserialize = false)
    private ArangoDBPropertyManager pManager;

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertexProperty() {
	    super();
        pManager = new ArangoDBPropertyManager(this);
	}

	/**
	 * Instantiates a new arango DB vertex property.
	 *
	 * @param name the name
	 * @param value the value
	 * @param owner the owner
	 */
	
	public ArangoDBVertexProperty(
        String name,
        V value,
        ArangoDBVertex owner,
        Cardinality cardinality) {
		super(name, value, owner);
        this.cardinality = cardinality;

	}

	@Override
	public Object id() {
		return key;
	}
	
	@Override
    public String label() {
        return key;
    }

    @Override
    public Vertex element() {
        return (Vertex) element;
    }
    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return pManager.properties(propertyKeys);
    }

    @Override
    public Set<String> keys() {
        return pManager.keys();
    }

    @Override
    public <U> Property<U> property(String key) {
        return pManager.property(key);
    }

    @Override
    public <U> Iterator<U> values(String... propertyKeys) {
        return pManager.values(propertyKeys);
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        return pManager.property(key, value);
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public void removeProperty(ArangoDBElementProperty<?> property) {
        pManager.removeProperty(property);
    }

    @Override
    public <PV> void attachProperties(Iterator<Property<PV>> properties) {
        pManager.attachProperties(properties);
    }


    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
	
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
    	return key().hashCode() + value().hashCode();
    }


}