package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.persistence.AdbValue;
import com.arangodb.tinkerpop.gremlin.persistence.SimplePropertyData;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public abstract class ArangoDBSimpleElement<D extends SimplePropertyData> extends ArangoDBElement<AdbValue, D> {
    public ArangoDBSimpleElement(ArangoDBGraph graph, D data) {
        super(graph, data);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <V> Property<V> createProperty(String key, AdbValue value) {
        return new ArangoDBProperty<>(this, key, (V) value.getValue());
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (removed()) throw Exceptions.elementAlreadyRemoved(id());
        ElementHelper.validateProperty(key, value);
        AdbValue dataValue = AdbValue.of(value);
        data().add(key, dataValue);
        doUpdate();
        return createProperty(key, dataValue);
    }

    public void removeProperty(String key) {
        if (removed()) throw Exceptions.elementAlreadyRemoved(id());
        data.remove(key);
        doUpdate();
    }

}
