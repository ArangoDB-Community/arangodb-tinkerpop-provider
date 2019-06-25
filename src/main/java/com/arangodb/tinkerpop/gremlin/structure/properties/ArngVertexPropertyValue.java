package com.arangodb.tinkerpop.gremlin.structure.properties;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex.CantAddValueToSinglePropertyException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;


public class ArngVertexPropertyValue<V> implements VertexPropertyValue<V> {

    private final VertexProperty.Cardinality cardinality;
    private final ArngVertexProperty<V> singleValue;
    private final Collection<ArngVertexProperty<V>> multiValue;

    public ArngVertexPropertyValue(VertexProperty.Cardinality cardinality, ArngVertexProperty<V> value) {
        this.cardinality = cardinality;
        switch(cardinality) {
            case set: {
                multiValue = new HashSet<>();
                multiValue.add(value);
                singleValue = null;
                break;
            }
            case list: {
                multiValue = new ArrayList<>();
                multiValue.add(value);
                singleValue = null;
                break;
            }
            case single:
            default: {
                singleValue = value;
                multiValue = Collections.emptyList();
            }
        }
    }

    @Override
    public VertexProperty<V> one(String key) {
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
            return singleValue;
        }
        Iterator<ArngVertexProperty<V>> iterator = multiValue.iterator();
        if (iterator.hasNext()) {
            final ArngVertexProperty<V> property = iterator.next();
            if (iterator.hasNext()) {
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            }
            return property;
        }
        return VertexProperty.empty();
    }

    @Override
    public Iterator<V> values() {
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
            return Collections.singletonList(singleValue.value()).iterator();
        }
        else {
            return multiValue.stream().map(ArngVertexProperty::value).iterator();
        }
    }

    @Override
    public Iterator<VertexProperty<V>> properties() {
        List<VertexProperty<V>> result = new ArrayList<>();
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
            result.add(singleValue);
        }
        else {
            result.addAll(multiValue);
        }
        return result.iterator();
    }


    @Override
    public VertexProperty.Cardinality cardinality() {
        return cardinality;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValues(Iterator<VertexProperty<V>> values) throws CantAddValueToSinglePropertyException {
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
            throw new CantAddValueToSinglePropertyException("Unssuported operation");
        }
        values.forEachRemaining(v -> multiValue.add((ArngVertexProperty) v));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(VertexProperty value) throws CantAddValueToSinglePropertyException {
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
            throw new CantAddValueToSinglePropertyException("Unssuported operation");
        }
        multiValue.add((ArngVertexProperty) value);
    }

    @Override
    public boolean removeOne(ArngVertexProperty<V> value) throws ArangoDBVertex.CantRemoveValueFromSinglePropertyException {
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
            throw new ArangoDBVertex.CantRemoveValueFromSinglePropertyException("Unssuported operation");
        }
        multiValue.remove(value);
        return multiValue.isEmpty();
    }


}
