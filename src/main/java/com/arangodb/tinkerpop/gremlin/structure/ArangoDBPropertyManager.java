package com.arangodb.tinkerpop.gremlin.structure;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class centralized the management of Element/VertexProperty vertexProperties. Vertices, edges and VertexProperties
 * delegate all property related methods to this class.
 */
public class ArangoDBPropertyManager {

    /** The Logger. */

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

    /** The element that owns the property */

    private final Element element;

    /** The properties managed by this manager, keyd by property key (name) */

    protected Map<String, Collection<Property>> properties = new HashMap<>();

    /** The cardinality of the properties managed by this mamanger, keyed by property key (name) */

    private Map<String, VertexProperty.Cardinality> cardinalities = new HashMap<>();

    ArangoDBPropertyManager(Element element) {
        this.element = element;
    }

    // Methods for All

    /**
     * Get the values of properties as an {@link Iterator}.
     */
    public <V> Iterator<V> values(String... propertyKeys) {
        List<Iterator<V>> iterators = new ArrayList<>();
        allPropertiesIfEmpty(propertyKeys).forEachRemaining(k -> iterators.add(propertyValues(k)));
        return Iterators.concat(iterators.iterator());
    }

    /**
     * Return the set of keys that the manager knows of
     * @return                      A set of the keys known to the manager
     */
    Set<String> keys() {
        return Collections.unmodifiableSet(properties.keySet());
    }

    /**
     * Remove a property from the manager
     * @param property              The property to remove
     */
    void removeProperty(Property<?> property) {

        Collection<Property> props = properties.get(property.key());
        boolean r = props.remove(property);
        if (!r) {
            logger.info("Attempting to remove unknown property %s from element.", property.key());
        }
        if (props.isEmpty()) {
            properties.remove(property.key());
            cardinalities.remove(property.key());
        }
    }

    /**
     * Get the cardinality of a given key.
     * @param key                   The key
     * @return                      The Cardinality of the key
     */
    VertexProperty.Cardinality cardinality(String key) {
        return cardinalities.get(key);
    }


    // Methods for Edges and VertexProperties

    /**
     * Retrieve the Property for the given key. If the cardinality of the Property is {#link Cardinality.list} or
     * {#link Cardinality.set} it returns only one value.
     * @param key                   The key of the property to retrive
     * @param <V>                   The expected type of the property value
     * @return
     */
    @SuppressWarnings("unchecked")
    public <V> Property<V> property(final String key) {
        if (properties.containsKey(key)) {
            final Collection<Property> properties = this.properties.get(key);
            assert !properties.isEmpty();
            return properties.iterator().next();
        }
        else {
            return Property.empty();
        }
    }

    /**
     * Get an {@link Iterator} of properties. If now arguments are provided all the {@link #element} properties are
     * returned.
     * @param propertyKeys          The list of properties to retrieve. If empty, all properties are retrieved
     * @return                      An iterator of all the requested properties
     */
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<Iterator<Property<V>>> iterators = new ArrayList<>();
        allPropertiesIfEmpty(propertyKeys).forEachRemaining(k -> iterators.add(propertyIterator(k)));
        return Iterators.concat(iterators.iterator());
    }

    /**
     * Add or set a property value for the {@link #element} given its key.
     * @param key                   The property key
     * @param value                 The property value
     * @return                      The created or modified property
     */
    public <V> Property<V> property(final String key, final V value) {
        ArangoDBElementProperty<V> p = new ArangoDBElementProperty<>(key, value, (ArangoDBElement) element);
        addSingleProperty(key, p);
        return p;
    }

    /**
     * Add a collection of Properties that the manager needs to manage. This methods is used during de-serialization
     * to add properties to an Edge or VertexProperty
     * @param properties            The collection of properties.
     */
    void attachProperties(Collection<ArangoDBElementProperty> properties) {
        for (ArangoDBElementProperty p : properties) {
            this.properties.put(p.key(), new HashSet<>(Collections.singleton(p)));
            this.cardinalities.put(p.key(), VertexProperty.Cardinality.single);
        }
    }

    // Methods for Vertices

    /**
     * Retrieve the VertexProperty for the given key. If the cardinality of the Property is {#link Cardinality.list} or
     * {#link Cardinality.set} it returns only one value.
     * @param key                   The key of the property to retrive
     * @param <V>                   The expected type of the property value
     * @return
     */
    <V> VertexProperty<V> vertexProperty(final String key) {
        if (properties.containsKey(key)) {
            Iterator<Property> it = properties.get(key).iterator();
            ArangoDBVertexProperty<V> value = (ArangoDBVertexProperty) it.next();
            if (it.hasNext())
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            return value;
        }
        else {
            return VertexProperty.empty();
        }
    }

    /**
     * Get an {@link Iterator} of properties.
     */
    <V> Iterator<VertexProperty<V>> vertexProperties(String... propertyKeys) {
        List<Iterator<VertexProperty<V>>> iterators = new ArrayList<>();
        allPropertiesIfEmpty(propertyKeys).forEachRemaining(k -> iterators.add(propertyIterator(k)));
        return Iterators.concat(iterators.iterator());

    }

    /**
     * Get the {@link VertexProperty} for the provided key. If the property does not exist, return
     * {@link VertexProperty#empty}.
     *
     * @param key                   the key of the vertex property to get
     * @param <V>                   the expected type of the vertex property value
     * @return                      the retrieved vertex property
     */
    <V> VertexProperty<V> vertexProperty(final String key, final V value) {
        ArangoDBVertexProperty<V> p = new ArangoDBVertexProperty<>(key, value, (ArangoDBVertex) element, VertexProperty.Cardinality.single);
        addSingleProperty(key, p);
        return p;
    }

    /**
     * Create a new vertex property. If the cardinality is {@link VertexProperty.Cardinality#single}, then set the key
     * to the value. If the cardinality is {@link VertexProperty.Cardinality#list}, then add a new value to the key.
     * If the cardinality is {@link VertexProperty.Cardinality#set}, then only add a new value if that value doesn't
     * already exist for the key.
     *
     * @param cardinality           the desired cardinality of the property key
     * @param key                   the key of the vertex property
     * @param value                 The value of the vertex property
     * @param <V>                   the type of the value of the vertex property
     * @return                      the newly created vertex property
     */
    <V> VertexProperty<V> vertexProperty(final String key, final V value, VertexProperty.Cardinality cardinality) {
        ArangoDBVertexProperty<V> p = new ArangoDBVertexProperty<>(key, value, (ArangoDBVertex) element, cardinality);
        Collection<Property> props = properties.get(key);
        if (props == null) {
            if (VertexProperty.Cardinality.list.equals(cardinality)) {
                props = new ArrayList<>();
            }
            else {
                props = new HashSet<>();
            }
            properties.put(key, props);
            cardinalities.put(key, cardinality);

        }
        else {
            VertexProperty.Cardinality c = cardinalities.get(key);
            if (!c.equals(cardinality)) { // Update cardinality storage
                if (cardinality.equals(VertexProperty.Cardinality.list)) {
                    props = new ArrayList<>(props);
                }
                else {
                    props = new HashSet<>(props);
                }
                properties.put(key, props);
                cardinalities.put(key, cardinality);
            }
        }
        props.add(p);
        // TODO Determine the correct place to save to reduce the amount of transactions
        ((ArangoDBElement) element).save();
        return p;
    }

    /**
     * Get all the vertexProperties that match the key:value pair.
     * @param key                   the key of the vertex property
     * @param value                 the value of the vertex property to match
     * @param <V>                   the type of the value of the vertex property
     * @return                      a collection of properties that match the value
     */
    <V> Collection<VertexProperty<V>> propertiesForValue(String key, V value) {
        if (!properties.containsKey(key)) {
            return Collections.emptyList();
        }
        if (VertexProperty.Cardinality.single.equals(cardinalities.get(key))) {
            throw new IllegalStateException("Matching values search can not be used for vertexProperties with SINGLE cardinallity");
        }
        Collection<VertexProperty<V>> result = null;
        if (VertexProperty.Cardinality.list.equals(cardinalities.get(key))) {
            result = new ArrayList<>();
        }
        else if (VertexProperty.Cardinality.set.equals(cardinalities.get(key))) {
            result = new HashSet<>();
        }
        if (result == null) {
            throw new IllegalStateException("Matching values search can not be used for vertexProperties with assigned cardinallity");
        }
        Iterator<? extends Property<Object>> itty = vertexProperties(key);
        while (itty.hasNext()) {
            final VertexProperty<V> property = (VertexProperty<V>) itty.next();
            if (property.value().equals(value)) {
                result.add(property);
            }
        }
        return result;
    }

    /**
     * Add a collection of Properties that the manager needs to manage. This methods is used during de-serialization
     * to add properties to an Edge or VertexProperty
     * @param properties            The collection of properties.
     */
    void attachVertexProperties(String key, Collection<ArangoDBVertexProperty> properties) {
        this.properties.put(key, properties.stream().map(p -> (ArangoDBElementProperty)p).collect(Collectors.toList()));
        this.cardinalities.put(key, properties.stream().findFirst().map(ArangoDBVertexProperty::getCardinality).orElse(VertexProperty.Cardinality.single));
    }


    // -----

    /**
     * Helper method to add Properties with {#link Cardinality.single} cardinality.
     * @param key                   the key of the property
     * @param p                     the property
     */
    private void addSingleProperty(String key, ArangoDBElementProperty<?> p) {
        this.properties.put(key, new HashSet<>(Collections.singleton(p)));
        this.cardinalities.put(key, VertexProperty.Cardinality.single);
        // TODO Determine the correct place to save to reduce the amount of transactions
        ((ArangoDBElement) element).save();
    }

    /**
     * Helper method for queries that want all properties.
     * @param propertyKeys
     * @return
     */
    private Iterator<String> allPropertiesIfEmpty(String[] propertyKeys) {
        if (propertyKeys.length > 0) {
            return Arrays.asList(propertyKeys).iterator();
        }
        return keys().iterator();
    }

    /**
     * Return an iterator of all properties for the given key.
     * @param key                   The property key
     * @param <V>                   The type of the property value
     * @param <P>                   The type of the properties to return
     * @return                      An iterator of the existing properties.
     */
    @SuppressWarnings("unchecked")
    private <V, P extends Property<V>> Iterator<P> propertyIterator(String key) {
        Collection<P> result = new ArrayList<>();
        if (properties.containsKey(key)) {
            properties.get(key).forEach(p -> result.add((P) p));
        }
        return result.iterator();
    }

    /**
     * Return an iteratod of all the values for the given key
     * @param key                   The property key
     * @param <V>                   The type of the property value
     * @return                      An iterator of the properties' values
     */
    @SuppressWarnings("unchecked")
    private <V> Iterator<V> propertyValues(String key) {
        Collection<V> result = new ArrayList<>();
        if (properties.containsKey(key)) {
            properties.get(key).forEach(p -> result.add((V) p.value()));
        }
        return result.iterator();
    }

}
