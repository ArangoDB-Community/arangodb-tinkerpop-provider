package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.properties.ArngVertexProperty;
import com.arangodb.tinkerpop.gremlin.structure.properties.ElementProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * The interface VPackVertexProperty defines the API to store VertexProperties in an ammenable format for VPack
 * serialization.
 * <p>
 * Mainly, we need to separate the property values from their type and nested properties. This will be usefull when
 * storing the properties in the proposed JSON format:
 * <p>
 * <pre>{@code
 * {
 *   ...
 *   <key> : <value>,
 *   ...
 *   "!tinkerpop": {
 *   ...
 *   <key> : {
 *     "cardinality": <CARDINALITY>,
 *     "type": [<type>(,<type>)*]
 *     "elementProperties": [
 *         {"key": <key>,
 *          "value": <value>,
 *          "type": <qualifiedName>}
 *          ,...
 *       ]
 *     }
 *   }
 * }</pre>
 */
public interface VPackVertexProperty {

    class PropertiesIterable implements Iterable<ElementProperty> {

        private final Iterator<ElementProperty> iterator;

        public PropertiesIterable(Iterator<ElementProperty> iterator) {
            this.iterator = iterator;
        }

        @Override
        public Iterator<ElementProperty> iterator() {
            return iterator;
        }
    }

    /**
     * A Class to capture the metadata information and let VPack serialize it
     */
    class TinkerPopMetadata {

        /**
         * The Cardinality.
         */
        private final VertexProperty.Cardinality cardinality;
        /**
         * The Type.
         */
        private final List<String> types;
        /**
         * The Properties.
         */
        private final List<List<ElementProperty>> properties;

        /**
         * Instantiates a new Tinker pop metadata.
         *
         * @param cardinality the cardinality
         */
        public TinkerPopMetadata(
                VertexProperty.Cardinality cardinality,
                Collection<String> types,
                Iterator<ElementProperty> properties) {
            this(cardinality, types,
                    new ArrayList<>(Collections.singleton(
                            StreamSupport.stream(
                                    new PropertiesIterable(properties).spliterator(), false)
                                .collect(Collectors.toList())))
                    );
        }

        private TinkerPopMetadata(
                VertexProperty.Cardinality cardinality,
                Collection<String> types,
                List<List<ElementProperty>> properties) {
            this.cardinality = cardinality;
            this.types = new ArrayList<>(types);
            this.properties = new ArrayList<>(properties);
        }


        public TinkerPopMetadata addMetadata(String type, List<ElementProperty> properties) {
            ArrayList<String> newtypes = new ArrayList<>(types);
            newtypes.add(type);
            ArrayList<List<ElementProperty>> newProperties = new ArrayList<>(this.properties);
            newProperties.add(properties);
            return new TinkerPopMetadata(cardinality, newtypes, newProperties);

        }
    }

    VPackVertexProperty addVertexProperty(VertexProperty property);

    /**
     * Key string.
     *
     * @return the string
     */
    String key();

    /**
     * Value object.
     *
     * @return the object
     */
    Object baseValue();

    /**
     * Metadata tinker pop metadata.
     *
     * @return the tinker pop metadata
     */
    //TinkerPopMetadata metadata();

}
