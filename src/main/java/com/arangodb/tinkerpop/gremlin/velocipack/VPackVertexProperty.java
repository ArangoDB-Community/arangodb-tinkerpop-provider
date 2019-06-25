package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.properties.ArngVertexProperty;
import com.arangodb.tinkerpop.gremlin.structure.properties.ElementProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * The interface V pack vertex properties.
 */
public interface VPackVertexProperty {
    /**
     * A Class to capture the metadata information and let VPack serialize it
     */
    public static class TinkerPopMetadata {

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
        private final List<Collection<ElementProperty>> properties;

        /**
         * Instantiates a new Tinker pop metadata.
         *
         * @param cardinality the cardinality
         */
        public TinkerPopMetadata(
                VertexProperty.Cardinality cardinality,
                Collection<String> types,
                Iterator<ElementProperty> properties) {
            this.cardinality = cardinality;
            this.types = new ArrayList<>(types);
            this.properties = new ArrayList<>();
//            StreamSupport.stream(new Iterable<ElementProperty>() {
//
//                @Override
//                public Iterator iterator() {
//                    return properties;
//                }
//            }).collect(Collectors.toList());
            List<ElementProperty> currentProps = new ArrayList<>();
            properties.forEachRemaining(currentProps::add);
            this.properties.add(currentProps);
        }

        private TinkerPopMetadata(
                VertexProperty.Cardinality cardinality,
                Collection<String> types,
                List<Collection<ElementProperty>> properties) {
            this.cardinality = cardinality;
            this.types = new ArrayList<>(types);
            this.properties = new ArrayList<>(properties);
        }


        public TinkerPopMetadata addMetadata(String type, Collection<ElementProperty> properties) {
            ArrayList<String> newtypes = new ArrayList<>(types);
            newtypes.add(type);
            ArrayList<Collection<ElementProperty>> newProperties = new ArrayList<>(this.properties);
            newProperties.add(properties);
            return new TinkerPopMetadata(cardinality, newtypes, newProperties);

        }
    }

    ArngVPackVertexProperty addPropertyInformation(ArngVertexProperty property);

    /**
     * Key string.
     *
     * @return the string
     */
    public String key();

    /**
     * Value object.
     *
     * @return the object
     */
    public Object baseValue();

    /**
     * Metadata tinker pop metadata.
     *
     * @return the tinker pop metadata
     */
    public TinkerPopMetadata metadata();

}
