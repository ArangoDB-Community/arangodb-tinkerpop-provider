package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElementProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertexProperty;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;
import com.arangodb.velocypack.*;
import com.arangodb.velocypack.exception.VPackException;
import com.arangodb.velocypack.exception.VPackParserException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * The ArangoDBVertexVPack is a specialized VPackSerializer/Deserializer that can traverse the vertex's vertexProperties
 * and store both their values and additional required Tinkerpop metadata: their expected Java types, the cardinality
 * and nested vertexProperties. In order to avoid breaking the expectations of the serialized documents, the Tinkerpop
 * metadata is stored separately under the "!tinkerpop" property. This allows reusing existing collections with
 * Tinkerpop and quering the documents via AQL without having to worry about the Tinkerpop metadata. The property's
 * value is stored with the property in the root JSON object using a single value for {@link VertexProperty.Cardinality#single}
 * and an array for {@link VertexProperty.Cardinality#set} and {@link VertexProperty.Cardinality#list}.
 *
 * We use the following JSON structure to persist the Tinkerpop metadata. For each property we have an entry which
 * captures the cardinality, type(s) and nested properties. Note that the <i>type</i> and <i>properties</i> values
 * are arrays so we can persist type and nested properties for each of the property's values.
 * <p>
 * <pre>{@code
 *   {
 *     ...
 *     "!tinkerpop" : {
 *       "<property_key>": {
 *         "cardinality": "<VertexProperty.Cardinality>"
 *         "type: ["<propValue.class.qualifiedName()>", ...]
 *         "properties": [
 *           {
 *             "primaryKey": "<primaryKey>"
 *             "value": "<value>"
 *             "type:   "<value.class.qualifiedName()>"
 *           },
 *           { ... }
 *           ...
 *         ]
 *       },
 *       ...
 *     }
 *   }
 * }</pre>
 * <p>
 * In the following example the base document attributes have been augmented with Tinkerpop metadata:
 * <pre>{@code
 * {
 *   "handle" : "myusers/3456789",
 *   "primaryKey" : "3456789",
 *   "revision" : "14253647",
 *   "firstName" : "John",
 *   "lastName" : "Doe",
 *   "address" : {
 *     "street" : "Road To Nowhere 1",
 *     "city" : "Gotham"
 *   },
 *   "hobbies" : [
 *     {name: "swimming", "howFavorite": 10},
 *     {name: "biking", "howFavorite": 6},
 *     {name: "programming", "howFavorite": 4}
 *   ],
 *   "!tinkerpop": {
 *     "firstName" : {
 *       "cardinality": "single",
 *       "type": ["lang.java.String"],
 *       "properties": [ { } ]
 *       },
 *     "lastName" : {
 *       "cardinality": "single",
 *       "type": ["lang.java.String"],
 *       "properties": [ { } ]
 *     },
 *     "address" : {
 *       "cardinality": "single",
 *       "type": "Object",
 *       "properties": [ { } ]
 *     },
 *     "hobbies" : {
 *       "cardinality": "list",
 *       "type": ["lang.java.String", "lang.java.String", "lang.java.String"],
 *       "properties": [
 *         [],
 *         [],
 *         [{
 *           "primaryKey": "since",
 *           "value": 1996,
 *           "type": "lang.java.Integer"
 *         }],
 *       ]
 *     }
 *   }
 * }
 * }</pre>
 */
public class ArangoDBVertexVPack implements VPackSerializer<ArangoDBVertex>, VPackDeserializer<ArangoDBVertex> {

    public static final String TINKERPOP_METADATA_KEY = "!tinkerpop";

    /**
     * A Class to capture the metadata information and let VPack serialize it
     */
    public static class TinkerPopMetadata {

        public VertexProperty.Cardinality cardinality;
        public List<String> type;
        public List<List<ArangoDBElementProperty>> properties;

        public TinkerPopMetadata() { }

        public TinkerPopMetadata(final VertexProperty.Cardinality cardinality) {
            this.cardinality = cardinality;
        }
    }

    @Override
    public void serialize(
        VPackBuilder builder,
        String attribute,
        ArangoDBVertex value,
        VPackSerializationContext context) throws VPackException {

        builder.add(attribute, ValueType.OBJECT);
        if (value.handle() != null) {
            builder.add("handle", value.handle());
        }
        if (value.primaryKey() != null) {
            builder.add("primaryKey", value.primaryKey());
        }
        Map<String, TinkerPopMetadata> metadataMap = new HashMap<>();
        Map<String, List<Object>> pValues = new HashMap<>();
        Map<String, List<String>> pTypes = new HashMap<>();
        Map<String, List<List<ArangoDBElementProperty>>> pProperties = new HashMap<>();
        Iterator<? extends Property<Object>> itty = value.properties();
        while (itty.hasNext()) {
            ArangoDBVertexProperty<?> p = (ArangoDBVertexProperty<?>) itty.next();
            TinkerPopMetadata md = metadataMap.get(p.key());
            if (md == null) {
                md = new TinkerPopMetadata(p.getCardinality());
                metadataMap.put(p.key(), md);
            }
            if (VertexProperty.Cardinality.single.equals(p.getCardinality())) {
                md.type = Collections.singletonList(p.value().getClass().getCanonicalName());
                md.properties = Collections.singletonList(propertyList(p));
                context.serialize(builder, p.key(), p.value());
            }
            else {
                List<Object> values = getOrInit(p.key(), pValues);
                values.add(p.value());
                List<String> types = getOrInit(p.key(), pTypes);
                types.add(p.value().getClass().getCanonicalName());
                List<List<ArangoDBElementProperty>> properties = getOrInit(p.key(), pProperties);
                ArrayList<ArangoDBElementProperty> nps = new ArrayList<>();
                p.properties(p.keys().toArray(new String[0])).forEachRemaining(np -> nps.add((ArangoDBElementProperty)np));
                properties.add(nps);
            }
        }
        for (String k : pValues.keySet()) {
            context.serialize(builder, k, pValues.get(k));
            TinkerPopMetadata md = metadataMap.get(k);
            md.type = pTypes.get(k);
            md.properties = pProperties.get(k);
        }
        builder.add(TINKERPOP_METADATA_KEY, ValueType.OBJECT);
        for (String k : metadataMap.keySet()) {
            context.serialize(builder, k, metadataMap.get(k));
        }
        builder.close();
        builder.close();
    }

    @Override
    public ArangoDBVertex deserialize(VPackSlice parent, VPackSlice vpack, VPackDeserializationContext context) throws VPackException {
        final ArangoDBVertex vertex = new ArangoDBVertex();
        Map<String, TinkerPopMetadata> nProperties = new HashMap<>();
        VPackSlice temp = vpack.get(TINKERPOP_METADATA_KEY);
        Iterator<Map.Entry<String, VPackSlice>> it = temp.objectIterator();
        while (it.hasNext()) {
            Map.Entry<String, VPackSlice> entry = it.next();
            nProperties.put(entry.getKey(), context.deserialize(entry.getValue(), TinkerPopMetadata.class));
        }
        it = vpack.objectIterator();
        while (it.hasNext()) {
            Map.Entry<String, VPackSlice> entry = it.next();
            String key = entry.getKey();
            if (key.equals(TINKERPOP_METADATA_KEY)) {
                continue;
            }
            else if (key.startsWith("_")) {
                Method method = null;
                try {
                    method = vertex.getClass().getMethod(key, String.class);
                } catch (NoSuchMethodException e) {
                    throw new VPackParserException(e);
                }
                try {
                    method.invoke(vertex, entry.getValue().getAsString());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new VPackParserException(e);
                }
            }
            else {
                TinkerPopMetadata md = nProperties.get(key);
                Object rawValue = context.deserialize(entry.getValue(), Object.class);
                if (md == null) {
                    if (rawValue instanceof Collections) {
                        // Without metadata we can not infer Set
                        Collection<?> value = (Collection<?>) rawValue;
                        List<ArangoDBVertexProperty> properties = new ArrayList<>();
                        for (Object v : value) {
                            ArangoDBVertexProperty<?> vp = new ArangoDBVertexProperty<>(key, v, vertex, VertexProperty.Cardinality.list);
                            properties.add(vp);
                        }
                        vertex.attachProperties(key, properties);
                    }
                    else {
                        ArangoDBVertexProperty<?> vp = new ArangoDBVertexProperty<>(key, rawValue, vertex, VertexProperty.Cardinality.single);
                        vertex.attachProperties(key, Collections.singletonList(vp));
                    }
                }
                else {
                    VertexProperty.Cardinality  cardinality = md.cardinality;
                    if (cardinality.equals(VertexProperty.Cardinality.single)) {
                        Object value = ArangoDBUtil.getCorretctPrimitive(rawValue, md.type.get(0));
                        ArangoDBVertexProperty<?> vp = new ArangoDBVertexProperty<>(key, value, vertex, cardinality);
                        vertex.attachProperties(key, Collections.singletonList(vp));
                    }
                    else if (cardinality.equals(VertexProperty.Cardinality.list)) {
                        List<Object> value = (List<Object>) rawValue;
                        List<ArangoDBVertexProperty> properties = new ArrayList<>();
                        for (int index = 0; index<value.size(); index++) {
                            Object v = ArangoDBUtil.getCorretctPrimitive(value.get(index), md.type.get(0));
                            ArangoDBVertexProperty<?> vp = new ArangoDBVertexProperty<>(key, v, vertex, VertexProperty.Cardinality.list);
                            properties.add(vp);
                            List<ArangoDBElementProperty> nps = md.properties.get(index);
                            vp.attachProperties(nps);
                        }
                        vertex.attachProperties(key, properties);
                    }
                }
            }
        }
        return vertex;
    }

    private <V> List<V> getOrInit(String key, Map<String, List<V>> map) {
        List<V> result = map.get(key);
        if (result == null) {
            result = new ArrayList<>();
            map.put(key, result);
        }
        return result;
    }

    private List<ArangoDBElementProperty> propertyList(ArangoDBVertexProperty<?> vertexProperty) {
        List<ArangoDBElementProperty> properties = new ArrayList<>();
        Iterator<Property<Object>> npit = vertexProperty.properties(vertexProperty.keys().toArray(new String[0]));
        npit.forEachRemaining(p -> properties.add((ArangoDBElementProperty)p));
        return properties;
    }
}
