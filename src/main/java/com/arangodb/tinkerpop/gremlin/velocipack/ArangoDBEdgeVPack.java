package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElementProperty;
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
 * The ArangoDBEdgeVPack is a specialized VPackSerializer/Deserializer that can traverse the edges's properties
 * and store both their values and additional required Tinkerpop metadata: their expected Java types. For edges the
 * cardinality is always {@link VertexProperty.Cardinality#single} and no nested properties are allowed. In order to
 * avoid breaking the expectations of the serialized documents, the Tinkerpop metadata is stored separately under the
 * "!tinkerpop" property. This allows reusing existing collections with Tinkerpop and quering the documents via AQL
 * without having to worry about the Tinkerpop metadata. The property's value is stored with the property in the root
 * JSON object using a single value.
 *
 * We use the following JSON structure to persist the Tinkerpop metadata. For each property we have a value that
 * captures the type. Note that the <i>type</i> and <i>properties</i> values
 * are arrays so we can persist type and nested properties for each of the property's values.
 * <p>
 * <pre>{@code
 *   {
 *     ...
 *     "!tinkerpop" : {
 *       "<property_key>": "<propValue.class.qualifiedName()>"
 *       },
 *       ...
 *     }
 *   }
 * }</pre>
 * <p>
 * In the following example the base document attributes have been augmented with Tinkerpop metadata:
 * <pre>{@code
 * {
 *   "handle" : "knows/3456789",
 *   "primaryKey" : "3456789",
 *   "revision" : "14253647",
 *   "_from": "persons/653214"
 *   "_to": "persons/5486214"
 *   "relation" : "Friend",
 *   "!tinkerpop": {
 *     "relation" : "lang.java.String"
 *   }
 * }
 * }</pre>
 */
public class ArangoDBEdgeVPack implements VPackSerializer<ArangoDBEdge>, VPackDeserializer<ArangoDBEdge> {

    public static final String TINKERPOP_METADATA_KEY = "!tinkerpop";

    @Override
    public void serialize(
        VPackBuilder builder,
        String attribute,
        ArangoDBEdge value,
        VPackSerializationContext context) throws VPackException {

        builder.add(attribute, ValueType.OBJECT);
        if (value.handle() != null) {
            builder.add("handle", value.handle());
        }
        if (value.primaryKey() != null) {
            builder.add("primaryKey", value.primaryKey());
        }
        if (value._from() != null) {
            builder.add("_from", value._from());
        }
        if (value._to() != null) {
            builder.add("_to", value._to());
        }
        Map<String, String> pTypes = new HashMap<>();
        Iterator<? extends Property<Object>> itty = value.properties();
        while (itty.hasNext()) {
            ArangoDBElementProperty<?> p = (ArangoDBElementProperty<?>) itty.next();
            pTypes.put(p.key(), p.value().getClass().getCanonicalName());
            context.serialize(builder, p.key(), p.value());
        }
        builder.add(TINKERPOP_METADATA_KEY, ValueType.OBJECT);
        for (String k : pTypes.keySet()) {
            context.serialize(builder, k, pTypes.get(k));
        }
        builder.close();
        builder.close();
    }

    @Override
    public ArangoDBEdge deserialize(VPackSlice parent, VPackSlice vpack, VPackDeserializationContext context) throws VPackException {
        final ArangoDBEdge edge = new ArangoDBEdge();
        VPackSlice temp = vpack.get(TINKERPOP_METADATA_KEY);
        Iterator<Map.Entry<String, VPackSlice>> it = temp.objectIterator();
        Map<String, String> pTypes = new HashMap<>();
        while (it.hasNext()) {
            Map.Entry<String, VPackSlice> entry = it.next();
            pTypes.put(entry.getKey(), context.deserialize(entry.getValue(), String.class));
        }
        // FIXME We KNOW the keys we want, so we wan use a constructor, not reflection!
        it = vpack.objectIterator();
        List<Property<?>> properties = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, VPackSlice> entry = it.next();
            String key = entry.getKey();
            if (key.equals(TINKERPOP_METADATA_KEY)) {
                continue;
            }
            else if (key.startsWith("_")) {
                Method method = null;
                try {
                    method = edge.getClass().getMethod(key, String.class);
                } catch (NoSuchMethodException e) {
                    throw new VPackParserException(e);
                }
                try {
                    method.invoke(edge, entry.getValue().getAsString());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new VPackParserException(e);
                }
            }
            else {
                Object rawValue = context.deserialize(entry.getValue(), Object.class);
                Object v = ArangoDBUtil.getCorretctPrimitive(rawValue, pTypes.get(key));
                ArangoDBElementProperty<Object> p = new ArangoDBElementProperty<>(key, v, edge);
                properties.add(p);
            }
        }
        edge.attachProperties(properties);
        return edge;
    }


}
