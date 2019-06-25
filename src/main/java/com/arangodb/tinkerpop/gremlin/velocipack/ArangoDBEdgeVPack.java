package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArngDocument;
import com.arangodb.tinkerpop.gremlin.structure.ArngEdge;
import com.arangodb.tinkerpop.gremlin.structure.properties.ArngElementProperty;
import com.arangodb.velocypack.*;
import com.arangodb.velocypack.exception.VPackBuilderException;
import com.arangodb.velocypack.exception.VPackException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

/**
 * The ArangoDBEdgeVPack is a specialized VPackSerializer/Deserializer that can traverse the edges's elementProperties
 * and store both their values and additional required Tinkerpop metadata: their expected Java types. For
 * {@link org.apache.tinkerpop.gremlin.structure.Edge}s and {@link VertexProperty}s the cardinality is always
 * {@link VertexProperty.Cardinality#single} and no nested properties are allowed.
 * <p>
 * In order to avoid breaking the expectations of the serialized documents (i.e. to allow reuse of exising graphs), the
 * Tinkerpop metadata is stored separately under the "!tinkerpop" property. This allows existing documents to be used
 * transparently as well as existing AQL queries that rely on previously defined attribtues.
 * <p>
 * The property's baseValue is stored with the property in the root JSON object using a single baseValue. The only additional
 * metadata stored is the actual java type of the baseValue.
 * <p>
 * We use the following JSON structure to persist the Tinkerpop metadata. For each property we have a baseValue that
 * captures the type.
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
public class ArangoDBEdgeVPack implements VPackSerializer<ArngEdge>, VPackDeserializer<ArngEdge> {

    public static final String TINKERPOP_METADATA_KEY = "!tinkerpop";

    @Override
    public void serialize(
        VPackBuilder builder,
        String attribute,
        ArngEdge value,
        VPackSerializationContext context) throws VPackException {

        try (VPackElement element = new VPackElement(builder)) {
            element.startObject(attribute);
            element.addEdgeAttributes(value);


        } catch (Exception e) {
            throw new VPackBuilderException(e);
        }



        Map<String, String> pTypes = new HashMap<>();
        Iterator<? extends Property<Object>> itty = value.properties();
        while (itty.hasNext()) {
            ArngElementProperty<?> p = (ArngElementProperty<?>) itty.next();
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

        VPackSlice temp = vpack.get(TINKERPOP_METADATA_KEY);
        Iterator<Map.Entry<String, VPackSlice>> it = temp.objectIterator();
        Map<String, String> pTypes = new HashMap<>();
        while (it.hasNext()) {
            Map.Entry<String, VPackSlice> entry = it.next();
            pTypes.put(entry.getKey(), context.deserialize(entry.getValue(), String.class));
        }
        it = vpack.objectIterator();
        String _id = null;
        String _key = null;
        String _rev = null;
        String label = null;
        List<Object> keyValues = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, VPackSlice> entry = it.next();
            String key = entry.getKey();
            if (key.equals(TINKERPOP_METADATA_KEY)) {
                continue;
            }
            switch(key) {
                case "_id":
                    _id = entry.getValue().getAsString();
                    label = _id.split("/")[1];
                    break;
                case "_key":
                    _key = entry.getValue().getAsString();
                    break;
                case "_rev":
                    _rev = entry.getValue().getAsString();
                    break;
                default:
                    keyValues.add(key);
                    keyValues.add(new JavaPrmitiveType(
                                context.deserialize(entry.getValue(), Object.class)
                            ).getCorretctPrimitive(pTypes.get(key)));
            }

        }
        ArangoDBEdge edge = new ArangoDBEdge(_id, _key, _rev, label);
        ElementHelper.attachProperties(edge, keyValues.toArray());
        return edge;
    }


}
