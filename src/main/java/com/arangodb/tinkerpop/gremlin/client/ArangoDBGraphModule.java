package com.arangodb.tinkerpop.gremlin.client;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.arangodb.entity.DocumentField;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElementProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertexProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertexProperty.ArangoDBVertexPropertyOwner;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackDeserializationContext;
import com.arangodb.velocypack.VPackDeserializer;
import com.arangodb.velocypack.VPackModule;
import com.arangodb.velocypack.VPackSerializationContext;
import com.arangodb.velocypack.VPackSerializer;
import com.arangodb.velocypack.VPackSetupContext;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.exception.VPackException;

/**
 * Provide custom VelocyPack serializer/deserializer to correctly handle the custom fields. We need to do 
 * this because of the Map nature of the implementation (VelocyPack captures the Map identity first).
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 *
 */
public class ArangoDBGraphModule implements VPackModule {

	@Override
	public <C extends VPackSetupContext<C>> void setup(C context) {
		context.registerDeserializer(ArangoDBVertexPropertyOwner.class, VERTEX_DESERIALIZER);
		//context.registerSerializer(ArangoDBVertex.class, VERTEX_SERIALIZER);
	}
	
	public static final VPackDeserializer<ArangoDBVertexPropertyOwner<Object>> VERTEX_DESERIALIZER = new VPackDeserializer<ArangoDBVertexPropertyOwner<Object>>() {

		@Override
		public ArangoDBVertexPropertyOwner<Object> deserialize(
			VPackSlice parent,
			VPackSlice vpack,
			VPackDeserializationContext context)
			throws VPackException {
			ArangoDBVertexPropertyOwner<Object> owner = new ArangoDBVertexPropertyOwner<Object>();
			owner.properties((Collection<ArangoDBVertexProperty<Object>>) vpack.get("properties"));
			ArangoDBVertexProperty prop = context.deserialize(vpack.get("property"), ArangoDBVertexProperty.class);
			prop.owner(owner);
			owner.property(prop);
			owner.key(vpack.get("key").getAsString());
			owner.cardinality(context.deserialize(vpack.get("cardinality"), VertexProperty.Cardinality.class));
			return owner;
		}
		
	};
	/*
	public static final VPackSerializer<ArangoDBVertex<Object>> VERTEX_SERIALIZER = new VPackSerializer<ArangoDBVertex<Object>>() {
		
		@Override
		public void serialize(
			VPackBuilder builder,
			String attribute,
			ArangoDBVertex<Object> value,
			VPackSerializationContext context)
					throws VPackException {
			final Map<String, Object> doc = new HashMap<String, Object>();
			doc.put(DocumentField.Type.ID.getSerializeName(), value._id());
			doc.put(DocumentField.Type.KEY.getSerializeName(), value._key());
			doc.put(DocumentField.Type.REV.getSerializeName(), value._rev());
			Iterator<VertexProperty<Object>> it = value.properties();
			while (it.hasNext()) {
				VertexProperty<Object> prop = it.next();
				doc.put(prop.key(), prop);
			}
			context.serialize(builder, attribute, doc);
		}
	};
	*/
	
	public static Object getCorretctPrimitive(Object value) {
		if (value instanceof Number) {
			if (value instanceof Float) {
				return value;
			}
			else if (value instanceof Double) {
				return value;
			}
			else {
				String numberStr = value.toString();
				BigInteger number = new BigInteger(numberStr);
	            if(number.longValue() < Integer.MAX_VALUE && number.longValue() > Integer.MIN_VALUE) {
	            	return new Integer(numberStr);
	            }
	            else if(number.longValueExact() < Long.MAX_VALUE && number.longValue() > Long.MIN_VALUE) {
	            	return new Long(numberStr);
	            }
	            else {
	            	return number;
	            }
			} 
		}
		else {
			return value;
		}
		
	}

}
