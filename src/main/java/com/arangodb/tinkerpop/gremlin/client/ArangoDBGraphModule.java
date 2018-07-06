package com.arangodb.tinkerpop.gremlin.client;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.arangodb.entity.DocumentField;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBElement.ArangoDBElementProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex.ArangoDBVertexProperty;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex.ArangoDBVertexProperty.ArangoDBVertexPropertyProperty;
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
		context.registerDeserializer(ArangoDBVertex.class, VERTEX_DESERIALIZER);
		context.registerSerializer(ArangoDBVertex.class, VERTEX_SERIALIZER);
		context.registerDeserializer(ArangoDBEdge.class, EDGE_DESERIALIZER);
		context.registerSerializer(ArangoDBEdge.class, EDGE_SERIALIZER);
		context.registerDeserializer(ArangoDBElementProperty.class, ELEMENT_PROPERTY_DESERIALIZER);
		context.registerSerializer(ArangoDBElementProperty.class, PROPERTY_SERIALIZER);
		context.registerDeserializer(ArangoDBVertexProperty.class, VERTEX_PROPERTY_DESERIALIZER);
		context.registerSerializer(ArangoDBVertexProperty.class, VERTEX_PROPERTY_SERIALIZER);
		context.registerDeserializer(ArangoDBVertexPropertyProperty.class, VERTEX_PROPERTY_PROPERTY_DESERIALIZER);
		context.registerSerializer(ArangoDBVertexPropertyProperty.class, VERTEX_PROPERTY_PROPERTY_SERIALIZER);
	}
	
	@SuppressWarnings("serial")
	public static final Set<String> ARANGODB_GRAPH_FIELDS = new HashSet<String>() {{
		add(DocumentField.Type.ID.getSerializeName());
		add(DocumentField.Type.KEY.getSerializeName());
		add(DocumentField.Type.REV.getSerializeName());
		add(DocumentField.Type.FROM.getSerializeName());
		add(DocumentField.Type.TO.getSerializeName());
		add(ArangoDBEdge.FROM_KEY);
		add(ArangoDBEdge.TO_KEY);
	}};
	
	public static final VPackDeserializer<ArangoDBVertex<Object>> VERTEX_DESERIALIZER = new VPackDeserializer<ArangoDBVertex<Object>>() {

		@Override
		public ArangoDBVertex<Object> deserialize(
			VPackSlice parent,
			VPackSlice vpack,
			VPackDeserializationContext context)
			throws VPackException {
			ArangoDBVertex<Object> vertex = new ArangoDBVertex<>();
			vertex._id(vpack.get(DocumentField.Type.ID.getSerializeName()).getAsString());
			vertex._key(vpack.get(DocumentField.Type.KEY.getSerializeName()).getAsString());
			vertex._rev(vpack.get(DocumentField.Type.REV.getSerializeName()).getAsString());
			Iterator<Entry<String, VPackSlice>> it = vpack.objectIterator();
			while (it.hasNext()){
				Entry<String, VPackSlice> entry = it.next();
				if (!ARANGODB_GRAPH_FIELDS.contains(entry.getKey())) {
					vertex.put(entry.getKey(), getCorretctPrimitive(context.deserialize(entry.getValue(), Object.class))); // FIXME This will not work for nested values!
				}
			}
			return vertex;
		}
		
	};
	
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
	
	public static final VPackDeserializer<ArangoDBEdge<Object>> EDGE_DESERIALIZER = new VPackDeserializer<ArangoDBEdge<Object>>() {

		@Override
		public ArangoDBEdge<Object> deserialize(
			VPackSlice parent,
			VPackSlice vpack, 
			VPackDeserializationContext context)
			throws VPackException {
			ArangoDBEdge<Object> edge = new ArangoDBEdge<>();
			edge._id(vpack.get(DocumentField.Type.ID.getSerializeName()).getAsString());
			edge._key(vpack.get(DocumentField.Type.KEY.getSerializeName()).getAsString());
			edge._rev(vpack.get(DocumentField.Type.REV.getSerializeName()).getAsString());
			edge._from(vpack.get(DocumentField.Type.FROM.getSerializeName()).getAsString());
			edge._to(vpack.get(DocumentField.Type.TO.getSerializeName()).getAsString());
			edge.from_key(vpack.get(ArangoDBEdge.FROM_KEY).getAsString());
			edge.to_key(vpack.get(ArangoDBEdge.TO_KEY).getAsString());
			Iterator<Entry<String, VPackSlice>> it = vpack.objectIterator();
			while (it.hasNext()){
				Entry<String, VPackSlice> entry = it.next();
				if (!ARANGODB_GRAPH_FIELDS.contains(entry.getKey())) {
					edge.put(entry.getKey(), getCorretctPrimitive(context.deserialize(entry.getValue(), Object.class)));
				}
			}
			return edge;
		}
		
	};
	
	public static final VPackSerializer<ArangoDBEdge<Object>> EDGE_SERIALIZER = new VPackSerializer<ArangoDBEdge<Object>>() {
		
		@Override
		public void serialize(
			VPackBuilder builder, 
			String attribute, 
			ArangoDBEdge<Object> value,
			VPackSerializationContext context) 
			throws VPackException {
			final Map<String, Object> doc = new HashMap<String, Object>();
			doc.put(DocumentField.Type.ID.getSerializeName(), value._id());
			doc.put(DocumentField.Type.KEY.getSerializeName(), value._key());
			doc.put(DocumentField.Type.REV.getSerializeName(), value._rev());
			doc.put(DocumentField.Type.FROM.getSerializeName(), value._from());
			doc.put(DocumentField.Type.TO.getSerializeName(), value._to());
			doc.put(ArangoDBEdge.FROM_KEY, value.from_key());
			doc.put(ArangoDBEdge.TO_KEY, value.to_key());
			doc.putAll(value);
			context.serialize(builder, attribute, doc);
		}
	};
	
	public static final VPackDeserializer<ArangoDBElementProperty<Object>> ELEMENT_PROPERTY_DESERIALIZER = new VPackDeserializer<ArangoDBElementProperty<Object>>() {

		@Override
		public ArangoDBElementProperty<Object> deserialize(
			VPackSlice parent,
			VPackSlice vpack, 
			VPackDeserializationContext context)
			throws VPackException {
			
			System.out.println("ELEMENT_PROPERTY_DESERIALIZER " + parent);
//			ArangoDBProperty<Object> property = new ArangoDBProperty<>();
//			edge._id(vpack.get(DocumentField.Type.ID.getSerializeName()).getAsString());
//			Iterator<Entry<String, VPackSlice>> it = vpack.objectIterator();
//			while (it.hasNext()){
//				Entry<String, VPackSlice> entry = it.next();
//				if (!ARANGODB_GRAPH_FIELDS.contains(entry.getKey())) {
//					edge.put(entry.getKey(), getCorretctPrimitive(context.deserialize(entry.getValue(), Object.class)));
//				}
//			}
			return null;
		}
		
	};
	
	public static final VPackSerializer<ArangoDBElementProperty<Object>> PROPERTY_SERIALIZER = new VPackSerializer<ArangoDBElementProperty<Object>>() {
		
		@Override
		public void serialize(
			VPackBuilder builder, 
			String attribute, 
			ArangoDBElementProperty<Object> value,
			VPackSerializationContext context) 
			throws VPackException {
			final Map<String, Object> doc = new HashMap<String, Object>();
			doc.put(ArangoDBEdge.CARDINALITY, value.cardinality());
			doc.put(ArangoDBEdge.VALUE, value.getValue());
			context.serialize(builder, attribute, doc);
		}
	};
	
	public static final VPackDeserializer<ArangoDBVertexProperty<Object>> VERTEX_PROPERTY_DESERIALIZER = new VPackDeserializer<ArangoDBVertexProperty<Object>>() {

		@Override
		public ArangoDBVertexProperty<Object> deserialize(
			VPackSlice parent,
			VPackSlice vpack, 
			VPackDeserializationContext context)
			throws VPackException {
			
			System.out.println("ELEMENT_PROPERTY_DESERIALIZER " + parent);
//			ArangoDBProperty<Object> property = new ArangoDBProperty<>();
//			edge._id(vpack.get(DocumentField.Type.ID.getSerializeName()).getAsString());
//			Iterator<Entry<String, VPackSlice>> it = vpack.objectIterator();
//			while (it.hasNext()){
//				Entry<String, VPackSlice> entry = it.next();
//				if (!ARANGODB_GRAPH_FIELDS.contains(entry.getKey())) {
//					edge.put(entry.getKey(), getCorretctPrimitive(context.deserialize(entry.getValue(), Object.class)));
//				}
//			}
			return null;
		}
		
	};
	
	public static final VPackSerializer<ArangoDBVertexProperty<Object>> VERTEX_PROPERTY_SERIALIZER = new VPackSerializer<ArangoDBVertexProperty<Object>>() {
		
		@Override
		public void serialize(
			VPackBuilder builder, 
			String attribute, 
			ArangoDBVertexProperty<Object> value,
			VPackSerializationContext context) 
			throws VPackException {
			final Map<String, Object> doc = new HashMap<String, Object>();
			doc.put(ArangoDBEdge.CARDINALITY, value.cardinality());
			doc.put(ArangoDBEdge.PROPERTIES, value.getElementProperties());
			doc.put(ArangoDBEdge.VALUE, value.getValue());
			context.serialize(builder, attribute, doc);
		}
	};
	
	public static final VPackDeserializer<ArangoDBVertexPropertyProperty<Object>> VERTEX_PROPERTY_PROPERTY_DESERIALIZER = new VPackDeserializer<ArangoDBVertexPropertyProperty<Object>>() {

		@Override
		public ArangoDBVertexPropertyProperty<Object> deserialize(
			VPackSlice parent,
			VPackSlice vpack, 
			VPackDeserializationContext context)
			throws VPackException {
			
			System.out.println("ELEMENT_PROPERTY_DESERIALIZER " + parent);
			ArangoDBVertexProperty<Object> element = null;  //parent.algo!
			Entry<String, VPackSlice> vp = vpack.objectIterator().next();
			ArangoDBVertexPropertyProperty<Object> p = new ArangoDBVertexPropertyProperty<>(vp.getKey(), vp.getValue(), element);
			return p;
		}
		
	};
	
	public static final VPackSerializer<ArangoDBVertexPropertyProperty<Object>> VERTEX_PROPERTY_PROPERTY_SERIALIZER = new VPackSerializer<ArangoDBVertexPropertyProperty<Object>>() {
		
		@Override
		public void serialize(
			VPackBuilder builder, 
			String attribute, 
			ArangoDBVertexPropertyProperty<Object> value,
			VPackSerializationContext context) 
			throws VPackException {
			final Map<String, Object> doc = new HashMap<String, Object>();
			doc.put(value.key(), value.value());
			context.serialize(builder, attribute, doc);
		}
	};
	
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
