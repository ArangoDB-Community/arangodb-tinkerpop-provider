package com.arangodb.tinkerpop.gremlin.velocipack;

import com.arangodb.velocypack.VPack;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.exception.VPackParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class JavaPrmitiveType {

    private static final Logger logger = LoggerFactory.getLogger(JavaPrmitiveType.class);
    private final Object value;

    public JavaPrmitiveType(Object value) {
        this.value = value;
    }

    /**
     * Gets the correct primitive.
     * @param valueClass            the canonical name of the class.
     * @return the 		correct Java primitive
     */

    @SuppressWarnings("unchecked")
    public Object getCorretctPrimitive(String valueClass) {

        switch(valueClass) {
            case "java.lang.Float":
            {
                if (value instanceof Double) {
                    double dv = (Double) value;
                    return (float) dv;
                }
                else if (value instanceof Long) {
                    return ((Long) value) * 1.0f;
                }
                else {
                    logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
                }
                break;
            }
            case "java.lang.Double":
            {
                if (value instanceof Double) {
                    return value;
                }
                else if (value instanceof Long) {
                    return ((Long) value) * 1.0;
                }
                else {
                    logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
                }
                break;
            }
            case "java.lang.Long":
            {
                if (value instanceof Long) {
                    return value;
                }
                else if (value instanceof Double) {
                    return ((Double)value).longValue();
                }
                else {
                    logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
                }
                break;
            }
            case "java.lang.Integer":
            {
                if (value instanceof Long) {
                    long lv = (Long) value;
                    return (int) lv;
                }
                break;
            }
            case "java.lang.String":
            case "java.lang.Boolean":
            case "":
                return value;
            case "java.util.HashMap":
                //logger.debug(((Map<?,?>)baseValue).keySet().stream().map(Object::getClass).collect(Collectors.toList()));
                //logger.debug("Add conversion for map values to " + valueClass);
                // Maps are handled by ArangoOK, but we have an extra field, remove it
                Map<String, ?> valueMap = (Map<String,?>)value;
                for (String key : valueMap.keySet()) {
                    if (key.startsWith("_")) {
                        valueMap.remove(key);
                    }
                    // We might need to check individual values...
                }
                break;
            case "java.util.ArrayList":
                // Should we save the type per item?
                List<Object> list = new ArrayList<>();
                ((ArrayList<?>)value).forEach(e -> list.add(new JavaPrmitiveType(e).getCorretctPrimitive("")));
                return list;
            case "boolean[]":
                List<Object> barray = (List<Object>)value;
                boolean[] br = new boolean[barray.size()];
                IntStream.range(0, barray.size())
                        .forEach(i -> br[i] = (boolean) barray.get(i));
                return br;
            case "double[]":
                List<Object> varray = (List<Object>)value;
                double[] dr = new double[varray.size()];
                IntStream.range(0, varray.size())
                        .forEach(i -> dr[i] = (double) new JavaPrmitiveType(varray.get(i)).getCorretctPrimitive("java.lang.Double"));
                return dr;
            case "float[]":
                List<Object> farray = (List<Object>)value;
                float[] fr = new float[farray.size()];
                IntStream.range(0, farray.size())
                        .forEach(i -> fr[i] = (float) new JavaPrmitiveType(farray.get(i)).getCorretctPrimitive("java.lang.Float"));
                return fr;
            case "int[]":
                List<Object> iarray = (List<Object>)value;
                int[] ir = new int[iarray.size()];
                IntStream.range(0, iarray.size())
                        .forEach(i -> ir[i] = (int) new JavaPrmitiveType(iarray.get(i)).getCorretctPrimitive("java.lang.Integer"));
                return ir;
            case "long[]":
                List<Object> larray = (List<Object>)value;
                long[] lr = new long[larray.size()];
                IntStream.range(0, larray.size())
                        .forEach(i -> lr[i] = (long) new JavaPrmitiveType(larray.get(i)).getCorretctPrimitive("java.lang.Long"));
                return lr;
            case "java.lang.String[]":
                List<Object> sarray = (List<Object>)value;
                String[] sr = new String[sarray.size()];
                IntStream.range(0, sarray.size())
                        .forEach(i -> sr[i] = (String) sarray.get(i));
                return sr;
            default:
                VPack vpack = new VPack.Builder().build();
                VPackSlice slice = vpack.serialize(value);
                Object result;
                try {
                    result = vpack.deserialize(slice, Class.forName(valueClass));
                    return result;
                } catch (VPackParserException | ClassNotFoundException e1) {
                    logger.warn("Type not deserializable using VPack", e1);
                }
                logger.debug("Add conversion for " + value.getClass().getName() + " to " + valueClass);
        }
        return value;
    }
}
