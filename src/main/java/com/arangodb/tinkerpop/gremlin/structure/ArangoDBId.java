package com.arangodb.tinkerpop.gremlin.structure;


import com.arangodb.shaded.fasterxml.jackson.core.JsonGenerator;
import com.arangodb.shaded.fasterxml.jackson.core.JsonParser;
import com.arangodb.shaded.fasterxml.jackson.databind.DeserializationContext;
import com.arangodb.shaded.fasterxml.jackson.databind.JsonDeserializer;
import com.arangodb.shaded.fasterxml.jackson.databind.JsonSerializer;
import com.arangodb.shaded.fasterxml.jackson.databind.SerializerProvider;
import com.arangodb.shaded.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.arangodb.shaded.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.Objects;

@JsonSerialize(using = ArangoDBId.Serializer.class)
@JsonDeserialize(using = ArangoDBId.Deserializer.class)
public class ArangoDBId {
    private final String prefix;
    private final String label;
    private final String key;

    public static ArangoDBId of(String prefix, String label, String key) {
        Objects.requireNonNull(prefix);
        Objects.requireNonNull(label);
        validateIdParts(prefix, label, key);
        return new ArangoDBId(prefix, label, key);
    }

    public static ArangoDBId parse(String prefix, String fullName) {
        String[] parts = fullName.replaceFirst("^" + prefix + "_", "").split("/");
        String label = parts[0];
        String key = parts[1];
        return ArangoDBId.of(prefix, label, key);
    }

    public static ArangoDBId parseWithDefaultLabel(String prefix, String defaultLabel, String fullName) {
        String[] parts = fullName.replaceFirst("^" + prefix + "_", "").split("/");
        String label = parts.length == 2 ? parts[0] : defaultLabel;
        String key = parts[parts.length - 1];
        return ArangoDBId.of(prefix, label, key);
    }

    public static ArangoDBId parse(String fullName) {
        String[] parts = fullName.split("_");
        String prefix = parts[0];
        parts = parts[1].split("/");
        String collection = parts[0];
        String key = parts[1];
        return ArangoDBId.of(prefix, collection, key);
    }

    private static void validateIdParts(String... names) {
        for (String name : names) {
            if (name == null)
                continue;
            if (name.contains("_")) {
                throw new IllegalArgumentException(String.format("key (%s) contains invalid character '_'", name));
            }
            if (name.contains("/")) {
                throw new IllegalArgumentException(String.format("key (%s) contains invalid character '/'", name));
            }
        }
    }

    private ArangoDBId(String prefix, String label, String key) {
        this.prefix = prefix;
        this.label = label.replaceFirst("^" + prefix + "_", "");
        this.key = key;
    }

    public ArangoDBId withKey(String newKey) {
        return ArangoDBId.of(prefix, label, newKey);
    }

    public String getCollection() {
        return prefix + "_" + label;
    }

    public String getLabel() {
        return label;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return prefix + "_" + label + "/" + key;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ArangoDBId)) return false;
        ArangoDBId arangoDBId = (ArangoDBId) o;
        return Objects.equals(prefix, arangoDBId.prefix) && Objects.equals(label, arangoDBId.label) && Objects.equals(key, arangoDBId.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefix, label, key);
    }

    static class Serializer extends JsonSerializer<ArangoDBId> {
        @Override
        public void serialize(ArangoDBId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(value.toString());
        }
    }

    static class Deserializer extends JsonDeserializer<ArangoDBId> {
        @Override
        public ArangoDBId deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return ArangoDBId.parse(p.getValueAsString());
        }
    }
}
