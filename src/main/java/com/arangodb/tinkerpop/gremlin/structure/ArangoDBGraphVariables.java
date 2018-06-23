package com.arangodb.tinkerpop.gremlin.structure;


import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;
import java.util.Set;

public class ArangoDBGraphVariables implements Graph.Variables {

    private final ArangoDBGraph graph;

    public ArangoDBGraphVariables(ArangoDBGraph graph) {
        this.graph = graph;
    }


    @Override
    public Set<String> keys() {
        return null;
    }

    @Override
    public <R> Optional<R> get(String key) {
        return Optional.empty();
    }

    @Override
    public void set(String key, Object value) {

    }

    @Override
    public void remove(String key) {

    }

    public static class ArangoDBGraphVariableFeatures implements Graph.Features.VariableFeatures {

        @Override
        public boolean supportsSerializableValues() {
            return true;
        }

        @Override
        public boolean supportsBooleanValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleValues() {
            return false;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerValues() {
            return false;
        }

        @Override
        public boolean supportsLongValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }
}
