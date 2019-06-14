package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.ArangoDB;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBEdge;
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBVertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public class PlainArangoDBConfiguration implements ArangoDBConfiguration {


    private final Configuration configuration;

    public PlainArangoDBConfiguration(Configuration configuration) {
        this.configuration = configuration.subset(PROPERTY_KEY_PREFIX);
    }

    @Override
    public Collection<String> vertexCollections() {
        return configuration.getList(PROPERTY_KEY_VERTICES).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<String> edgeCollections() {
        return configuration.getList(PROPERTY_KEY_EDGES).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<String> relations() {
        return configuration.getList(PROPERTY_KEY_RELATIONS).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<String> graphName() {
        return Optional.of(configuration.getString(PROPERTY_KEY_GRAPH_NAME));
    }

    @Override
    public Optional<String> databaseName() {
        return Optional.of(configuration.getString(PROPERTY_KEY_DB_NAME));
    }

    @Override
    public boolean shouldPrefixCollectionNames() {
        return configuration.getBoolean(PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES, true);
    }

    @Override
    public Properties transformToProperties() {
        return ConfigurationConverter.getProperties(configuration);
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public ArangoDB buildDriver() {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            transformToProperties().store(os, null);
            ByteArrayInputStream targetStream = new ByteArrayInputStream(os.toByteArray());
            ArangoDBVertexVPack vertexVpack = new ArangoDBVertexVPack();
            ArangoDBEdgeVPack edgeVPack = new ArangoDBEdgeVPack();
            return new ArangoDB.Builder().loadProperties(targetStream)
                    .registerDeserializer(ArangoDBVertex.class, vertexVpack)
                    .registerSerializer(ArangoDBVertex.class, vertexVpack)
                    .registerDeserializer(ArangoDBEdge.class, edgeVPack)
                    .registerSerializer(ArangoDBEdge.class, edgeVPack)
                    .build();
        } catch (IOException e) {
           throw new IllegalStateException("Error writing to the output stream when creating drivier.", e);
        }
    }

    @Override
    public boolean createDatabase() {
        return configuration.getBoolean(PROPERTY_KEY_DB_CREATE, false);
    }
}
