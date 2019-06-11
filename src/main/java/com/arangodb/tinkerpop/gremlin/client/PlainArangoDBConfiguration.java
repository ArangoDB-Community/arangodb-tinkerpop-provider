package com.arangodb.tinkerpop.gremlin.client;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;

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
    public boolean createDatabase() {
        return configuration.getBoolean(PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES, false);
    }
}
