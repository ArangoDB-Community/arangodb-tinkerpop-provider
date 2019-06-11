package com.arangodb.tinkerpop.gremlin.client;

import org.apache.commons.configuration.Configuration;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class PlainArangoDBConfiguration implements ArangoDBConfiguration {


    private final Configuration arangoConfig;

    public PlainArangoDBConfiguration(Configuration configuration) {
        arangoConfig = configuration.subset(PROPERTY_KEY_PREFIX);
    }

    @Override
    public Collection<String> vertexCollections() {
        return arangoConfig.getList(PROPERTY_KEY_VERTICES).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<String> edgeCollections() {
        return arangoConfig.getList(PROPERTY_KEY_EDGES).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<String> relations() {
        return arangoConfig.getList(PROPERTY_KEY_RELATIONS).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<String> graphName() {
        return Optional.of(arangoConfig.getString(PROPERTY_KEY_GRAPH_NAME));
    }

    @Override
    public Optional<String> databaseName() {
        return Optional.of(arangoConfig.getString(PROPERTY_KEY_DB_NAME));
    }

    @Override
    public boolean shouldPrefixCollectionNames() {
        return arangoConfig.getBoolean(PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES, true);
    }
}
