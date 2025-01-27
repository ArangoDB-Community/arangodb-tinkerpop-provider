package com.arangodb.tinkerpop.gremlin.client;

import com.arangodb.Compression;
import com.arangodb.Protocol;
import com.arangodb.config.ArangoConfigProperties;
import com.arangodb.config.HostDescription;
import com.arangodb.entity.LoadBalancingStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public final class ArangoConfigMap implements ArangoConfigProperties {
    private static final String PREFIX = "arangodb.";
    private final Properties properties;

    public ArangoConfigMap(final Properties properties) {
        this.properties = properties;
    }

    private String getProperty(String key) {
        return properties.getProperty(PREFIX + key);
    }

    @Override
    public Optional<List<HostDescription>> getHosts() {
        return Optional.ofNullable(getProperty("hosts"))
                .map(s -> {
                    List<HostDescription> hostDescriptions = new ArrayList<>();
                    String[] hosts = s.split(",");
                    for (String host : hosts) {
                        hostDescriptions.add(HostDescription.parse(host));
                    }
                    return hostDescriptions;
                });
    }

    @Override
    public Optional<Protocol> getProtocol() {
        return Optional.ofNullable(getProperty("protocol")).map(Protocol::valueOf);
    }

    @Override
    public Optional<String> getUser() {
        return Optional.ofNullable(getProperty("user"));
    }

    @Override
    public Optional<String> getPassword() {
        return Optional.ofNullable(getProperty("password"));
    }

    @Override
    public Optional<String> getJwt() {
        return Optional.ofNullable(getProperty("jwt"));
    }

    @Override
    public Optional<Integer> getTimeout() {
        return Optional.ofNullable(getProperty("timeout")).map(Integer::valueOf);
    }

    @Override
    public Optional<Boolean> getUseSsl() {
        return Optional.ofNullable(getProperty("useSsl")).map(Boolean::valueOf);
    }

    @Override
    public Optional<Boolean> getVerifyHost() {
        return Optional.ofNullable(getProperty("verifyHost")).map(Boolean::valueOf);
    }

    @Override
    public Optional<Integer> getChunkSize() {
        return Optional.ofNullable(getProperty("chunkSize")).map(Integer::valueOf);
    }

    @Override
    public Optional<Integer> getMaxConnections() {
        return Optional.ofNullable(getProperty("maxConnections")).map(Integer::valueOf);
    }

    @Override
    public Optional<Long> getConnectionTtl() {
        return Optional.ofNullable(getProperty("connectionTtl")).map(Long::valueOf);
    }

    @Override
    public Optional<Integer> getKeepAliveInterval() {
        return Optional.ofNullable(getProperty("keepAliveInterval")).map(Integer::valueOf);
    }

    @Override
    public Optional<Boolean> getAcquireHostList() {
        return Optional.ofNullable(getProperty("acquireHostList")).map(Boolean::valueOf);
    }

    @Override
    public Optional<Integer> getAcquireHostListInterval() {
        return Optional.ofNullable(getProperty("acquireHostListInterval")).map(Integer::valueOf);
    }

    @Override
    public Optional<LoadBalancingStrategy> getLoadBalancingStrategy() {
        return Optional.ofNullable(getProperty("loadBalancingStrategy")).map(LoadBalancingStrategy::valueOf);
    }

    @Override
    public Optional<Integer> getResponseQueueTimeSamples() {
        return Optional.ofNullable(getProperty("responseQueueTimeSamples")).map(Integer::valueOf);
    }

    @Override
    public Optional<Compression> getCompression() {
        return Optional.ofNullable(getProperty("compression")).map(Compression::valueOf);
    }

    @Override
    public Optional<Integer> getCompressionThreshold() {
        return Optional.ofNullable(getProperty("compressionThreshold")).map(Integer::valueOf);
    }

    @Override
    public Optional<Integer> getCompressionLevel() {
        return Optional.ofNullable(getProperty("compressionLevel")).map(Integer::valueOf);
    }

    @Override
    public Optional<String> getSerdeProviderClass() {
        return Optional.ofNullable(getProperty("serdeProviderClass"));
    }

}
