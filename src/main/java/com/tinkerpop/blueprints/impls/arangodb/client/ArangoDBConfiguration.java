//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

public class ArangoDBConfiguration {

    /**
     * the ArangoDB Port 
     */

	private int port;

	/**
     * the ArangoDB Hostname 
     */

	private String host;
	
	/**
     * use https or not 
     */

	private boolean https;

	/**
     * connection timeout 
     */

	private int connectionTimeout;

	/**
     * request timeout 
     */

	private int timeout;
	
	/**
     * number of connections 
     */

	private int maxTotalConnection;

	/**
     * number of requests per connection 
     */

	private int maxPerConnection;
			
    /**
     * the ArangoDB database cursor batch size 
     */

	private Integer batchSize;
	
    /**
     * the default constructor 
     */

	public ArangoDBConfiguration() {
		this("127.0.0.1", 8529);
	}
	public ArangoDBConfiguration(String host, int port) {
		this.host = host;
		this.port = port;
		this.maxPerConnection = 20;
		this.maxTotalConnection = 20;
		this.https = false;
		this.batchSize = 100;
	}
	
	public String getBaseUrl() {
		if (https) {
			return "https://" + this.host + ":" + this.port;			
		}
				
		return "http://" + this.host + ":" + this.port;
	}
	

	public int getPort() {
		return port;
	}

	public String getHost() {
		return host;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public int getTimeout() {
		return timeout;
	}

	public int getMaxTotalConnection() {
		return maxTotalConnection;
	}

	public int getMaxPerConnection() {
		return maxPerConnection;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setMaxTotalConnection(int maxTotalConnection) {
		this.maxTotalConnection = maxTotalConnection;
	}

	public void setMaxPerConnection(int maxPerConnection) {
		this.maxPerConnection = maxPerConnection;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public ClientConnectionManager createClientConnectionManager() {
		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(
		         new Scheme("http", port, PlainSocketFactory.getSocketFactory()));
		schemeRegistry.register(
		         new Scheme("https", port, SSLSocketFactory.getSocketFactory()));
		
		PoolingClientConnectionManager cm = new PoolingClientConnectionManager(schemeRegistry);
		cm.setDefaultMaxPerRoute(maxPerConnection);
		cm.setMaxTotal(maxTotalConnection);        
        
        return cm;
	}	
}
