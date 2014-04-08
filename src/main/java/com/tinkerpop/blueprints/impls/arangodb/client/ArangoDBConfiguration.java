//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb.client;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

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
     * socket timeout (in milliseconds)
     */

	private int socketTimeout;

	/**
     * keep-alive timeout (in milliseconds)
     */

	private int keepAliveTimeout;

	/**
     * number of connections
     */

	private int maxTotalConnection;

	/**
     * number of requests per connection
     */

	private int maxPerConnection;

    /**
     * the ArangoDB database cursor batch size, also used as BatchGraph batch size
     */

	private Integer batchSize;

    /**
     * whether or not a stale connection check should be performed
     * on each HTTP request. turning this on will hurt performance
     */

	private boolean staleConnectionCheck;

    /**
     * whether or not keep-alive should be enabled on socket level
     */
	private boolean socketKeepAlive;

	private String db;

    /**
     * the default constructor
     */

	public ArangoDBConfiguration() {
		this("127.0.0.1", 8529);
	}

	public ArangoDBConfiguration(String host, int port) {
		this.host = host;
		this.port = port;
		this.https = false;
                this.connectionTimeout = 3000;
                this.socketTimeout = 30000;
                this.keepAliveTimeout = 90000;
		this.maxPerConnection = 20;
		this.maxTotalConnection = 20;
		this.batchSize = 100;
                this.staleConnectionCheck = false;
                this.socketKeepAlive = false;
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

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public int getKeepAliveTimeout() {
		return keepAliveTimeout;
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

	public boolean getStaleConnectionCheck() {
		return staleConnectionCheck;
	}

	public boolean getSocketKeepAlive() {
		return socketKeepAlive;
	}

	/**
	 * Sets the port number
	 *
	 * @param port
         */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Sets the host name
	 *
	 * @param host
         */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Sets the connection timeout (in milliseconds)
	 *
	 * @param connectionTimeout
         */
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	/**
	 * Sets the socket timeout (in milliseconds)
	 *
	 * @param socketTimeout
         */
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	/**
	 * Sets the keep-alive timeout (in milliseconds)
	 *
	 * @param keepAliveTimeout
         */
	public void setKeepAliveTimeout(int keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
	}

	public void setMaxTotalConnection(int maxTotalConnection) {
		this.maxTotalConnection = maxTotalConnection;
	}

	public void setMaxPerConnection(int maxPerConnection) {
		this.maxPerConnection = maxPerConnection;
	}

	/**
	 * Sets the batch size for cursor operations and for BatchGraph chunks
	 *
	 * @param batchSize
         */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * Enables or disables the stale connection checking
	 *
	 * @param staleConnectionCheck
         */
        public void setStaleConnectionCheck(boolean staleConnectionCheck) {
		this.staleConnectionCheck = staleConnectionCheck;
	}

	/**
	 * Enables or disables keep-alive on socket level
	 *
	 * @param socketKeepAlive
         */
        public void setSocketKeepAlive(boolean socketKeepAlive) {
		this.socketKeepAlive = socketKeepAlive;
	}

	public ClientConnectionManager createClientConnectionManager() {
		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(
		         new Scheme("http", port, PlainSocketFactory.getSocketFactory()));
		schemeRegistry.register(
		         new Scheme("https", port, SSLSocketFactory.getSocketFactory()));

		PoolingClientConnectionManager cm = new PoolingClientConnectionManager(schemeRegistry);
		cm.setMaxTotal(maxTotalConnection);
		cm.setDefaultMaxPerRoute(maxPerConnection);

                boolean cleanupIdleConnections = true;
                if (cleanupIdleConnections) {
		  IdleConnectionMonitor.monitor(cm);
		}

                return cm;
	}

	public void setDb(String db){
		this.db = db;
	}

	public String requestDbPrefix(){
		String dbPrefix = EMPTY;
		if (isNotBlank(db)) {
			dbPrefix = "_db/" + db + "/";
		}
		return dbPrefix;
	}
}
