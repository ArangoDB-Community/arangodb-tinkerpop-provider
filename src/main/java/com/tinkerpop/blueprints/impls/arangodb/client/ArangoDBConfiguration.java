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

/**
 * The arangodb configuration class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

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
	 * the ArangoDB database cursor batch size, also used as BatchGraph batch
	 * size
	 */

	private Integer batchSize;

	/**
	 * whether or not a stale connection check should be performed on each HTTP
	 * request. turning this on will hurt performance
	 */

	private boolean staleConnectionCheck;

	/**
	 * whether or not keep-alive should be enabled on socket level
	 */
	private boolean socketKeepAlive;

	private String db;

	/**
	 * Creates a default configuration.
	 * 
	 * Connects to arangodb database on localhost:8529.
	 */

	public ArangoDBConfiguration() {
		this("127.0.0.1", 8529);
	}

	/**
	 * Creates a configuration
	 * 
	 * @param host
	 *            Host name of the arangodb
	 * @param port
	 *            Port number of arangodb
	 */
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

	/**
	 * Returns the base URL to connect arangodb
	 * 
	 * @return the base URL
	 */
	public String getBaseUrl() {
		if (https) {
			return "https://" + this.host + ":" + this.port;
		}

		return "http://" + this.host + ":" + this.port;
	}

	/**
	 * Returns the arangodb port
	 * 
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Returns the arangodb host name
	 * 
	 * @return the host name
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Returns the connection timeout
	 * 
	 * @return the connection timeout
	 */
	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	/**
	 * Returns the socket timeout
	 * 
	 * @return the socket timeout
	 */
	public int getSocketTimeout() {
		return socketTimeout;
	}

	/**
	 * Returns the keep alive timeout
	 * 
	 * @return the keep alive timeout
	 */
	public int getKeepAliveTimeout() {
		return keepAliveTimeout;
	}

	/**
	 * Returns the maximum number of connections
	 * 
	 * @return the maximum number of connections
	 */
	public int getMaxTotalConnection() {
		return maxTotalConnection;
	}

	/**
	 * Returns the maximum number of requests per connection
	 * 
	 * @return the maximum number of requests per connection
	 */
	public int getMaxPerConnection() {
		return maxPerConnection;
	}

	/**
	 * Returns the batch size for queries
	 * 
	 * @return the batch size
	 */
	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * Returns true for stale connection checks (slow)
	 * 
	 * @return true for stale connection checks
	 */
	public boolean getStaleConnectionCheck() {
		return staleConnectionCheck;
	}

	/**
	 * Returns true for socket keep alive
	 * 
	 * @return true for socket keep alive
	 */
	public boolean getSocketKeepAlive() {
		return socketKeepAlive;
	}

	/**
	 * Sets the port number
	 * 
	 * @param port
	 *            the port number of the arangodb server
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Sets the host name
	 * 
	 * @param host
	 *            the host name of the arangodb server
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Sets the connection timeout (in milliseconds)
	 * 
	 * @param connectionTimeout
	 *            the connection timeout
	 */
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	/**
	 * Sets the socket timeout (in milliseconds)
	 * 
	 * @param socketTimeout
	 *            the socket time out
	 */
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	/**
	 * Sets the keep-alive timeout (in milliseconds)
	 * 
	 * @param keepAliveTimeout
	 *            the keep-alive timeout
	 */
	public void setKeepAliveTimeout(int keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
	}

	/**
	 * Sets the maximum number of connections
	 * 
	 * @param maxTotalConnection
	 *            the maximum number of connections
	 */
	public void setMaxTotalConnection(int maxTotalConnection) {
		this.maxTotalConnection = maxTotalConnection;
	}

	/**
	 * Sets the maximum number of request per connection
	 * 
	 * @param maxPerConnection
	 *            the maximum number of request per connection
	 */
	public void setMaxPerConnection(int maxPerConnection) {
		this.maxPerConnection = maxPerConnection;
	}

	/**
	 * Sets the batch size for cursor operations and for BatchGraph chunks
	 * 
	 * @param batchSize
	 *            the batch size
	 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * Enables or disables the stale connection checking
	 * 
	 * @param staleConnectionCheck
	 *            true, for stale connection checking
	 */
	public void setStaleConnectionCheck(boolean staleConnectionCheck) {
		this.staleConnectionCheck = staleConnectionCheck;
	}

	/**
	 * Enables or disables keep-alive on socket level
	 * 
	 * @param socketKeepAlive
	 *            true, for keep-alive on socket level
	 */
	public void setSocketKeepAlive(boolean socketKeepAlive) {
		this.socketKeepAlive = socketKeepAlive;
	}

	/**
	 * Returns a connection manager
	 * 
	 * @return a connection manager
	 */
	public ClientConnectionManager createClientConnectionManager() {
		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(new Scheme("http", port, PlainSocketFactory.getSocketFactory()));
		schemeRegistry.register(new Scheme("https", port, SSLSocketFactory.getSocketFactory()));

		PoolingClientConnectionManager cm = new PoolingClientConnectionManager(schemeRegistry);
		cm.setMaxTotal(maxTotalConnection);
		cm.setDefaultMaxPerRoute(maxPerConnection);

		boolean cleanupIdleConnections = true;
		if (cleanupIdleConnections) {
			IdleConnectionMonitor.monitor(cm);
		}

		return cm;
	}

	/**
	 * Sets the name of the arangodb database (null for the system database)
	 * 
	 * @param db
	 *            name of the arangodb database
	 */
	public void setDb(String db) {
		this.db = db;
	}

	/**
	 * Returns the database prefix for requests
	 * 
	 * @return the database prefix
	 */
	public String requestDbPrefix() {
		String dbPrefix = EMPTY;
		if (isNotBlank(db)) {
			dbPrefix = "_db/" + db + "/";
		}
		return dbPrefix;
	}
}
