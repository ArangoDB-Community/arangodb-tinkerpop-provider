package com.arangodb.tinkerpop.gremlin.utils;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;


/**
 * Rexster configuration for ArangoDB graphs. Accepts configuration in
 * rexster.xml as follows: {@code
 * <code>
 *  <graph>
 *     <graph-name>arangodb</graph-name>
 *     <graph-type>ArangoDBConfiguration</graph-type>
 *     <properties>
 *       <graph-name>arangodb-rexster-graph</graph-name>
 *       <vertex-name>arangodb-rexster-graph-vertices</vertex-name>
 *       <edge-name>arangodb-rexster-graph-edges</edge-name>
 *       <host>localhost</host>
 *       <port>8529</port>
 *     </properties>
 *     <extensions>
 *       <allows>
 *         <allow>tp:gremlin</allow>
 *       </allows>
 *     </extensions>
 *   </graph>
 * </code>
 * }
 * 
 * @author Achim Brandt (http://www.triagens.de)
 */

public class ArangoDBConfiguration implements GraphConfiguration {

	/**
	 * Creates a ArangoDBGraph by a Rexster configuration context
	 * 
	 * @param context
	 *            Rexster configuration context
	 * @return Graph a ArangoDBGraph
	 */
	@Override
	public Graph configureGraphInstance(GraphConfigurationContext context) throws GraphConfigurationException {

		final HierarchicalConfiguration graphSectionConfig = (HierarchicalConfiguration) context.getProperties();
		SubnodeConfiguration conf;

		try {
			conf = graphSectionConfig.configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
		} catch (IllegalArgumentException iae) {
			throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: "
					+ Tokens.REXSTER_GRAPH_PROPERTIES, iae);
		}

		try {

			final String host = conf.getString("host");
			final int port = conf.getInt("port", 5000);
			final String graphName = conf.getString("graph-name");
			final String vertexCollectionName = conf.getString("vertex-name");
			final String edgeCollectionName = conf.getString("edge-name");

			return new ArangoDBGraph(host, port, graphName, vertexCollectionName, edgeCollectionName);

		} catch (Exception ex) {
			throw new GraphConfigurationException(ex);
		}
	}

}
