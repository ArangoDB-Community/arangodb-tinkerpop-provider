package com.tinkerpop.blueprints.impls.arangodb.utils;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.arangodb.ArangoDBGraph;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

/**
* Rexster configuration for ArangoDB graphs. Accepts configuration in rexster.xml as follows:
*
* <code>
* <graph>
* <graph-name>arangodb-rexter-graph</graph-name>
* <graph-type>com.tinkerpop.blueprints.impls.arangodb.util.ArangoDBGraphConfiguration</graph-type>
* <properties>
* <vertex-name>arangodb-rexter-graph-vertices</vertex-name>
* <edge-name>arangodb-rexter-graph-edges</edge-name>
* <host>localhost</host>
* <port>8529</port>
* </properties>
* </graph>
* </code>
*
*
* @author Achim Brandt (http://www.triagens.de)
*/

public class ArangoDBConfiguration implements GraphConfiguration {

	/**
	 * Creates a ArangoDBGraph by a Rexster configuration
	 * 
	 * @param   configuration        Rexster configuration
	 * @return  Graph                a ArangoDBGraph
	 */
		
    public Graph configureGraphInstance(Configuration configuration) throws GraphConfigurationException {
    	
        final HierarchicalConfiguration graphSectionConfig = (HierarchicalConfiguration) configuration;
        SubnodeConfiguration conf;

        try {
        	conf = graphSectionConfig.configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
        } catch (IllegalArgumentException iae) {
            throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_PROPERTIES);
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
