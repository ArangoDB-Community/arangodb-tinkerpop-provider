//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop OLTP Provider API for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

import com.arangodb.tinkerpop.gremlin.client.*;
import com.arangodb.tinkerpop.gremlin.structure.*;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;

/**
 * The Class ArangoDBGremlinPlugin.
 * @author Horacio Hoyos Rodriguez (https://www.york.ac.uk)
 */
public class ArangoDBGremlinPlugin extends AbstractGremlinPlugin {

	/** The Constant NAME. */
	private static final String NAME = "tinkerpop.arangodb";

	/** The Constant IMPORTS. */
	private static final ImportCustomizer IMPORTS;

    static {
        try {
            IMPORTS = DefaultImportCustomizer.build().addClassImports(
                    ArangoDBBaseDocument.class,
                    ArangoDBBaseEdge.class,
                    PlainClient.class,
                    ArangoDBGraphException.class,
                    ArangoDBIterator.class,
                    ArangoDBPropertyFilter.class,
                    ArangoDBPropertyIterator.class,
                    ArangoDBQueryBuilder.class,
                    ArangoDBEdge.class,
            		ArangoDBEdgeProperty.class,
            		ArangoDBElementProperty.class,
                    ArangoDBGraph.class,
                    ArangoDBGraphVariables.class,
                    ArangoDBPropertyProperty.class,
                    ArangoDBVertex.class,
                    ArangoDBVertexProperty.class,
                    ArangoDBUtil.class
            		)
            	.create();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /** The Constant INSTANCE. */
    private static final ArangoDBGremlinPlugin INSTANCE = new ArangoDBGremlinPlugin();

    /**
     * Instantiates a new Arango DB gremlin plugin.
     */
    public ArangoDBGremlinPlugin() {
        super(NAME, IMPORTS);
    }

    /**
     * Instance.
     *
     * @return the arango DB gremlin plugin
     */
    public static ArangoDBGremlinPlugin instance() {
        return INSTANCE;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

}
