//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.tinkerpop.blueprints.impls.arangodb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.tinkerpop.blueprints.impls.arangodb.client.*;
import com.tinkerpop.blueprints.impls.arangodb.utils.ArangoDBUtil;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 */

public class ArangoDBGraph implements Graph, MetaGraph<ArangoDBSimpleGraph>, KeyIndexableGraph {
	
    private static final Features FEATURES = new Features();

    static {
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isRDFModel = false;
        
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = false;
        
        FEATURES.supportsTransactions = false;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.isWrapper = true;
        FEATURES.supportsIndices = false;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;


        // For more information on supported types, please see:
        // http://code.google.com/p/orient/wiki/Types
        FEATURES.supportsSerializableObjectProperty = true;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true;
        FEATURES.supportsStringProperty = true;
        FEATURES.supportsThreadedTransactions = false;
    }
	
    /**
     *  ArangoDBSimpleGraph
     */
    
	private ArangoDBSimpleGraph rawGraph = null;	

	/**
     *  A ArangoDBSimpleGraphClient to handle the connection to the Database
     */
    
	public ArangoDBSimpleGraphClient client = null;	

	/**
     *  A cache for all created vertices
     */
    
	public HashMap<String, ArangoDBVertex> vertexCache = null;

	/**
     *  A cache for all created edges
     */
    
	public HashMap<String, ArangoDBEdge> edgeCache = null;	
	
	/**
     *  Creates a Graph (simple configuration)
     *  
     *  @param host                     the ArangoDB host name
     *  @param port                     the ArangoDB port
     *  @param name                     the name of the graph
     *  @param verticesCollectionName   the name of the vertices collection
     *  @param edgesCollectionName      the name of the edges collection
     *  
     *  @throws ArangoDBGraphException  if the graph could not be created
     */
    
	public ArangoDBGraph(String host, int port, String name, String verticesCollectionName, String edgesCollectionName) throws ArangoDBGraphException		
	{		
		ArangoDBConfiguration configuration = new ArangoDBConfiguration();
		configuration.setHost(host);
		configuration.setPort(port);
		vertexCache = new HashMap<String, ArangoDBVertex>();
		edgeCache = new HashMap<String, ArangoDBEdge>();
		client = new ArangoDBSimpleGraphClient(configuration);						
		try {
			rawGraph = this.client.createGraph(name, verticesCollectionName, edgesCollectionName);			
		} catch (ArangoDBException e) {			
			throw new ArangoDBGraphException(e.getMessage());			
		}
	}

	/**
     *  Creates a Graph
     *  
     *  @param configuration            an ArangoDB configuration object
     *  @param name                     the name of the graph
     *  @param verticesCollectionName   the name of the vertices collection
     *  @param edgesCollectionName      the name of the edges collection
     *  
     *  @throws ArangoDBGraphException  if the graph could not be created
     */
    
	public ArangoDBGraph(ArangoDBConfiguration configuration, String name, String verticesCollectionName, String edgesCollectionName) throws ArangoDBGraphException		
	{	
		vertexCache = new HashMap<String, ArangoDBVertex>();
		edgeCache = new HashMap<String, ArangoDBEdge>();
		client = new ArangoDBSimpleGraphClient(configuration);						
		try {
			rawGraph = this.client.createGraph(name, verticesCollectionName, edgesCollectionName);			
		} catch (ArangoDBException e) {			
			throw new ArangoDBGraphException(e.getMessage());			
		}
	}
	
	public Features getFeatures() {
		return FEATURES;
	}

	public void shutdown() {
		save();		
		client.shutdown();
	}
	
	public Vertex addVertex(Object id) {
		return ArangoDBVertex.create(this, id);
	}

	public Vertex getVertex(Object id) {
		return ArangoDBVertex.load(this, id);
	}

	public void removeVertex(Vertex vertex) {
		if (vertex.getClass().equals(ArangoDBVertex.class)) {
			ArangoDBVertex e = (ArangoDBVertex) vertex;
			e.delete();
		}
	}

	public Iterable<Vertex> getVertices() {
		ArangoDBQuery q = new ArangoDBQuery(this, null);					
		return q.vertices();
	}

	public Iterable<Vertex> getVertices(String key, Object value) {
		ArangoDBQuery q = new ArangoDBQuery(this, null);			
		q.has(key, value);
		return q.vertices();
	}

	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
		return ArangoDBEdge.create(this, id, outVertex, inVertex, label);
	}

	public Edge getEdge(Object id) {
		return ArangoDBEdge.load(this, id);
	}

	public void removeEdge(Edge edge) {
		if (edge.getClass().equals(ArangoDBEdge.class)) {
			ArangoDBEdge e = (ArangoDBEdge) edge;
			e.delete();
		}
	}

	public Iterable<Edge> getEdges() {
		ArangoDBQuery q = new ArangoDBQuery(this, null);			
		return q.edges();
	}

	public Iterable<Edge> getEdges(String key, Object value) {
		ArangoDBQuery q = new ArangoDBQuery(this, null);			
		q.has(key, value);
		return q.edges();
	}

    public Object getProperty(String key) 
	{
		return this.rawGraph.getProperty(key);
	}
	
	public ArangoDBSimpleGraph getRawGraph() 
	{
		return rawGraph;
	}

	public String toString() {
        return StringFactory.graphString(this, this.rawGraph.toString());
    }

	public <T extends Element> void dropKeyIndex(String key,
			Class<T> elementClass) {

		List<ArangoDBIndex> indices = null;
		try {
	        if (elementClass.isAssignableFrom(Vertex.class)) {
	        	indices = client.getVertexIndices(rawGraph);
	        }
	        else if (elementClass.isAssignableFrom(Edge.class)) {
	        	indices = client.getEdgeIndices(rawGraph);
	        }
		} catch (ArangoDBException e) {			
		}		

		String n = ArangoDBUtil.normalizeKey(key);
		
		if (indices != null) {
	        for (ArangoDBIndex i: indices) {
	        	if (i.getFields().size() == 1) {
	        		String field = i.getFields().elementAt(0);
	        		
	        		if (field.equals(n)) {
	        			try {
							client.deleteIndex(i.getId());
						} catch (ArangoDBException e) {
						}
	        		}	        		
	        	}
	        }
		}
	        
	}

	public <T extends Element> void createKeyIndex(String key,
			Class<T> elementClass) {
		// blueprints 2.2.0
		Vector<String> fields = new Vector<String>();
		
		String n = ArangoDBUtil.normalizeKey(key);
		fields.add(n);

		try {
	        if (elementClass.isAssignableFrom(Vertex.class)) {
	        	client.createVertexIndex(rawGraph, "hash", false, fields);
	        }
	        else if (elementClass.isAssignableFrom(Edge.class)) {
	        	client.createEdgeIndex(rawGraph, "hash", false, fields);
	        }
		} catch (ArangoDBException e) {			
		}				
	}	

	public <T extends Element> void createKeyIndex(String key,
			Class<T> elementClass, Parameter... indexParameters) {		
		// blueprints 2.3.0
		
		String type = "hash";
		boolean unique = false;						
		Vector<String> fields = new Vector<String>();
		
		String n = ArangoDBUtil.normalizeKey(key);
		fields.add(n);
		
		for (Parameter p: indexParameters) {
			if (p.getKey().equals("type")) {
				type = p.getValue().toString();
			}
			if (p.getKey().equals("unique")) {
				unique = (Boolean)p.getValue();
			}
		}		
		
		try {
	        if (elementClass.isAssignableFrom(Vertex.class)) {
	        	client.createVertexIndex(rawGraph, type, unique, fields);
	        }
	        else if (elementClass.isAssignableFrom(Edge.class)) {
	        	client.createEdgeIndex(rawGraph, type, unique, fields);	        	
	        }
		} catch (ArangoDBException e) {			
		}		
	}

	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		HashSet<String> result = new HashSet<String>();
		List<ArangoDBIndex> indices = null;
		try {
	        if (elementClass.isAssignableFrom(Vertex.class)) {
	        	indices = client.getVertexIndices(rawGraph);
	        }
	        else if (elementClass.isAssignableFrom(Edge.class)) {
	        	indices = client.getEdgeIndices(rawGraph);
	        }
	        
	        for (ArangoDBIndex i: indices) {
	        	if (i.getFields().size() == 1) {
	        		String key = i.getFields().elementAt(0);
	        		
	        		// ignore system index
	        		if (key.charAt(0) != '_') {
	        			result.add(ArangoDBUtil.denormalizeKey(key));
	        		}	        		
	        	}
	        }
	        
		} catch (ArangoDBException e) {			
		}		
		
		return result;
	}
	
	/**
	 * Save changed vertices and edges
	 * This functions has to be called in the shutdown function and before a query creates the cursor.
	 */
	
	public void save () {
		Set<String> keys = vertexCache.keySet();		
		for (String key: keys) {
			ArangoDBVertex v =  vertexCache.get(key);
			if (v != null) {
				v.save();
			}			
		}
		
		keys = edgeCache.keySet();		
		for (String key: keys) {
			ArangoDBEdge v =  edgeCache.get(key);
			if (v != null) {
				v.save();
			}			
		}		
		
	}

}