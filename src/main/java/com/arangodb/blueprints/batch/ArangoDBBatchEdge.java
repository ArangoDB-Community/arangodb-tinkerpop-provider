//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints.batch;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.arangodb.blueprints.client.ArangoDBBaseDocument;
import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBSimpleEdge;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * The arangodb batch edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBBatchEdge extends ArangoDBBatchElement implements Edge {

	/**
	 * the logger
	 */
	private static final Logger logger = Logger.getLogger(ArangoDBBatchEdge.class);

	/**
	 * the _from vertex
	 */
	private Vertex outVertex = null;

	/**
	 * the _to vertex
	 */
	private Vertex inVertex = null;

	private ArangoDBBatchEdge(ArangoDBBatchGraph graph, ArangoDBSimpleEdge edge, Vertex outVertex, Vertex inVertex) {
		this.graph = graph;
		this.document = edge;
		this.outVertex = outVertex;
		this.inVertex = inVertex;
	}

	static ArangoDBBatchEdge create(
		ArangoDBBatchGraph graph,
		Object id,
		Vertex outVertex,
		Vertex inVertex,
		String label) {
		String key = (id != null) ? id.toString() : null;

		if (key == null) {
			key = graph.getNewId().toString();
		}

		Map<String, Object> properties = new HashMap<String, Object>();

		if (outVertex instanceof ArangoDBBatchVertex && inVertex instanceof ArangoDBBatchVertex) {
			ArangoDBBatchVertex from = (ArangoDBBatchVertex) outVertex;
			ArangoDBBatchVertex to = (ArangoDBBatchVertex) inVertex;

			properties.put(ArangoDBBaseDocument._REV, "");
			properties.put(ArangoDBBaseDocument._ID, "");
			properties.put(ArangoDBBaseDocument._KEY, key);
			if (label != null) {
				properties.put(StringFactory.LABEL, label);
			}

			properties.put(ArangoDBSimpleEdge._FROM,
				graph.getRawGraph().getVertexCollection() + "/" + from.getRawVertex().getDocumentKey());
			properties.put(ArangoDBSimpleEdge._TO,
				graph.getRawGraph().getVertexCollection() + "/" + to.getRawVertex().getDocumentKey());

			try {
				ArangoDBSimpleEdge v = new ArangoDBSimpleEdge(properties);
				return build(graph, v, outVertex, inVertex);
			} catch (ArangoDBException e) {
				if (e.errorNumber() == 1210) {
					throw ExceptionFactory.vertexWithIdAlreadyExists(id);
				}
				logger.warn("could not create batch edge", e);
				throw new IllegalArgumentException(e.getMessage());
			}
		}
		throw new IllegalArgumentException("Wrong vertex class.");

	}

	static ArangoDBBatchEdge load(ArangoDBBatchGraph graph, Object id) {
		if (id == null) {
			throw ExceptionFactory.edgeIdCanNotBeNull();
		}

		String key = id.toString();

		ArangoDBBatchEdge edge = graph.edgeCache.get(key);
		if (edge != null) {
			return edge;
		}

		try {
			ArangoDBSimpleEdge v = graph.client.getEdge(graph.getRawGraph(), key);
			return build(graph, v, null, null);
		} catch (ArangoDBException e) {
			// do nothing
			logger.warn("could not load batch edge", e);
			return null;
		}
	}

	static ArangoDBBatchEdge build(
		ArangoDBBatchGraph graph,
		ArangoDBSimpleEdge simpleEdge,
		Vertex outVertex,
		Vertex inVertex) throws ArangoDBException {
		String id = simpleEdge.getDocumentKey();

		ArangoDBBatchEdge vert = graph.edgeCache.get(id);
		if (vert != null) {
			vert.setDocument(simpleEdge);
			return vert;
		}

		ArangoDBBatchEdge newEdge = new ArangoDBBatchEdge(graph, simpleEdge, outVertex, inVertex);
		graph.edgeCache.put(newEdge.getRaw().getDocumentKey(), newEdge);
		graph.addCreatedEdge(newEdge);
		return newEdge;
	}

	@Override
	public Vertex getVertex(Direction direction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getLabel() {
		Object l = document.getProperty(StringFactory.LABEL);
		if (l != null) {
			return l.toString();
		}

		return null;
	}

	/**
	 * Returns the arangodb raw edge
	 * 
	 * @return a ArangoDBSimpleEdge
	 */
	public ArangoDBSimpleEdge getRawEdge() {
		return (ArangoDBSimpleEdge) document;
	}

	@Override
	public String toString() {
		return StringFactory.edgeString(this);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * not supported in batch mode
	 */
	@Override
	public void save() {
		throw new UnsupportedOperationException();
	}

	public Vertex getOutVertex() {
		return outVertex;
	}

	public Vertex getInVertex() {
		return inVertex;
	}

}
