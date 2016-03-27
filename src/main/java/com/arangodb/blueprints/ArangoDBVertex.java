//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints;

import org.apache.log4j.Logger;

import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBSimpleVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * The ArangoDB vertex class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBVertex extends ArangoDBElement implements Vertex {

	/**
	 * the logger
	 */
	private static final Logger logger = Logger.getLogger(ArangoDBVertex.class);

	private ArangoDBVertex(ArangoDBGraph graph, ArangoDBSimpleVertex vertex) {
		this.graph = graph;
		this.document = vertex;
	}

	/**
	 * Creates a vertex
	 * 
	 * @param graph
	 *            a ArangoDBGraph
	 * @param id
	 *            the id (key) of the vertex (can be null)
	 * 
	 * @throws IllegalArgumentException
	 */

	static ArangoDBVertex create(ArangoDBGraph graph, Object id) {
		String key = (id != null) ? id.toString() : null;

		try {
			ArangoDBSimpleVertex v = graph.getClient().createVertex(graph.getRawGraph(), key, null);
			return build(graph, v);
		} catch (ArangoDBException e) {
			if (e.errorNumber() == 1210) {
				throw ExceptionFactory.vertexWithIdAlreadyExists(id);
			}
			logger.debug("could not create vertex", e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	/**
	 * Creates a vertex by loading it
	 * 
	 * @param graph
	 *            a ArangoDBGraph
	 * @param id
	 *            the id (key) of the vertex (can be null)
	 * 
	 * @throws IllegalArgumentException
	 */

	static ArangoDBVertex load(ArangoDBGraph graph, Object id) {
		if (id == null) {
			throw ExceptionFactory.vertexIdCanNotBeNull();
		}

		String key = id.toString();

		try {
			ArangoDBSimpleVertex v = graph.getClient().getVertex(graph.getRawGraph(), key);
			return build(graph, v);
		} catch (ArangoDBException e) {
			// nothing found
			logger.debug("graph not found", e);
			return null;
		}
	}

	static ArangoDBVertex build(ArangoDBGraph graph, ArangoDBSimpleVertex simpleVertex) {
		return new ArangoDBVertex(graph, simpleVertex);
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		if (document.isDeleted()) {
			return null;
		}
		ArangoDBVertexQuery q = new ArangoDBVertexQuery(graph, this);
		q.direction(direction);
		q.labels(labels);

		return q.edges();
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		if (document.isDeleted()) {
			return null;
		}
		ArangoDBVertexQuery q = new ArangoDBVertexQuery(graph, this);
		q.direction(direction);
		q.labels(labels);

		return q.vertices();
	}

	@Override
	public VertexQuery query() {
		if (document.isDeleted()) {
			return null;
		}
		return new ArangoDBVertexQuery(graph, this);
	}

	/**
	 * Returns the ArangoDBSimpleVertex
	 * 
	 * @return a ArangoDBSimpleVertex
	 */

	public ArangoDBSimpleVertex getRawVertex() {
		return (ArangoDBSimpleVertex) document;
	}

	@Override
	public String toString() {
		return StringFactory.vertexString(this);
	}

	@Override
	public void remove() {
		if (document.isDeleted()) {
			return;
		}

		try {
			graph.getClient().deleteVertex(graph.getRawGraph(), (ArangoDBSimpleVertex) document);
		} catch (ArangoDBException ex) {
			// ignore error
			logger.debug("could not delete vertex", ex);
		}
	}

	@Override
	public void save() throws ArangoDBException {
		if (document.isDeleted()) {
			return;
		}
		graph.getClient().saveVertex(graph.getRawGraph(), (ArangoDBSimpleVertex) document);
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex) {

		if (label == null) {
			throw ExceptionFactory.edgeLabelCanNotBeNull();
		}

		return ArangoDBEdge.create(this.graph, null, this, inVertex, label);
	}

}
