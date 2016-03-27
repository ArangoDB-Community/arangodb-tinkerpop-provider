//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the Blueprints Interface for ArangoDB by triAGENS GmbH Cologne.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.blueprints.client.ArangoDBException;
import com.arangodb.blueprints.client.ArangoDBSimpleEdge;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * The ArangoDB edge class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 */

public class ArangoDBEdge extends ArangoDBElement implements Edge {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

	/**
	 * the _from vertex
	 */
	private Vertex outVertex = null;

	/**
	 * the _to vertex
	 */
	private Vertex inVertex = null;

	private ArangoDBEdge(ArangoDBGraph graph, ArangoDBSimpleEdge edge, Vertex outVertex, Vertex inVertex) {
		this.graph = graph;
		this.document = edge;
		this.outVertex = outVertex;
		this.inVertex = inVertex;
	}

	static ArangoDBEdge create(ArangoDBGraph graph, Object id, Vertex outVertex, Vertex inVertex, String label) {
		String key = (id != null) ? id.toString() : null;

		if (outVertex instanceof ArangoDBVertex && inVertex instanceof ArangoDBVertex) {
			ArangoDBVertex from = (ArangoDBVertex) outVertex;
			ArangoDBVertex to = (ArangoDBVertex) inVertex;

			try {
				ArangoDBSimpleEdge v = graph.getClient().createEdge(graph.getRawGraph(), key, label,
					from.getRawVertex(), to.getRawVertex(), null);
				return build(graph, v, outVertex, inVertex);
			} catch (ArangoDBException e) {
				if (e.errorNumber() == 1210) {
					throw ExceptionFactory.vertexWithIdAlreadyExists(id);
				}

				logger.debug("error while creating an edge", e);

				throw new IllegalArgumentException(e.getMessage());
			}
		}
		throw new IllegalArgumentException("Wrong vertex class.");

	}

	static ArangoDBEdge load(ArangoDBGraph graph, Object id) {
		if (id == null) {
			throw ExceptionFactory.edgeIdCanNotBeNull();
		}

		String key = id.toString();

		try {
			ArangoDBSimpleEdge v = graph.getClient().getEdge(graph.getRawGraph(), key);
			return build(graph, v, null, null);
		} catch (ArangoDBException e) {
			// do nothing
			logger.debug("error while reading an edge", e);

			return null;
		}
	}

	static ArangoDBEdge build(ArangoDBGraph graph, ArangoDBSimpleEdge simpleEdge, Vertex outVertex, Vertex inVertex) {
		return new ArangoDBEdge(graph, simpleEdge, outVertex, inVertex);
	}

	@Override
	public Vertex getVertex(Direction direction) throws IllegalArgumentException {
		if (direction.equals(Direction.IN)) {
			if (inVertex == null) {
				Object id = document.getProperty(ArangoDBSimpleEdge._TO);
				inVertex = graph.getVertex(getKey(id));
			}
			return inVertex;
		} else if (direction.equals(Direction.OUT)) {
			if (outVertex == null) {
				Object id = document.getProperty(ArangoDBSimpleEdge._FROM);
				outVertex = graph.getVertex(getKey(id));
			}
			return outVertex;
		} else {
			throw ExceptionFactory.bothIsNotSupported();
		}
	}

	private String getKey(Object id) {
		if (id == null) {
			return "";
		}

		String[] parts = id.toString().split("/");

		if (parts.length > 1) {
			return parts[1];
		}

		return parts[0];
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
		if (document.isDeleted()) {
			return;
		}
		try {
			graph.getClient().deleteEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
		} catch (ArangoDBException ex) {
			// ignore error
			logger.debug("error while deleting an edge", ex);
		}
	}

	@Override
	public void save() throws ArangoDBException {
		if (document.isDeleted()) {
			return;
		}
		graph.getClient().saveEdge(graph.getRawGraph(), (ArangoDBSimpleEdge) document);
	}

}
