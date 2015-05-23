package com.tinkerpop.blueprints.impls.arangodb.client;

import java.util.ArrayList;
import java.util.List;

import com.arangodb.entity.IndexEntity;
import com.arangodb.entity.IndexType;

/**
 * The arangodb index class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBIndex {

	/**
	 * id of the index
	 */

	private String id;

	/**
	 * the index type
	 */

	private IndexType type;

	/**
	 * is the index unique
	 */

	private boolean unique;

	/**
	 * the fields of the index
	 */

	private List<String> fields = new ArrayList<String>();

	/**
	 * Creates an index by a given JSON document
	 * 
	 * @param indexEntity
	 *            The ArangoDB index entity
	 * 
	 * @throws ArangoDBException
	 *             if an error occurs
	 */
	public ArangoDBIndex(IndexEntity indexEntity) throws ArangoDBException {
		if (indexEntity == null) {
			throw new ArangoDBException("No index data found.");
		}

		this.id = indexEntity.getId();
		this.type = indexEntity.getType();
		this.unique = indexEntity.isUnique();
		this.fields = indexEntity.getFields();
	}

	/**
	 * Returns the index identifier
	 * 
	 * @return the index identifier
	 */
	public String getId() {
		return id;
	}

	/**
	 * Returns the index type
	 * 
	 * @return the index type
	 */
	public IndexType getType() {
		return type;
	}

	/**
	 * Returns true if the index is unique
	 * 
	 * @return true, if the index is unique
	 */
	public boolean isUnique() {
		return unique;
	}

	/**
	 * Returns the list of fields
	 * 
	 * @return the list of fields
	 */
	public List<String> getFields() {
		return fields;
	}

}
