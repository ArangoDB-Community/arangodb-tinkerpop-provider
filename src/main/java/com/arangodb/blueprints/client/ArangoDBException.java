//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of a simple graph client for the ArangoDB.
//
// Copyright triAGENS GmbH Cologne.
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.blueprints.client;

import com.arangodb.ArangoException;

/**
 * The arangodb exception class
 * 
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Jan Steemann (http://www.triagens.de)
 */

public class ArangoDBException extends Exception {

	private static final long serialVersionUID = -163216237496410756L;

	private final Integer errorNum;

	/**
	 * Creates an exception
	 * 
	 * @param message
	 *            the error message
	 */
	public ArangoDBException(String message) {
		super(message);
		this.errorNum = 0;
	}

	/**
	 * Creates an exception with error number
	 * 
	 * @param message
	 *            the error message
	 * @param errorNum
	 *            the error number
	 */
	public ArangoDBException(String message, Integer errorNum) {
		super(message);
		this.errorNum = errorNum;
	}

	/**
	 * Creates an exception by an ArangoException
	 * 
	 * @param ex
	 *            the ArangoException
	 */
	public ArangoDBException(ArangoException ex) {
		super(ex.getErrorMessage(), ex);
		this.errorNum = ex.getErrorNumber();
	}

	/**
	 * Returns the error number of an exception
	 * 
	 * @return the error number
	 */
	public Integer errorNumber() {
		return this.errorNum;
	}

}
