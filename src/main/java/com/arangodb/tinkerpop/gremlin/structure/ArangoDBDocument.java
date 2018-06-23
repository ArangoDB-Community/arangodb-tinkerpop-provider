package com.arangodb.tinkerpop.gremlin.structure;

/**
 * An interface to access ArangoDB document information
 * @author horacio
 *
 */
public interface ArangoDBDocument {
	
	String _id();
	
	String _rev(); 
	
	String _key();
	
    void _id(String  id);
	
	void _rev(String rev); 
	
	void _key(String  key);
	
	String collection();

	void collection(String collection);

}
