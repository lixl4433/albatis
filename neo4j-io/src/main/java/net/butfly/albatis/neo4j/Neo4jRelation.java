package net.butfly.albatis.neo4j;

import net.butfly.albatis.io.Rmap;

public class Neo4jRelation extends Rmap{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected Neo4jNode from;
	protected Neo4jNode to;
	protected Object key;
	protected String keyField;
	protected String table;
}
