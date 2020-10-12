package net.butfly.albatis.neo4j;

import java.util.Map;

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
	public Neo4jRelation() {
		super();
	}
	
	@SuppressWarnings("deprecation")
	public Neo4jRelation(String table, String key, Map<? extends String, ? extends Object> map, String keyField, Neo4jNode from, Neo4jNode to) {
		super(table, key, map);
		this.table = table;
		this.key = key;
		this.keyField = keyField;
		this.from = from;
		this.to = to;
	}
}
