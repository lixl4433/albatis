package net.butfly.albatis.neo4j;

import java.util.Map;

import net.butfly.albatis.io.Rmap;

public class Neo4jNode extends Rmap{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected Object key;
	protected String keyField;
	protected String table;
	
	public Neo4jNode() {
		super();
	}
	
	@SuppressWarnings("deprecation")
	public Neo4jNode(String key, String keyField, String table, Map<? extends String, ? extends Object> map) {
		super(table, key, map);
		this.key = key;
		this.keyField = keyField;
		this.table = table;
	}
	
}
