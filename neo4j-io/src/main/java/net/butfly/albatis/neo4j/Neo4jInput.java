package net.butfly.albatis.neo4j;

import java.io.IOException;

import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class Neo4jInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final Neo4jConnection conn;
	
	protected Neo4jInput(String name, Neo4jConnection conn) throws IOException {
		super(name);
		this.conn = conn;
		closing(this::closeNeo4j);
	}

	@Override
	public Rmap dequeue() {
		return null;
	}
	
	
	private void closeNeo4j() {
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
