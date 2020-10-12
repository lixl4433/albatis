package net.butfly.albatis.neo4j;

import java.io.IOException;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;

public class Neo4jOutput extends OutputBase<Neo4jRelation> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final Neo4jConnection conn;

	protected Neo4jOutput(String name, Neo4jConnection conn) throws IOException {
		super(name);
		this.conn = conn;
		closing(() -> {
			try {
				this.conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}
	@Override
	protected void enqsafe(Sdream<Neo4jRelation> rs) {
		rs.each(r -> {
			Neo4jUtils neo4jUtils = new Neo4jUtils(conn);
			neo4jUtils.upsertNode(r.from);
			neo4jUtils.upsertNode(r.to);
			neo4jUtils.upsertRelation(r);
		});
	}

}
