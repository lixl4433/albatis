package net.butfly.albatis.arangodb;

import static net.butfly.albatis.io.IOProps.propI;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput2 extends OutputBase<AqlEdge> {
	private static final long serialVersionUID = -2376114954650957250L;
	private final ArangoDBConnection conn;
	private static final int MAX_RETRY = propI(ArangoOutput2.class, "retry.max", 50); // 3M

	protected ArangoOutput2(String name, ArangoDBConnection conn) {
		super(name);
		this.conn = conn;
	}

	@Override
	public void close() {
		conn.close();
	}

	@Override
	protected void enqsafe(Sdream<AqlEdge> edges) {
		edges.list().forEach(aqlEdge -> {
			ArangoDBNode from = aqlEdge.getFrom();
			ArangoDBNode to = aqlEdge.getTo();
			ArangoDBEdge edge = aqlEdge.getEdge();
			conn.upsertNode(from);
			conn.upsertNode(to);
			conn.upsertEdge(edge);
		});
	}
}
