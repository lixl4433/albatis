package net.butfly.albatis.arangodb;

import static net.butfly.albatis.io.IOProps.propI;

import java.io.IOException;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<AqlEdge> {
	private static final long serialVersionUID = -2376114954650957250L;
	private final ArangoDBConnection conn;
	private static final int MAX_RETRY = propI(ArangoOutput.class, "retry.max", 50); // 3M

	protected ArangoOutput(String name, ArangoDBConnection conn) {
		super(name);
		this.conn = conn;
	}

	@Override
	public void close() {
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
