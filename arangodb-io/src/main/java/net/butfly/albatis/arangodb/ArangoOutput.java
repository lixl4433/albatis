package net.butfly.albatis.arangodb;

import static net.butfly.albatis.io.IOProps.propI;

import java.io.IOException;
import java.util.Map;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<AqlEdge> {
	private static final long serialVersionUID = -2376114954650957250L;
	private final ArangoDBConnection conn;
	private static final int MAX_RETRY = propI(ArangoOutput.class, "retry.max", 50); // 3M
	private static final Logger logger = new Logger(ArangoOutput.class);

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
			Map<String, Object> from = aqlEdge.getFrom();
			Map<String, Object> to = aqlEdge.getTo();
			Map<String, Object> edge = aqlEdge.getEdge();
			conn.upsert(from);
			conn.upsert(to);
			conn.upsert(edge);
			logger.info("upsert data: [from]->"+from+"  [to]->"+to+"  [dege]->"+edge);
		});
	}
}
