package net.butfly.albatis.http;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
//import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class MilvusOutput extends OutputBase<Rmap> {

	private static final long serialVersionUID = 5114292900867103434L;
	//private static final Logger logger = new Logger(HttpOutput.class);
	private final MilvusConnection conn;;

	/**
	 * @param name output name
	 * @param conn standard http connection
	 */
	public MilvusOutput(String name, MilvusConnection conn) {
		super(name);
		this.conn = conn;
		closing(this::close);
		open();
	}

	@Override
	public URISpec target() {
		return conn.uri();
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		conn.upserts(items.list());
	}

	@Override
	public void close() {
		conn.close();
	}
}
