package net.butfly.albatis.http;

import com.alibaba.fastjson.JSONObject;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class HttpOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 5114292900867103434L;
	private static final Logger logger = new Logger(HttpOutput.class);
	private final HttpConnection conn;;

	/**
	 * @param name output name
	 * @param conn standard http connection
	 */
	public HttpOutput(String name, HttpConnection conn) {
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
		String url = "http://" + conn.uri().getHost() + conn.uri().getPath();
		items.list().forEach(item -> {
			JSONObject data = new JSONObject();
			item.entrySet().forEach(kv -> data.put(kv.getKey(), kv.getValue()));
			logger.info(data.toString());
			HttpRequestUtils.doPost(url, data.toString(), Maps.of());
		});
	}

	@Override
	public void close() {
		conn.close();
	}
}
