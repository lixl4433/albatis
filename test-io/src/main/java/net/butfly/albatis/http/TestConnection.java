package net.butfly.albatis.http;

import java.io.IOException;
import java.util.List;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

public class TestConnection extends DataConnection<Object> {
	final static String schema = "test";
	public URISpec url;
	
	public TestConnection(URISpec uri) throws IOException {
		super(uri, schema);
		url =uri;
	}

	public TestConnection(String urispec) throws IOException {
		this(new URISpec(urispec));
	}

	protected Object initialize(URISpec url) {
		return null;
	}

	public TestInput inputRaw(TableDesc... table) {
		return new TestInput("TestInput", this);
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<TestConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public TestConnection connect(URISpec uriSpec) throws IOException {
			return new TestConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("test");
		}
	}

	public List<String> schemas() {
		return Colls.list("test");
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}
}
