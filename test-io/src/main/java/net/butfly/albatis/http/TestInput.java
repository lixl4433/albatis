package net.butfly.albatis.http;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class TestInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -7766621067006571958L;
	public String table;
	public URISpec url;

	public TestInput(String name, TestConnection conn) {
		super(name);
	}

	public static ThreadLocal<ObjectMapper> httpGetMapper = new ThreadLocal<ObjectMapper>() {
		@Override
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};
	public static ThreadLocal<ObjectMapper> httpPostMapper = new ThreadLocal<ObjectMapper>() {
		@Override
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};

	@SuppressWarnings("deprecation")
	@Override
	public Rmap dequeue() {
		while (opened()) {
			return new Rmap(table, "{\"test\": \"test\"}");
		}
		return null;
	}
}
