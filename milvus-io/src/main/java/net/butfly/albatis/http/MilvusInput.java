package net.butfly.albatis.http;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.QueryResults;
import io.milvus.param.R;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class MilvusInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -7766621067006571958L;
	public String table;
	public URISpec url;
	private MilvusServiceClient milvusServiceClient;

	public MilvusInput(String name, MilvusConnection conn) {
		super(name);
		this.milvusServiceClient = conn.milvusServiceClient;
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
			//-----------not code 
			R<QueryResults> query = milvusServiceClient.query(null);
			return new Rmap(table, query);
		}
		return null;
	}
}
