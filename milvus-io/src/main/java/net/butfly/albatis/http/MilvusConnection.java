package net.butfly.albatis.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ProtocolStringList;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.InsertParam.Field;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;

public class MilvusConnection extends DataConnection<MilvusServiceClient> {
	final static String schema = "milvus";
	public MilvusServiceClient milvusServiceClient;
	public URISpec url;
	private static Map<String, List<FieldSchema>>  collectionOrderlyFields = new HashMap<String, List<FieldSchema>>();
	
	public MilvusConnection(URISpec uri) throws IOException {
		super(uri, schema);
		url =uri;
	}

	public MilvusConnection(String urispec) throws IOException {
		this(new URISpec(urispec));
	}

	protected MilvusServiceClient initialize(URISpec url) {
		String host = uri.getHost();
		String ips = host.split(":")[0];
		int port = Integer.parseInt(host.split(":")[1]);
		milvusServiceClient = new MilvusServiceClient(ConnectParam.newBuilder().withHost(ips).withPort(port).build());
		loadMilvusSchema();
		return milvusServiceClient;
	}

	@SuppressWarnings("unchecked")
	public MilvusInput inputRaw(TableDesc... table) {
		return new MilvusInput("MilvusInput", this);
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<MilvusConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public MilvusConnection connect(URISpec uriSpec) throws IOException {
			return new MilvusConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("milvus");
		}
	}

	public List<String> schemas() {
		return Colls.list("milvus");
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}
	
	public void loadMilvusSchema() {
		ShowCollectionsParam sp = ShowCollectionsParam.newBuilder().build();
		R<ShowCollectionsResponse> showCollections = milvusServiceClient.showCollections(sp);
		ProtocolStringList collectionNamesList = showCollections.getData().getCollectionNamesList();
		Iterator<String> iterator = collectionNamesList.iterator();
		while(iterator.hasNext()) {
			String collectionName = iterator.next();
			
			List<FieldSchema> fields = new ArrayList<>();
			HashMap<String, FieldSchema> fieldsMap = new HashMap<String, FieldSchema>();
			
			R<DescribeCollectionResponse> describeCollection = milvusServiceClient
					.describeCollection(DescribeCollectionParam.newBuilder().withCollectionName(collectionName).build());
			CollectionSchema schema = describeCollection.getData().getSchema();
			int fieldsCount = schema.getFieldsCount();
			for (int i = 0; i < fieldsCount; i++) {
				FieldSchema fieldSchema = schema.getFields(i);
				fields.add(fieldSchema);
				fieldsMap.put(fieldSchema.getName(), fieldSchema);
			}
			collectionOrderlyFields.put(collectionName, fields);
		}
	}
	
	public void upsert(Rmap data) {
		try {
			List<FieldSchema> fieldSchemas = collectionOrderlyFields.get(data.table().name);
			List<Field> fields = new ArrayList<>();
			for (FieldSchema fieldSchema : fieldSchemas) {
				String fieldName = fieldSchema.getName();
				DataType fieldDataType = fieldSchema.getDataType();
				fields.add(new Field(fieldName, fieldDataType, data.containsKey(fieldName) ? Colls.list(getFieldVal(data.get(fieldName), fieldDataType)) : Colls.list()));
			}
			milvusServiceClient
					.insert(InsertParam.newBuilder().withCollectionName(data.table().name).withFields(fields).build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void upserts(List<Rmap> datas) {
		try {
			if(datas.size() > 0) {
				String collectionName = datas.get(0).table().name;
				List<FieldSchema> fieldSchemas = collectionOrderlyFields.get(collectionName);
				List<Field> fields = new ArrayList<>();
				for (FieldSchema fieldSchema : fieldSchemas) {
					String fieldName = fieldSchema.getName();
					DataType fieldDataType = fieldSchema.getDataType();
					List<Object> fieldCache = new ArrayList<>();
					datas.parallelStream().forEach(data -> {
						if (data.containsKey(fieldName)) {
							fieldCache.add(getFieldVal(data.get(fieldName), fieldDataType));
						} else {
							fieldCache.add(null);
						}
					});
					fields.add(new Field(fieldName, fieldDataType, fieldCache));
				}
				milvusServiceClient
						.insert(InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public static Object getFieldVal(Object value, DataType dataType) {
		switch (dataType) {
		case Int8:
			return Integer.parseInt((String)value);
		case Int16:
			return Integer.parseInt((String)value);
		case Int32:
			return Integer.parseInt((String)value);
		case Int64:
			return Long.parseLong((String) value);
		case FloatVector:
			return  (List<Float>)value;
		case BinaryVector:
			return (List<Byte>)value;
		default:
			return Integer.parseInt((String)value);
		}
	}
	
	
	
	
	
}
