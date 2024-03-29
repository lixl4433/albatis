package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.serder.json.Jsons;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static net.butfly.albacore.utils.collection.Colls.empty;

public class ElasticConnection extends DataConnection<TransportClient> implements ElasticConnect {
	public ElasticConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri.extra(props), 39300, "es", "elastic", "elasticsearch", "xpack");
	}

	public ElasticConnection(URISpec uri) throws IOException {
		this(uri, null);
	}

	public ElasticConnection(String url, Map<String, String> props) throws IOException {
		this(new URISpec(url), props);
	}

	public ElasticConnection(String url) throws IOException {
		this(new URISpec(url));
	}

	@Override
	protected TransportClient initialize(URISpec uri) {
		if(!"xpack".equals(uri.getSchema(0)))
			return ElasticConnect.Builder.buildTransportClient(uri, uri.extras);
		else
			return xpackClient(uri);
	}

	@Override
	public String getDefaultIndex() {
		return uri.getPathAt(0);
	}

	@Override
	public String getDefaultType() {
		return uri.getPathAt(1);
	}

	@Override
	public void construct(Qualifier qualifier, FieldDesc... fields) {
		Map<String, Object> indexConfig = new HashMap<>();
		indexConfig.put("index/type", qualifier.name);
		construct(indexConfig, fields);
	}

	/**
	 * Please at least put index/type into indexConfig, that is to say indexConfig could not be null. Except index/type, please make sure
	 * the index configure to be according to es official config (e.g. indexConfig.put("index.number_of_shards", 3) ,
	 * indexConfig.put("index.number_of_replicas", 2)).
	 */
	@Override
	public void construct(Map<String, Object> indexConfig, FieldDesc... fields) {
		if (Colls.empty(indexConfig)) throw new RuntimeException("Please at least put index/type into indexConfig!");
		String alias = String.valueOf(indexConfig.get("alias"));
		indexConfig.remove("alias");
		assert null != alias;
		String table = String.valueOf(indexConfig.remove("index/type"));
		String[] tables;
		if (table.contains(".")) tables = table.split("\\.");
		else if (table.contains("/")) tables = table.split("/");
		else throw new RuntimeException("es not support other split ways!");
		String index, type;
		if (tables.length == 1) index = type = tables[0];
		else if (tables.length == 2) {
			index = tables[0];
			type = tables[1];
		} else throw new RuntimeException("Please type in correct es table format: index/type or index.type !");
		Map<String, Object> mapping = new MappingConstructor(indexConfig).construct(fields);
		logger().debug(() -> "Mapping constructing: \n\t" + JsonSerder.JSON_MAPPER.ser(mapping));
		if (client.admin().indices().prepareExists(index).execute().actionGet().isExists()) {
			PutMappingRequest req = new PutMappingRequest(index);
			req.source(mapping);
			AcknowledgedResponse r = client.admin().indices().putMapping(req.type(type)).actionGet();
			if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + type + "]" + req.toString());
			else logger().info(() -> "Mapping on [" + type + "] construced sussesfully.");
			return;
		}
		CreateIndexResponse r;
		if (indexConfig.isEmpty()) r = client.admin().indices().prepareCreate(index).addMapping(type, mapping).get();
		else r = client.admin().indices().prepareCreate(index).setSettings(indexConfig).addMapping(type, mapping).get();
		if (!r.isAcknowledged()) logger().error("Mapping failed on index [" + index + "] type [" + type + "]" + r.toString());
		else logger().info(() -> "Mapping on index [" + index + "] type [" + type + "] construct successfully: \n\t"
				+ JsonSerder.JSON_MAPPER.ser(mapping));
		IndicesExistsRequest indexExists = new IndicesExistsRequest(index);
		if (client.admin().indices().exists(indexExists).actionGet().isExists()) {
			AcknowledgedResponse response = client.admin().indices().prepareAliases().addAlias(index, alias).execute().actionGet();
			if (!response.isAcknowledged()) logger().error("create elastic index alias failure:" + response.toString());
			else logger().info("create elastic index alias successful");
		} else logger().info("es aliases also index duplicate names.");
	}

	@Override
	public boolean judge(String table) {
		boolean exists = false;
		String[] tables;
		if (table.contains(".")) tables = table.split("\\.");
		else if (table.contains("/")) tables = table.split("/");
		else throw new RuntimeException("es not support other split ways!");
		String index;
		if (tables.length == 1) index = tables[0];
		else if (tables.length == 2) index = tables[0];
		else throw new RuntimeException("Please type in correct es table format: index/type or index.type !");
		try (ElasticConnection elasticConnection = new ElasticConnection(new URISpec(uri.toString()))) {
			IndicesExistsRequest existsRequest = new IndicesExistsRequest(index);
			exists = elasticConnection.client.admin().indices().exists(existsRequest).actionGet().isExists();
		} catch (IOException e) {
			logger().error("es judge table isExists error", e);
		}
		return exists;
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
		// await();
		client.close();
	}

	protected final void await() {
		boolean closed = false;
		logger().debug("ES connection thread pool terminating...");
		while (!closed)
			try {
				closed = client.threadPool().awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				closed = true;
			}
		logger().debug("ES connection thread pool terminated...");
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<ElasticConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public ElasticConnection connect(URISpec uriSpec) throws IOException {
			return new ElasticConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("es");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public ElasticOutput outputRaw(TableDesc... table) throws IOException {
		return new ElasticOutput("ElasticOutput", this);
	}

	/**
	 * @param indexAndType
	 *            format:
	 *            <ul>
	 *            <li><b>index</b>/<b>type</b></li>
	 *            <li><b>index</b>/</li>&nbsp;(default type: <b>_doc</b>)</li>
	 *            <li>/<b>type</b></li>
	 *            <li><b>index</b>&nbsp;(default type: <b>_doc</b>)</li>
	 *            <ul>
	 */
	public void construct(Map<String, Object> mapping, String indexAndType) {
		String[] it = indexAndType.split("/", 2);
		String[] its = 2 == it.length ? it : new String[] { it[0], null };
		if (empty(its[0])) its[0] = getDefaultIndex();
		if (empty(its[1])) its[1] = getDefaultType();
		if (empty(its[1])) its[1] = "_doc";
		logger().debug("Mapping constructing on " + String.join("/", its) + ": \n\t" + Jsons.pretty(mapping));

		PutMappingRequest req = new PutMappingRequest(its[0]);
		req.source(mapping);
		AcknowledgedResponse r = client.admin().indices().putMapping(req.type(its[1])).actionGet();
		if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + its[1] + "]" + req.toString());
		else logger().info(() -> "Mapping on " + its + " construced sussesfully.");
	}
	
	public TransportClient xpackClient(URISpec urispec) {
		TransportClient client = null;
		try {
			String ips = urispec.getHost();
			String cluster = urispec.getFile();
			String username = urispec.getUsername();
			String password = urispec.getPassword();
			String keypath = urispec.getParameter("keypath");
			
			Settings settings = Settings.builder().put("cluster.name", cluster)
					.put("xpack.security.user", username + ":" + password)
					.put("client.transport.sniff", false)
					.put("xpack.security.enabled", true)
					.put("xpack.security.transport.ssl.enabled", true)
					.put("xpack.security.transport.ssl.keystore.path", keypath)// 这里的文件要保证能访问到
					.put("xpack.security.transport.ssl.truststore.path", keypath)
					.put("xpack.security.transport.ssl.verification_mode", "certificate")
					// .put("client.ping.timeout", client_ping_timeout)
					.build();
			client = new PreBuiltXPackTransportClient(settings);
			List<TransportAddress> transportAddresses = new ArrayList<>();
			for (String node : Arrays.asList(ips.split(",")))
				transportAddresses.add(new TransportAddress(InetAddress.getByName(node.split(":")[0]),
						Integer.parseInt(node.split(":")[1])));
			client.addTransportAddresses(
					transportAddresses.toArray(new TransportAddress[transportAddresses.size()]));
			System.out.println("-------------------");
		}catch (Exception e) {
			logger().info(e.getMessage());
		}
		return client;
	}
}
