package net.butfly.albatis.arangodb;

import java.io.IOException;
import java.text.Format;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.async.ArangoCollectionAsync;
import com.arangodb.async.ArangoDBAsync;
import com.arangodb.async.ArangoDBAsync.Builder;
import com.arangodb.async.ArangoDatabaseAsync;
import com.arangodb.async.ArangoGraphAsync;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.LoadBalancingStrategy;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.GraphCreateOptions;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

public class ArangoDBConnection  extends DataConnection<ArangoDBAsync>{

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBConnection.class);
	/*** connection pool , max connection number. */
	private int max_connections;
	private static final int defaut_max_connections = 16;
	/*** connection timeout*/
	private int timeout_secs;
	private static final int defaut_timeout_secs = 10*60*1000;
	/**
	 * VelocyStream Chunk content-size (bytes)	30000 , VelocyStream是双向异步二进制协议，支持通过管道，多路复用，单向或双向发送消息。
	 * chunk_size VelocyStream block . (byte)
	 */
	private int chunk_size; 
	private static final int default_chunk_size = 5*1024*1024;
	/*** cluster/single ip*/
	private String[] ips;
	private int port;
	/*** user ,  Basic Authentication User */
	private String userName;
	/*** pwd ,  Basic Authentication Password*/
	private String pwd;
	public ArangoDBAsync db;
	public ArangoDatabaseAsync database; 
	public URISpec uri;

	public ArangoDBConnection ips(String ips) {
		try {
			this.ips = ips.split(",");
		} catch (Exception e) {
			logger.error("Illegal parameter : ips ");
		}
		return this;
	}

	public ArangoDBConnection port(int port) {
		this.port = port;
		return this;
	}

	public ArangoDBConnection userName(String userName) {
		this.userName = userName;
		return this;
	}

	public ArangoDBConnection pwd(String pwd) {
		this.pwd = pwd;
		return this;
	}

	public ArangoDBConnection max_connections(int max_connections) {
		this.max_connections = max_connections;
		return this;
	}

	public ArangoDBConnection timeout_secs(int timeout_secs) {
		this.timeout_secs = timeout_secs;
		return this;
	}

	public ArangoDBConnection chunk_size(int chunk_size) {
		this.chunk_size = chunk_size;
		return this;
	}

	protected ArangoDBAsync initialize() {
		Builder b = new ArangoDBAsync.Builder();
		for (String ip : ips)
			b.host(ip, 0 == port ? 8529 : port);
		if (null != userName)
			b.user(userName);
		if (null != pwd)
			b.password(pwd);
		
		if (0 < timeout_secs)
			b.timeout(timeout_secs);
		else
			b.timeout(defaut_timeout_secs);
		
		if (max_connections > 0)
			b.maxConnections(max_connections);
		else if (ips.length > 1)
			b.maxConnections(ips.length);
		else
			b.maxConnections(defaut_max_connections);
		
		if (ips.length > 1) {
			//群集设置的负载平衡。
			b.loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN);
			b.acquireHostList(true);
		}
		return  db = b.chunksize(0 == chunk_size ? default_chunk_size : chunk_size).build();
	}
	
	
	public ArangoDBConnection connect(String ips, int port, String userName, String pwd, String db) {
		database = this
				.ips(ips)
				.port(port)
				.userName(userName)
				.pwd(pwd)
				.max_connections(max_connections > 0 ? max_connections : defaut_max_connections)
				.chunk_size(chunk_size > 0 ? chunk_size : default_chunk_size)
				.timeout_secs(timeout_secs > 0 ? timeout_secs : defaut_timeout_secs)
				.initialize()
				.db(db);
		return this;
	}
	
	
	public ArangoDBConnection(URISpec uri) throws IOException {
		super(uri, 8529, "arango", "arangodb");
		this.uri = uri;
	}
	
	@Override
	protected ArangoDBAsync initialize(URISpec uri) {
		String password = uri.getPassword();
		String username = uri.getUsername();
		String db = uri.getFile();
		String host = uri.getHost();
		String ips = StringUtils.join(Arrays.asList(host.split(",")).stream().map(t -> t.split(":")[0]).collect(Collectors.toList()),",");
		int port = Integer.parseInt(host.split(",")[0].split(":")[1]);
		ArangoDBAsync initialize = this
				.ips(ips)
				.port(port)
				.userName(username)
				.pwd(password)
				.max_connections(max_connections > 0 ? max_connections : defaut_max_connections)
				.chunk_size(chunk_size > 0 ? chunk_size : default_chunk_size)
				.timeout_secs(timeout_secs > 0 ? timeout_secs : defaut_timeout_secs)
				.initialize();
		database = initialize.db(db);
		this.db = initialize;
		return initialize;
	}
	
	@Override
	public void close() throws IOException {
		db.shutdown();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArangoOutput outputRaw(TableDesc... table) throws IOException {
		return new ArangoOutput("ArangoOutput", this);
	}
	
	/**
	 * @param name . node name
	 * @return 0 failed 1 success 2 exist
	 */
	public int createNodeCollection(String name) {
		try {
			CollectionCreateOptions options = new CollectionCreateOptions();
			options.waitForSync(true);
			options.type(CollectionType.DOCUMENT);
			ArangoCollectionAsync collection = database.collection(name);
			if (collection.exists().get()) {
				logger.error("node["+name+"] exist");
				return 2;
			}else {
				database.createCollection(name, options).toCompletableFuture().join();
				logger.error("success : create node["+name+"]");
				return 1;
			}
		} catch (Exception e) {
			logger.error("failed : create node["+name+"]");
		}
		return 0;
	}
	
	/**
	 * @param name . edge name
	 * @return 0 failed 1 success 2 exist
	 */
	public int createEdgeCollection(String name) {
		try {
			CollectionCreateOptions options = new CollectionCreateOptions();
			options.waitForSync(true);
			options.type(CollectionType.EDGES);
			ArangoCollectionAsync collection = database.collection(name);
			if (collection.exists().get()) {
				logger.error("edge["+name+"] exist");
				return 2;
			}else {
				database.createCollection(name, options).toCompletableFuture().join();
				logger.error("success : create edge["+name+"]");
				return 1;
			}
		} catch (Exception e) {
			logger.error("failed : create edge["+name+"]");
		}
		return 0;
	}
	
	public void createNodeCollections(String... nodes) {
		Arrays.asList(nodes).forEach(nc -> this.createNodeCollection(nc));
	}

	public void createEdgeCollections(String... edges) {
		Arrays.asList(edges).forEach(ec -> this.createEdgeCollection(ec));
	}
	
	/**
	 * set edge into graph, if graph not exist, we will create graph without any edge.
	 * @param graphName
	 * @param edge
	 * @param from
	 * @param to
	 * @return
	 */
	public int updateGraph(String graphName, String edge, String from, String to) {
		try {
			ArangoGraphAsync graph = database.graph(graphName);
			if(!graph.exists().get()) {
				database.createGraph(graphName, null);
				logger.error("success : create graph["+graphName+"]");
			}
			graph.addEdgeDefinition(new EdgeDefinition().collection(edge).from(from).to(to)).join();
			logger.error("success : set  edge["+edge+"] from["+from+"] to["+to+"] into graph["+graphName+"]");
		} catch (Exception e) {
			logger.error("failed : set  edge["+edge+"] from["+from+"] to["+to+"] into graph["+graphName+"]");
		}
		return 0;
	}
	
	
	/**
	 * update graph settings
	 * @param graph  graph_name
	 * @param edge_name edge_name
	 * @param from  from_node_name
	 * @param to to_node_name
	 * @return 0 success 1 failed
	 */
	public int updateOneEdgeIntoGraph(String graph, String edge_name, String from, String to) {
		try {
			EdgeDefinition edge = new EdgeDefinition();
			edge.collection(edge_name);
			edge.from(from);
			edge.to(to);
			ArangoGraphAsync g = database.graph(graph);
			if (g.exists().get()) {
				g.addEdgeDefinition(edge).toCompletableFuture().join();
			} else {
				GraphCreateOptions options = new GraphCreateOptions();
				options.isSmart(true);
				List<EdgeDefinition> colls = new ArrayList<>();
				colls.add(edge);
				g.createGraph(colls, options).toCompletableFuture().join();
			}
			logger.info("success : update one edge["+edge_name+":("+from+")--->("+to+")] into graph{"+graph+"}");
			return 0;
		} catch (Exception e) {
			logger.error("failed : update one edge["+edge_name+":("+from+")--->("+to+")] into graph{"+graph+"}");
		}
		return 1;
	}
	
	/**
	 * use _id remove one document
	 * @param _id   xxxx/xxxx   e.g. people/100
	 * @return 0 success  1 failed
	 */
	public int removeById(String _id) {
		try {
			if(StringUtils.isEmpty(_id) || !(_id.indexOf("/")>0) || StringUtils.isEmpty(_id.split("/")[0]) || StringUtils.isEmpty(_id.split("/")[1])) 
				throw new Exception("failed : into param [_id] error");
			List<String> query_result = this.executeAQL("for data in "+_id.split("/")[0]+" filter data._id=='"+_id+"' return data", String.class);
			if(query_result.size() > 0) {
				List<String> result = this.executeAQL("REMOVE {_key:'"+_id.split("/")[1]+"'}  IN "+_id.split("/")[0]+" RETURN OLD", String.class);
				//if(result.size()>0) 
					//logger.info("success : "+result.get(0));
			}
			//else 
				//logger.info("can not remove, don't have this data["+_id+"].");
			return 0;
		} catch (Exception e) {
			logger.error("failed : "+e.getMessage());
		}
		return 1;
	}

	/**
	 * arangodb node upsert
	 * @param data
	 * @return 0 success  1 failed
	 */
	public int upsert(Map<String, Object> data) {
		try {
			Object _id = null;
			String _key = null;
			String table = null;
			if (!data.containsKey("_id") || null == (_id = data.get("_id")) || StringUtils.isEmpty(_id.toString()))
				throw new Exception();
			else {
				String[] tmp = _id.toString().split("/");
				if(tmp.length < 2 || StringUtils.isEmpty(tmp[0]) || StringUtils.isEmpty(tmp[1])) 
					throw new Exception();
				else {
					table = tmp[0];
					_key = tmp[1];
					data.remove("_id");
					data.put("_key", _key);
					String aql = getAql(data, table);
					this.executeAQL(aql, data, BaseDocument.class);
					return 0;
				}
			}
		} catch (Exception e) {
			logger.error("failed : " + e.getMessage());
		}
		return 1;
	}
	
	/**
	 * arangodb node upsert
	 * @param data
	 * @return 0 success  1 failed
	 */
	public int upserts(List<Map<String, Object>> datas) {
		datas.parallelStream().forEach(data ->{
			try {
				Object _id = null;
				String _key = null;
				String table = null;
				if (!data.containsKey("_id") || null == (_id = data.get("_id")) || StringUtils.isEmpty(_id.toString()))
					throw new Exception();
				else {
					String[] tmp = _id.toString().split("/");
					if(tmp.length < 2 || StringUtils.isEmpty(tmp[0]) || StringUtils.isEmpty(tmp[1])) 
						throw new Exception();
					else {
						table = tmp[0];
						_key = tmp[1];
						data.remove("_id");
						data.put("_key", _key);
						String aql = getAql(data, table);
						this.executeAQL(aql, data, BaseDocument.class);
					}
				}
			} catch (Exception e) {
				logger.error("failed : " + e.getMessage());
			}
		});
		return 1;
	}
	
	public <T> List<T> executeAQL(String aql, Map<String, Object> data, Class<T> clazz) {
		return database.query(aql, data, clazz).toCompletableFuture().join().asListRemaining();
	}
	
	public <T> List<T> executeAQL(String aql, Class<T> clazz) {
		return database.query(aql, clazz).toCompletableFuture().join().asListRemaining();
	}
	
	
	protected static final Format AQL_UPSERT = new MessageFormat("upsert '{'_key: @_key} insert {1} update {1} in {0} OPTIONS '{' exclusive: true '}' return NEW"); //

	@Override
	public String toString() {
		return super.toString();
	}

	private static String parseAqlAsBindParams(Map<String, Object> data) {
		return "{" + data.keySet().stream().map(k -> k + ": @" + k).collect(Collectors.joining(", ")) + "}";
	}
	
	public static String getAql(Map<String, Object> data, String table){
		String aql = AQL_UPSERT.format(new String[] { table, parseAqlAsBindParams(data)});
		return aql;
	}
}

