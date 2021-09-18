package net.butfly.albatis.arangodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.arangodb.async.ArangoCollectionAsync;
import com.arangodb.async.ArangoDBAsync;
import com.arangodb.async.ArangoDBAsync.Builder;
import com.arangodb.async.ArangoDatabaseAsync;
import com.arangodb.async.ArangoGraphAsync;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.LoadBalancingStrategy;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.GraphCreateOptions;

import net.butfly.albacore.io.URISpec;

public class ArangoDBConnection {

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBConnection.class);
	//连接池 最大连接数
	private int max_connections;
	private static final int defaut_max_connections = 16;
	//连接超时时间
	private int timeout_secs;
	private static final int defaut_timeout_secs = 10*60*1000;
	// VelocyStream Chunk content-size (bytes)	30000 , VelocyStream是双向异步二进制协议，支持通过管道，多路复用，单向或双向发送消息。
	//chunk_size VelocyStream 快的大小单位byte
	private int chunk_size; 
	private static final int default_chunk_size = 5*1024*1024;
	//集群、单节点ip
	private String[] ips;
	//端口
	private int port;
	//用户 Basic Authentication User
	private String userName;
	//密码  Basic Authentication Password
	private String pwd;
	public ArangoDBAsync db;
	public ArangoDatabaseAsync database; 

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
	
	public void close() {
		db.shutdown();
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
	
	public ArangoDBConnection connect(URISpec uri) {
		uri.get
		this.connect(uri.get, port, userName, pwd, pwd)
		return this;
	}
	
	public <T> List<T> executeAQL(String aql, Class<T> clazz) {
		return database.query(aql, clazz).toCompletableFuture().join().asListRemaining();
	}
	
	/**
	 * @param name 节点名称
	 * @return 0 失败 1 成功 2 已存在
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
	 * @param name 边名称
	 * @return 0 失败 1 成功 2 已存在
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
	 * 更新关系如图
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
	 * arangodb node upsert
	 * @param data
	 * @return 0 success  1 failed
	 */
	public int upsertNode(ArangoDBNode data) {
		try {
			String aql = ArangoDBLanguage.getUpsertNodeAQL(data);
			//logger.info(aql);
			List<String> result = this.executeAQL(aql, String.class);
		//	if (result.size() > 0)
				//logger.debug("success : " + result.get(0));
			return 0;
		} catch (Exception e) {
			logger.error("failed : " + e.getMessage());
		}
		return 1;
	}
	
	/**
	 * arangodb node batch upsert
	 * @param List<ArangoDBNode> datas
	 * @return 0 success  1 failed
	 * @apiNote 无序插入
	 */
	public int upsertNodes(List<ArangoDBNode> datas) {
		try {
			datas.parallelStream().forEach(data ->{
				List<String> result = new ArrayList<>();
				try {
					String aql = ArangoDBLanguage.getUpsertNodeAQL(data);
					//logger.info(aql);
					result = this.executeAQL(aql, String.class);
				} catch (Exception e) {
					logger.info("failed : "+data);
				}
				//if(result.size()>0) 
				//	logger.info("success : "+result.get(0));
			});
			return 0;
		}catch (Exception e) {
		}
		return 1;
	}
	
	/**
	 * arangodb edge upsert
	 * @param data
	 * @return 0 success  1 failed
	 */
	public int upsertEdge(ArangoDBEdge data) {
		try {
			String aql = ArangoDBLanguage.getUpsertEdgeAQL(data);
			//logger.info(aql);
			List<String> result = this.executeAQL(aql, String.class);
			//if(result.size()>0) 
				//logger.debug("success : "+result.get(0));
			return 0;
		} catch (Exception e) {
			logger.error("failed : "+e.getMessage());
		}
		return 1;
	}
	
	/**
	 * arangodb edge batch upsert
	 * @param List<ArangoDBEdge> datas
	 * @return 0 success  1 failed
	 * @apiNote 无序插入
	 */
	public int upsertEdges(List<ArangoDBEdge> datas) {
		try {
			datas.parallelStream().forEach(data ->{
				List<String> result = new ArrayList<>();
				try {
					String aql = ArangoDBLanguage.getUpsertEdgeAQL(data);
				//	logger.info(aql);
					result = this.executeAQL(aql, String.class);
				} catch (Exception e) {
					logger.info("failed : "+data);
				}
				//if(result.size()>0) 
					//logger.info("success : "+result.get(0));
			});
			return 0;
		}catch (Exception e) {
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
	 * @param param  coming param  must contains  attribute _id, like this {"_id":"collections_name/xxxxx"}
	 * @return 0 success  1 failed
	 */
	public int upsertNode(JSONObject _node) {
		try {
			if(_node.containsKey("_id") && null !=_node.get("_id") && !StringUtils.isEmpty(_node.get("_id").toString())) {
				String _key = _node.get("_id").toString();
				String[] key_table = _key.split("/");
				if(key_table.length ==2 && !StringUtils.isEmpty(key_table[0]) && !StringUtils.isEmpty(key_table[1])) {
					ArangoDBNode node = new ArangoDBNode();
					node.key = key_table[1];
					node.table = key_table[0];
					_node.remove("_id");
					_node.entrySet().forEach(n -> node.put(n.getKey(), n.getValue()));
					return this.upsertNode(node);
				}
			}
		}catch (Exception e) {
			logger.info("fialed : add node["+_node+"] .");
		}
		return 1;
	}
	
	
	/**
	 * arangodb edge upsert
	 * @param param  coming param  must contains  attributes _id _from _to, like this {"_id":"collections_name/xxxxx", "_from":"collections_name/xxxxx", "_to":"collections_name/xxxxx"}
	 * @return 0 success  1 failed
	 */
	public int upsertEdge(JSONObject _edge) {
		try {
			if(_edge.containsKey("_id") && null !=_edge.get("_id") && !StringUtils.isEmpty(_edge.get("_id").toString()) &&
				_edge.containsKey("_from") && null !=_edge.get("_from") && !StringUtils.isEmpty(_edge.get("_from").toString()) &&
				_edge.containsKey("_to") && null !=_edge.get("_to") && !StringUtils.isEmpty(_edge.get("_to").toString())) {
				
				String _key = _edge.get("_id").toString();
				String[] key_table = _key.split("/");
				
				String _from = _edge.get("_from").toString();
				String[] from_key_table = _from.split("/");
				
				String _to = _edge.get("_to").toString();
				String[] to_key_table = _to.split("/");
				
				if(key_table.length ==2 && !StringUtils.isEmpty(key_table[0]) && !StringUtils.isEmpty(key_table[1]) &&
					from_key_table.length ==2 && !StringUtils.isEmpty(from_key_table[0]) && !StringUtils.isEmpty(from_key_table[1]) &&
					to_key_table.length ==2 && !StringUtils.isEmpty(to_key_table[0]) && !StringUtils.isEmpty(to_key_table[1])) {
					
					ArangoDBEdge edge = new ArangoDBEdge();
					edge.key = key_table[1];
					edge.table = key_table[0];
					edge.from = _from;
					edge.to = _to;
					_edge.remove("_id");
					_edge.remove("_from");
					_edge.remove("_to");
					_edge.entrySet().forEach(n -> edge.put(n.getKey(), n.getValue()));
					return this.upsertEdge(edge);
				}
			}
		}catch (Exception e) {
			logger.info("fialed : add node["+_edge+"] .");
		}
		return 1;
	}
}
