package net.butfly.albatis.neo4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import net.butfly.albatis.io.Rmap;

public class Neo4jUtils {

	private Driver driver;
	private static AsyncSession asyncSession;
	public Neo4jUtils(Neo4jConnection nc) {
		driver = nc.driver;
		initSession();
	}
	
	public AsyncSession initSession() {
		try {
			asyncSession = driver.asyncSession();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 根据入参Neo4jNode<Object, Object> m --> key_field:"" upsert 因此 其值不能为空
	 * @param m
	 * @return 
	 */
	public int upsertNode(Neo4jNode m) {
		try {
			String cql = Neo4jLanguage.getUpsertNodeNql(m);
			System.out.println(cql);
			CompletionStage<ResultCursor> runAsync = asyncSession.runAsync(cql);
			runAsync.toCompletableFuture().join();
            return 0;
        }catch (Exception e) {
        	e.printStackTrace();
		}
		return 1;
	}
	
	
	public void close() {
		asyncSession.closeAsync();
		driver.closeAsync();
	}
	
	/**
	 * 清空所有关系和数据
	 * @return
	 */
	public int clear() {
		String relation = "MATCH (n1)<-[r]->(n2) DELETE r";
		String node = "MATCH (n) DELETE n";
		try {
			asyncSession.runAsync(relation);
			asyncSession.runAsync(node);
            return 0;
        }catch (Exception e) {
        	e.printStackTrace();
		}
		return 1;
	}
	
	/**
	 * 根据key upsert
	 * @param r
	 * @return 
	 */
	public static int upsertRelation(Neo4jRelation r) {
		try {
			String cql = Neo4jLanguage.getUpsertRelationNQL(r);
			System.out.println(cql);
			CompletionStage<ResultCursor> runAsync = asyncSession.runAsync(cql);
			runAsync.toCompletableFuture().join();
            return 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 1;
	}
	
	
	/**
	 * 查询
	 * @param cql
	 * @return
	 */
	public List<Rmap> query(String cql) {
		System.out.println(cql);
		List<Rmap> l = new ArrayList<Rmap>();
		CompletionStage<ResultCursor> query = asyncSession.runAsync(cql);
		query.whenComplete((rs, ex)->{
			rs.forEachAsync(r -> {
				List<Value> values = r.values();
				values.forEach(v ->{
					if(v instanceof NodeValue) {
						Node asNode = v.asNode();
						Iterable<String> labels = asNode.labels();
						long id = asNode.id();
						Map<String, Object> asMap = asNode.asMap();
						Neo4jNode nn = new Neo4jNode();
						nn.key = id;
						nn.keyField = "id";
						nn.table = labels.iterator().next();
						asMap.entrySet().forEach(kv ->{
							nn.put(kv.getKey(), kv.getValue());
						});
						l.add(nn);
					}
					if(v instanceof RelationshipValue) {
						Relationship asRelationship = v.asRelationship();
						long id = asRelationship.id();
						long startNodeId = asRelationship.startNodeId();
						long endNodeId = asRelationship.endNodeId();
						String type = asRelationship.type();
						Map<String, Object> asMap = asRelationship.asMap();
						Neo4jRelation nr = new Neo4jRelation();
						Neo4jNode from = new Neo4jNode();
						from.key = startNodeId;
						from.keyField = "id";
						Neo4jNode to = new Neo4jNode();
						to.key = endNodeId;
						to.keyField = "id";
						nr.from = from;
						nr.to = to;
						nr.key = id;
						nr.keyField = "id";
						nr.table = type;
						asMap.entrySet().forEach(kv ->{
							nr.put(kv.getKey(), kv.getValue());
						});
						l.add(nr);
					}
				});
			});
		}).toCompletableFuture().join();
		return l;
	}
	
	/**
	 * 删除关系
	 * @return 
	 * @throws Exception 
	 */
	public int deleteRelation(Neo4jRelation r) {
		try {
			String cql = Neo4jLanguage.getDeleteRelationCql(r);
			System.out.println(cql);
			CompletionStage<ResultCursor> runAsync = asyncSession.runAsync(cql);
			runAsync.toCompletableFuture().join();
            return 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 1;
	}
	
	
	/**
	 * 删除节点
	 * @return 
	 */
	public int deleteNode(Neo4jNode n) {
		try {
			String cql = Neo4jLanguage.getDeleteNodeCql(n);
			System.out.println(cql);
			CompletionStage<ResultCursor> runAsync = asyncSession.runAsync(cql);
			runAsync.toCompletableFuture().join();
            return 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 1;
	}
	
	/**
	 * 执行语法
	 * @param cql
	 * @return
	 */
	public int executeCQL(String cql) {
		try {
			System.out.println(cql);
			CompletionStage<ResultCursor> runAsync = asyncSession.runAsync(cql);
			runAsync.toCompletableFuture().join();
            return 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 1;
	}
}

