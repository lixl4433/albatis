package net.butfly.albatis.neo4j;

import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.driver.internal.shaded.io.netty.util.internal.StringUtil;

public class Neo4jLanguage {
	/**
	 * @param param
	 * @return merge (n:Person {name:""}) set n.age = 20 return n
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public static String getUpsertNodeNql(Neo4jNode param) throws Exception {
		StringBuffer sb = new StringBuffer();
		Object key = param.key;
		String keyField = param.keyField;
		String table = param.table;
		if(StringUtil.isNullOrEmpty(keyField) || StringUtil.isNullOrEmpty(table)) 
			throw new Exception();
		if((null == key || StringUtil.isNullOrEmpty(key.toString())) && param.containsKey(keyField) && null != param.get(keyField) && !StringUtil.isNullOrEmpty(param.get(keyField).toString())) 
			key = param.remove(keyField);
		if(null != key && !StringUtil.isNullOrEmpty(key.toString()) && param.containsKey(keyField))
			param.remove(keyField);
		
		boolean has_key = false;
		if(null != key && !StringUtil.isNullOrEmpty(key.toString())) has_key = true;
		if(!has_key) throw new Exception();
		Set<?> entrySet = param.entrySet();
		int size = entrySet.size();
		if(size==0) throw new Exception();
		sb.append(" merge (n:{0}");
		if(has_key)
			sb.append(" {1}");
		else
			sb.append("");
		sb.append(")");
		sb.append(" set");
		
		for (int i = 0; i < size; i++) {
			if(has_key)
				sb.append(" {"+(i+2)+"}");
			else
				sb.append(" {"+(i+1)+"}");
		}
		sb.append(" return n");
		MessageFormat mf = new MessageFormat(sb.toString());
		Object[] arr = new Object[has_key ? size+2 : size+1];
		Iterator<?> iterator = entrySet.iterator();
		int i = 0;
		arr[0] = table;
		if(has_key) {
			arr[1] = " {"+keyField+":'"+key+"'} ";
			i++;
		}
		int j = 0;
		while(iterator.hasNext()) {
			j++;
			Entry<Object, Object> next = (Entry<Object, Object>)iterator.next();
			Object k = next.getKey();
			Object v = next.getValue();
			String kv_ = " n."+k+" = '"+v+"'";
			if(size != j) kv_+=",";
			arr[++i] = kv_;
		}
		return mf.format(arr);
	}
	
	/**
	 * MATCH (from:Person {sfzh: '34xxxx'}) </br>
	 * MATCH (to:LG {ZZJG: 'zz6733'}) </br>
	 * MERGE (from)-[r:LGRZ {ZKLSH:'sh223311'}]-(to) </br>
	 * ON CREATE SET r.RZSJ = date('2018-03-01 18:00:00'), r.LDSJ = date('2018-03-02 12:00:00') </br>
	 * ON MATCH SET r.RZSJ = date('2018-03-01 18:00:00'), r.LDSJ = date('2018-03-02 12:00:00') </br>
	 * RETURN from, to, r </br>
	 * @return
	 * @throws Exception 
	 */
	public static String getUpsertRelationNQL(Neo4jRelation r) throws Exception {
		if(r.from == null || r.to == null) 
			throw new Exception();
		if(StringUtil.isNullOrEmpty(r.from.table )|| r.from.key == null  
				|| StringUtil.isNullOrEmpty(r.from.key.toString()) || StringUtil.isNullOrEmpty(r.from.keyField))
			throw new Exception();
		if(StringUtil.isNullOrEmpty(r.to.table )|| r.to.key == null  
				|| StringUtil.isNullOrEmpty(r.to.key.toString()) || StringUtil.isNullOrEmpty(r.to.keyField))
			throw new Exception();
		if(StringUtil.isNullOrEmpty(r.table) || StringUtil.isNullOrEmpty(r.keyField)) 
			throw new Exception();
		//��� table key key_field
		String from_table = r.from.table;
		Object from_key = r.from.key;
		String from_key_field = r.from.keyField;
		
		//�յ� table key key_field
		String to_table = r.to.table;
		Object to_key = r.to.key;
		String to_key_field = r.to.keyField;
		
		//��ϵ table key key_field
		String relation_table = r.table;
		Object relation_key = r.key;
		String relation_key_field = r.keyField;
		
		if((relation_key == null || StringUtil.isNullOrEmpty(relation_key.toString())) && r.containsKey(relation_key_field) && r.get(relation_key_field) != null && !StringUtil.isNullOrEmpty(r.get(relation_key_field).toString())) {
			relation_key = r.remove(relation_key_field);
		}
		if(relation_key != null && !StringUtil.isNullOrEmpty(relation_key.toString()) && r.containsKey(relation_key_field)) {
			r.remove(relation_key_field);
		}
		
		StringBuffer sb = new StringBuffer();
		sb.append("MATCH  (from:"+from_table+" {"+from_key_field+": '"+from_key+"'}) ");
		sb.append("MATCH  (to:"+to_table+" {"+to_key_field+": '"+to_key+"'}) ");
		if(relation_key != null && !StringUtil.isNullOrEmpty(relation_key.toString()))
			sb.append("MERGE (from)-[r:"+relation_table+" {"+relation_key_field+":'"+relation_key+"'}]-(to) ");
		else
			sb.append("MERGE (from)-[r:"+relation_table+"]-(to) ");
		
		StringBuffer on_match = new StringBuffer("ON MATCH SET ");
		StringBuffer on_create = new StringBuffer(" ON CREATE SET ");
		r.entrySet().stream().filter(kv ->kv.getKey() != "from" && kv.getKey() != "to" && kv.getKey() != "table" && kv.getKey() != "key" && kv.getKey() != "keyField").forEach(kv ->{
			on_match.append("r."+kv.getKey()+"='"+kv.getValue()+"', ");
			on_create.append("r."+kv.getKey()+"='"+kv.getValue()+"', ");
		});
		
		String on_match_ = on_match.toString();
		if(!on_match_.equals("ON MATCH SET ")) {
			sb.append(on_match_.substring(0, on_match_.length()-2));
		}
		String on_create_ = on_create.toString();
		if(!on_create_.equals(" ON CREATE SET ")) {
			sb.append(on_create_.substring(0, on_create_.length()-2));
		}
		sb.append(" return from,to,r");
		return sb.toString();
	}
	
	
	/**
	  *    ɾ����ϵ�﷨</br>
	 *  1. match ()-[r:LGRZ {ZZLSH:"zhlsh1111111"}]-() delete r   //ֻ�й�ϵ����Ϣʱ��ɾ���﷨ </br>
	 *  2. match ()-[r:LGRZ]-() delete r   // </br>ɾ�����нڵ�֮���ĳһ��ϵ
	 *  3. match ()-[r]-() delete r   //</br>ɾ�����нڵ�֮������
	 *  4.MATCH (n1:Database)<-[r]->(n2:Message) DELETE r //ɾ��ĳ����ڵ�֮������й�ϵ</br>
	 *  5.MATCH (n1:Database)<-[r:SAYS {"id":"22","name":"xxx"}]->(n2:Message) DELETE r //ɾ��ĳ����ڵ�֮���ĳһ����ϵ</br>
	 *  6.MATCH (n1:Database)<-[r:SAYS]->(n2:Message) DELETE r //ɾ��ĳ����ڵ�֮���ĳһ���ϵ</br>
	 *  7. MATCH (n1:Database {"id":"121","name":"xxx"})<-[r:SAYS {"id":"22","name":"xxx"}]->(n2:Message {"id":"121","name":"xxx"}) DELETE r  //ɾ��ĳ�����ڵ�֮���ĳһ����ϵ</br>
	 *  8. MATCH (n1:Database {"id":"121","name":"xxx"})<-[r:SAYS]->(n2:Message {"id":"121","name":"xxx"}) DELETE r  //ɾ��ĳ�����ڵ�֮���ĳһ�ֹ�ϵ</br>
	 * @throws Exception 
	 */
	public static String getDeleteRelationCql(Neo4jRelation r) {
		StringBuffer sb = new StringBuffer();
		sb.append("match ");
		Neo4jNode from = r.from;
		Neo4jNode to = r.to;
		String table = r.table;
		sb.append("(");
		Object key = r.key;
		String key_field = r.keyField;
		if(from!=null && !StringUtil.isNullOrEmpty(from.table)) {
			String from_table = from.table;
			sb.append("from:"+from_table);
			String from_key_field = from.keyField;
			Object from_key = from.key;
			Map<Object, Object> from_map = from.entrySet().stream().filter(kv -> !kv.getKey().equals("keyField") && !kv.getKey().equals("key") && !kv.getKey().equals("table")).collect(Collectors.toMap(kv->kv.getKey(),kv->kv.getValue()));
			if(!StringUtil.isNullOrEmpty(from_key_field) && from_key !=null && !StringUtil.isNullOrEmpty(from_key.toString())) {
				from_map.put(from_key_field, from_key);
			}
			if(from_map.size()>0) {
				StringBuffer temp = new StringBuffer();
				temp.append("{");
				from_map.entrySet().forEach(kv->{
					Object k = kv.getKey();
					Object v = kv.getValue();
					temp.append(k+":'"+v+"',");
				});
				String kv_s = temp.toString();
				sb.append(" "+kv_s.substring(0, kv_s.length()-1)+"}");
			}
		}
		sb.append(")-");
		
		sb.append("[r");
		if(!StringUtil.isNullOrEmpty(table)) 
			sb.append(":"+table);
		
		Map<Object, Object> r_map = r.entrySet().stream().filter(kv -> !kv.getKey().equals("keyField") 
				&& !kv.getKey().equals("key") && !kv.getKey().equals("table") 
				&& !kv.getKey().equals("from") && !kv.getKey().equals("to")).collect(Collectors.toMap(kv->kv.getKey(),kv->kv.getValue()));
		if(!StringUtil.isNullOrEmpty(key_field) && key !=null && !StringUtil.isNullOrEmpty(key.toString())) {
			r_map.put(key_field, key);
		}
		if(r_map.size()>0) {
			StringBuffer temp = new StringBuffer();
			temp.append("{");
			r_map.entrySet().forEach(kv->{
				Object k = kv.getKey();
				Object v = kv.getValue();
				temp.append(k+":'"+v+"',");
			});
			String kv_s = temp.toString();
			sb.append(" "+kv_s.substring(0, kv_s.length()-1)+"}");
		}
		sb.append("]-(");
		
		if(to!=null && !StringUtil.isNullOrEmpty(to.table)) {
			String to_table = to.table;
			sb.append("to:"+to_table);
			Object to_key = to.key;
			String to_keyField = to.keyField;
			Map<Object, Object> to_map = to.entrySet().stream().filter(kv -> !kv.getKey().equals("keyField") 
					&& !kv.getKey().equals("key") && !kv.getKey().equals("table")).collect(Collectors.toMap(kv->kv.getKey(),kv->kv.getValue()));
			if(!StringUtil.isNullOrEmpty(to_keyField) && to_key !=null && !StringUtil.isNullOrEmpty(to_key.toString())) {
				to_map.put(to_keyField, to_key);
			}
			if(to_map.size()>0) {
				StringBuffer temp = new StringBuffer();
				temp.append("{");
				to_map.entrySet().forEach(kv->{
					Object k = kv.getKey();
					Object v = kv.getValue();
					temp.append(k+":'"+v+"',");
				});
				String kv_s = temp.toString();
				sb.append(" "+kv_s.substring(0, kv_s.length()-1)+"}");
			}
		}
		sb.append(") delete r");
		return sb.toString();
	}
	
	/**
	 * 1.MATCH (n:Person) DELETE n
	 * 2.MATCH (n:Person {"id":"xxx",""}) DELETE n
	 * 
	 * @param n
	 * @return
	 */
	public static String getDeleteNodeCql(Neo4jNode n) {
		StringBuffer sb = new StringBuffer();
		sb.append("match ");
		String table = n.table;
		sb.append("(");
		Object key = n.key;
		String key_field = n.keyField;
		if(n!=null && !StringUtil.isNullOrEmpty(table)) {
			sb.append("n:"+table);
			Map<Object, Object> n_map = n.entrySet().stream().filter(kv -> !kv.getKey().equals("keyField") && !kv.getKey().equals("key") && !kv.getKey().equals("table")).collect(Collectors.toMap(kv->kv.getKey(),kv->kv.getValue()));
			if(!StringUtil.isNullOrEmpty(key_field) && key !=null && !StringUtil.isNullOrEmpty(key.toString())) {
				n_map.put(key_field, key);
			}
			if(n_map.size()>0) {
				StringBuffer temp = new StringBuffer();
				temp.append("{");
				n_map.entrySet().forEach(kv->{
					Object k = kv.getKey();
					Object v = kv.getValue();
					temp.append(k+":'"+v+"',");
				});
				String kv_s = temp.toString();
				sb.append(" "+kv_s.substring(0, kv_s.length()-1)+"}");
			}
		}
		sb.append(") delete n");
		return sb.toString();
	}
}
