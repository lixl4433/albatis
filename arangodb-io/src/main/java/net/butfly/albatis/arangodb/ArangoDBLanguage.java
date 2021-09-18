package net.butfly.albatis.arangodb;

import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class ArangoDBLanguage {
	/**
	 * @param param
	 * @return 
	 * notice : if no key </p>
	 * INSERT { field1: 'xxx', field2: xxx, field3: xxx }</p>
	 * IN xxx</p>
	 * RETURN NEW</p>
	 * -----------------------------------------</p>
	 * notice : if exist key </p>
	 * UPSERT { _id: 'xxx' } </p>
	 * INSERT { field1: 'xxx', field2: xxx, field3: xxx } </p>
	 * UPDATE { field1: 'xxx', field2: xxx, field3: xxx } </p>
	 * IN xxx</p>
	 * options {exclusive:true}
	 * RETURN NEW</p>
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public static String getUpsertNodeAQL(ArangoDBNode param) throws Exception {
		StringBuffer sb = new StringBuffer();
		Object key = param.key;
		String keyField = param.getKeyField();
		String table = param.table;
		if(StringUtils.isEmpty(table)) 
			throw new Exception();
		if((null == key || StringUtils.isEmpty(key.toString())) && param.containsKey(keyField) && null != param.get(keyField) && !StringUtils.isEmpty(param.get(keyField).toString())) 
			key = param.remove(keyField);
		if(null != key && !StringUtils.isEmpty(key.toString()) && param.containsKey(keyField))
			param.remove(keyField);
		
		boolean has_key = false;
		if(null != key && !StringUtils.isEmpty(key.toString())) has_key = true;
		Set<?> entrySet = param.entrySet();
		int size = entrySet.size();
		if(size==0) throw new Exception();
		Iterator<?> iterator = entrySet.iterator();
		Object[] arr = new Object[has_key ? size+1 : size];
		int i = 0;
		if(!has_key) {
			StringBuffer tmp = new StringBuffer();
			for (int ii = 0; ii < size; ii++) {
					tmp.append("{"+(ii)+"}");
			}
			int j = 0;
			while(iterator.hasNext()) {
				j++;
				Entry<Object, Object> next = (Entry<Object, Object>)iterator.next();
				Object k = next.getKey();
				Object v = next.getValue();
				String kv_ = k+":"+getSubString(v);
				if(size != j) kv_+=",";
				arr[i++] = kv_;
			}
			MessageFormat mf = new MessageFormat("insert '{'"+tmp.toString()+"'}' into "+table+" return NEW");
			return mf.format(arr);
		}else {
			sb.append(" upsert '{'");
			if(has_key)
				sb.append("{0}");
			sb.append("'}'");
			
			StringBuffer tmp = new StringBuffer();
			for (int ii = 0; ii < size; ii++) {
				tmp.append(" {"+(ii+1)+"}");
			}
			sb.append(" insert '{'"+keyField+":'''"+key+"''', "+tmp.toString()+"'}'");
			sb.append(" update '{'"+keyField+":'''"+key+"''', "+tmp.toString()+"'}'");
			sb.append(" in "+table);
			sb.append(" options '{'exclusive:true'}' return NEW");
			MessageFormat mf = new MessageFormat(sb.toString());
			arr[0] = keyField+":'"+key+"'";
			i++;
			int j = 0;
			while(iterator.hasNext()) {
				j++;
				Entry<Object, Object> next = (Entry<Object, Object>)iterator.next();
				Object k = next.getKey();
				Object v = next.getValue();
				String kv_ = k+":"+getSubString(v)+"";
				if(size != j) kv_+=",";
				arr[i++] = kv_;
			}
			return mf.format(arr);
		}
	}
	
	/**
	 *  识别数据类型
	 * @param v
	 * @return
	 */
	public static Object getSubString(Object v) {
		if(v instanceof Number) //数字类型
			return v;
		else if(v instanceof String) //字符类型
			return "'"+v+"'";
		else 
			return "'"+v+"'"; //默认字符类型
	}
	
	/**
	 * @param param
	 * @return 
	 * notice : if no key </p>
	 * INSERT { field1: 'xxx', field2: xxx, field3: xxx }</p>
	 * IN xxx</p>
	 * RETURN NEW</p>
	 * -----------------------------------------</p>
	 * notice : if exist key </p>
	 * UPSERT { _id: 'xxx' } </p>
	 * INSERT { field1: 'xxx', field2: xxx, field3: xxx } </p>
	 * UPDATE { field1: 'xxx', field2: xxx, field3: xxx } </p>
	 * IN xxx</p>
	 * options {exclusive:true}
	 * RETURN NEW</p>
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public static String getUpsertEdgeAQL(ArangoDBEdge param) throws Exception {
		StringBuffer sb = new StringBuffer();
		
		Object from = param.from;
		Object to = param.to;
		Object key = param.key;
		String keyField = param.getKeyField();
		String fromField = param.getFromField();
		String toField = param.getToField();
		String table = param.table;
		
		
		if(param.from == null || param.to == null || StringUtils.isEmpty(param.table)) 
			throw new Exception();
		if((null == key || StringUtils.isEmpty(key.toString())) && param.containsKey(keyField) && null != param.get(keyField) && !StringUtils.isEmpty(param.get(keyField).toString())) 
			key = param.remove(keyField);
		if(null != key && !StringUtils.isEmpty(key.toString()) && param.containsKey(keyField))
			param.remove(keyField);
		//加入起点和终点
		param.put(fromField, from);
		param.put(toField, to);
		
		boolean has_key = false;
		if(null != key && !StringUtils.isEmpty(key.toString())) has_key = true;
		Set<?> entrySet = param.entrySet();
		int size = entrySet.size();
		if(size==0) throw new Exception();
		Iterator<?> iterator = entrySet.iterator();
		Object[] arr = new Object[has_key ? size+1 : size];
		int i = 0;
		if(!has_key) {
			StringBuffer tmp = new StringBuffer();
			for (int ii = 0; ii < size; ii++) {
					tmp.append("{"+(ii)+"}");
			}
			int j = 0;
			while(iterator.hasNext()) {
				j++;
				Entry<Object, Object> next = (Entry<Object, Object>)iterator.next();
				Object k = next.getKey();
				Object v = next.getValue();
				String kv_ = k+":"+getSubString(v);
				if(size != j) kv_+=",";
				arr[i++] = kv_;
			}
			MessageFormat mf = new MessageFormat("insert '{'"+tmp.toString()+"'}' into "+table+" return NEW");
			return mf.format(arr);
		}else {
			sb.append(" upsert '{'");
			if(has_key)
				sb.append("{0}");
			sb.append("'}'");
			
			StringBuffer tmp = new StringBuffer();
			for (int ii = 0; ii < size; ii++) {
				tmp.append(" {"+(ii+1)+"}");
			}
			sb.append(" insert '{'"+keyField+":'''"+key+"''', "+tmp.toString()+"'}'");
			sb.append(" update '{'"+keyField+":'''"+key+"''', "+tmp.toString()+"'}'");
			sb.append(" in "+table);
			sb.append(" options '{'exclusive:true'}' return NEW");
			MessageFormat mf = new MessageFormat(sb.toString());
			arr[0] = keyField+":'"+key+"'";
			i++;
			int j = 0;
			while(iterator.hasNext()) {
				j++;
				Entry<Object, Object> next = (Entry<Object, Object>)iterator.next();
				Object k = next.getKey();
				Object v = next.getValue();
				String kv_ = k+":"+getSubString(v)+"";
				if(size != j) kv_+=",";
				arr[i++] = kv_;
			}
			return mf.format(arr);
		}
	}
}
