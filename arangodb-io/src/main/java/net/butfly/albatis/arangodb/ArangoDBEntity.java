package net.butfly.albatis.arangodb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class ArangoDBEntity extends ConcurrentHashMap<Object, Object>{
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEntity.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	
	
	
	public ArangoDBEntity putVl(Object o) {
		Map<Object, Object> map = new HashMap<Object, Object>();
		if (o instanceof Map) 
			((Map<?,?>)o).entrySet().forEach(kv -> map.put(kv.getKey(), kv.getValue()));
		else if(o instanceof JSONObject)
			((JSONObject)o).entrySet().forEach(kv -> map.put(kv.getKey(), kv.getValue()));
		else {
			logger.info("unknow object type");
		}
		
		if(this instanceof ArangoDBNode) {
			map.entrySet().forEach(kv ->{
				Object k = kv.getKey();
				Object v = kv.getKey();
				if(k !=null && StringUtils.isEmpty(k.toString()) && v !=null && StringUtils.isEmpty(v.toString()))
					((ArangoDBNode)this).put(kv.getKey(), kv.getValue());
			});
		}else if(this instanceof ArangoDBEdge){
			map.entrySet().forEach(kv ->{
				Object k = kv.getKey();
				Object v = kv.getKey();
				if(k !=null && StringUtils.isEmpty(k.toString()) && v !=null && StringUtils.isEmpty(v.toString()))
					((ArangoDBEdge)this).put(kv.getKey(), kv.getValue());
			});
		}else {
			logger.info("unknow object type");
		}
		
		return this;
	}
}
