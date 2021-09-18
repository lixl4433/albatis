package net.butfly.albatis.arangodb;

import org.apache.commons.lang3.StringUtils;

//[{"_key":"1","_id":"friends/1","_from":"people/1","_to":"people/2","_rev":"_bxgJIFi---","rel":"朋友"}]
public class ArangoDBEdge  extends ArangoDBEntity{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public Object from;
	public Object to;
	public Object key;
	private static final String keyField = "_key";
	private static final String fromField = "_from";
	private static final String toField = "_to";
	public String table;
	public static ArangoDBEdge of() {
        return new ArangoDBEdge();
    }
    public static  ArangoDBEdge of(Object fieldName, Object fieldValue) {
    	ArangoDBEdge map = of();
        if (null != fieldName && null != fieldValue) map.put(fieldName, fieldValue);
        return map;
    }
    
    public String getKeyField() {
    	return keyField;
    }
    public String getFromField() {
    	return fromField;
    }
    public String getToField() {
    	return toField;
    }
    public String getFromTable() {
    	String s = null;
    	if(null != from && !StringUtils.isEmpty(s = from.toString()) && s.indexOf("/")>0) {
    		return s.split("/")[0];
    	}
    	return null;
    }
    public String getToTable() {
    	String s = null;
    	if(null != to && !StringUtils.isEmpty(s = to.toString()) && s.indexOf("/")>0) {
    		return s.split("/")[0];
    	}
    	return null;
    }
}
