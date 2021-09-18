package net.butfly.albatis.arangodb;

public class ArangoDBNode extends ArangoDBEntity{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public Object key;
	private static final String keyField = "_key";
	public String table;
	
	public static ArangoDBNode of() {
        return new ArangoDBNode();
    }
    public static ArangoDBNode of(Object fieldName, Object fieldValue) {
    	ArangoDBNode map = of();
        if (null != fieldName && null != fieldValue) map.put(fieldName, fieldValue);
        return map;
    }
    
    public String getKeyField() {
    	return keyField;
    }
}
