package net.butfly.albatis.arangodb;

import java.text.Format;
import java.text.MessageFormat;
import java.util.Map;
import java.util.stream.Collectors;

public class ArangoDBLanguage {
	
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
