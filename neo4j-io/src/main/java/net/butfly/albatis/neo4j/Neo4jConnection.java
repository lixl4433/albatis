package net.butfly.albatis.neo4j;

import java.io.IOException;
import java.util.List;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

public class Neo4jConnection extends DataConnection<org.neo4j.driver.Driver> {
	
	protected org.neo4j.driver.Driver driver;
	
	public Neo4jConnection(URISpec uri) throws IOException {
		super(uri, 7687, "neo4j", "bolt");
	}

	@Override
	protected org.neo4j.driver.Driver initialize(URISpec uri) {
		driver = GraphDatabase.driver(uri.getSchema()+"://"+uri.getHost()+"/"+uri.getFile(), AuthTokens.basic(uri.getUsername(), uri.getPassword()));
		return driver;
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<Neo4jConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public Neo4jConnection connect(URISpec uriSpec) throws IOException {
			return new Neo4jConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("neo4j", "bolt");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Neo4jOutput outputRaw(TableDesc... table) throws IOException {
		return new Neo4jOutput("neo4jOutput", this);
	}
}
