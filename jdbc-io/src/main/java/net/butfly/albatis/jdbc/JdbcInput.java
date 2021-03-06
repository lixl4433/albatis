package net.butfly.albatis.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class JdbcInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -8772607845694039875L;
	private final JdbcConnection jdbc;
	//
	private Connection conn;
	private PreparedStatement stat;
	private ResultSet rs;
	// resultset infomation
	private boolean next;
	private ResultSetMetaData meta;
	private int colCount;
	private String[] colNames;
	private String tableName;

	public JdbcInput(final String name, JdbcConnection conn) throws IOException, SQLException {
		super(name);
		this.jdbc = conn;
		opening(this::openJdbc);
		closing(this::closeJdbc);
	}

	public JdbcInput(final String name, JdbcConnection conn, String tableName) throws IOException, SQLException {
		super(name);
		this.tableName = tableName;
		this.jdbc = conn;
		query("select * from " + tableName);
		opening(this::openJdbc);
		closing(this::closeJdbc);
	}
	
	public JdbcInput(final String name, JdbcConnection conn, String tableName, String sql) throws IOException, SQLException {
		super(name);
		this.tableName = tableName;
		this.jdbc = conn;
		query(sql);
		opening(this::openJdbc);
		closing(this::closeJdbc);
	}

	public void query(String sql, Object... params) throws SQLException {
		logger().debug("[" + name + "] query begin...");
		conn = jdbc.client.getConnection();
		conn.setAutoCommit(false);
		stat = conn.prepareStatement(sql);
		stat.setFetchDirection(ResultSet.FETCH_FORWARD);
		stat.setFetchSize(100);
		int i = 1;
		for (Object p : params)
			stat.setObject(i++, p);
	}

	private void parseMeta() throws SQLException {
		meta = rs.getMetaData();
		colCount = meta.getColumnCount();
		colNames = new String[colCount];
		for (int i = 0; i < colCount; i++) {
			int j = i + 1;
			colNames[i] = meta.getColumnLabel(j);
			int ctype = meta.getColumnType(j);
			logger().debug("ResultSet col[" + j + ":" + colNames[i] + "]: " + meta.getColumnClassName(j) + "/" + meta.getColumnTypeName(j)
					+ "[" + ctype + "]");
		}
	}

	private void openJdbc() {
		long now = System.currentTimeMillis();
		try {
			rs = stat.executeQuery();
			logger().debug("Query spent: " + (System.currentTimeMillis() - now) + " ms.");
			next = rs.next();
			parseMeta();
		} catch (SQLException e) {
			logger().error("SQL Error on JDBC opening", e);
			rs = null;
			next = false;
		}
	}

	private void closeJdbc() {
		try {
			rs.close();
		} catch (SQLException e) {}
		try {
			stat.close();
		} catch (SQLException e) {}
		try {
			conn.close();
		} catch (SQLException e) {}
		jdbc.close();
	}

	@Override
	public boolean empty() {
		return !next;
	}

	private final ReentrantLock lock = new ReentrantLock();

	@Override
	public Rmap dequeue() {
		if (!next || !lock.tryLock()) return null;
		try {
			Map<String, Object> r = new HashMap<>();
			try {
				for (int i = 0; i < colCount; i++) {
					Object v;
					try {
						v = rs.getObject(i + 1);
					} catch (SQLException e) {
						logger().error("SQL Error on JDBC column fetching", e);
						v = null;
					}
					if (null != v) r.put(colNames[i], v);
				}
				return new Rmap(tableName, r);
			} finally {
				try {
					next = rs.next();
				} catch (SQLException e) {
					logger().error("SQL Error on JDBC row nexting", e);
					next = false;
				}
			}
		} finally {
			lock.unlock();
		}
	}
}
