package com.outsidethebox.etl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.outsidethebox.etl.jaxb.OnError;
import com.outsidethebox.etl.jaxb.Query;

class ETLQuery extends Thread {
	private static final int MINUTE = 60 * 1000;
	private ETL etl;
	private Query query;
	private int interval;

	public ETLQuery(ETL etl, Query q, int interval) {
		this.etl = etl;
		this.query = q;
		this.interval = interval;
	}

	@Override
	public void run() {
		while (true) {
			try {
				execute(query, null, query.getOnError() == null ? OnError.CONTINUE : query.getOnError());
				etl.commitConnections();
				System.out.println("Finished ");
			} catch (SQLException ex) {
				try {
					etl.rollbackConnections();
				} catch (SQLException e) {
					// TODO log error
					// e.printStackTrace();
				}
			} catch (IOException ex) {
			}
			try {
				Thread.sleep(interval * MINUTE);
			} catch (InterruptedException e) {
			}
		}
	}

	private void execute(Query query, Map<String, Object> keyValue, OnError onError) throws IOException, SQLException {

		// execute and get columns and values for first row
		if (keyValue == null) {
			keyValue = new HashMap<String, Object>();
		}
		Connection conn = etl.getConnection(query.getDatabase());
		if (onError == OnError.ROLLBACK) {
			conn.setAutoCommit(false);
		}
		Statement stmt = conn.createStatement();

		String sql = query.getSql().trim();
		String[] prop = ETLUtils.getProperty(sql);
		if (prop != null) {
			sql = etl.getPropery(prop[0], prop[1]);
		} else if (sql.startsWith("{file(") && sql.endsWith((")}"))) {
			String path = sql.substring(sql.indexOf("{file(") + 6, sql.indexOf(")}"));
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = null;
			sql = "";
			while ((line = br.readLine()) != null) {
				sql += line;
			}
			br.close();
		}
		for (Entry<String, Object> e : keyValue.entrySet()) {
			if (e.getValue() == null) {
				sql = sql.replace("'" + e.getKey() + "'", "NULL");
				sql = sql.replace(e.getKey(), "NULL");
			} else {
				sql = sql.replace(e.getKey(), String.valueOf(e.getValue()));
			}
		}
		sql = sql.trim();
		boolean isResultSet = stmt.execute(sql);
		if (isResultSet) {
			ResultSet result = stmt.getResultSet();
			int count = result.getMetaData().getColumnCount();
			while (result.next()) {
				for (int c = 1; c <= count; c++) {
					keyValue.put(
							query.getId() == null || query.getId().length() == 0 ? "?" + result.getMetaData().getColumnName(c) : "?" + query.getId()
									+ "." + result.getMetaData().getColumnName(c), result.getObject(c));
				}
				if (query.getQuery() == null || query.getQuery().size() == 0) {
					// execute
					// System.out.println("Execute : " + query.value);
					return;
				}
				for (Query q : query.getQuery()) {

					execute(q, keyValue, onError);
				}
			}
			result.close();
		} else {
			stmt.close();
		}
		// System.out.println("[" + query.getId() + "] = Execute : " + sql);
	}
}
