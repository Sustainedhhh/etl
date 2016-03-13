package com.outsidethebox.etl.pojo;

public class Database extends AbstractProcess {

	private String connectionID;
	private String SQL;

	public String getConnectionID() {
		return connectionID;
	}

	public void setConnectionID(String connectionID) {
		this.connectionID = connectionID;
	}

	public String getSQL() {
		return SQL;
	}

	public void setSQL(String sQL) {
		SQL = sQL;
	}

	@Override
	public String toString() {
		return "Database ID[" + id + "] Connection-ID[" + connectionID + "] SQL[" + SQL + "] Process :=>[" + (process==null?"":process.toString() )+ "]";
	}
}
