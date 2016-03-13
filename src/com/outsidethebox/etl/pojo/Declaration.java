package com.outsidethebox.etl.pojo;

import java.util.List;

public class Declaration {
	private List<PropertyFile> properties;
	private List<DBConnection> dbConnections;

	public List<PropertyFile> getProperties() {
		return properties;
	}

	public void setProperties(List<PropertyFile> properties) {
		this.properties = properties;
	}

	public List<DBConnection> getDbConnections() {
		return dbConnections;
	}

	public void setDbConnections(List<DBConnection> dbConnections) {
		this.dbConnections = dbConnections;
	}

	@Override
	public String toString() {
		return "Properties {" + properties + "} \nConnections {" + dbConnections + "}";
	}
}
