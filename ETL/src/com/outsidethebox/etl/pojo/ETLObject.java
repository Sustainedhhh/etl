package com.outsidethebox.etl.pojo;

import java.util.List;

public class ETLObject {
	private List<PropertyFile> properties;
	private List<DBConnection> dbConnections;
	private List<Process> processes;
	
	public List<PropertyFile> getProperties() {
		return properties;
	}

	public void setProperties(List<PropertyFile> properties) {
		this.properties = properties;
	}

	public List<DBConnection> getDBConnections() {
		return dbConnections;
	}

	public void setDBConnections(List<DBConnection> dbConnections) {
		this.dbConnections = dbConnections;
	}

	
	public List<Process> getProcesses() {
		return processes;
	}

	public void setProcesses(List<Process> processes) {
		this.processes = processes;
	}

	public boolean hasDBConnections(){
		return dbConnections!=null && !dbConnections.isEmpty();
	}
	
	public boolean hasProperties(){
		return processes!=null && !processes.isEmpty();
	}
	
	@Override
	public String toString() {
		return "ETL Declaration [Properties {" + properties + "} \nConnections {" + dbConnections + "}] \nProcess <"+processes+">";
	}
}
