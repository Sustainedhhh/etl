package com.outsidethebox.etl.pojo;

import java.util.List;

public class ETLObject {
	private Declaration declaration;
	private List<Process> processes;

	public Declaration getDeclaration() {
		return declaration;
	}

	public void setDeclaration(Declaration declaration) {
		this.declaration = declaration;
	}

	public List<Process> getProcesses() {
		return processes;
	}

	public void setProcesses(List<Process> processes) {
		this.processes = processes;
	}

	@Override
	public String toString() {
		return "ETL Declaration <"+declaration+"> \nProcess <"+processes+">";
	}
}
