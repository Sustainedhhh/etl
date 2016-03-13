package com.outsidethebox.etl.pojo;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractProcess {
	protected String id;
	protected List<Process> process;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Process> getProcess() {
		return process;
	}

	public void setProcess(List<Process> process) {
		this.process = process;
	}

	public void addProcess(Process proc) {
		if (process == null) {
			process = new LinkedList<Process>();
		}
		process.add(proc);
	}
}
