package com.outsidethebox.etl.pojo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Process {
	public static final String XPATH = Constants.ProcessXPath;

	private List<AbstractProcess> subProcess;

	public Process() {
		subProcess = new LinkedList<AbstractProcess>();
	}

	public void add(AbstractProcess object) {
		subProcess.add(object);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (AbstractProcess proc : subProcess) {
			sb.append(proc.getClass().getSimpleName()).append(" => ").append((proc == null ? "" : proc.toString()));
		}
		return "Process [" + sb.toString() + "]";
	}
}
