package com.outsidethebox.etl.pojo;

import java.util.LinkedList;
import java.util.List;

import org.w3c.dom.Node;

public class Process {
	public static final String XPATH = Constants.ProcessXPath;

	private String id;
	private List<AbstractProcess> subProcess;

	public Process() {
		subProcess = new LinkedList<AbstractProcess>();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}



	public void add(AbstractProcess object) {
		subProcess.add(object);
	}
	
	public List<AbstractProcess> getSubProcesses(){
		return subProcess;
	}
	
	public static Process fill(Node node) {
		Process process = new Process();
		process.setId(node.getAttributes().getNamedItem("id").getNodeValue());
		for (int i = 0; i < node.getChildNodes().getLength(); i++) {
			Node procNode =node.getChildNodes().item(i);
			if (procNode.getNodeType() == Node.ELEMENT_NODE) {
				if (procNode.getNodeName().equals("Database")) {
					process.add(Database.fill(procNode));
				} else if (procNode.getNodeName().equals("RESTfulWebservice")) {
					process.add(RESTfulWebService.fill(procNode));
				}
			}
		}
		return process;
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
