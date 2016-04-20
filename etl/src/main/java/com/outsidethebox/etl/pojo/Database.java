package com.outsidethebox.etl.pojo;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

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

	public static AbstractProcess fill(Node node) {
		Database db = new Database();
		NamedNodeMap attrs = node.getAttributes();
		db.setId(attrs.getNamedItem("id").getNodeValue());
		db.setConnectionID(attrs.getNamedItem("connection-id").getNodeValue());
		for (int i = 0; i < node.getChildNodes().getLength(); i++) {
			Node dbNode =node.getChildNodes().item(i); 
			if (dbNode.getNodeType() == Node.ELEMENT_NODE) {
				if (dbNode.getNodeName().equals("sql")) {
					db.setSQL(dbNode.getTextContent());
				} else if (dbNode.getNodeName().equals("Process")) {
					db.addProcess(Process.fill((dbNode)));
				}
			}
		}
		return db;
	}
	
	@Override
	public String toString() {
		return "Database ID[" + id + "] Connection-ID[" + connectionID + "] SQL[" + SQL + "]";
	}
}
