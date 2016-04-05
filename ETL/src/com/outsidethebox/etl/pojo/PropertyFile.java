package com.outsidethebox.etl.pojo;

import org.w3c.dom.NamedNodeMap;

public class PropertyFile {
	public static final String XPATH = Constants.ProprtyFileXPath;
	private String id;
	private String path;

	public PropertyFile() {
	}
	
	public PropertyFile(NamedNodeMap attrs) {
		setId(attrs.getNamedItem("id").getNodeValue());
		setPath(attrs.getNamedItem("path").getNodeValue());
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public String toString() {
		return "Property-File ID [" + id + "] Path [" + path + "]";
	}
}
