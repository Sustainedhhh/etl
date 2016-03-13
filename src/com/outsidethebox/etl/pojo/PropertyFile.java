package com.outsidethebox.etl.pojo;

public class PropertyFile {
	public static final String XPATH = Constants.ProprtyFileXPath;
	private String id;
	private String path;

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
