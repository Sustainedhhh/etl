package com.outsidethebox.etl.pojo;

public class DBConnection {
	public static final String XPATH = Constants.DBConnectionXPath;

	private String id;
	private String username;
	private String password;
	private String url;
	private String driver;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	@Override
	public String toString() {
		return "DBConnection ID[" + id + "] Username[" + username + "] Password[" + password + "] URL[" + url + "] Driver[" + driver + "]";
	}
}
