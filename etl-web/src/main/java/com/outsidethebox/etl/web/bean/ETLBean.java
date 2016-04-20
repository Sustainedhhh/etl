package com.outsidethebox.etl.web.bean;

import java.util.ArrayList;
import java.util.List;

import javax.faces.bean.ManagedBean;

import org.primefaces.extensions.event.CompleteEvent;

import com.outsidethebox.etl.ETL;
import com.outsidethebox.etl.ETLException;

@ManagedBean(name="etlBean")
public class ETLBean {
	private String username;
	private String password;
	private boolean error;
	private String errorMessage;
	private String content;
	ETL etl;

	public ETLBean() {
	}

	public List<String> complete(final CompleteEvent event) {
		final ArrayList<String> suggestions = new ArrayList<String>();

		suggestions.add(event.getContext());
		suggestions.add(event.getToken());
		suggestions.add("ETL");
		suggestions.add("Process");
		suggestions.add("Database");
		suggestions.add("RESTfulWebservice");
		suggestions.add("SOAPWebservice");
		suggestions.add("Declartion");
		suggestions.add("Connections");
		suggestions.add("Connection");

		return suggestions;
	}

	public boolean isError() {
		return error;
	}

	public void setError(boolean error) {
		this.error = error;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
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

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public void execute() {
		try {
			etl = new ETL(content);
			content = etl.execute();
		} catch (ETLException e) {
			error = true;
			errorMessage = e.getMessage();
		}
	}
}
