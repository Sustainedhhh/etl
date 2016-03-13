package com.outsidethebox.etl.pojo;

public class RESTfulWebService extends AbstractProcess {
	private String URL;
	private String input;

	public String getURL() {
		return URL;
	}

	public void setURL(String uRL) {
		URL = uRL;
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		this.input = input;
	}

	@Override
	public String toString() {
		return "RESFulWebservice ID[" + id + "] URL[" + URL + "] Input[" + input + "] Process :=>[" + (process==null?"":process.toString() )+ "]";
	}
}
