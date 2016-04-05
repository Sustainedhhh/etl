package com.outsidethebox.etl.pojo;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class RESTfulWebService extends AbstractProcess {
	private String URL;
	private String method;
	private String input;
	private String accept;
	private String contentType;
	boolean defaultAccpet;
	boolean defaultContentType;

	public RESTfulWebService() {
		accept = "application/*";
		contentType = "application/xml";
		defaultAccpet = true;
		defaultContentType = true;
	}

	public String getURL() {
		return URL;
	}

	public void setURL(String uRL) {
		URL = uRL;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		this.input = input;
	}

	public String getAccept() {
		return accept;
	}

	public void setAccept(String accept) {
		this.accept = accept;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public static AbstractProcess fill(Node node) {
		RESTfulWebService rest = new RESTfulWebService();
		NamedNodeMap attrs = node.getAttributes();
		rest.setId(attrs.getNamedItem("id").getNodeValue());
		for (int i = 0; i < node.getChildNodes().getLength(); i++) {
			Node restNode = node.getChildNodes().item(i);
			if (restNode.getNodeType() == Node.ELEMENT_NODE) {
				if (restNode.getNodeName().equals("URL")) {
					rest.setURL(restNode.getTextContent());
					NamedNodeMap urlAttrs = restNode.getAttributes();
					if (urlAttrs != null && urlAttrs.getLength() > 0) {
						Node acceptNode = urlAttrs.getNamedItem("Accept");
						if (acceptNode != null) {
							String accept = acceptNode.getNodeValue();
							rest.setAccept(accept);
							rest.defaultAccpet = false;
						}
						Node contentNode = urlAttrs.getNamedItem("Content-Type");
						if (contentNode != null) {
							String contentType = contentNode.getNodeValue();
							rest.setContentType(contentType);
							rest.defaultContentType = false;
						}
					}

				} else if (restNode.getNodeName().equals("Input")) {
					rest.setInput(restNode.getTextContent());
					if (rest.getInput().startsWith("<")) {
						if(rest.defaultAccpet){
							rest.setAccept("application/xml");
						}
						if(rest.defaultContentType){
							rest.setContentType("application/xml");
						}
					} else if (rest.getInput().startsWith("{") || rest.getInput().startsWith("[")) {
						if(rest.defaultAccpet){
							rest.setAccept("application/json");
						}
						if(rest.defaultContentType){
							rest.setContentType("application/json");
						}
					}
				} else if (restNode.getNodeName().equals("Method")) {
					rest.setMethod(restNode.getTextContent());
				} else if (restNode.getNodeName().equals("Process")) {
					rest.addProcess(Process.fill((restNode)));
				}
			}
		}
		return rest;
	}

	@Override
	public String toString() {
		return "RESFulWebservice ID[" + id + "], URL[" + URL + "], Input[" + input + "], Accpet["+accept+"], Content-Type["+contentType+"]";
	}
}
