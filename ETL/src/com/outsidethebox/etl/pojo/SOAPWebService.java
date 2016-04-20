package com.outsidethebox.etl.pojo;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class SOAPWebService extends AbstractProcess {

	private String URL;
	private String action;
	private String input;

	public String getURL() {
		return URL;
	}

	public void setURL(String uRL) {
		URL = uRL;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		this.input = input;
	}

	public static AbstractProcess fill(Node node) {
		SOAPWebService soap = new SOAPWebService();
		NamedNodeMap attrs = node.getAttributes();
		soap.setId(attrs.getNamedItem("id").getNodeValue());
		for (int i = 0; i < node.getChildNodes().getLength(); i++) {
			Node soapNode = node.getChildNodes().item(i);
			if (soapNode.getNodeType() == Node.ELEMENT_NODE) {
				if (soapNode.getNodeName().equals("Input")) {
					soap.setInput(soapNode.getTextContent());
				} else if (soapNode.getNodeName().equals("Action")) {
					soap.setAction(soapNode.getNodeValue());
				}
			}
		}
		return soap;
	}
}
