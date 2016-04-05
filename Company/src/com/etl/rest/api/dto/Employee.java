package com.etl.rest.api.dto;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonProperty;

@XmlRootElement(name = "Employee")
public class Employee {
	private int id;
	private String name;

	public Employee() {
	}

	public Employee(int id, String name) {
		super();
		this.id = id;
		this.name = name;
	}

	@JsonProperty(value="ID")
	public int getId() {
		return id;
	}

	@XmlElement(name = "ID")
	public void setId(int id) {
		this.id = id;
	}

	@JsonProperty(value="Name")
	public String getName() {
		return name;
	}

	@XmlElement(name = "Name")
	public void setName(String name) {
		this.name = name;
	}

}
