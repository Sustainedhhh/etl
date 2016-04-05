package com.etl.rest.api.dto;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Employees")
public class Employees {
	private List<Employee> emps;

	public List<Employee> getEmps() {
		return emps;
	}

	@XmlElement(name="Employee")
	public void setEmps(List<Employee> emps) {
		this.emps = emps;
	}

}
