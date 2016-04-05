package com.etl.rest.api;

import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.etl.rest.api.dto.Employee;
import com.etl.rest.api.dto.Employees;
import com.etl.rest.api.dto.Error;

@Path("/Employee")
public class EmployeeService {

	static List<Employee> emps = new LinkedList<Employee>();
	static {
		emps.add(new Employee(1, "Employee1"));
		emps.add(new Employee(2, "Employee2"));
		emps.add(new Employee(3, "Employee3"));
		emps.add(new Employee(4, "Employee4"));
	}

	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_XML)
	public Response getEmployees() {
		Employees empss = new Employees();
		empss.setEmps(emps);
		return Response.status(200).entity(empss).build();
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.TEXT_XML)
	public Response getEmployeeByID(@PathParam("id") int id) {
		if(id >= emps.size()){
			com.etl.rest.api.dto.Error err = new Error();
			err.setId("00001");
			err.setMessage("Can not find this ID ["+id+"]");
			return Response.status(200).entity(err).build();
		}
		return Response.status(200).entity(emps.get(id)).build();

	}
	
	@POST
	@Path("/add")
	@Consumes({MediaType.APPLICATION_XML})
	@Produces({MediaType.APPLICATION_XML})
	public Response addEmployee(Employee emp){
		if(emp==null){
			return Response.status(200).entity("<Error/>").build();
		}
		Employees empss = new Employees();
		empss.setEmps(emps);
		 return Response.status(200).entity(empss).build();
	}
	
	@POST
	@Path("/add")
	@Consumes({MediaType.APPLICATION_JSON})
	@Produces({MediaType.APPLICATION_JSON})
	public Response addEmployeeJSON(Employee emp){
		if(emp==null){
			return Response.status(200).entity("<Error/>").build();
		}
		Employees empss = new Employees();
		empss.setEmps(emps);
		 return Response.status(200).entity(emps).build();
	}
}
