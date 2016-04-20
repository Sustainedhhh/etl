package com.outsidethebox.etl;

public class ConnectionIDNotFoundException extends Exception {

	private static final long serialVersionUID = -3457355873874997175L;

	public ConnectionIDNotFoundException(String connID){
		super("ConnectionID ["+connID+"] is not found, please check the defined connection IDs at ETL file");
	}
	
	
}
