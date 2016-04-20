package com.outsidethebox.etl;

public class KeyNotFoundException extends Exception {

	private static final long serialVersionUID = -3457355873874997175L;

	public KeyNotFoundException(String key){
		super("ID ["+key+"] is not found, please check the defined <Declaration> IDs at ETL file");
	}
	
	
}
