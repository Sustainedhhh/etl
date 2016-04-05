package com.outsidethebox.etl;

public class ETLException extends Exception {
	private static final long serialVersionUID = 1L;

	public ETLException(String message){
		super(message);
	}
	
	public ETLException(Throwable t){
		super(t);
	}
}
