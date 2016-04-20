package com.outsidethebox.etl;

import java.io.File;
import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.apache.log4j.PropertyConfigurator;

import com.outsidethebox.etl.ETL;
import com.outsidethebox.etl.ETLException;

public class ETLtest extends TestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		PropertyConfigurator.configure(ETL.class.getResource("/log4j.properties").getFile());
	}
	
	public void testExecute() {
		try {
			ETL etl = new ETL(new File(ETL.class.getResource("/ETL.xml").toURI()));
			etl.execute();
			assertEquals(true, true);
		} catch (ETLException e) {
			fail("Failed to execute ETL; "+e.getMessage());
		} catch (URISyntaxException e) {
			fail("Failed to get ETL.xml file; "+e.getMessage());
		}
	}

}
