import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import com.outsidethebox.etl.ETL;
import com.outsidethebox.etl.ETLException;

public class Main {

	public static void main(String[] args) throws  ETLException {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("G:/log4j.properties");
		File file = new File("G:/outside-the-box/etl-lib/ETL/xsd/ETL.xml");
		ETL etl = new ETL(file);
		etl.execute();
		System.out.println("Finished");
	}

}
