package com.outsidethebox.etl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class Main {

	public static void main(String[] args) throws Exception {
		if (args == null || args.length == 0) {
			try {
				File file = new File("./etl-web-0.0.1-SNAPSHOT.war");
				if (!file.exists()) {
					InputStream input = new FileInputStream(Main.class.getResource("/etl-web-0.0.1-SNAPSHOT.war").getFile());
					OutputStream writer = new FileOutputStream(file);
					IOUtils.copyLarge(input,writer);
					IOUtils.closeQuietly(input);
					IOUtils.closeQuietly(writer);
				}
				startServer(file);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if(args.length == 1) {
			File file = new File(args[0]);
			startServer(file);
		}else{
			System.out.println("Please specifiy war file");
		}
	}
	
	private static void startServer(File file){
		Server server = new Server(9090);
		try {
			WebAppContext webapp = new WebAppContext();
			webapp.setContextPath("/");
			webapp.setWar(file.getPath());
			System.out.println("chceking jett folder...");
			File jettyDir = new File("./jetty");
			if(!jettyDir.exists()){
				jettyDir.mkdir();
				System.out.println("jetty folder created");
			}
			webapp.setTempDirectory(jettyDir);
			webapp.setAttribute("org.eclipse.jetty.webapp.basetempdir", jettyDir);
			webapp.setAttribute("javax.servlet.context.tempdir", jettyDir);
			server.setHandler(webapp);
			server.start();
			System.out.println("Server started at 9090");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
