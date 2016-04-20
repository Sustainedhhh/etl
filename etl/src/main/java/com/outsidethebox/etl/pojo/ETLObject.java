package com.outsidethebox.etl.pojo;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.outsidethebox.etl.ETL;
import com.outsidethebox.etl.ETLException;
import com.outsidethebox.etl.KeyNotFoundException;

public class ETLObject {
	private static Logger LOGGER = Logger.getLogger(ETL.class);
	private List<Process> processes;
	private Map<String, Properties> properties;
	private Map<String, Connection> DBConnections;

	private ETLObject(Document document) throws XPathException, IOException, ClassNotFoundException, SQLException {
		initProperties(document);
		initDBConnections(document);
		processes = Process.fill(document);

	}

	private void initProperties(Document document) throws XPathException, IOException {
		properties = new HashMap<String, Properties>();
		LOGGER.debug("Loading properties...");
		XPath propertyFileXPath = XPathFactory.newInstance().newXPath();
		NodeList propertyFileNodeList = (NodeList) propertyFileXPath.compile(Constants.ProprtyFileXPath).evaluate(document, XPathConstants.NODESET);
		for (int i = 0; i < propertyFileNodeList.getLength(); i++) {
			NamedNodeMap attrs = propertyFileNodeList.item(i).getAttributes();
			String id = attrs.getNamedItem("id").getNodeValue();
			String path = attrs.getNamedItem("path").getNodeValue();
			Properties props = new Properties();
			props.load(new FileInputStream(path));
			properties.put(id, props);
		}
		LOGGER.debug("Loaded [" + properties.size() + "] properties");
	}

	private void initDBConnections(Document document) throws XPathException, ClassNotFoundException, SQLException {
		DBConnections = new HashMap<String, Connection>();
		LOGGER.debug("Loading database connections...");
		XPath connXPath = XPathFactory.newInstance().newXPath();
		NodeList connectionsNodeList = (NodeList) connXPath.compile(Constants.DBConnectionXPath).evaluate(document, XPathConstants.NODESET);
		for (int i = 0; i < connectionsNodeList.getLength(); i++) {
			NamedNodeMap attrs = connectionsNodeList.item(i).getAttributes();
			String id = attrs.getNamedItem("id").getNodeValue();
			String username = attrs.getNamedItem("username").getNodeValue();
			String password = attrs.getNamedItem("password").getNodeValue();
			String url = attrs.getNamedItem("url").getNodeValue();
			String driver = attrs.getNamedItem("driver").getNodeValue();
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, username, password);
			DBConnections.put(id, conn);
		}
		LOGGER.debug("Loaded [" + DBConnections.size() + "] database connections");
	}

	public static ETLObject parse(String xml) throws ETLException {
		try {
			ByteArrayInputStream xmlBytes = new ByteArrayInputStream(xml.getBytes());
			DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document document = documentBuilder.parse(xmlBytes);
			return new ETLObject(document);
		} catch (IOException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		} catch (SAXException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		} catch (ParserConfigurationException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		} catch (XPathException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		} catch (SQLException ex) {
			LOGGER.error("Error while parsing ETL database connections from xml", ex);
			throw new ETLException(ex);
		} catch (ClassNotFoundException ex) {
			LOGGER.error("Error while parsing ETL database connections from xml", ex);
			throw new ETLException(ex);
		}
	}

	public String getPropery(String id, String key) throws IOException, KeyNotFoundException {
		if (!properties.containsKey(key)) {
			throw new KeyNotFoundException(key);
		}
		Properties prop = properties.get(id);
		String value = prop.getProperty(key);
		LOGGER.debug("Property{" + id + "} [" + key + "] = '" + value + "'");
		return value;
	}

	public Connection getConnection(String id) throws KeyNotFoundException {
		if (!DBConnections.containsKey(id)) {
			throw new KeyNotFoundException(id);
		}
		return DBConnections.get(id);
	}

	/**
	 * Checks the input strToEval is a property matches {property(propID, key)}
	 * or a file matches {file(path)}
	 * 
	 * @param strToEval
	 * @return The Property value if property , File content if is file,
	 *         otherwise same input strToEval
	 * @throws KeyNotFoundException
	 *             if property key is not exists
	 * @throws IOException
	 *             failed to read from file
	 */
	public String eval(String strToEval) throws IOException, KeyNotFoundException {
		if (strToEval != null) {
			try {
				Pattern p = Pattern.compile("\\{property\\((.*)[,]{1}(.*)\\)\\}", Pattern.CASE_INSENSITIVE);
				Matcher matcher = p.matcher(strToEval);
				if (matcher.find()) {
					String propID = matcher.group(1);
					String key = matcher.group(2);
					return getPropery(propID, key);
				}
				p = Pattern.compile("\\{file\\((.*)\\)\\}", Pattern.CASE_INSENSITIVE);
				matcher = p.matcher(strToEval);
				if (matcher.find()) {
					String path = matcher.group(1);
					return IOUtils.toString(new FileInputStream(path));
				}
			} catch (PatternSyntaxException ex) {
				LOGGER.error("Error while parsing ["+strToEval+"]",ex);
			}
			return strToEval;
		}
		return null;
	}

	public void rollbackConnections() throws SQLException {
		for (Entry<String, Connection> entry : DBConnections.entrySet()) {
			DBConnections.get(entry.getKey()).rollback();
		}
	}

	public void commitConnections() throws SQLException {
		for (Entry<String, Connection> entry : DBConnections.entrySet()) {
			DBConnections.get(entry.getKey()).commit();
		}
	}

	public void closeConnections() {
		if (DBConnections.size() >= 0) {
			LOGGER.debug("Closing database connections...");
		}
		for (Entry<String, Connection> entry : DBConnections.entrySet()) {
			try {
				LOGGER.debug("Closing database connection[" + entry.getKey() + "]");
				DBConnections.get(entry.getKey()).close();
				LOGGER.debug("Database connection[" + entry.getKey() + "] Closed");
			} catch (SQLException ex) {
				LOGGER.error("Error while closing connection[" + entry.getKey() + "]", ex);
			}
		}
		if (DBConnections.size() >= 0) {
			LOGGER.debug("Database connections closed.");
		}
	}

	public Map<String, Properties> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Properties> properties) {
		this.properties = properties;
	}

	public Map<String, Connection> getDBConnections() {
		return DBConnections;
	}

	public void setDBConnections(Map<String, Connection> dBConnections) {
		DBConnections = dBConnections;
	}

	public List<Process> getProcesses() {
		return processes;
	}

	public void setProcesses(List<Process> processes) {
		this.processes = processes;
	}

	public boolean hasProperties() {
		return processes != null && !processes.isEmpty();
	}

	@Override
	public String toString() {
		return "ETL Declaration [Properties {" + properties + "} \nConnections {" + DBConnections + "}] \nProcess <" + processes + ">";
	}
}
