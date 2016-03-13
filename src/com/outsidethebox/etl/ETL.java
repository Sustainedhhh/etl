package com.outsidethebox.etl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.outsidethebox.etl.jaxb.Database;
import com.outsidethebox.etl.jaxb.Declartion.Properties.PropertyFile;
import com.outsidethebox.etl.jaxb.Process;
import com.outsidethebox.etl.pojo.AbstractProcess;
import com.outsidethebox.etl.pojo.Declaration;
import com.outsidethebox.etl.pojo.ETLObject;

public class ETL {

	private com.outsidethebox.etl.jaxb.ETL etl;
	private Logger LOGGER = Logger.getLogger(ETL.class);
	private String xml;

	public ETL(File xmlFile) throws ETLException {
		String line = null;
		StringBuilder xml = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(xmlFile));
			while ((line = br.readLine()) != null) {
				xml.append(line);
			}
			br.close();
		} catch (IOException ex) {
			LOGGER.error("Error while reading '" + xmlFile.getName() + "'", ex);
			throw new ETLException(ex);
		}
		init(xml.toString());
	}

	public ETL(String xml) throws ETLException {
		init(xml);
	}

	private void init(String xml) throws ETLException {
		InputStream xmlStream = new ByteArrayInputStream(xml.getBytes());
		if (!validateAgainstXSD(xmlStream)) {
			LOGGER.error("ETL xml file is not valid against schema 'ETL.xsd'");
			throw new ETLException("Not valid against ETL.xsd");
		}
		LOGGER.debug("ETL xml file has been validated.");
		try {
			this.xml = xml;
			ETLObject list = parseXML();
			System.out.println(list);
			etl = Marshaller.unmarshal(xml, com.outsidethebox.etl.jaxb.ETL.class);
		} catch (JAXBException ex) {
			LOGGER.error("ETL xml file is not valid against schema 'ETL.xsd'", ex);
			throw new ETLException(ex);
		}
		LOGGER.debug("ETL object has been unmarshaled.");
	}

	private ETLObject parseXML() throws ETLException {
		try {
			ByteArrayInputStream xmlBytes = new ByteArrayInputStream(xml.getBytes());
			DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document document = documentBuilder.parse(xmlBytes);
			XPath propertyFileXPath = XPathFactory.newInstance().newXPath();
			NodeList propertyFiles = (NodeList) propertyFileXPath.compile(com.outsidethebox.etl.pojo.PropertyFile.XPATH).evaluate(document,
					XPathConstants.NODESET);
			XPath connXPath = XPathFactory.newInstance().newXPath();
			NodeList connections = (NodeList) connXPath.compile(com.outsidethebox.etl.pojo.DBConnection.XPATH).evaluate(document,
					XPathConstants.NODESET);
			List<com.outsidethebox.etl.pojo.PropertyFile> props = new LinkedList<com.outsidethebox.etl.pojo.PropertyFile>();
			List<com.outsidethebox.etl.pojo.DBConnection> conns = new LinkedList<com.outsidethebox.etl.pojo.DBConnection>();
			for (int i = 0; i < propertyFiles.getLength(); i++) {
				com.outsidethebox.etl.pojo.PropertyFile p = new com.outsidethebox.etl.pojo.PropertyFile();
				NamedNodeMap attrs = propertyFiles.item(i).getAttributes();
				p.setId(attrs.getNamedItem("id").getNodeValue());
				p.setPath(attrs.getNamedItem("path").getNodeValue());
				props.add(p);
			}

			for (int i = 0; i < connections.getLength(); i++) {
				com.outsidethebox.etl.pojo.DBConnection p = new com.outsidethebox.etl.pojo.DBConnection();
				NamedNodeMap attrs = connections.item(i).getAttributes();
				p.setId(attrs.getNamedItem("id").getNodeValue());
				p.setUsername(attrs.getNamedItem("username").getNodeValue());
				p.setPassword(attrs.getNamedItem("password").getNodeValue());
				p.setUrl(attrs.getNamedItem("url").getNodeValue());
				p.setDriver(attrs.getNamedItem("driver").getNodeValue());
				conns.add(p);
			}

			XPath proccessXPath = XPathFactory.newInstance().newXPath();
			NodeList procs = (NodeList) proccessXPath.compile(com.outsidethebox.etl.pojo.Process.XPATH).evaluate(document, XPathConstants.NODESET);

			Declaration dec = new Declaration();
			dec.setProperties(props);
			dec.setDbConnections(conns);
			ETLObject etl = new ETLObject();
			etl.setDeclaration(dec);
			etl.setProcesses(fillMProcess(procs));
			return etl;
		} catch (IOException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		} catch (SAXException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		} catch (ParserConfigurationException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		} catch (XPathExpressionException ex) {
			LOGGER.error("Error while parsing ETL xml", ex);
			throw new ETLException(ex);
		}
	}

	private com.outsidethebox.etl.pojo.Process fillProcess(Node procNode) {
		com.outsidethebox.etl.pojo.Process process = new com.outsidethebox.etl.pojo.Process();
		for (int i = 0; i < procNode.getChildNodes().getLength(); i++) {
			if (procNode.getChildNodes().item(i).getNodeType() == Node.ELEMENT_NODE) {
				if (procNode.getChildNodes().item(i).getNodeName().equals("Database")) {
					process.add(fillDatabase(procNode.getChildNodes().item(i)));
				} else if (procNode.getChildNodes().item(i).getNodeName().equals("RESfulWebservice")) {
					// process.put(i+1,
					// fillDatabase(procNode.getChildNodes().item(i)));
					process.add(fillRESTfulWebservice(procNode.getChildNodes().item(i)));
				}
			}

		}
		return process;
	}

	private AbstractProcess fillRESTfulWebservice(Node node) {
		com.outsidethebox.etl.pojo.RESTfulWebService db = new com.outsidethebox.etl.pojo.RESTfulWebService();
		NamedNodeMap attrs = node.getAttributes();
		db.setId(attrs.getNamedItem("id").getNodeValue());
		for(int i=0;i<node.getChildNodes().getLength();i++){
			if(node.getChildNodes().item(i).getNodeType() == Node.ELEMENT_NODE){
				if(node.getChildNodes().item(i).getNodeName().equals("URL")){
					db.setURL(node.getChildNodes().item(i).getTextContent());
				}else if(node.getChildNodes().item(i).getNodeName().equals("Input")){
					db.setInput(node.getChildNodes().item(i).getTextContent());
				}else if(node.getChildNodes().item(i).getNodeName().equals("Process")){
					db.addProcess(fillProcess((node.getChildNodes().item(i))));
				}
			}
		}
		return db;
	}

	private AbstractProcess fillDatabase(Node dbNode) {
		com.outsidethebox.etl.pojo.Database db = new com.outsidethebox.etl.pojo.Database();
		NamedNodeMap attrs = dbNode.getAttributes();
		db.setId(attrs.getNamedItem("id").getNodeValue());
		db.setConnectionID(attrs.getNamedItem("connection-id").getNodeValue());
		for(int i=0;i<dbNode.getChildNodes().getLength();i++){
			if(dbNode.getChildNodes().item(i).getNodeType() == Node.ELEMENT_NODE){
				if(dbNode.getChildNodes().item(i).getNodeName().equals("sql")){
					db.setSQL(dbNode.getChildNodes().item(i).getTextContent());
				}else if(dbNode.getChildNodes().item(i).getNodeName().equals("Process")){
					db.addProcess(fillProcess((dbNode.getChildNodes().item(i))));
				}
			}
		}
		return db;
	}

	private List<com.outsidethebox.etl.pojo.Process> fillMProcess(NodeList node) {
		List<com.outsidethebox.etl.pojo.Process> procs = new LinkedList<com.outsidethebox.etl.pojo.Process>();
		for (int i = 0; i < node.getLength(); i++) {
			if (node.item(i).getNodeType() == Node.ELEMENT_NODE)
				procs.add(fillProcess(node.item(i)));
		}
		return procs;
	}

	private boolean validateAgainstXSD(InputStream xml) {
		try {
			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(new StreamSource(ETL.class.getResourceAsStream("/METL.xsd")));
			Validator validator = schema.newValidator();
			validator.validate(new StreamSource(xml));
			return true;
		} catch (Exception ex) {
			LOGGER.error("Validation error", ex);
			return false;
		}
	}

	public void execute() throws ETLException {
		try {
			LOGGER.debug("Loading <property> from xml...");
			loadProperties();
			LOGGER.debug("Loading <database> from xml...");
			loadDatabases();
		} catch (IOException ex) {
			LOGGER.error("Error while loading properties files", ex);
			throw new ETLException(ex);
		} catch (SQLException ex) {
			LOGGER.error("Error while loading databases connections", ex);
			throw new ETLException(ex);
		} catch (ClassNotFoundException ex) {
			LOGGER.error("Error while loading databases connections", ex);
			throw new ETLException(ex);
		}
		for (com.outsidethebox.etl.jaxb.Process p : etl.getOperations().getProcess()) {
			if (p.getDatabase() != null) {
				Database db = p.getDatabase();
				LOGGER.debug("Query [" + db.getSql() + "] executing with interval [0]...");
				try {
					execute(db, null);
					commitConnections();
					LOGGER.debug("Query executed successfully.");
				} catch (SQLException ex) {
					try {
						rollbackConnections();
						LOGGER.error("Query rolledback.", ex);
					} catch (SQLException e) {
						LOGGER.error("Failed to rollback.", e);
					}
				} catch (IOException ex) {
					throw new ETLException(ex);
				}
			}
		}
	}

	private void execute(Database query, Map<String, Object> keyValue) throws IOException, SQLException {

		// execute and get columns and values for first row
		if (keyValue == null) {
			keyValue = new HashMap<String, Object>();
		}
		Connection conn = getConnection(query.getConnectionId());
		Statement stmt = conn.createStatement();

		String sql = query.getSql().trim();
		String[] prop = ETLUtils.getProperty(sql);
		if (prop != null) {
			sql = getPropery(prop[0], prop[1]);
		} else if (sql.startsWith("{file(") && sql.endsWith((")}"))) {
			String path = sql.substring(sql.indexOf("{file(") + 6, sql.indexOf(")}"));
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = null;
			sql = "";
			while ((line = br.readLine()) != null) {
				sql += line;
			}
			br.close();
		}
		for (Entry<String, Object> e : keyValue.entrySet()) {
			if (e.getValue() == null) {
				sql = sql.replace(e.getKey(), "NULL");
				sql = sql.replace("'" + e.getKey() + "'", "NULL");
			} else {
				sql = sql.replace(e.getKey(), String.valueOf(e.getValue()));
			}
		}
		sql = sql.trim();
		LOGGER.debug("Executing SQL [" + sql + "]...");
		boolean isResultSet = stmt.execute(sql);
		LOGGER.debug("SQL executed.");
		if (isResultSet) {
			ResultSet result = stmt.getResultSet();
			int count = result.getMetaData().getColumnCount();
			while (result.next()) {
				for (int c = 1; c <= count; c++) {
					String colName = result.getMetaData().getColumnName(c);
					Object value = result.getObject(c);
					String key = query.getId() == null || query.getId().length() == 0 ? "?" + colName : "?" + query.getId() + "." + colName;
					keyValue.put(key, value);
				}
				if (query.getProcess() == null || query.getProcess().size() == 0) {
					// execute
					// System.out.println("Execute : " + query.value);
					return;
				}
				for (Process procs : query.getProcess()) {
					if (procs.getDatabase() != null) {
						execute(procs.getDatabase(), keyValue);
					}
				}
			}
			result.close();
		} else {
			stmt.close();
		}
		// System.out.println("[" + query.getId() + "] = Execute : " + sql);
	}

	private Map<String, Properties> PROPERTIES;
	private Map<String, Connection> DATABASES;

	private void loadProperties() throws IOException {
		if (etl.getDeclartion() != null && etl.getDeclartion().getProperties() != null
				&& etl.getDeclartion().getProperties().getPropertyFile() != null && etl.getDeclartion().getProperties().getPropertyFile().size() > 0) {
			PROPERTIES = new HashMap<String, Properties>();
			for (PropertyFile prop : etl.getDeclartion().getProperties().getPropertyFile()) {
				Properties properties = new Properties();
				properties.load(new FileInputStream(prop.getPath()));
				PROPERTIES.put(prop.getId(), properties);
			}
			LOGGER.debug("Loaded [" + PROPERTIES.size() + "] <property>");
		}
	}

	private void loadDatabases() throws SQLException, ClassNotFoundException {
		if (etl.getDeclartion() != null && etl.getDeclartion().getConnections() != null
				&& etl.getDeclartion().getConnections().getConnection() != null && etl.getDeclartion().getConnections().getConnection().size() > 0) {
			DATABASES = new HashMap<String, Connection>();
			for (com.outsidethebox.etl.jaxb.Connection db : etl.getDeclartion().getConnections().getConnection()) {
				Class.forName(db.getDriver());
				Connection conn = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());
				DATABASES.put(db.getId(), conn);
			}
			LOGGER.debug("Loaded [" + DATABASES.size() + "] <database>");
		}
	}

	void rollbackConnections() throws SQLException {
		for (Entry<String, Connection> entry : DATABASES.entrySet()) {
			DATABASES.get(entry.getKey()).rollback();
		}
	}

	void commitConnections() throws SQLException {
		for (Entry<String, Connection> entry : DATABASES.entrySet()) {
			DATABASES.get(entry.getKey()).commit();
		}
	}

	String getPropery(String id, String key) throws IOException {
		Properties prop = PROPERTIES.get(id);
		String value = prop.getProperty(key);
		LOGGER.debug("Property{" + id + "} [" + key + "] = '" + value + "'");
		return value;
	}

	Connection getConnection(String id) {
		return DATABASES.get(id);
	}
}
