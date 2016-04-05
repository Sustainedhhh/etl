package com.outsidethebox.etl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.outsidethebox.etl.pojo.AbstractProcess;
import com.outsidethebox.etl.pojo.DBConnection;
import com.outsidethebox.etl.pojo.Database;
import com.outsidethebox.etl.pojo.ETLObject;
import com.outsidethebox.etl.pojo.PropertyFile;
import com.outsidethebox.etl.pojo.RESTfulWebService;

public class ETL {

	private ETLObject etl;
	private Logger LOGGER = Logger.getLogger(ETL.class);

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
		parseXML(xml);
		LOGGER.debug("ETL object has been unmarshaled.");
	}

	/**
	 * Validate XML with ETL.xsd Schema
	 * 
	 * @param xml
	 *            Input to validate with
	 * @return true if the XML is valid, otherwise false
	 */
	private boolean validateAgainstXSD(InputStream xml) {
		try {
			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(new StreamSource(ETL.class.getResourceAsStream("/ETL.xsd")));
			Validator validator = schema.newValidator();
			validator.validate(new StreamSource(xml));
			return true;
		} catch (Exception ex) {
			LOGGER.error("Validation error", ex);
			return false;
		}
	}

	/**
	 * Parse XML and fill in ETLObject 'etl'
	 * 
	 * @param xml
	 *            Input 'etl.xml'
	 * @throws ETLException
	 */
	private void parseXML(String xml) throws ETLException {
		try {
			ByteArrayInputStream xmlBytes = new ByteArrayInputStream(xml.getBytes());
			DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document document = documentBuilder.parse(xmlBytes);

			/* Get PropertyFile from XML */
			XPath propertyFileXPath = XPathFactory.newInstance().newXPath();
			NodeList propertyFileNodeList = (NodeList) propertyFileXPath.compile(PropertyFile.XPATH).evaluate(document, XPathConstants.NODESET);
			List<PropertyFile> propertyFiles = new LinkedList<PropertyFile>();
			for (int i = 0; i < propertyFileNodeList.getLength(); i++) {
				NamedNodeMap attrs = propertyFileNodeList.item(i).getAttributes();
				propertyFiles.add(new PropertyFile(attrs));
			}

			/* Get Connections from XML */
			XPath connXPath = XPathFactory.newInstance().newXPath();
			NodeList connectionsNodeList = (NodeList) connXPath.compile(DBConnection.XPATH).evaluate(document, XPathConstants.NODESET);
			List<DBConnection> connections = new LinkedList<DBConnection>();
			for (int i = 0; i < connectionsNodeList.getLength(); i++) {
				NamedNodeMap attrs = connectionsNodeList.item(i).getAttributes();
				connections.add(new DBConnection(attrs));
			}

			/* Get Processes from XML */
			XPath proccessXPath = XPathFactory.newInstance().newXPath();
			NodeList processNodeList = (NodeList) proccessXPath.compile(com.outsidethebox.etl.pojo.Process.XPATH).evaluate(document,
					XPathConstants.NODESET);
			List<com.outsidethebox.etl.pojo.Process> processes = new LinkedList<com.outsidethebox.etl.pojo.Process>();
			for (int i = 0; i < processNodeList.getLength(); i++) {
				if (processNodeList.item(i).getNodeType() == Node.ELEMENT_NODE)
					processes.add(com.outsidethebox.etl.pojo.Process.fill(processNodeList.item(i)));
			}

			etl = new ETLObject();
			etl.setProperties(propertyFiles);
			etl.setDBConnections(connections);
			etl.setProcesses(processes);
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
		processIterator(etl.getProcesses(), null);
		closeConnections();
	}

	private void closeConnections() {
		for (Entry<String, Connection> entry : DATABASES.entrySet()) {
			try {
				DATABASES.get(entry.getKey()).close();
			} catch (SQLException ex) {
				LOGGER.error("Error while closing connections",ex);
			}
		}
	}

	private void execute(RESTfulWebService rest, Map<String, Object> keyValue) throws ETLException, JSONException {
		if (keyValue == null) {
			keyValue = new HashMap<String, Object>();
		}
		try {
			String restURL = rest.getURL().trim();
			LOGGER.debug("[RAW] REST URL : " + restURL);
			for (Entry<String, Object> e : keyValue.entrySet()) {
				if (e.getValue() == null) {
					restURL = restURL.replace(e.getKey(), "NULL");
					restURL = restURL.replace("'" + e.getKey() + "'", "NULL");
				} else {
					restURL = restURL.replace(e.getKey(), String.valueOf(e.getValue()));
				}
			}
			LOGGER.debug("[GEN] REST URL : " + restURL);
			URL url = new URL(restURL);
			HttpURLConnection conn = null;
			if (restURL.startsWith("https")) {
				conn = (HttpsURLConnection) url.openConnection();
			} else {
				conn = (HttpURLConnection) url.openConnection();
			}
			conn.setRequestMethod(rest.getMethod());
			conn.setRequestProperty("Accept", rest.getAccept());
			conn.setRequestProperty("Content-Type", rest.getContentType());
			
			/* Handling Input */
			String input = rest.getInput();
			if (input != null) {
				conn.setDoOutput(true);
				// If input is starts with < means the input is XML, otherwise
				// JSON
				LOGGER.debug("[RAW] Input : " + input);
				for (Entry<String, Object> e : keyValue.entrySet()) {
					if (e.getValue() == null) {
						input = input.replace(e.getKey(), "NULL");
						input = input.replace("'" + e.getKey() + "'", "NULL");
					} else {
						input = input.replace(e.getKey(), String.valueOf(e.getValue()));
					}
				}
				LOGGER.debug("[GEN] Input : " + input);
				input.trim();
				OutputStream os = conn.getOutputStream();
				os.write(input.getBytes());
				os.flush();
			}

			if (conn.getResponseCode() != 200) {
				// throw new RuntimeException("Failed : HTTP error code : " +
				// conn.getResponseCode());
				LOGGER.error("RESTful response = " + conn.getResponseCode());
				return;
			}

			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

			String output;
			StringBuilder sb = new StringBuilder();
			while ((output = br.readLine()) != null) {
				sb.append(output);
			}
			LOGGER.debug("Output : " + sb.toString());
			// If input is starts with < means the output is XML, otherwise JSON
			boolean isJSON = false,isArrJSON = false;
			if (true) {
				String source = sb.toString();
				if (sb.toString().startsWith("{")) {
					isJSON = true;
					source = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"+ XML.toString(new JSONObject(source),"Root");
				} else if (sb.toString().startsWith("[")) {
					isArrJSON = true;
					source = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Root>" + XML.toString( new JSONArray(source),"Array")+"</Root>";
				}
				/**
				 * XML is formatted to an arrays of lines which contains every
				 * XPath in a line with [\d] define the index of each XPath
				 */
				Source xslt = new StreamSource(new StringReader(ETLUtils.XSLT));
				StringWriter stringWriter = new StringWriter();
				Transformer xform = TransformerFactory.newInstance().newTransformer(xslt);
				xform.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				xform.setOutputProperty(OutputKeys.INDENT, "yes");
				xform.transform(new StreamSource(new StringReader(source)), new StreamResult(stringWriter));
				/**
				 * XML after XSLT Transformation
				 */
				String xmlLines = new String(stringWriter.toString());
				String[] lines = xmlLines.split("\n");

				boolean iterable = false;
				for (int i = 0; i < lines.length; i++) {
					// if(lines[i].contains("@))
					String key = lines[i].substring(0, lines[i].lastIndexOf("=")).trim();
					if (Pattern.compile("[\\d+]").matcher(key).find()) {
						iterable = true;
						break;
					}
				}
				LOGGER.debug("Output-XML [" + iterable + "]\n" + Arrays.toString(lines));
				int lastIndx = -1;
				if (iterable) {
					String key, val;
					for (int j = 0; j < lines.length; j++) {
						key = lines[j].substring(0, lines[j].lastIndexOf("=")).trim();
						val = lines[j].substring(lines[j].lastIndexOf("=") + 1).trim();
						Matcher matcher = Pattern.compile("([\\d+])").matcher(key);
						String indx = lastIndx + "";
						if (matcher.find()) {
							indx = matcher.group();
						}
						int myIndx = Integer.parseInt(indx);
						if (myIndx != lastIndx) {
							lastIndx = myIndx;
							processIterator(rest.getProcess(), keyValue);
							// continue;
						}
						key = key.replaceAll("\\[\\d+\\]", "");
						if (isJSON) {
							key = key.replaceFirst("/Root", "");
						}else if(isArrJSON){
							key = key.replaceFirst("/Root/Array", "");
						}
						key = rest.getId() == null || rest.getId().length() == 0 ? "?" + key : "?" + rest.getId() + "." + key;
						keyValue.put(key, val.replace("'", ""));
					}
					processIterator(rest.getProcess(), keyValue);
				} else {
					for (int i = 0; i < lines.length; i++) {
						String key = lines[i].substring(0, lines[i].lastIndexOf("=")).trim();
						String val = lines[i].substring(lines[i].lastIndexOf("=") + 1).trim();
						key = rest.getId() == null || rest.getId().length() == 0 ? "?" + key : "?" + rest.getId() + "." + key;
						keyValue.put(key, val.replace("'", ""));
					}
					processIterator(rest.getProcess(), keyValue);
				}
			}

			conn.disconnect();
		} catch (MalformedURLException ex) {
			throw new ETLException(ex);
		} catch (IOException ex) {
			throw new ETLException(ex);
		} catch (TransformerException ex) {
			throw new ETLException(ex);
		}
	}

	@SuppressWarnings(value = "unused")
	private void parseJSONArray(JSONArray arr, Map<String, Object> keyValue) {

	}

	private void processIterator(List<com.outsidethebox.etl.pojo.Process> process, Map<String, Object> keyValue) throws ETLException {
		if (process != null && !process.isEmpty()) {
			for (com.outsidethebox.etl.pojo.Process procs : process) {
				for (AbstractProcess ap : procs.getSubProcesses()) {
					System.out.printf("================ PROCESS [%s][Start] ================\n", procs.getId());
					LOGGER.debug("Executing "+ap.toString()+"...");
					if (ap instanceof Database) {
						Database db = (Database) ap;// .getDatabase();
						try {
							execute(db, keyValue);
							// commitConnections();
						} catch (Exception ex) {
							// try {
							// // rollbackConnections();
							// LOGGER.error("Query rolledback.", ex);
							// } catch (SQLException e) {
							// LOGGER.error("Failed to rollback.", e);
							// }
							throw new ETLException(ex);
						}
					} else if (ap instanceof RESTfulWebService) {
						try {
							RESTfulWebService rest =(RESTfulWebService) ap;
							execute(rest, keyValue);
							LOGGER.debug("RESTfulWebservice ID["+rest.getId()+"] Executed.");
						} catch (Exception ex) {
							throw new ETLException(ex);
						}
					}
					System.out.printf("================ PROCESS [%s][End ] ================\n", procs.getId());
				}
			}
		}
	}

	private void execute(Database query, Map<String, Object> keyValue) throws IOException, SQLException, TransformerException, ETLException, ConnectionIDNotFoundException {

		// execute and get columns and values for first row
		if (keyValue == null) {
			keyValue = new HashMap<String, Object>();
		}
		Connection conn = getConnection(query.getConnectionID());
		Statement stmt = conn.createStatement();

		String sql = query.getSQL().trim();
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
				processIterator(query.getProcess(), keyValue);
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
		if (etl.hasProperties()) {
			PROPERTIES = new HashMap<String, Properties>();
			for (PropertyFile prop : etl.getProperties()) {
				Properties properties = new Properties();
				properties.load(new FileInputStream(prop.getPath()));
				PROPERTIES.put(prop.getId(), properties);
			}
			LOGGER.debug("Loaded [" + PROPERTIES.size() + "] <property>");
		}
	}

	private void loadDatabases() throws SQLException, ClassNotFoundException {
		if (etl.hasDBConnections()) {
			DATABASES = new HashMap<String, Connection>();
			for (DBConnection db : etl.getDBConnections()) {
				Class.forName(db.getDriver());
				Connection conn = DriverManager.getConnection(db.getURL(), db.getUsername(), db.getPassword());
				DATABASES.put(db.getId(), conn);
			}
			LOGGER.debug("Loaded [" + DATABASES.size() + "] <database>");
		}
	}

	@SuppressWarnings("unused")
	private void rollbackConnections() throws SQLException {
		for (Entry<String, Connection> entry : DATABASES.entrySet()) {
			DATABASES.get(entry.getKey()).rollback();
		}
	}

	@SuppressWarnings("unused")
	private void commitConnections() throws SQLException {
		for (Entry<String, Connection> entry : DATABASES.entrySet()) {
			DATABASES.get(entry.getKey()).commit();
		}
	}

	private String getPropery(String id, String key) throws IOException {
		Properties prop = PROPERTIES.get(id);
		String value = prop.getProperty(key);
		LOGGER.debug("Property{" + id + "} [" + key + "] = '" + value + "'");
		return value;
	}

	private Connection getConnection(String id) throws ConnectionIDNotFoundException{
		if(!DATABASES.containsKey(id)){
			throw new ConnectionIDNotFoundException(id);
		}
		return DATABASES.get(id);
	}
}
