package com.outsidethebox.etl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.XMLConstants;
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

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import com.outsidethebox.etl.pojo.AbstractProcess;
import com.outsidethebox.etl.pojo.Constants;
import com.outsidethebox.etl.pojo.Database;
import com.outsidethebox.etl.pojo.ETLObject;
import com.outsidethebox.etl.pojo.RESTfulWebService;
import com.outsidethebox.etl.pojo.SOAPWebService;

public class ETL {

	private ETLObject etl;
	private StringWriter logBuilder;
	private Logger LOGGER = Logger.getLogger(ETL.class);

	public ETL(File xmlFile) throws ETLException {
		try {
			init(IOUtils.toString(new FileInputStream(xmlFile)));
		} catch (IOException ex) {
			LOGGER.error("Error while reading from '" + xmlFile.getName() + "'", ex);
			throw new ETLException(ex);
		}
	}

	public ETL(String xml) throws ETLException {
		init(xml);
	}

	public String execute() throws ETLException {
		iterateProcess(etl.getProcesses(), null);
		etl.closeConnections();
		return logBuilder.toString();
	}
	
	private void init(String xml) throws ETLException {
		Layout layout = new PatternLayout("[%x]=>[%d{yyyy-MM-dd HH:mm:ss}] [%-5p] [%c.%M] - %m%n");
		logBuilder = new StringWriter();
	    WriterAppender writerAppender = new WriterAppender(layout, logBuilder);
	    LOGGER.addAppender(writerAppender);
		LOGGER.debug("Validating ETL xml...");
		if (!validateAgainstXSD(xml)) {
			LOGGER.error("ETL xml file is not valid against schema 'ETL.xsd'");
			throw new ETLException("Not valid against ETL.xsd");
		}
		LOGGER.debug("ETL xml file has been validated.");
		etl = ETLObject.parse(xml);
		LOGGER.debug("ETL object has been parsed.");
	}

	/**
	 * Validate XML with ETL.xsd Schema
	 * 
	 * @param xml
	 *            Input to validate with
	 * @return true if the XML is valid, otherwise false
	 */
	private boolean validateAgainstXSD(String xml) {
		try {
			InputStream xmlStream = new ByteArrayInputStream(xml.getBytes());
			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(new StreamSource(ETL.class.getResourceAsStream("/ETL.xsd")));
			Validator validator = schema.newValidator();
			validator.validate(new StreamSource(xmlStream));
			return true;
		} catch (Exception ex) {
			LOGGER.error("Validation error", ex);
			return false;
		}
	}

	private void execute(RESTfulWebService rest, Map<String, Object> keyValue) throws ETLException, JSONException, KeyNotFoundException {
		if (keyValue == null) {
			keyValue = new HashMap<String, Object>();
		}
		try {
			String restURL = rest.getURL().trim();
			LOGGER.debug("[RAW] REST URL : " + restURL);
			restURL = replaceFromKeyValue(restURL, keyValue);
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
				LOGGER.debug("[RAW] Input : " + input);
				input = replaceFromKeyValue(input, keyValue);
				LOGGER.debug("[GEN] Input : " + input);
				if (input.startsWith("<")) {
					if (rest.defaultAccpet) {
						rest.setAccept("application/xml");
					}
					if (rest.defaultContentType) {
						rest.setContentType("application/xml");
					}
				} else if (input.startsWith("{") || input.startsWith("[")) {
					if (rest.defaultAccpet) {
						rest.setAccept("application/json");
					}
					if (rest.defaultContentType) {
						rest.setContentType("application/json");
					}
				}
			}
			if (input != null) {
				conn.setDoOutput(true);
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
			boolean isJSON = false, isArrJSON = false;
			if (true) {
				String source = sb.toString();
				if (sb.toString().startsWith("{")) {
					isJSON = true;
					source = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" + XML.toString(new JSONObject(source), "Root");
				} else if (sb.toString().startsWith("[")) {
					isArrJSON = true;
					source = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Root>" + XML.toString(new JSONArray(source), "Array")
							+ "</Root>";
				}
				String[] lines = transformXML(source);

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
						Matcher matcher = Pattern.compile("\\[(\\d+)\\]").matcher(key);
						String indx = lastIndx + "";
						if (matcher.find()) {
							indx = matcher.group(1);
						}
						int myIndx = Integer.parseInt(indx);
						if (myIndx != lastIndx) {
							lastIndx = myIndx;
							iterateProcess(rest.getProcess(), keyValue);
							// continue;
						}
						key = key.replaceAll("\\[\\d+\\]", "");
						if (isJSON) {
							key = key.replaceFirst("/Root", "");
						} else if (isArrJSON) {
							key = key.replaceFirst("/Root/Array", "");
						}
						key = rest.getId() == null || rest.getId().length() == 0 ? "?" + key : "?" + rest.getId() + "." + key;
						keyValue.put(key, val.replace("'", ""));
					}
					iterateProcess(rest.getProcess(), keyValue);
				} else {
					for (int i = 0; i < lines.length; i++) {
						String key = lines[i].substring(0, lines[i].lastIndexOf("=")).trim();
						String val = lines[i].substring(lines[i].lastIndexOf("=") + 1).trim();
						key = rest.getId() == null || rest.getId().length() == 0 ? "?" + key : "?" + rest.getId() + "." + key;
						keyValue.put(key, val.replace("'", ""));
					}
					iterateProcess(rest.getProcess(), keyValue);
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

	private void execute(SOAPWebService soap, Map<String, Object> keyValue) throws ETLException, KeyNotFoundException {
		if (keyValue == null) {
			keyValue = new HashMap<String, Object>();
		}
		try {
			String soapURL = soap.getURL().trim();
			LOGGER.debug("[RAW] SOAP URL : " + soapURL);
			soapURL = replaceFromKeyValue(soapURL, keyValue);
			LOGGER.debug("[GEN] SOAP URL : " + soapURL);
			URL url = new URL(soapURL);
			HttpURLConnection conn = null;
			if (soapURL.startsWith("https")) {
				conn = (HttpsURLConnection) url.openConnection();
			} else {
				conn = (HttpURLConnection) url.openConnection();
			}
			String soapAction = soap.getAction().trim();
			LOGGER.debug("[RAW] SOAP Action : " + soapAction);
			soapAction = replaceFromKeyValue(soapAction, keyValue);
			LOGGER.debug("[GEN] SOAP Action : " + soapAction);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("SOAPAction", soapAction);
			conn.setRequestProperty("Accept", "*/*");
			conn.setRequestProperty("Content-Type", "text/xml");

			/* Handling Input */
			String input = soap.getInput();
			if (input != null) {
				conn.setDoOutput(true);
				// If input is starts with < means the input is XML, otherwise
				// JSON
				LOGGER.debug("[RAW] Input : " + input);
				input = replaceFromKeyValue(input, keyValue);
				LOGGER.debug("[GEN] Input : " + input);
				OutputStream os = conn.getOutputStream();
				os.write(input.getBytes());
				os.flush();
			}

			if (conn.getResponseCode() != 200) {
				// throw new RuntimeException("Failed : HTTP error code : " +
				// conn.getResponseCode());
				LOGGER.error("SOAP response = " + conn.getResponseCode());
				return;
			}

			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

			String output;
			StringBuilder sb = new StringBuilder();
			while ((output = br.readLine()) != null) {
				sb.append(output);
			}
			LOGGER.debug("Output : " + sb.toString());
			String[] lines = transformXML(sb.toString());
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
					if (key.contains("@")) {
						continue;
					}
					val = lines[j].substring(lines[j].lastIndexOf("=") + 1).trim();
					Matcher matcher = Pattern.compile("\\[(\\d+)\\]").matcher(key);
					String indx = lastIndx + "";
					if (matcher.find()) {
						indx = matcher.group(1);
					}
					int myIndx = Integer.parseInt(indx);
					if (myIndx != lastIndx) {
						lastIndx = myIndx;
						iterateProcess(soap.getProcess(), keyValue);
						// continue;
					}
					key = key.replaceAll("\\[\\d+\\]", "");
					int count = 0, sindx = 1;
					while (count != 2) {
						sindx = key.indexOf("/", sindx);
						if (sindx != -1) {
							key = key.substring(sindx);
						}
						count++;
					}
					key = soap.getId() == null || soap.getId().length() == 0 ? "?" + key : "?" + soap.getId() + "." + key;
					keyValue.put(key, val.replace("'", ""));
				}
				iterateProcess(soap.getProcess(), keyValue);
			} else {
				for (int i = 0; i < lines.length; i++) {
					String key = lines[i].substring(0, lines[i].lastIndexOf("=")).trim();
					if (key.contains("@")) {
						continue;
					}
					String val = lines[i].substring(lines[i].lastIndexOf("=") + 1).trim();
					int count = 0, indx = 1;
					while (count != 2) {
						indx = key.indexOf("/", indx);
						if (indx != -1) {
							key = key.substring(indx);
						}
						count++;
					}
					key = soap.getId() == null || soap.getId().length() == 0 ? "?" + key : "?" + soap.getId() + "." + key;
					keyValue.put(key, val.replace("'", ""));
				}
				iterateProcess(soap.getProcess(), keyValue);
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

	private void iterateProcess(List<com.outsidethebox.etl.pojo.Process> process, Map<String, Object> keyValue) throws ETLException {
		if (process != null && !process.isEmpty()) {
			for (com.outsidethebox.etl.pojo.Process procs : process) {
				for (AbstractProcess ap : procs.getSubProcesses()) {
					System.out.printf("================ PROCESS [%s][Start] ================\n", procs.getId());
					LOGGER.debug("Executing " + ap.toString() + "...");
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
							RESTfulWebService rest = (RESTfulWebService) ap;
							execute(rest, keyValue);
							LOGGER.debug("RESTfulWebservice ID[" + rest.getId() + "] Executed.");
						} catch (Exception ex) {
							throw new ETLException(ex);
						}
					} else if (ap instanceof SOAPWebService) {
						try {
							SOAPWebService soap = (SOAPWebService) ap;
							execute(soap, keyValue);
							LOGGER.debug("SOAPWebService ID[" + soap.getId() + "] Executed.");
						} catch (Exception ex) {
							throw new ETLException(ex);
						}
					}
					System.out.printf("================ PROCESS [%s][End ] ================\n", procs.getId());
				}
			}
		}
	}

	private void execute(Database database, Map<String, Object> keyValue) throws IOException, SQLException, TransformerException, ETLException,
			KeyNotFoundException {

		// execute and get columns and values for first row
		if (keyValue == null) {
			keyValue = new HashMap<String, Object>();
		}
		Connection conn = etl.getConnection(database.getConnectionID());
		Statement stmt = conn.createStatement();

		String sql = replaceFromKeyValue(database.getSQL(), keyValue);
		
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
					String key = database.getId() == null || database.getId().length() == 0 ? "?" + colName : "?" + database.getId() + "." + colName;
					keyValue.put(key, value);
				}
				if (database.getProcess() == null || database.getProcess().size() == 0) {
					// execute
					// System.out.println("Execute : " + query.value);
					return;
				}
				iterateProcess(database.getProcess(), keyValue);
			}
			result.close();
		} else {
			stmt.close();
		}
		// System.out.println("[" + query.getId() + "] = Execute : " + sql);
	}

	/**
	 * XML is formatted to an arrays of lines which contains every XPath in a
	 * line with [\d] define the index of each XPath
	 * 
	 * @param xml
	 * @return String[] Array of string with xpaths
	 * @throws TransformerException
	 */
	public static String[] transformXML(String xml) throws TransformerException {
		Source xslt = new StreamSource(new StringReader(Constants.XML_XSLT));
		StringWriter stringWriter = new StringWriter();
		Transformer xform = TransformerFactory.newInstance().newTransformer(xslt);
		xform.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		xform.setOutputProperty(OutputKeys.INDENT, "yes");
		xform.transform(new StreamSource(new StringReader(xml)), new StreamResult(stringWriter));
		/**
		 * XML after XSLT Transformation
		 */
		String xmlLines = new String(stringWriter.toString());
		return xmlLines.split("\n");
	}

	/**
	 * Replace input with dictionary of key and value
	 * 
	 * @param input
	 *            Input to be replaces
	 * @param keyValue
	 *            Dictionary of key value
	 * @return input String with replaced values or the same input
	 * @throws KeyNotFoundException
	 * @throws IOException
	 */
	private String replaceFromKeyValue(String input, Map<String, Object> keyValue) throws IOException, KeyNotFoundException {
		if (input != null && input.trim().length() > 0) {
			input = etl.eval(input.trim());
			for (Entry<String, Object> e : keyValue.entrySet()) {
				if (e.getValue() == null) {
					input = input.replace(e.getKey(), "NULL");
					input = input.replace("'" + e.getKey() + "'", "NULL");
				} else {
					input = input.replace(e.getKey(), String.valueOf(e.getValue()));
				}
			}
		}
		return input;
	}
}
