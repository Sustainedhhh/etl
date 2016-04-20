package com.outsidethebox.etl.util;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import com.outsidethebox.etl.pojo.Constants;

public class ETLUtils {
	private ETLUtils() {

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
		Transformer xform = TransformerFactory.newInstance().newTransformer(
				xslt);
		xform.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		xform.setOutputProperty(OutputKeys.INDENT, "yes");
		xform.transform(new StreamSource(new StringReader(xml)),
				new StreamResult(stringWriter));
		/**
		 * XML after XSLT Transformation
		 */
		String xmlLines = new String(stringWriter.toString());
		return xmlLines.split("\n");
	}
}
