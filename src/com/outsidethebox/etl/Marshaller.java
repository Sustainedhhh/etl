package com.outsidethebox.etl;

import java.io.ByteArrayInputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

class Marshaller {
	private static final Logger LOGGER = Logger.getLogger(Marshaller.class);

	@SuppressWarnings("unchecked")
	public static <T> T unmarshal(String xml, Class<?> cls) throws JAXBException {
		try {
			JAXBContext jaxContext = JAXBContext.newInstance(cls);
			Unmarshaller unmarshaller = jaxContext.createUnmarshaller();
			ByteArrayInputStream byStream = new ByteArrayInputStream(xml
					.getBytes());
			return (T) unmarshaller.unmarshal(byStream);
		} catch (Exception ex) {
			LOGGER.debug("XML = " + xml);
			LOGGER.error("Failed to unmrshalling input xml string.",ex);
			throw new JAXBException(ex.getMessage());
		}
	}
}
