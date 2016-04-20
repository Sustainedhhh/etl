package etl.web.test.rest.dto;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Error")
public class Error {

	private String id;
	private String message;

	public String getId() {
		return id;
	}

	@XmlElement(name="ID")
	public void setId(String id) {
		this.id = id;
	}

	public String getMessage() {
		return message;
	}

	@XmlElement(name="Message")
	public void setMessage(String message) {
		this.message = message;
	}

}
