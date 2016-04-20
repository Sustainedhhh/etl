package com.outsidethebox.etl.web.bean;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.xml.namespace.QName;

import org.reficio.ws.builder.SoapBuilder;
import org.reficio.ws.builder.SoapOperation;
import org.reficio.ws.builder.core.Wsdl;

import com.outsidethebox.etl.util.ETLUtils;

@ManagedBean(name="soapBean")
public class SOAPBean {
	private Map<String, String[]> requests;
	private String wsdl;
	private boolean error;
	private String errorMessage;

	public SOAPBean() {
	
	}
	
	public Map<String, String[]> getRequests() {
		return requests;
	}

	public void setRequests(Map<String, String[]> requests) {
		this.requests = requests;
	}

	public String getWsdl() {
		return wsdl;
	}

	public void setWsdl(String wsdl) {
		this.wsdl = wsdl;
	}

	public boolean isError() {
		return error;
	}

	public void setError(boolean error) {
		this.error = error;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public void generate() {
		try {
			System.out.println(this.wsdl);
			requests = new HashMap<String, String[]>();
			Wsdl wsdl = Wsdl.parse(new URL(this.wsdl));

			List<QName> builders = wsdl.getBindings();

			for (QName name : builders) {
				SoapBuilder builder = wsdl.binding().name(name).find();//
				List<SoapOperation> operations = builder.getOperations();
				for (SoapOperation operation : operations) {
					try {
						String request = builder.buildInputMessage(operation);
						String response = builder.buildOutputMessage(operation);
						String[] resArr = ETLUtils.transformXML(response);
						for(int i=0;i<resArr.length;i++){
							String key = resArr[i];
							int count = 0, indx = 1;
							while (count != 2) {
								indx = key.indexOf("/", indx);
								if (indx != -1) {
									key = key.substring(indx);
								}
								count++;
							}
							resArr[i] = key.replace("/[a-zA-Z]+:","");
						}
						response +="\n"+Arrays.toString(resArr);
						requests.put(operation.getOperationName(), new String[]{request,response});
					} catch (Exception ex) {

					}
				}
			}
		} catch (Exception ex) {
			error = true;
			errorMessage = ex.getMessage();
		}
		// SoapClient client = SoapClient.builder()
		// .endpointUrl("http://www.webservicex.net/CurrencyConvertor.asmx")
		// .build();
		// String response = client.post(request);
	}
}
