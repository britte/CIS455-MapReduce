package edu.upenn.cis455.httpclient;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

public class HttpRequest {
	
	private String baseUrl;
	private String method; 
	private HashMap<String, HashSet<String>> params = new HashMap<String, HashSet<String>>();
	private File bodyFile;
	
	public HttpRequest(String baseUrl, String method){
		this.baseUrl = baseUrl;
		this.method = method.toUpperCase();
	}
	
	public void setParam(String name, String value) {
		HashSet<String> vals = this.params.get(name);
		if (vals == null) vals = new HashSet<String>();
		
		vals.add(value);
		
		this.params.put(name, vals);
	}
	
	public void setBody(File f) {
		this.bodyFile = f;
	}
	
	public String getBaseUrl() { return this.baseUrl; }
	public String getMethod() { return this.method; }
	public String getEncodedParams() throws UnsupportedEncodingException { 
		if (this.params.size() == 0) return "";
		StringBuilder pString = new StringBuilder();
		Iterator<Entry<String, HashSet<String>>> i = this.params.entrySet().iterator();
		while (i.hasNext()) {
			Entry<String, HashSet<String>> param = i.next();
			for (String value : param.getValue()) {
				String encodedKey = URLEncoder.encode(param.getKey(), "UTF-8");
				String encodedVal = URLEncoder.encode(value, "UTF-8");
				pString.append(encodedKey + "=" + encodedVal + "&");
			}
		}
		return pString.substring(0, pString.length() - 1);
	}
	public File getBodyFile() {
		return this.bodyFile;
	}
}
