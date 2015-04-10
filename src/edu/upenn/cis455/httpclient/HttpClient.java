package edu.upenn.cis455.httpclient;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

public class HttpClient {
	
	public HttpClient() {}

	// Create a connection to the given url 
	public int sendRequest(HttpRequest req) throws IOException {
		if (req.getMethod().equals("GET")) {
			return this.sendGet(req);
		} else if (req.getMethod().equals("POST")) {
			return this.sendPost(req);
		} else {
			return -1;
		}
	}
	
	public int sendGet(HttpRequest req) {
		try {
			// Establish connection to url (with params in query string)
			URL url = new URL(req.getBaseUrl() + "?" + req.getEncodedParams());
			HttpURLConnection conn;
			if (url.getProtocol().equals("http")) {
				conn = (HttpURLConnection) url.openConnection();
			} else if (url.getProtocol().equals("https")){
				conn = (HttpsURLConnection) url.openConnection();
			} else {
				// Invalid connection
				return HttpURLConnection.HTTP_BAD_REQUEST;
			}
			
			conn.setRequestMethod("GET");
			conn.connect();
			int status = conn.getResponseCode();
			conn.disconnect();
			return status;
			
		} catch (Exception e){
			e.printStackTrace();
			return HttpURLConnection.HTTP_BAD_REQUEST;
		}
	}
	
	public int sendPost(HttpRequest req) {
		try {
			// Establish connection to base url
			URL url = new URL(req.getBaseUrl());
			HttpURLConnection conn;
			if (url.getProtocol().equals("http")) {
				conn = (HttpURLConnection) url.openConnection();
			} else if (url.getProtocol().equals("https")){
				conn = (HttpsURLConnection) url.openConnection();
			} else {
				// Invalid connection
				return HttpURLConnection.HTTP_BAD_REQUEST;
			}
			
			conn.setDoOutput(true); // sets POST
			
			// Send params as a body
			PrintWriter out = new PrintWriter(new BufferedOutputStream(conn.getOutputStream()), true);	
		    out.write(req.getEncodedParams());
		    
		    // Send body file (if one exists)
		    if (req.getBodyFile() != null && req.getBodyFile().exists()) {
		    	BufferedReader reader = new BufferedReader(new FileReader(req.getBodyFile()));
		    	String line = reader.readLine();
		    	while (line != null) {
		    		out.println(line);
		    		line = reader.readLine();
		    	}
		    }
		    out.flush();
		    
			int status = conn.getResponseCode();
			conn.disconnect();
			return status;
			
		} catch (Exception e){
			e.printStackTrace();
			return HttpURLConnection.HTTP_BAD_REQUEST;
		}
	}

}
