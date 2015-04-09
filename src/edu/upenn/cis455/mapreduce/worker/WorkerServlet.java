package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.xml.bind.DatatypeConverter;

import edu.upenn.cis455.httpclient.HttpClient;
import edu.upenn.cis455.httpclient.HttpRequest;
import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.StringTools;
import edu.upenn.cis455.mapreduce.mapContext;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  private String master;
  private String storageName;
  private File storageDir;
  private Timer t;
  
  private String ip = "localhost"; // TODO: not this
  private String port = "8080"; // TODO: not this
  private String jobClass = "";
  private String status = "idle";
  private int keysRead = 0;
  private int keysWritten = 0;
  private ArrayList<String> workers = new ArrayList<String>();
  
  public void init() throws ServletException {
	  // Get init parameters
	  this.master = getServletConfig().getInitParameter("master");
	  this.storageName = getServletConfig().getInitParameter("storagedir");
	  if (this.master == null || storageName == null) {
	  } else {
		  this.storageDir = new File(storageName);
		  if (!this.storageDir.exists()) throw new ServletException();
	  }
	  
	  // Begin timer for workerstatus posts updates
//	  this.t = new Timer();
//	  this.t.scheduleAtFixedRate(new NotifyMaster(), 0, 10000);
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  File outDir = cleanMkdir("output");
	  this.workers.add("w1");
	  Context c = new mapContext(outDir, this.workers);
	  c.write("test", "key");
	  
	  response.setContentType("text/html");
	  PrintWriter out = response.getWriter();
	  out.println("<html><head><title>Worker</title></head>");
	  out.println("<body>Hi, I'm a worker.</body></html>");
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  // Determine which course of action to take based on what request was made
	  String reqType = request.getServletPath();
	  if (reqType.equals("/runmap")) {
		  runMap(request, response);
	  } else if (reqType.equals("/runreduce")) {
		  runReduce(request, response);
	  } else if (reqType.equals("/test")) {
		  
	  }
  }
  
  private class NotifyMaster extends TimerTask {

	@Override
	public void run() {
		HttpRequest req = new HttpRequest("http://" + master + "/HW3/workerstatus", "GET");
		req.setParam("port", port);
		req.setParam("status", status);
		req.setParam("job", jobClass);
		req.setParam("keysRead", Integer.toString(keysRead));
		req.setParam("keysWritten", Integer.toString(keysWritten));
		HttpClient client = new HttpClient();
		client.sendGet(req);
	}
	  
  }
  
  // Safely create a subdirectory within storage directory 
  // If the directory exists, clear contents; otherwise create
  private File cleanMkdir(String dir) {
	  System.out.println(this.storageDir.getAbsolutePath());
	  System.out.println(this.storageDir.exists());
	  File[] storageContains = this.storageDir.listFiles();
	  for (File f : storageContains) {
		  // Sub directory already exists
		  if (f.isDirectory() && f.getName().equals(dir)) {
			  // Delete all contents
			  File[] subDirContains = f.listFiles();
			  for (File sf : subDirContains) {
				  sf.delete();
			  }
			  return f;
		  }
	  }
	  // Sub directory did not exist
	  File subDir = new File(StringTools.safeDirName(this.storageDir.getAbsolutePath(), dir));
	  subDir.mkdirs();
	  return subDir;
  }
  
  private void runMap(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  try {
		  // Process parameters
		  String jobName = request.getParameter("job");
		  String inputDir = request.getParameter("input"); 
		  int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		  int numWorkers = Integer.parseInt(request.getParameter("numWorkers"));
		  
		  Map<String, String[]> params = request.getParameterMap();
		  Iterator<Map.Entry<String, String[]>> i = params.entrySet().iterator();
		  while (i.hasNext()) {
			  Map.Entry<String, String[]> p = i.next();
			  if (p.getKey().startsWith("worker")) {
				  this.workers.add(p.getValue()[0]);
			  }
		  }
		  
		  // Check parameter validity
		  if (jobName == null || 
			  inputDir == null || !(new File(StringTools.safeDirName(this.storageName, inputDir)).exists()) ||
			  numThreads < 1 || 
			  numWorkers < 1 || 
			  workers.isEmpty()) {
			  throw new IllegalArgumentException();
		  } 
		  
		  
		  // Initialize class
		  Class<?> c = Class.forName(jobClass);
		  Job job = (Job) c.newInstance();
		  		  
		  // Create (or clean) spool-out and spool-in directories
		  File outDir = cleanMkdir("spool-out");
		  File indir = cleanMkdir("spool-in");
		  
		  Context outCtxt = new mapContext(outDir, this.workers);
//		  Context in = new myContext();
		  
		  // TODO: instantiate threads
		  
		  // TODO: run threads
	  } catch (Exception e) {
		  
	  }
	  
  }
  
  private void runReduce(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  // Process parameters
	  String jobName = request.getParameter("job");
	  String outputDir = request.getParameter("output");
	  int numThreads = Integer.parseInt(request.getParameter("numThreads"));
	  
	  // Check parameter validity
	  if (jobName == null || 
		  outputDir == null || !(new File(outputDir).exists()) ||
		  numThreads < 1 ) {
		  throw new IllegalArgumentException();
	  }
  }
    
  private class MapRun implements Runnable {
	  
	  Job j;
	  Context in;
	  Context out;
	  
	  private MapRun(Job j, Context in, Context out){ 
		  this.j = j; 
		  this.in = in;
		  this.out = out;
	  }

	  @Override
	  public void run() {
		  // TODO: Read line from file
		  String line = "";
		  if (line == null) return;
		  
		  // Parse line of the form <key> <tab> <value>
		  String[] keyVal = line.split("\\t");
		  if (keyVal.length < 2) return;
		  String key = keyVal[0];
		  String val = keyVal[1];
		  
		  
		  // Map
		  this.j.map(key, val, out);
		  
		  
	  }
	  
  }
}
  
