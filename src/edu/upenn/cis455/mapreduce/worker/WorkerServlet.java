package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
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
import edu.upenn.cis455.mapreduce.InputReader;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.DirectoryTools;
import edu.upenn.cis455.mapreduce.mapContext;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  private String master;
  private String storageName;
  private File storageDir;
  private File spoolOut;
  private File spoolIn;
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
//	  File outDir = cleanMkdir("output");
//	  this.workers.add("w1");
//	  this.workers.add("w2");
//	  Context c = new mapContext(outDir, this.workers);
//	  c.write("test", "key");
	  
	  
	  
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
	  } else if (reqType.equals("/pushdata")) {
		  pushData(request, response);
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
  
  private void runMap(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  try {
		  // Process parameters
		  String jobName = request.getParameter("job");
		  String inputName = request.getParameter("input"); 
		  File inputDir = new File(DirectoryTools.safeDirName(this.storageName, inputName));
		  int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		  int numWorkers = Integer.parseInt(request.getParameter("numWorkers"));
		  
		  Map<String, String[]> params = request.getParameterMap();
		  Iterator<Map.Entry<String, String[]>> iter = params.entrySet().iterator();
		  while (iter.hasNext()) {
			  Map.Entry<String, String[]> p = iter.next();
			  if (p.getKey().startsWith("worker")) {
				  this.workers.add(p.getValue()[0]);
			  }
		  }
		  
		  // Check parameter validity
		  if (jobName == null || 
			  inputName == null || !(inputDir.exists()) ||
			  numThreads < 1 || 
			  numWorkers < 1 || 
			  workers.isEmpty()) {
			  throw new IllegalArgumentException();
		  } else {
			  this.jobClass = jobName;
		  }
		  
		  
		  // Initialize class
		  Class<?> c = Class.forName(jobClass);
		  Job job = (Job) c.newInstance();
		  
		  // Get input reader to manage synchronous reading of all files
		  InputReader reader = new InputReader(inputDir);
		  		  
		  // Create (or clean) spool-out and spool-in directories
		  spoolOut = DirectoryTools.cleanMkdir(this.storageDir, "spool-out");
		  spoolIn = DirectoryTools.cleanMkdir(this.storageDir, "spool-in");

		  Context outCtxt = new mapContext(spoolOut, this.workers);
		  
		  // Instantiate and start threads
		  ArrayList<Thread> threads = new ArrayList<Thread>();
		  for (int i = 0; i < numThreads; i++) {
			  threads.add(new Thread(new MapRun(job, reader, outCtxt, i)));
		  }
		  
		  for (Thread t : threads) {
			  t.start();
		  }
		  
		  // Wait until all files are read
		  for (Thread t : threads) {
			  t.join();
		  }
		  
		  System.out.println("Input files read");
		  
		  // Distribute spool-out files
		  for (File f : spoolOut.listFiles()) {
			  // Get worker name
			  String worker = f.getName().split("-")[0];
			  HttpRequest req = new HttpRequest("http://" + worker + "/HW3/pushdata", "POST");
			  req.setBody(f);
			  HttpClient client = new HttpClient();
			  client.sendPost(req);
		  }
		  
	  } catch (Exception e) {
		  e.printStackTrace();
		  response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
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
   
  private void pushData(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  System.out.println("push recieved");
  }
  
  private class MapRun implements Runnable {
	  
	  Job j;
	  InputReader in;
	  Context out;
	  int id;
	  
	  private MapRun(Job j, InputReader in, Context out, int id) throws FileNotFoundException{ 
		  this.j = j; 
		  this.in = in;
		  this.out = out;
		  this.id = id;
	  }

	  @Override
	  public void run() {
		  // Read input files and map into spool-out
		  String line = in.readLine();
		  while (line != null) {
			  
			  // Parse line of the form <key> <tab> <value>
			  String[] keyVal = line.split("\\t");
			  if (keyVal.length < 2) return;
			  String key = keyVal[0];
			  String val = keyVal[1];
			  
			  // Map line
			  this.j.map(key, val, out);
			  System.out.println("Thread " + this.id + " mapping");
			  
			  // Get line
			  line = in.readLine();
		  }
		  System.out.println("Thread " + this.id + " terminating");
	  }
  }
}
  
