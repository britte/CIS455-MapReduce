package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
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
import edu.upenn.cis455.mapreduce.ReduceContext;
import edu.upenn.cis455.mapreduce.mapContext;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  private String master;
  private String storageName;
  private File storageDir;
  private int pushfile = 0;
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
		  }
		  
		  // Create (or clean) spool-out and spool-in directories
		  File spoolOut = DirectoryTools.cleanMkdir(this.storageDir, "spool-out");
		  File spoolIn = DirectoryTools.cleanMkdir(this.storageDir, "spool-in");
		  
		  // Initialize class, input file reader, and output context for threads
		  Class<?> c = Class.forName(jobName);
		  Job job = (Job) c.newInstance();
		  
		  InputReader reader = new InputReader(inputDir, false);
		  
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
		  
		  System.out.println("Input files mapped");
		  
		  // Distribute spool-out files
		  HttpClient client = new HttpClient();
		  for (File f : spoolOut.listFiles()) {
			  // Get worker name
			  String worker = f.getName().split("-")[0];
			  HttpRequest req = new HttpRequest("http://" + worker + "/HW3/pushdata", "POST");
			  req.setBody(f);
			  client.sendPost(req);
		  }
		  
		  // Send /workerstatus update
		  this.status = "waiting";
		  new NotifyMaster().run();
		  
		  // Send successful response
		  response.setStatus(HttpServletResponse.SC_OK);
		  
	  } catch (Exception e) {
		  e.printStackTrace();
		  response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	  }
	  
  }
  
  private void runReduce(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  try {
		  // Process parameters
		  String jobName = request.getParameter("job");
		  String outputName = request.getParameter("output"); 
		  File outputDir = DirectoryTools.cleanMkdir(this.storageDir, outputName);
		  int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		  
		  // Check parameter validity
		  if (jobName == null || 
			  outputName == null || !outputDir.exists() ||
			  numThreads < 1 ) {
			  throw new IllegalArgumentException();
		  }
		  
		  // Initialize job class, spool-in file reader, and output context for threads
		  Class<?> c = Class.forName(jobName);
		  Job job = (Job) c.newInstance();
		  
		  File spoolInDir = new File(DirectoryTools.safeDirName(this.storageName, "spool-in"));
		  InputReader reader = new InputReader(spoolInDir, true); 
		  
		  Context outCtxt = new ReduceContext(outputDir);
		  
		  // Instantiate and start threads
		  ArrayList<Thread> threads = new ArrayList<Thread>();
		  for (int i = 0; i < numThreads; i++) {
			  threads.add(new Thread(new ReduceRun(job, reader, outCtxt, i)));
		  }
		  
		  for (Thread t : threads) {
			  t.start();
		  }
		  
		  // Wait until all files are read
		  for (Thread t : threads) {
			  t.join();
		  }
		  
		  System.out.println("Map files reduced");
		  
		  // Send /workerstatus update
		  this.status = "idle";
		  new NotifyMaster().run();
		  
		  // Send successful response
		  response.setStatus(HttpServletResponse.SC_OK);
		  
	  } catch (Exception e) {
		  e.printStackTrace();
		  response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	  }
	  
  }
   
  private void pushData(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  BufferedReader reader = request.getReader();
	  String spoolInDir = DirectoryTools.safeDirName(this.storageName, "spool-in");
	  File spoolInFile = new File(DirectoryTools.safeDirName(spoolInDir, "map" + this.pushfile));
	  pushfile ++;
	  PrintWriter writer = new PrintWriter(new FileWriter(spoolInFile));
	  
	  String line = reader.readLine();
	  while (line != null) {
		  writer.println(line);
		  writer.flush();
		  line = reader.readLine();
	  };
	  reader.close();
	  writer.close();
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
			  String[] keyVal = line.split("\\t", 2);
			  if (keyVal.length < 2) return;
			  String key = keyVal[0];
			  String val = keyVal[1];
			  
			  // Map line
			  this.j.map(Integer.toString(this.id), val, out);
			  System.out.println("Thread " + this.id + " mapping");
			  
			  // Get next line
			  line = in.readLine();
		  }
		  System.out.println("Map thread " + this.id + " terminating");
	  }
  }
  
  private class ReduceRun implements Runnable {
	  
	  Job j;
	  InputReader in;
	  Context out;
	  int id;
	  
	  private ReduceRun(Job j, InputReader in, Context out, int id) throws FileNotFoundException{ 
		  this.j = j; 
		  this.in = in;
		  this.out = out;
		  this.id = id;
	  }

	  @Override
	  public void run() {
		  String line = in.readLine();
		  while (line != null) {
			  // Read all lines with the same key from spool-in
			  ArrayList<String> vals = new ArrayList<String>();
			  String lastKey = "";
			  while (line != null) {
				  // Parse line of the form <key> <tab> <value>
				  String[] keyVal = line.split("\\t", 2);
				  if (keyVal.length < 2) {
					  line = in.readLine();
					  continue;
				  }
				  String key = keyVal[0];
				  String val = keyVal[1];
				  
				  if (!lastKey.equals(key)) { // new key found
					  // Reduce values
					  this.j.reduce(key, vals.toArray(new String[vals.size()]), out);
					  System.out.println("Thread " + this.id + " reducing for key " + key);
					  lastKey = key;
					  vals.clear();
				  } else {
					  line = in.readLine();
				  }
				  // No need to get a new line, because the last line read 
				  // was for a new key already
			  }
		  }
		  System.out.println("Reduce thread " + this.id + " terminating");
	  }
  }
}
  
