package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  private HashSet<WorkerStatus> activeWorkers = new HashSet<WorkerStatus>();
  private HashMap<String, JobStatus> activeJobs = new HashMap<String, JobStatus>();
  private HashMap<String, JobStatus> completedJobs = new HashMap<String, JobStatus>();
  
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  
	  // Determine which course of action to take based on what request was made
	  String reqType = request.getServletPath();
	  if (reqType.equals("/status")) {
		  getStatus(request, response);
	  } else if (reqType.equals("/workerstatus")) {
		  getWorkerStatus(request, response);
	  }
		
	}
  
  private void getStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  
	  response.setContentType("text/html");
	  PrintWriter out = response.getWriter();
	  out.println("<html><head><title>Master</title></head><body>");
	  // For each active worker, report their most recent reported status
	  out.println("<table>");
	  out.println("<tr>");
	  out.println("<th>Name(IP:port)</th>");
	  out.println("<th>Status</th>");
	  out.println("<th>Job</th>");
	  out.println("<th>Keys Read</th>");
	  out.println("<th>Keys Written</th>");
	  out.println("</tr>");
	  for (WorkerStatus w : this.activeWorkers) {
		  out.println("<tr>");
		  out.println("<td>" + w.getName() + "</td>");
		  out.println("<td>" + w.getStatus() + "</td>");
		  out.println("<td>" + w.getJob() + "</td>");
		  out.println("<td>" + w.getKeysRead() + "</td>");
		  out.println("<td>" + w.getKeysWritten() + "</td>");
		  out.println("</tr>");
	  }
	  out.println("</table><br />");
	  // Print out the job submission form
	  out.println("<form action=\"/HW3/job\" method=\"post\">");
	  out.println("Class Name: <input type=\"text\" name=\"class\"><br />");
	  out.println("Input Directory: <input type=\"text\" name=\"in\"><br />");
	  out.println("Output Directory: <input type=\"text\" name=\"out\"><br />");
	  out.println("Map Threads Per Worker: <input type=\"text\" name=\"map\"><br />");
	  out.println("Reduce Threads Per Worker: <input type=\"text\" name=\"reduce\"><br />");
	  out.println("<input type=\"submit\" value=\"Submit\">");
	  out.println("</form>");
	  out.println("</body></html>");
	  
  }
  
  private void getWorkerStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  
	  try {
		  // Process parameters
		  String ip = request.getRemoteAddr();
		  String port = request.getParameter("port");
		  String status = request.getParameter("status");
		  String job = request.getParameter("job");
		  int keysRead = Integer.parseInt(request.getParameter("keysRead"));
		  int keysWritten = Integer.parseInt(request.getParameter("keysWritten"));
		  
		  // Confirm that all parameters are pass basic validity checks
		  Integer.parseInt(port); // port is valid int
		  if (ip.isEmpty() || status.isEmpty() || job.isEmpty() || // empty params
			  !(status.equals("mapping") || // invalid status param ...
				status.equals("waiting") ||
				status.equals("reducing") || 
				status.equals("idle"))) { 
			  throw new IllegalArgumentException();
		  }
		  
		  // Create status and record active worker
		  WorkerStatus workerStatus = new WorkerStatus(ip, port, job, status, keysRead, keysWritten);
		  this.activeWorkers.add(workerStatus);
		  
		  // Report status to relevant job
		  JobStatus jobStatus = this.activeJobs.get(job);
		  if (jobStatus != null) {
			  jobStatus.updateWorkerStatus(workerStatus);
		  } else {
			  // TODO: error case
		  }
		  
		  // Check job status to determine next action
		  if (jobStatus.checkMapComplete()) {
			  if (!jobStatus.checkReduceComplete()) {
				  // TODO: reduce init
			  } else {
				  // The job is complete 
				  this.completedJobs.put(job, this.activeJobs.remove(job));
			  }
		  }
		  
	  } catch (IllegalArgumentException e) {
		  response.sendError(HttpServletResponse.SC_BAD_REQUEST);
	  } 
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  try {
		  // Process params
		  String jobName = request.getParameter("class");
		  String inputDir = request.getParameter("in");
		  String outputDir = request.getParameter("out");
		  int mapThreads = Integer.parseInt(request.getParameter("map"));
		  int reduceThreads = Integer.parseInt(request.getParameter("reduce"));
		  
		  // Confirm that all parameters are pass basic validity checks
		  if (jobName.isEmpty() || inputDir.isEmpty() || outputDir.isEmpty()) {
			  throw new IllegalArgumentException();
		  }
		  
		  // Determine how many active workers are available for the job
		  HashSet<WorkerStatus> workers = new HashSet<WorkerStatus>();
		  for (WorkerStatus w : this.activeWorkers) {
			  // Check if worker is "active" (i.e. has reported within the past 30 seconds)
			  long now = System.currentTimeMillis();
			  long then = w.getLastActive().getTime();
			  if (now - then > 30000)  { 
				  WorkerStatus initialStatus = new WorkerStatus(w.getName(), jobName, "idle", 0, 0);
				  workers.add(initialStatus);
			  } else {
				  // If worker has expired, remove from active list
				  this.activeWorkers.remove(w);
			  }
		  }
		  
		  // Create and store job on master
		  JobStatus job = new JobStatus(jobName, inputDir, outputDir, mapThreads, reduceThreads, workers);
		  this.activeJobs.put(jobName, job);
		  
		  // TODO: Notify active workers of the job
		  
		  
		  // Report success/failure
		  response.setContentType("text/html");
		  PrintWriter out = response.getWriter();
		  out.println("<html><head><title>Master</title></head><body>");
		  out.println("</body></html>");
		  
	  } catch (IllegalArgumentException|ClassNotFoundException e) {
		  response.sendError(HttpServletResponse.SC_BAD_REQUEST);
	  } 
  }
  
}
  
