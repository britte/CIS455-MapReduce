package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.util.ArrayList;

import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;

  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  
	  String reqType = request.getServletPath();
	  if (reqType.equals("/status")) {
		  getStatus(request, response);
	  } else if (reqType.equals("/workerstatus")) {
		  getWorkerStatus(request, response);
	  }
		
	}
  
  private void getStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  
	  // TODO: Get WorkerStatus objects to report
	  ArrayList<WorkerStatus> statuses = new ArrayList<WorkerStatus>();
	  
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
	  for (WorkerStatus w : statuses) {
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
		  int port = Integer.parseInt(request.getParameter("port"));
		  String status = request.getParameter("status");
		  String job = request.getParameter("job");
		  int keysRead = Integer.parseInt(request.getParameter("keysRead"));
		  int keysWritten = Integer.parseInt(request.getParameter("keysWritten"));
		  
		  // Confirm that all parameters are pass basic validity checks
		  if (ip.isEmpty() || status.isEmpty() || job.isEmpty() || // empty params
			  !(status.equals("mapping") || // invalid status param ...
				status.equals("waiting") ||
				status.equals("reducing") || 
				status.equals("idle"))) { 
			  throw new IllegalArgumentException();
		  }
		  
		  // Create status 
		  WorkerStatus wrkrStatus = new WorkerStatus(ip, port, job, status, keysRead, keysWritten);
		  
		  // TODO: Save status somewhere somehow idk
		  
		  // TODO: Check statuses to determine if the reduce stage should be initiated
		  
	  } catch (IllegalArgumentException e) {
		  response.sendError(HttpServletResponse.SC_BAD_REQUEST);
	  } 
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  try {
		  // Process params
		  String className = request.getParameter("class");
		  String inputDir = request.getParameter("in");
		  String outputDir = request.getParameter("out");
		  int mapThreads = Integer.parseInt(request.getParameter("map"));
		  int reduceThreads = Integer.parseInt(request.getParameter("reduce"));
		  
		  // Confirm that all parameters are pass basic validity checks
		  if (className.isEmpty() || inputDir.isEmpty() || outputDir.isEmpty()) {
			  throw new IllegalArgumentException();
		  }
		  
		  // TODO: create and store job 
		  
		  // TODO: figure out worker distribution & notify workers
		  
		  // Report success/failure
		  response.setContentType("text/html");
		  PrintWriter out = response.getWriter();
		  out.println("<html><head><title>Master</title></head><body>");
		  out.println("</body></html>");
		  
	  } catch (IllegalArgumentException e) {
		  response.sendError(HttpServletResponse.SC_BAD_REQUEST);
	  }
  }
  
}
  
