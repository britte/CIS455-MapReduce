package edu.upenn.cis455.mapreduce.master;

import java.util.HashSet;
import java.util.Iterator;

public class JobStatus {
	
	private String jobClass;
	private String inDir;
	private String outDir;
	private int mapThreads;
	private int reduceThreads;
	
	private HashSet<WorkerStatus> workers;
	private boolean jobStarted = false;
	private boolean mapComplete = false;
	private boolean reduceComplete = false;
	
	public JobStatus(String jobClass, String inDir, String outDir, int mapThreads, int reduceThreads, HashSet<WorkerStatus> workers) throws ClassNotFoundException {
		this.jobClass = jobClass;
		this.inDir = inDir;
		this.outDir = outDir;
		this.mapThreads = mapThreads;
		this.reduceThreads = reduceThreads;
		this.workers = workers; 
		
		// Try to instantiate class and report error if the name is invalid
		Class<?> c = Class.forName(jobClass);
	}
	
	public HashSet<WorkerStatus> getWorkers() { return this.workers; }
	public String getInputDir() { return this.inDir; }
	public String getOutputDir() { return this.outDir; }
	public int getMapThreads() { return this.mapThreads; }
	public int getReduceThreads() { return this.reduceThreads; }
	public String getJobClass() { return this.jobClass; }
	
	public void updateWorkerStatus(WorkerStatus status) {
//		if (!status.getJob().equals(this.jobClass)) return; // don't update status for another job
		Iterator<WorkerStatus> iter = workers.iterator();
		while (iter.hasNext()) {
			WorkerStatus w = iter.next();
			if (w.equals(status)) {
				iter.remove();
			}
		}
		workers.add(status); // add new or updated status
	}
	
	public boolean checkMapComplete() {
		if (this.mapComplete) return true;
		// Determine if any workers are still mapping
		for (WorkerStatus w : workers) {
			if (w.getStatus().equals("idle") || w.getStatus().equals("mapping")) {
				return false;
			}
		}
		// If all workers have completed mapping, 
		// set mapComplete to true and notify caller
		this.mapComplete = true;
		return true;
	}
	
	public boolean checkReduceComplete() {
		if (this.reduceComplete) return true;
		if (!this.mapComplete) return false; 
		// Determine if any workers are still reducing
		for (WorkerStatus w : workers) {
			if (w.getStatus().equals("waiting") || w.getStatus().equals("reducing")) {
				return false;
			}
		}
		// If all workers have completed reducing, 
		// set reduceComplete to true and notify caller
		this.reduceComplete = true;
		return true;
	}
	
}
