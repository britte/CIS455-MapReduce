package edu.upenn.cis455.mapreduce.master;

public class WorkerStatus {
	private String ip; 
	private int port;
	private String job;
	private String status;
	private int keysRead;
	private int keysWritten;
	
	public WorkerStatus(String ip, int port, String job, String status, int keysRead, int keysWritten) {
		this.ip = ip;
		this.port = port;
		this.job = job;
		this.status = status;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
	}
	
	public String getIP() { return this.ip; }
	public int getPort() { return this.port; }
	public String getName() { return this.ip + ":" + Integer.toString(this.port); }
	public String getStatus() { return this.status; }
	public String getJob() { return this.job; }
	public int getKeysRead() { return this.keysRead; }
	public int getKeysWritten() { return this.keysWritten; }
	
}
