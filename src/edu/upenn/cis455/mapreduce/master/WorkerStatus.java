package edu.upenn.cis455.mapreduce.master;

import java.util.Date;

public class WorkerStatus {
	private String ip; 
	private String port;
	private String job;
	private String status;
	private int keysRead;
	private int keysWritten;
	private Date lastActive;
	
	public WorkerStatus(String ip, String port, String job, String status, int keysRead, int keysWritten) {
		this.ip = ip;
		this.port = port;
		this.job = job;
		this.status = status;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
		this.lastActive = new Date();
	}
	
	public WorkerStatus(String name, String job, String status, int keysRead, int keysWritten) {
		this(name.split(":")[0], name.split(":")[1], job, status, keysRead, keysWritten);
	}
	
	public String getIP() { return this.ip; }
	public String getPort() { return this.port; }
	public String getName() { return this.ip + ":" + this.port; }
	public String getStatus() { return this.status; }
	public String getJob() { return this.job; }
	public int getKeysRead() { return this.keysRead; }
	public int getKeysWritten() { return this.keysWritten; }
	public Date getLastActive() { return this.lastActive; }
	
    @Override
    public int hashCode() {
    	// Use the name as a id for hashing workerstatus
        return getName().hashCode();
    }
	
}
