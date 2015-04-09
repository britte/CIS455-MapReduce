package edu.upenn.cis455.mapreduce;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

public class mapContext implements Context {

	ArrayList<String> workers;
	HashMap<String, PrintWriter> workerFiles = new HashMap<String, PrintWriter>(); 
	
	public mapContext(File outDir, ArrayList<String> workers) throws IOException {
		this.workers = workers;
		
		// Create output file and writer for each worker in spool-out 
		for (String workerName : workers) {
			File outFile = new File(DirectoryTools.safeDirName(outDir.getAbsolutePath(), workerName + "-out"));
			outFile.createNewFile();
			PrintWriter writer = new PrintWriter(new FileWriter(outFile));
			workerFiles.put(workerName, writer);
		}
	}
	
	@Override
	public void write(String key, String value) {
		// Create line
		if (key.isEmpty() || value.isEmpty()) return;
		String line = key + "\t" + value;
		
		// Determine the hash of the key for mapping
		String worker = assignWorker(key);
		PrintWriter writer = this.workerFiles.get(worker);
		
		synchronized(writer) {
			writer.println(line);
			writer.flush();
		}
	}
	
	// Get the SHA-1 hash of the given key
	private BigInteger hashKey(String key) {
		try {
			// Get byte[] of hash
			MessageDigest digest = MessageDigest.getInstance("SHA-1");
			digest.update(key.getBytes());
			byte[] hashArr = digest.digest();
			  
			// Convert byte[] to (positive) BigInteger
			return new BigInteger(1, hashArr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
	}
	  
	// Assign a worker based on a given key
	private String assignWorker(String key) {
		// Get hash for the given key
		BigInteger hash = hashKey(key);
		BigInteger mod = new BigInteger(Integer.toString(this.workers.size()));
		  
		int i = hash.mod(mod).intValue();
		  
		return this.workers.get(i);
	}

}
