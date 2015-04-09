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

public class ReduceContext implements Context {
	
	PrintWriter writer;
	
	public ReduceContext(File outDir) throws IOException {
		
		// Create output file
		File outFile = new File(DirectoryTools.safeDirName(outDir.getAbsolutePath(), "reduce-out"));
		outFile.createNewFile();
		this.writer = new PrintWriter(new FileWriter(outFile));
	}
	
	@Override
	public void write(String key, String value) {
		// Create line
		if (key.isEmpty() || value.isEmpty()) return;
		String line = key + "\t" + value;
		
		synchronized(this.writer) {
			this.writer.println(line);
			this.writer.flush();
		}
	}
	
	public void close() {
		// TODO
	}

}
