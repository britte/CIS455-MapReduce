package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class InputReader {
	
	private ArrayList<BufferedReader> readers = new ArrayList<BufferedReader>(); 
	
	public InputReader(File inputDir, boolean sort) throws IOException {
		File[] files = inputDir.listFiles();
		for (int i = 0; i < files.length; i++) {
			File f = files[i];
			if (sort) {
				// Get relative file path 
				String relativeFile = new File(".").toURI().relativize(f.toURI()).getPath();
				
				// Sort file
				String command = "sort " + relativeFile + " -o " + relativeFile;
				Runtime.getRuntime().exec(command);
			}
			readers.add(new BufferedReader(new FileReader(files[i])));
		}
	}
	
	public String readLine() {
		try {
			String line = null;
			while (line == null) {
				// Get current reader
				BufferedReader reader = null;
				if (readers.isEmpty()) { // All files have been read
					break; 
				} else {
					reader = this.readers.get(0);
				}
				  
				// Wait until the reader is available then read in a line
				line = reader.readLine();
				synchronized(reader) {
					if (line == null) { // Buffer finished
						reader.close();
						this.readers.remove(0);
						continue;
					}
				}
			}
			return line;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}  
}
