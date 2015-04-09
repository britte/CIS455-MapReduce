package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class InputReader {
	
	private ArrayList<BufferedReader> readers = new ArrayList<BufferedReader>(); 
	
	public InputReader(File inputDir) throws FileNotFoundException {
		File[] files = inputDir.listFiles();
		for (int i = 0; i < files.length; i++) {
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
				  synchronized (reader) {
					  line = reader.readLine();
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
