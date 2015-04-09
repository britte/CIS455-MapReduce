package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

	public void map(String key, String value, Context context) {
		// Expect input file of form: key <tab> line of text
		String[] words = value.split("\\s");
		for (String word : words) {
			context.write(word, "1");
		}
	}
  
  public void reduce(String key, String[] values, Context context) {
	  // Sum all values
	  int cnt = 0;
	  for (String val : values) {
		  try {
			  cnt += Integer.parseInt(val);
		  } catch (NumberFormatException e) {
			  continue;
		  }
	  }
	  
	  context.write(key, Integer.toString(cnt));

  }
  
}
