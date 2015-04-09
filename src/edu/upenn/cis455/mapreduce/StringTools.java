package edu.upenn.cis455.mapreduce;

public class StringTools {
	public StringTools(){};
	
	public static String safeDirName(String sup, String sub) {
		if (sup.lastIndexOf("/") == sup.length() - 1) {
			sup = sup.substring(0, sup.length() - 1);
		}
		if (sub.indexOf("/") == 0) {
			sub = sub.substring(1);
		}
		return sup + "/" + sub;
	  }
}
