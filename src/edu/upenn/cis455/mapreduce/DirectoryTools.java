package edu.upenn.cis455.mapreduce;

import java.io.File;

public class DirectoryTools {
	public DirectoryTools(){};
	
	public static String safeDirName(String sup, String sub) {
		if (sup == null) sup = "";
		if (sub == null) sub = "";
		if (sup.lastIndexOf("/") == sup.length() - 1) {
			sup = sup.substring(0, sup.length() - 1);
		}
		if (sub.indexOf("/") == 0) {
			sub = sub.substring(1);
		}
		return sup + "/" + sub;
	}
	
	// Safely create a sub-directory within storage directory 
	// If the directory exists, clear contents; otherwise create
	public static File cleanMkdir(File rootDir, String dirName) {
		File[] storageContains = rootDir.listFiles();
		for (File f : storageContains) {
			// Sub directory already exists
			if (f.isDirectory() && f.getName().equals(dirName)) {
				// Delete all contents
				File[] subDirContains = f.listFiles();
				for (File sf : subDirContains) {
					sf.delete();
				}
				return f;
			}
		}
		// Sub directory did not exist
		File subDir = new File(safeDirName(rootDir.getAbsolutePath(), dirName));
		subDir.mkdirs();
		return subDir;
	}
}
