package com.fpt.ftel.core.utils;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FileUtils {
	public static void sortListFile(List<File> files) {
		Collections.sort(files, new Comparator<File>() {
			DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");

			public DateTime getDateTime(File file) {
				return dtf.parseDateTime(file.getAbsolutePath().split("/")[7].substring(5, 15));
			}

			public int compare(File file1, File file2) {
				try {
					return getDateTime(file1).compareTo(getDateTime(file2));
				} catch (Exception e) {
					throw e;
				}
			}
		});
	}

	public static void sortListFilePath(List<String> listFile) {
		Collections.sort(listFile, new Comparator<String>() {
			DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");

			public DateTime getDateTime(String o) {
				return dtf.parseDateTime(o.split("/")[7].substring(5, 15));
			}

			public int compare(String o1, String o2) {
				try {
					return getDateTime(o1).compareTo(getDateTime(o2));
				} catch (Exception e) {
					throw e;
				}
			}
		});
	}

	public static void loadListFilePath(List<String> listFile, File file) {
		File[] subdir = file.listFiles();
		for (File f : subdir) {
			if (f.isFile()) {
				listFile.add(f.getAbsolutePath());
			}
			if (f.isDirectory()) {
				loadListFilePath(listFile, f);
			}
		}
	}

	public static boolean isExitFile(File filePath) {
		if (filePath.exists() && !filePath.isDirectory()) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isExitFolder(File folderPath) {
		if (folderPath.exists()) {
			return true;
		} else {
			return false;
		}
	}
	
	public static void createFolder(String folderPath) {
		File theDir = new File(folderPath);
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException se) {
			}
		}
	}

}
