package com.fpt.ftel.core.utils;

import java.io.File;

public class FileUtils {
	public static boolean exitFile(File filePath) {
		if (filePath.exists() && !filePath.isDirectory()) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean exitFolder(File folderPath) {
		if (folderPath.exists()) {
			return true;
		} else {
			return false;
		}
	}

	public static void createFolder(File folderPath) {
		if (!exitFolder(folderPath)) {
			try {
				folderPath.mkdir();
			} catch (SecurityException se) {
			}
		}
	}

}
