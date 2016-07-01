package com.fpt.ftel.raw;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fpt.ftel.core.utils.CommonConfig;
import com.fpt.ftel.core.utils.Utils;

public class FileEdit {
	
	public static void main(String[] args) {
		FileEdit fileEdit = new FileEdit();
	}
	
	public void process(){
		renameMultiFile(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "_test_hadoop");
	}
	
	public void renameMultiFile(String folderPath){
		List<String> listFilePath = new ArrayList<>();
		Utils.loadListFilePath(listFilePath, new File(folderPath));
		for(String file : listFilePath){
			System.out.println(file);
		}
	}
}
