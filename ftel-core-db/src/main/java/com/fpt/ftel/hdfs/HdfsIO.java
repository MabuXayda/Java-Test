package com.fpt.ftel.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.config.PayTVConfig;

public class HdfsIO {
	private final FileSystem fileSystem;

	public HdfsIO() throws IOException {
		Configuration configuration = new Configuration();
		configuration.addResource(new Path(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE)));
		configuration.addResource(new Path(CommonConfig.get(PayTVConfig.HDFS_SITE)));
		fileSystem = FileSystem.get(configuration);
	}

	public BufferedReader getReadStream(String filePath) throws UnsupportedEncodingException, IOException {
		Path path = new Path(filePath);
		return new BufferedReader(new InputStreamReader(fileSystem.open(path), "UTF-8"));
	}

	public BufferedWriter getWriteStreamNew(String filePath) throws UnsupportedEncodingException, IOException {
		Path path = new Path(filePath);
		return new BufferedWriter(new OutputStreamWriter(fileSystem.create(path), "UTF-8"));
	}

	public BufferedWriter getWriteStreamAppend(String filePath) throws UnsupportedEncodingException, IOException {
		Path path = new Path(filePath);
		return new BufferedWriter(new OutputStreamWriter(fileSystem.append(path), "UTF-8"));
	}

	public void createFolder(String folderPath) throws IOException {
		Path newFolderPath = new Path(folderPath);
		fileSystem.mkdirs(newFolderPath);
	}

	public void move(String src, String dst) throws FileNotFoundException, IOException {
		Path oldPath = new Path(src);
		Path newPath = new Path(dst);
		fileSystem.rename(oldPath, newPath);
	}

	public void delete(String dir) throws FileNotFoundException, IOException {
		Path path = new Path(dir);
		fileSystem.delete(path, true);
	}

	public boolean isExist(String dir) throws FileNotFoundException, IOException {
		Path path = new Path(dir);
		return fileSystem.exists(path);
	}

	public void close() throws IOException {
		fileSystem.close();
	}

	public List<String> getListFileInDir(String dir) throws IOException {
		Path path = new Path(dir);
		List<String> listFile = new ArrayList<>();
		FileStatus[] arrFileStatus = fileSystem.listStatus(path);
		for (FileStatus fileStatus : arrFileStatus) {
			if (fileStatus.isDirectory()) {
				listFile.addAll(getListFileInDir(fileStatus.getPath().toString()));
			} else if (fileStatus.isFile()) {
				if (!fileStatus.getPath().toString().contains("_SUCCESS")) {
					listFile.add(fileStatus.getPath().toString());
				}
			}
		}
		return listFile;
	}
}
