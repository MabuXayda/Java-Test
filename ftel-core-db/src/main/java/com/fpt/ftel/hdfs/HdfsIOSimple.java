package com.fpt.ftel.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fpt.ftel.core.config.CommonConfig;

public class HdfsIOSimple {
	private final FileSystem fileSystem;

	public static void main(String[] args) throws IOException, URISyntaxException {
		HdfsIOSimple hdfsIOSimple = new HdfsIOSimple();
		String content = "ghi thu 2 dong trogn hdfs";
		String content2 = "ghi dong thu tiếp theo dong vào hdfs";
		String path = "/data/payTV/test.txt";
		BufferedWriter br = hdfsIOSimple.getWriteStreamNewToHdfs(path);
		br.write(content);
		br.close();
		br = hdfsIOSimple.getWriteStreamAppendToHdfs(path);
		br.write(content2);
		br.close();
	}

	public void test(String path) throws IOException {
		Path pt = new Path(path);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fileSystem.append(pt)));
		String text = "ghi tiep 1 dong vào có Tiếng Việt";
		br.write(text);
		br.close();
	}
	
	public BufferedReader getReadStreamFromHdfs(String filePath) throws UnsupportedEncodingException, IOException{
		Path path = new Path(filePath);
		return new BufferedReader(new InputStreamReader(fileSystem.open(path), "UTF-8"));
	}
	
	public BufferedWriter getWriteStreamNewToHdfs(String filePath) throws UnsupportedEncodingException, IOException {
		Path path = new Path(filePath);
		return new BufferedWriter(new OutputStreamWriter(fileSystem.create(path), "UTF-8"));
	}

	public BufferedWriter getWriteStreamAppendToHdfs(String filePath) throws UnsupportedEncodingException, IOException {
		Path path = new Path(filePath);
		return new BufferedWriter(new OutputStreamWriter(fileSystem.append(path), "UTF-8"));
	}

	public HdfsIOSimple() throws IOException {
		Configuration configuration = new Configuration();
		configuration.addResource(new Path(CommonConfig.getInstance().get(CommonConfig.HDFS_CORE_SITE)));
		configuration.addResource(new Path(CommonConfig.getInstance().get(CommonConfig.HDFS_SITE)));
		fileSystem = FileSystem.get(configuration);
	}

	public void createFolder(String folderPath) throws IOException {
		Path newFolderPath = new Path(folderPath);
		fileSystem.mkdirs(newFolderPath);
	}

	public void write(String path, String content) throws IOException {
		Path file = new Path(path);
		OutputStream os = fileSystem.create(file, true);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		br.write(content);
		br.close();
	}

	public void write(String path, List<String> contents) throws IOException {
		Path file = new Path(path);
		OutputStream os = fileSystem.create(file, true);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		for (String string : contents) {
			br.write(string);
		}
		br.close();
	}

	public List<String> getAllFileInDir(String dir) throws FileNotFoundException, IOException {
		List<String> files = new ArrayList<String>();
		Path path = new Path(dir);
		FileStatus[] status = fileSystem.listStatus(path);
		for (int i = 0; i < status.length; i++) {
			String name = status[i].getPath().getName();
			files.add(name);
		}
		return files;
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

	public long getSpaceConsumed(String dir) throws FileNotFoundException, IOException {
		Path path = new Path(dir);
		return fileSystem.getContentSummary(path).getSpaceConsumed();
	}


	public void readFile(String file) throws FileNotFoundException, IOException {
		Path path = new Path(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 9) {
				System.out.println(arr[0]);
			}
			line = br.readLine();
		}
		br.close();
	}

	public void close() throws IOException {
		fileSystem.close();
	}

	public List<String> getAllFilePath(String dir) throws IOException {
		Path path = new Path(dir);
		List<String> listFile = new ArrayList<>();
		FileStatus[] arrFileStatus = fileSystem.listStatus(path);
		for (FileStatus fileStatus : arrFileStatus) {
			if (fileStatus.isDirectory()) {
				listFile.addAll(getAllFilePath(fileStatus.getPath().toString()));
			} else if (fileStatus.isFile()) {
				if (!fileStatus.getPath().toString().contains("_SUCCESS")) {
					listFile.add(fileStatus.getPath().toString());
				}
			}
		}
		return listFile;
	}
}
