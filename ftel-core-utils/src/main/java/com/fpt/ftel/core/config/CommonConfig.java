package com.fpt.ftel.core.config;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CommonConfig {

	private static final String COMMON_PAYTV_CONFIGURATION_FOLDER = "COMMON_PAYTV_CONFIGURATION_FOLDER";
	private static final String COMMON_PAYTV_CONFIGURATION_FILE = "common_paytv_configuration.json";
	public static final String HDFS_CORE_SITE = "HDFS_CORE_SITE";
	public static final String HDFS_SITE = "HDFS_SITE";
	
	public static final String MAIN_DIR = "MAIN_DIR";
	public static final String RAW_LOG_DIR = "RAW_LOG_DIR";
	public static final String PARSED_LOG_DIR = "PARSED_LOG_DIR";
	public static final String PARSED_LOG_HDFS_DIR = "PARSED_LOG_HDFS_DIR";
	public static final String SUPPORT_DATA_DIR = "SUPPORT_DATA_DIR";
	public static final String USER_SPECIAL_FILE = "USER_SPECIAL_FILE";

	private String configurationFile = "";
	private String configurationFolder = "";
	private Map<String, Object> configurationData = new HashMap<>();
	private static CommonConfig instance;

	public static void main(String[] args) {
		System.out.println(CommonConfig.getInstance().get(RAW_LOG_DIR));
	}

	public static CommonConfig getInstance() {
		if (instance == null) {
			synchronized (CommonConfig.class) {
				if (instance == null) {
					instance = new CommonConfig();
				}
			}
		}
		return instance;
	}

	private CommonConfig() {
		loadCommonConfiguration();
	}

	@SuppressWarnings("unchecked")
	private void loadCommonConfiguration() {
		configurationFolder = getEnvironmentVariable(COMMON_PAYTV_CONFIGURATION_FOLDER);
		configurationFile = configurationFolder + "/" + COMMON_PAYTV_CONFIGURATION_FILE;
		configurationData.clear();
		JSONParser parser = new JSONParser();
		try {
			Object object = parser.parse(new FileReader(configurationFile));
			JSONObject jsonObject = (JSONObject) object;
			Iterator<String> keys = jsonObject.keySet().iterator();
			while (keys.hasNext()) {
				String key = keys.next();
				Object value = jsonObject.get(key);
				configurationData.put(key, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static String getEnvironmentVariable(String variable) {
		try {
			String environmentVariable = System.getenv(variable);
			System.out.println("Environment Variable: " + environmentVariable);
			return environmentVariable;
		} catch (Exception e) {
			System.out.println("Exception caught: " + e.getMessage());
		}
		return "";
	}

	public String get(String key) {
		if (configurationData.containsKey(key)) {
			return configurationData.get(key).toString();
		}
		return "";
	}

}
