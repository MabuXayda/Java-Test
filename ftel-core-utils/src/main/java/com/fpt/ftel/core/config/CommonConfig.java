package com.fpt.ftel.core.config;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CommonConfig {

	private static CommonConfig instance = new CommonConfig();
	private static final String COMMON_CONFIGURATION_FOLDER = "COMMON_CONFIGURATION_FOLDER";
	private static final String COMMON_CONFIGURATION_FILE = "common_configuration.json";

	private Map<String, Object> configurationData = new HashMap<>();

	// private static CommonConfig instance;
	// public static CommonConfig getInstance() {
	// if (instance == null) {
	// synchronized (CommonConfig.class) {
	// if (instance == null) {
	// instance = new CommonConfig();
	// }
	// }
	// }
	// return instance;
	// }

	private CommonConfig() {
		loadCommonConfiguration();
	}

	@SuppressWarnings("unchecked")
	private void loadCommonConfiguration() {
		configurationData.clear();
		JSONParser parser = new JSONParser();
		try {
			Object object = parser.parse(new FileReader(
					getEnvironmentVariable(COMMON_CONFIGURATION_FOLDER) + "/" + COMMON_CONFIGURATION_FILE));
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

	private String getEnvironmentVariable(String variable) {
		try {
			String environmentVariable = System.getenv(variable);
			System.out.println("Environment Variable: " + environmentVariable);
			return environmentVariable;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	public static String get(String key) {
		if (instance.configurationData.containsKey(key)) {
			return instance.configurationData.get(key).toString();
		}
		return "";
	}

}
