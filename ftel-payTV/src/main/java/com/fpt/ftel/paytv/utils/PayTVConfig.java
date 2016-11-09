package com.fpt.ftel.paytv.utils;

import com.fpt.ftel.core.config.CommonConfig;

public class PayTVConfig {
	public static final String LOG4J_CONFIG_DIR = "LOG4J_CONFIG_DIR";
	public static final String SERVICE_MONITOR_DIR = "SERVICE_MONITOR_DIR";
	public static final String HDFS_CORE_SITE = "HDFS_CORE_SITE";
	public static final String HDFS_SITE = "HDFS_SITE";

	public static final String MAIN_DIR = "MAIN_DIR";
	public static final String RAW_LOG_DIR = "RAW_LOG_DIR";
	public static final String PARSED_LOG_HDFS_DIR = "PARSED_LOG_HDFS_DIR";
	public static final String SUPPORT_DATA_DIR = "SUPPORT_DATA_DIR";

	public static final String USER_SPECIAL_FILE = "USER_SPECIAL_FILE";
	public static final String USER_INFO_FIRST_LOAD_FILE = "USER_INFO_FIRST_LOAD_FILE";
	public static final String USER_INFO_UPDATE_POP_FILE = "USER_INFO_UPDATE_POP_FILE";
	public static final String USER_INFO_UPDATE_ISC_FILE = "USER_INFO_UPDATE_ISC_FILE";
	public static final String CONTRACT_INFO = "CONTRACT_INFO";
	public static final String LOCATION_MAPPING = "LOCATION_MAPPING";
	public static final String USER_CANCEL_FILE = "USER_CANCEL_FILE";
	public static final String USER_REGISTER_FILE = "USER_REGISTER_FILE";
	public static final String CALL_LOG_PURPOSE_FILE = "CALL_LOG_PURPOSE_FILE";

	public static final String RTP_MAX = "RTP_MAX";
	public static final String SMM_MAX = "SMM_MAX";
	public static final String DELAY_ALLOW_RTP = "DELAY_ALLOW_RTP";
	public static final String GET_USER_CHURN_API = "GET_USER_CHURN_API";
	public static final String GET_USER_REGISTER_API = "GET_USER_REGISTER_API";

	public static final String POSTGRESQL_PAYTV_HOST = "POSTGRESQL_PAYTV_HOST";
	public static final String POSTGRESQL_PAYTV_PORT = "POSTGRESQL_PAYTV_PORT";
	public static final String POSTGRESQL_PAYTV_DATABASE = "POSTGRESQL_PAYTV_DATABASE";
	public static final String POSTGRESQL_PAYTV_USER = "POSTGRESQL_PAYTV_USER";
	public static final String POSTGRESQL_PAYTV_USER_PASSWORD = "POSTGRESQL_PAYTV_USER_PASSWORD";

	public static final String POSTGRESQL_PAYTV_TABLE_NOW_TIMETOLIVE = "POSTGRESQL_PAYTV_TABLE_NOW_TIMETOLIVE";
	public static final String POSTGRESQL_PAYTV_TABLE_DAILY_TIMETOLIVE = "POSTGRESQL_PAYTV_TABLE_DAILY_TIMETOLIVE";
	public static final String POSTGRESQL_PAYTV_TABLE_PROFILE_WEEK_TIMETOLIVE = "POSTGRESQL_PAYTV_TABLE_PROFILE_WEEK_TIMETOLIVE";
	public static final String POSTGRESQL_PAYTV_TABLE_PROFILE_MONTH_TIMETOLIVE = "POSTGRESQL_PAYTV_TABLE_PROFILE_MONTH_TIMETOLIVE";

	public static int getRTPMax() {
		return Integer.parseInt(CommonConfig.get(RTP_MAX)) * 3600;
	}

	public static int getSMMMax() {
		return Integer.parseInt(CommonConfig.get(SMM_MAX)) * 3600;
	}
}
