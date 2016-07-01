package com.fpt.tv.statistic;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

public class UserUsageStatistic {
	private static Map<String, DateTime> _mapUserDateCondition;
	private static Map<String, Map<String, Integer>> _mapUserVectorApp;
	private static Map<String, Map<String,Integer>> _mapUserVectorDaily;
	private static Map<String, Map<Integer,Integer>> _mapUserVectorHourly;
	private static Map<String, Map<Integer, Integer>> _mapUserVectorWeek;
	private static Map<String, Map<String, Integer>> _mapUserReuseTime;
	
	public void getUseUsageStatistic(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			String outputFolderPath, boolean processHourly, boolean processDaily, boolean processApp, boolean processReuse, boolean processWeek){
		
		
	}
	
	private void initMap(boolean processHourly, boolean processDaily, boolean processApp, boolean processReuse, boolean processWeek){
		if(processHourly){
			_mapUserVectorHourly = Collections.synchronizedMap(new HashMap<>());
		}
		if(processDaily){
			_mapUserVectorDaily = Collections.synchronizedMap(new HashMap<>());
		}
		if(processApp){
			_mapUserVectorApp = Collections.synchronizedMap(new HashMap<>());
		}
		if(processReuse){
			_mapUserReuseTime = Collections.synchronizedMap(new HashMap<>());
		}
		if(processWeek){
			_mapUserVectorWeek = Collections.synchronizedMap(new HashMap<>());
		}
	}
}
