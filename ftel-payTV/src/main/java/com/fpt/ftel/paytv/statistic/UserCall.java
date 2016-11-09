package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.paytv.db.TableCallLogDAO;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class UserCall {

	private Map<String, Map<String, Integer>> mapContractCallCount;
	private Map<String, Map<String, Integer>> mapContractCallDuration;
	private Map<String, Double> mapContractRecallAvg;

	public void initMap() {
		mapContractCallCount = new HashMap<>();
		mapContractCallDuration = new HashMap<>();
		mapContractRecallAvg = new HashMap<>();
	}

	public static void main(String[] args) throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter("/home/tunn/data/tv/checkCall.csv"));
		List<String> listPurpose = new ArrayList<>(TableCallLogDAO.getMapCallLogPurpose().keySet());
		UserCall object = new UserCall();
		object.processRawLog("");
		Map<String, Map<String, Integer>> callCount = object.getMapContractCallCount();
		Map<String, Map<String, Integer>> callDuration = object.getMapContractCallDuration();
		Map<String, Double> reCall = object.getMapContractRecallAvg();

		pr.print("Contract");
		for (int i = 0; i < listPurpose.size(); i++) {
			pr.print("\tCNT " + listPurpose.get(i));
		}
		for (int i = 0; i < listPurpose.size(); i++) {
			pr.print("\tDUR " + listPurpose.get(i));
		}
		pr.print("\trecall avg");
		pr.println();
		for (String id : callCount.keySet()) {
			Map<String, Integer> mapCount = callCount.get(id);
			Map<String, Integer> mapDuration = callDuration.get(id);

			pr.print(id);
			for (int i = 0; i < listPurpose.size(); i++) {
				// String key = listPurpose.get(i);
				// int value = mapCount.get(key) == null ? 0 :
				// mapCount.get(key);
				// System.out.println(value);
				pr.print("\t" + (mapCount.get(listPurpose.get(i)) == null ? 0 : mapCount.get(listPurpose.get(i))));
			}
			for (int i = 0; i < listPurpose.size(); i++) {
				pr.print(
						"\t" + (mapDuration.get(listPurpose.get(i)) == null ? 0 : mapDuration.get(listPurpose.get(i))));
			}
			pr.print("\t" + (reCall.get(id) == null ? 0 : reCall.get(id)));
			pr.println();

		}
		pr.close();
		System.out.println("DONE");
	}

	public void processRawLog(String path) throws IOException {
		initMap();
		Set<String> setPurpose = TableCallLogDAO.getMapCallLogPurpose().keySet();
		Map<String, DateTime> mapContractCallDate = new HashMap<>();
		Map<String, List<Integer>> mapContractRecallDuration = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		while (line != null) {
			try {
				String[] arr = line.split("\t");
				String contract = arr[0];
				String purpose = arr[1].substring(2);
				DateTime date = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(arr[2].substring(0, 19));
				int duration = Integer.parseInt(arr[3]);
				if (setPurpose.contains(purpose)) {
					Map<String, Integer> mapCallCount = mapContractCallCount.get(contract) == null ? new HashMap<>()
							: mapContractCallCount.get(contract);
					mapCallCount.put(purpose, (mapCallCount.get(purpose) == null ? 0 : mapCallCount.get(purpose)) + 1);
					mapContractCallCount.put(contract, mapCallCount);

					Map<String, Integer> mapCallDuration = mapContractCallDuration.get(contract) == null
							? new HashMap<>() : mapContractCallDuration.get(contract);
					mapCallDuration.put(purpose,
							(mapCallDuration.get(purpose) == null ? 0 : mapCallDuration.get(purpose)) + duration);
					mapContractCallDuration.put(contract, mapCallDuration);

					if (!mapContractCallDate.containsKey(contract)) {
						mapContractCallDate.put(contract, date);
					} else {
						int dayDuration = DateTimeUtils.getDayDuration(mapContractCallDate.get(contract), date);
						if (dayDuration >= 1) {
							List<Integer> listRecallDuration = mapContractRecallDuration.get(contract) == null
									? new ArrayList<>() : mapContractRecallDuration.get(contract);
							listRecallDuration.add(dayDuration);
							mapContractRecallDuration.put(contract, listRecallDuration);
						}
						mapContractCallDate.put(contract, date);
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			line = br.readLine();
		}
		br.close();

		for (String contract : mapContractRecallDuration.keySet()) {
			List<Integer> listRecallDuration = mapContractRecallDuration.get(contract);
			int sum = 0;
			for (int i = 0; i < listRecallDuration.size(); i++) {
				sum += listRecallDuration.get(i);
			}
			double val = Math.round((sum / listRecallDuration.size()) * 100.0) / 100.0;
			mapContractRecallAvg.put(contract, val);
		}

	}

	public Map<String, Map<String, Integer>> getMapContractCallCount() {
		return mapContractCallCount;
	}

	public Map<String, Map<String, Integer>> getMapContractCallDuration() {
		return mapContractCallDuration;
	}

	public Map<String, Double> getMapContractRecallAvg() {
		return mapContractRecallAvg;
	}

}
