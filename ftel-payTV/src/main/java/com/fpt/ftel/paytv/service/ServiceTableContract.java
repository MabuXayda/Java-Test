package com.fpt.ftel.paytv.service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.paytv.db.TableContractDAO;
import com.fpt.ftel.paytv.db.wrapper.TableContractWrapper;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class ServiceTableContract {
	private TableContractDAO tableContractDAO;
	private String status;

	public static void main(String[] args) {
		ServiceTableContract serviceTableContract = new ServiceTableContract();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table ..........");
			try {
				serviceTableContract.processCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("insert") && args.length == 1) {
			System.out.println("Start insert job ..........");
			try {
				serviceTableContract.processInsert();
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceTableContract() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableContractService.properties");
		tableContractDAO = new TableContractDAO();
	}

	public void processCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableContractDAO.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}

	public void processInsert() throws IOException, SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		Map<String, TableContractWrapper> mapContractWrapper = loadContractInfo();
		insertDB(connection, mapContractWrapper);
		ConnectionFactory.closeConnection(connection);
	}

	private void insertDB(Connection connection, Map<String, TableContractWrapper> mapContractWrapper)
			throws SQLException {
		List<Set<String>> listSet = ListUtils.splitSetToSmallerSet(mapContractWrapper.keySet(), 200);
		int countInsert = 0;
		for (Set<String> set : listSet) {
			Map<String, TableContractWrapper> subMap = new HashMap<>();
			for (String id : set) {
				subMap.put(id, mapContractWrapper.get(id));
			}
			tableContractDAO.insertNewContract(connection, subMap);
			countInsert += subMap.size();
		}

		status = "------> Done insertDB CONTRACT | Insert: " + countInsert;
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}

	private Map<String, TableContractWrapper> loadContractInfo() throws FileNotFoundException, IOException {
		Map<String, TableContractWrapper> mapContractWrapper = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(CommonConfig.get(PayTVConfig.CONTRACT_INFO)))) {
			String line = br.readLine();
			line = br.readLine();
			while (line != null) {
				TableContractWrapper wrapper = new TableContractWrapper();
				String[] arr = line.split("\t");
				wrapper.setContract(arr[0]);
				wrapper.setBox_count(Integer.parseInt(arr[1]));
				wrapper.setPayment_method(arr[2]);
				wrapper.setStatus_id(Integer.parseInt(arr[3]));
				wrapper.setStart_date(PayTVUtils.FORMAT_DATE_TIME.parseDateTime(arr[4]));
				if (!arr[5].isEmpty()) {
					wrapper.setStop_date(PayTVUtils.FORMAT_DATE_TIME.parseDateTime(arr[5]));
					if (!arr[6].isEmpty()) {
						wrapper.setStop_reason(arr[6]);
					}
				}
				wrapper.setLocation(arr[7]);
				wrapper.setRegion(arr[8]);
				wrapper.setPoint_set(arr[9]);
				if (!arr[10].isEmpty()) {
					wrapper.setLate_pay_score(Double.parseDouble(arr[10]));
				}
				mapContractWrapper.put(wrapper.getContract(), wrapper);
				line = br.readLine();
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return mapContractWrapper;
	}
}
