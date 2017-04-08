package iii_dataTransformation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import databaseMySQL.MySQLDBController;
import databaseMySQL.MySQLDBViewer;
import ii_dataAnalysis.FinancialStatementAnalysisYearly;
import settings.ExternalFilePath;

/**
 * <<DEPRECATED CLASS>> please use MongoDB version.
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description For generate RapidMiner training file format
 */
public class FinancialPositionCSVGenerator_MySQL {
	private final static Logger logger = LogManager.getLogger(FinancialPositionCSVGenerator_MySQL.class);
	private static Connection con;
	private static Vector<String> stockList;

	public static void main(String[] args) {
		con = MySQLDBController.openDBConnection();
		stockList = MySQLDBViewer.getSymbolList(con);
		writeCSV("fp_model_v2", ExternalFilePath.FINANCIAL_POS_ATTR_SELECTED_FILEPATH,
				createDataMap(ExternalFilePath.SETSMART_FINANCIAL_POSITION_YEARLY_FILEPATH));
		writeCSV("ci_model_v2", ExternalFilePath.COMPREHEN_INCOME_ATTR_SELECTED_FILEPATH,
				createDataMap(ExternalFilePath.SETSMART_COMPREHENSIVE_INCOME_YEARLY_FILEPATH));
		writeCSV("cf_model_v2", ExternalFilePath.CASH_FLOW_ATTR_SELECTED_FILEPATH,
				createDataMap(ExternalFilePath.SETSMART_CASH_FLOW_YEARLY_FILEPATH));
	}

	/**
	 * @param map
	 *            3 layer hash map ["stock_name",[year,["attribute",value]]]
	 * @param stockList
	 * @return 3 layer hash map ["stock_name",[year,["attribute",value]]]
	 */
	private static HashMap<String, HashMap<Integer, HashMap<String, Double>>> insertEmptyMap(
			HashMap<String, HashMap<Integer, HashMap<String, Double>>> map) {
		for (String stock_name : stockList) {
			HashMap<Integer, HashMap<String, Double>> map_2 = new HashMap<Integer, HashMap<String, Double>>();
			map.put(stock_name, map_2);
		}
		return map;
	}

	private static HashMap<String, HashMap<Integer, HashMap<String, Double>>> createDataMap(
			String financial_statement_file_path) {
		HashMap<String, HashMap<Integer, HashMap<String, Double>>> data_map = new HashMap<String, HashMap<Integer, HashMap<String, Double>>>();
		data_map = insertEmptyMap(data_map);
		for (String stock_name : stockList) {
			Vector<Vector<String>> data_vec = FinancialStatementAnalysisYearly
					.readFinancialStatementToVector(stock_name, financial_statement_file_path);
			Vector<String> year_vec = data_vec.get(0);
			data_vec.remove(0); // remove year vector
			HashMap<Integer, HashMap<String, Double>> year_map = data_map.get(stock_name);
			for (int i = 0; i < year_vec.size(); i++) {
				int year = Integer.parseInt(year_vec.get(i));
				HashMap<String, Double> attr_map = new HashMap<String, Double>();
				for (Vector<String> attr : data_vec) {
					String attr_name = attr.get(0);
					Double attr_value;
					try {
						// System.out.println(
						// "year :" + year + " attr : " + attr_name + "parsing
						// -> " + attr.get(i * 2 + 1));
						attr_value = Double.parseDouble(attr.get((i + 1) * 2).replaceAll(",", ""));
					} catch (NumberFormatException e) {
						logger.debug(e.getMessage());
						attr_value = null;
					}
					attr_map.put(attr_name, attr_value);
				}
				year_map.put(year, attr_map);
			}
			data_map.put(stock_name, year_map);
		}
		return data_map;
	}

	/**
	 * @param year
	 * @return output string (CSV format) on each year of financial statement
	 */
	private static String readDataMapIntoCSV(int year, String selected_attr_file_path,
			HashMap<String, HashMap<Integer, HashMap<String, Double>>> data_map) {
		// generate all output string
		String result_txt = "symbol";
		try {
			FileReader fr = new FileReader(selected_attr_file_path);
			BufferedReader br = new BufferedReader(fr);
			Vector<String> attr_selected_list = new Vector<String>();
			String attr;
			while ((attr = br.readLine()) != null) {
				attr_selected_list.add(attr);
				result_txt += "," + attr + " " + year;
			}
			br.close();
			for (String stock_name : stockList) {
				result_txt += "\n";
				result_txt += stock_name;
				if (!data_map.get(stock_name).containsKey(year)) {
					for (int i = 0; i < attr_selected_list.size(); i++) {
						result_txt += ",?";
					}
					continue;
				}
				for (String attr_name : attr_selected_list) {
					Double val = data_map.get(stock_name).get(year).get(attr_name);
					if (val == null) {
						result_txt += ",?";
					} else {
						result_txt += "," + val;
					}
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return result_txt;
	}

	private static void writeCSV(String file_name, String selected_attr_file_path,
			HashMap<String, HashMap<Integer, HashMap<String, Double>>> data_map) {
		for (int year = 2012; year <= 2014; year++) {
			// CSV prepared format from selected attribute
			String result_txt = readDataMapIntoCSV(year, selected_attr_file_path, data_map);
			try {
				FileWriter fw = new FileWriter(
						ExternalFilePath.OUTPUT_MODEL_CSV_FILEPATH + file_name + "_" + year + ".txt");
				BufferedWriter bw = new BufferedWriter(fw);
				// System.out.println(result_txt);
				bw.write(result_txt);
				bw.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
			logger.info("finish write " + file_name + "_" + year + ".txt");
		}
	}

}
