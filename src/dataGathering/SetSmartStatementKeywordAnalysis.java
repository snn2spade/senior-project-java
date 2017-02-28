package dataGathering;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import database.DatabaseController;
import settings.ExternalFilePath;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class SetSmartStatementKeywordAnalysis {
	private final static Logger logger = LogManager.getLogger(SetSmartStatementKeywordAnalysis.class);
	private static Connection con;

	public static void main(String[] args) {
		List<String> stock_list = retreiveStockList();
		Map<String, Integer> map_st = createStatementMap(stock_list,
				ExternalFilePath.SETSMART_CASH_FLOW_YEARLY_FILEPATH);
		Vector<String> sorted_key = sortStatementMap(map_st);
		 sorted_key = sortYearKey(sorted_key);
		sorted_key.stream().forEach(e -> {
			System.out.println(e + " : " + map_st.get(e));
		});
	}

	private static Vector<String> sortYearKey(Vector<String> vec) {
		for (int i = 0; i < vec.size(); i++) {
			if (vec.get(i).matches("\\d+")) {
				String tmp = (vec.get(i));
				vec.remove(i);
				vec.insertElementAt(tmp, 0);
			}
		}
		return vec;
	}

	private static Vector<String> sortStatementMap(Map<String, Integer> map_st) {
		Set<String> set = map_st.keySet();
		Vector<String> sorted_key = new Vector<>();
		set.stream().forEach(e -> sorted_key.add(e));
		for (int i = 0; i < sorted_key.size() - 1; i++) {
			for (int j = 1; j < sorted_key.size(); j++) {
				if (map_st.get(sorted_key.get(j)) > map_st.get(sorted_key.get(j - 1))) {
					String tmp = sorted_key.get(j);
					sorted_key.set(j, sorted_key.get(j - 1));
					sorted_key.set(j - 1, tmp);
				}
			}
		}
		return sorted_key;
	}

	private static Map<String, Integer> createStatementMap(List<String> stock_list, String file_path) {
		Map<String, Integer> map_st = new HashMap<>();
		stock_list.stream().forEach(e -> {
			Vector<Vector<String>> vec = financialStatementKeywordAnalysis(e, file_path);
			vec.get(0).forEach(e2 -> map_st.put(e2, map_st.containsKey(e2) ? map_st.get(e2) + 1 : 1));
			vec.remove(0);
			vec.stream().forEach(e2 -> {
				if (!e2.isEmpty()) {
					String item_name = e2.get(0);
					map_st.put(item_name, map_st.containsKey(item_name) ? map_st.get(item_name) + 1 : 1);
				}
			});
		});
		return map_st;
	}

	private static List<String> retreiveStockList() {
		con = DatabaseController.openDBConnection();
		ResultSet stock_set = DatabaseController.executeQuerySQL(con, "select * from symbol");
		List<String> stock_list = new ArrayList<String>();
		try {
			while (stock_set.next()) {
				stock_list.add(stock_set.getString(2));
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		DatabaseController.closeDBConnection(con);
		con = null;
		return stock_list;
	}

	private static Vector<Vector<String>> financialStatementKeywordAnalysis(String stock_name, String file_path) {
		BufferedReader buf;
		String line, doc = "";
		try {
			FileReader in = new FileReader(file_path + stock_name + ".xls");
			buf = new BufferedReader(in);
			while ((line = buf.readLine()) != null) {
				if (!line.trim().equals("")) {
					doc += line;
					doc += "\n";
				}
			}
			buf.close();
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		doc = doc.toLowerCase();
		Vector<Vector<String>> data = new Vector<>();
		data.addElement(new Vector<String>());
		Scanner scan = new Scanner(doc);
		String txt;
		int count = 0;
		while (scan.hasNextLine()) { // skip 1st table
			txt = scan.nextLine();
			if (txt.contains("/table")) {
				break;
			}
		}
		while (scan.hasNextLine()) {// gather year
			txt = scan.nextLine();
			if (txt.contains("yearly")) {
				txt = scan.nextLine();
				data.lastElement().add(txt.replaceAll("\\D", ""));
			}
			if (txt.contains("/tr")) {
				count++;
			}
			if (count == 2) {
				break;
			}
		}

		while (scan.hasNextLine()) {
			txt = scan.nextLine();
			if (txt.contains("/table")) {
				break;
			}
			if (txt.contains("tr")) {
				data.add(new Vector<String>());
			}
			if (txt.matches(".*[^/]td.*")) {
				txt = scan.nextLine();
				if (txt.contains("&nbsp;")) {
					txt = scan.nextLine();
					txt = txt.trim();
					txt = "(sub) " + txt;
				}
				txt = txt.trim();
				data.lastElement().add(txt);
			}
		}
		scan.close();
		logger.info("Financial statement gathering for " + stock_name);
//		data.stream().forEach(e -> {
//			e.stream().forEach(s -> System.out.print(s + ", "));
//			System.out.println("");
//		});
		return data;
	}

	private static Vector<Vector<String>> insertFinancialPositionItemType(Vector<Vector<String>> data) {
		int first_idx = 1;
		for (int i = 0; i < data.size(); i++) {
			if (!data.get(i).isEmpty()) {
				String txt2 = data.get(i).get(0);
				if (txt2.contains("total assets")) {
					first_idx = insertItemType(data, first_idx, i, "(asset) ");
				} else if (txt2.contains("total liabilities")) {
					first_idx = insertItemType(data, first_idx, i, "(liabilities) ");
				} else if (txt2.contains("total equity")) {
					first_idx = insertItemType(data, first_idx, i, "(equity) ");
				}
			}
		}
		return data;
	}

	/**
	 * @param data
	 *            vector that contains vector of each statement record (total
	 *            asset,100,100,100..)
	 * @param first_idx
	 *            first index for inserting type
	 * @param last_idx
	 *            last index for inserting type
	 * @return next first index for inserting type
	 */
	private static int insertItemType(Vector<Vector<String>> data, int first_idx, int last_idx, String type) {
		Vector<String> vec;
		for (int j = first_idx; j <= last_idx; j++) {
			vec = data.get(j);
			if (!vec.isEmpty()) {
				vec.set(0, type + data.get(j).get(0));
				data.set(j, vec);
			}
		}
		first_idx = last_idx + 1;
		return first_idx;
	}

}
