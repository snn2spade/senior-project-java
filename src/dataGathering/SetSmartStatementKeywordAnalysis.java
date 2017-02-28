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
import database.DatabaseViewer;
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
		con = DatabaseController.openDBConnection();
		// List<String> stock_list = DatabaseViewer.getSymbolList(con);
		List<String> stock_list = new ArrayList<String>();
		stock_list.add("AEONTS");
		DatabaseController.closeDBConnection(con);
		Map<String, Integer> map_st = createStatementMap(stock_list,
				ExternalFilePath.SETSMART_COMPREHENSIVE_INCOME_YEARLY_FILEPATH);
		// Vector<String> sorted_key = sortStatementMap(map_st);
		// sorted_key = sortYearKey(sorted_key);
		// sorted_key.stream().forEach(e -> {
		// System.out.println(e + " : " + map_st.get(e));
		// });
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

	/**
	 * @param stock_name
	 *            name of stock
	 * @param file_path
	 *            excel file (.xls) path of Set Smart Financial Statement
	 * @return the first index 0 is Vector that contains avaliable year in file
	 *         <2012,2013,2014,...>. The rest index are vectors contain
	 *         statement infomation <"net_income","100","200","300",..>
	 */
	public static Vector<Vector<String>> financialStatementKeywordAnalysis(String stock_name, String file_path) {
		logger.info("Financial statement gathering for " + stock_name);
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
			logger.debug(" start reading next line : " + txt);
			if (txt.contains("/table")) {
				break;
			}
			if (txt.matches(".*<tr.*")) {
				logger.debug("contains <tr adding new vector");
				data.add(new Vector<String>());
			}
			if (txt.matches(".*<td.*")) {
				if (txt.contains("<strong>")) {
					int last_idx = data.size() - 1;
					data.remove(last_idx);
					logger.debug("remove last vector");
					while (!(txt = scan.nextLine()).contains("/tr")) {
					}
					continue;
				}
				txt = scan.nextLine();
				logger.debug("After found td : " + txt);
				if (txt.contains("&nbsp;")) {
					txt = scan.nextLine();
					logger.debug("line after match &nbsp" + txt);
					txt = txt.trim();
					txt = "(sub) " + txt;
				}

				txt = txt.trim();
				data.lastElement().add(txt);
			}
		}
		scan.close();
		// remove duplicate year (recent column)
		if (data.get(0).size() >= 2 && data.get(0).get(0).equals(data.get(0).get(1))) {
			Vector<String> year_vec = data.get(0);
			year_vec.remove(0);
			data.set(0, year_vec);
			for (int i = 1; i < data.size(); i++) {
				Vector<String> item_vec = data.get(i);
				item_vec.remove(2);
				item_vec.remove(1);
				data.set(i, item_vec);
			}
		}
		// data.stream().forEach(e -> {
		// e.stream().forEach(s -> System.out.print(s + " , "));
		// System.out.println("");
		// });
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
