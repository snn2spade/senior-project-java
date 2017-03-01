package dataTransformation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import database.DatabaseController;
import database.DatabaseViewer;
import settings.ExternalFilePath;
import settings.ModelParameter;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description Attribute (Column) transformation and blending for preparing
 *              before modelize data
 */
public class MultifoldClassify {
	private final static Logger logger = LogManager.getLogger(MultifoldClassify.class);
	private static Connection con;

	public static void main(String[] args) {
		con = DatabaseController.openDBConnection();
		TreeMap<String, String> res_map = createMultifoldClassifyOnStockList(con);
		DatabaseController.closeDBConnection(con);
		writeCSVMultifoldResult(res_map, "mulfold_model_1_mar_17_v1");
	}

	private static void writeCSVMultifoldResult(TreeMap<String, String> res_map, String file_name) {
		Set<String> key_set = res_map.keySet();
		key_set.stream().sorted().collect(Collectors.toList());
		FileWriter fw;
		try {
			fw = new FileWriter(new File(ExternalFilePath.OUTPUT_MODEL_CSV_FILEPATH + file_name + ".txt"));
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("symbol,isMultifold");
			bw.newLine();
			for (String key : key_set) {
				String txt = key;
				txt += "," + res_map.get(key);
				bw.write(txt);
				bw.newLine();
			}
			bw.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

	/**
	 * @param con
	 *            DatabaseConnection
	 * @return Map <"symbol_name",result> , result set is {"true","false","?"}
	 */
	private static TreeMap<String, String> createMultifoldClassifyOnStockList(Connection con) {
		Vector<String> symbol_list = DatabaseViewer.getSymbolList(con);
		TreeMap<String, String> res_map = new TreeMap<String, String>();
		int count_true = 0, count_false = 0;
		for (String symbol : symbol_list) {
			try {
				// System.out.print(symbol + " : ");
				int res = -1;
				switch (ModelParameter.MULTIFOLD_DETECT_MODE) {
				case 1:
					res = MultifoldClassify.isMultifoldStock_1(symbol, ModelParameter.S_DATE_MULFOLD,
							ModelParameter.E_DATE_MULFOLD, ModelParameter.MUL_TIME_MULFOLD, con);
					break;
				case 2:
					res = MultifoldClassify.isMultifoldStock_2(symbol, ModelParameter.S_DATE_MULFOLD,
							ModelParameter.E_DATE_MULFOLD, ModelParameter.MUL_TIME_MULFOLD, con);
					break;
				}
				if (res == 1) {
					res_map.put(symbol, "true");
				} else if (res == 0) {
					res_map.put(symbol, "false");
				} else {
					res_map.put(symbol, "?");
				}

				// System.out.println(res);
				if (res == 1) {
					count_true++;
				} else if (res == 0) {
					count_false++;
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}
		int answered_size = count_true + count_false;
		int missing_size = symbol_list.size() - answered_size;
		Double missing_percent = Double.valueOf(missing_size) / symbol_list.size() * 100.0;
		Double true_percent = Double.valueOf(count_true) / Double.valueOf(answered_size) * 100.0;
		logger.info("multifold " + ModelParameter.MUL_TIME_MULFOLD + " times num of mulfold stock/answered : "
				+ count_true + "/" + (answered_size) + " (" + new DecimalFormat("#.##").format(true_percent) + "%)");
		logger.info("Missing value :" + missing_size + "/" + symbol_list.size() + " ("
				+ new DecimalFormat("#.##").format(missing_percent) + "%)");
		return res_map;
	}

	/**
	 * This method will answer multifold stock if max price/min price (in
	 * specific period) >= multiple factor
	 * 
	 * @param symbol
	 *            stock's symbol name
	 * @param s_date
	 *            start date for detection (inclusive)
	 * @param e_date
	 *            end date for detection (inclusive)
	 * @param multiple
	 *            multiple time parameter for classifier
	 * @param con
	 *            database connection from java.sql.Connection
	 * @return 1 is true, 0 is false, -1 is Unknown
	 * @throws Exception
	 *             SQLStatement error occur
	 */
	public static int isMultifoldStock_1(String symbol, String s_date, String e_date, Double multiple, Connection con) {
		Double min_price = 0.0, max_price = 0.0;
		String get_min_price_st = "select stock.get_min_price('" + symbol + "', '" + s_date + "', '" + e_date + "')";
		ResultSet min_price_set = DatabaseController.executeQuerySQL(con, get_min_price_st);
		try {
			if (min_price_set.next()) {
				min_price = min_price_set.getDouble(1);
				if (min_price_set.wasNull()) {
					return -1;
				}
			} else {
				return -1;
			}
		} catch (SQLException e) {
			logger.error(e.toString());
		}
		String get_max_price_st = "select stock.get_max_price('" + symbol + "', '" + s_date + "', '" + e_date + "')";
		ResultSet max_price_set = DatabaseController.executeQuerySQL(con, get_max_price_st);
		try {
			if (max_price_set.next()) {
				max_price = max_price_set.getDouble(1);
				if (max_price_set.wasNull()) {
					return -1;
				}
			} else {
				return -1;
			}
		} catch (SQLException e) {
			logger.error(e.toString());
		}
		if (max_price / min_price >= multiple)
			return 1;
		else
			return 0;
	}

	/**
	 * This method will answer multifold stock if max price in specific
	 * period/today price >= multiple factor
	 * 
	 * @param symbol
	 *            stock's symbol name
	 * @param s_date
	 *            start date for detection (inclusive)
	 * @param e_date
	 *            end date for detection (inclusive)
	 * @param multiple
	 *            multiple time parameter for classifier
	 * @param con
	 *            database connection from java.sql.Connection
	 * @return 1 is true, 0 is false, -1 is Unknown
	 * @throws Exception
	 *             SQLStatement error occur
	 */
	public static int isMultifoldStock_2(String symbol, String s_date, String e_date, Double multiple, Connection con) {
		Double today_price = 0.0, max_price = 0.0;
		String today_price_st = "select close_price from historicaltrading join symbol on symbol.id = symbol_id where symbol.name='"
				+ symbol + "' and price_date ='" + s_date + "';";
		ResultSet today_price_set = DatabaseController.executeQuerySQL(con, today_price_st);
		try {
			if (today_price_set.next()) {
				today_price = today_price_set.getDouble(1);
				if (today_price_set.wasNull()) {
					return -1;
				}
			} else {
				return -1;
			}
		} catch (SQLException e) {
			logger.error(e.toString());
		}
		String get_max_price_st = "select stock.get_max_price('" + symbol + "','" + s_date + "', '" + e_date + "')";
		ResultSet max_price_set = DatabaseController.executeQuerySQL(con, get_max_price_st);
		try {
			if (max_price_set.next()) {
				max_price = max_price_set.getDouble(1);
				if (max_price_set.wasNull()) {
					return -1;
				}

			} else {
				return -1;
			}
		} catch (SQLException e) {
			logger.error(e.toString());
		}
		// System.out.print(" today : " + today_price + ", max price : " +
		// max_price + " , res : ");
		if (max_price / today_price >= multiple)
			return 1;
		else
			return 0;
	}

}
