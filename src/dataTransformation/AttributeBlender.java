package dataTransformation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import database.DatabaseController;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description Attribute (Column) transformation and blending for preparing
 *              before modelize data
 */
public class AttributeBlender {
	private final static Logger logger = LogManager.getLogger(AttributeBlender.class);
	private static Connection con;

	public static void main(String[] args) {
		con = DatabaseController.openDBConnection();
		try {
			System.out.println(isMultifoldStock_1("JAS", "2015-01-01", "2017-01-01", 2.0, con));

		} catch (Exception e) {
			logger.error(e.toString());
		}

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
		//System.out.print(" today : " + today_price + ", max price : " + max_price + " , res : ");
		if (max_price / today_price >= multiple)
			return 1;
		else
			return 0;
	}

}
