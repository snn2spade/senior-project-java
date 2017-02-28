package database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description aggregated frequently simple query statement
 */
public class DatabaseViewer {
	private static final Logger logger = LogManager.getLogger(DatabaseViewer.class);

	/**
	 * @param con Database connection from javal.sql.Connection
	 * @return vector of symbol list
	 */
	public static Vector<String> getSymbolList(Connection con) {
		String symbol_query = "SELECT `name` FROM stock.symbol;";
		ResultSet symbol_set = DatabaseController.executeQuerySQL(con, symbol_query);
		Vector<String> symbol_list = new Vector<>();
		try {
			while (symbol_set.next()) {
				symbol_list.add(symbol_set.getString(1));
			}
		} catch (SQLException e) {
			logger.error(e.toString());
		}
		return symbol_list;
	}
}
