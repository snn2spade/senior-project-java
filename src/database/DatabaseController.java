package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import settings.DatabaseConfig;

/**
 * @author NAPAT PAOPONGPAIBUL
 * This source code was used in my senior project 2016 for Education purpose ONLY
 * @description 
 * for connector to mySQL database and execute SQL
 */
public class DatabaseController {
	private static final Logger logger = LogManager.getLogger(DatabaseController.class);
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/" + DatabaseConfig.DB_NAME + "?user=" + DatabaseConfig.DB_USER
			+ "&password=" + DatabaseConfig.DB_PASS;

	/**
	 * @return Connection for execute SQL, must be call closeDBConnection() after finished
	 */
	public static Connection openDBConnection() {
		Connection connect = null;
		try {
			Class.forName(JDBC_DRIVER);
			connect = DriverManager.getConnection(DB_URL);
			if (connect == null) {
				logger.error("Cannot connect to stock database");
			} else {
				logger.info("Establish connection to stock database");
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
		}
		return connect;
	}

	public static void closeDBConnection(Connection con) {
		try {
			if (con != null) {
				con.close();
				logger.info("Closed connection for stock database");
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * method for excuteSQL that doesn't have reply result, must be open connection first
	 * @param con Java.sql.Connection that already connected to database
	 * @param sql SQL statement
	 */
	public static void executeSQL(Connection con, String sql) {
		try {
			Statement stmt = con.createStatement();
			stmt.execute(sql);
			logger.debug("Execute statement: " + sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			logger.error("Execute statement: " + sql);
			e.printStackTrace();
		}
	}
	/**
	 * method for excuteSQL that does have reply result, must be open connection first
	 * @param con Java.sql.Connection that already connected to database
	 * @param sql SQL statement 
	 * @return result of SQL statement
	 */
	public static ResultSet executeQuerySQL(Connection con, String sql) {
		ResultSet result = null;
		try {
			Statement stmt = con.createStatement();
			result = stmt.executeQuery(sql);
			logger.debug("Execute statement: " + sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			logger.error("Execute statement: " + sql);
			e.printStackTrace();
		}
		return result;
	}
}
