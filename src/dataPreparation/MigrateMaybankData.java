package dataPreparation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mysql.jdbc.StringUtils;

public class MigrateMaybankData {
	private static final Logger logger = LogManager.getLogger(MigrateMaybankData.class);
	static final String stockListFilePath = "D:/Dropbox/Senior project/History Stock Data/stockList.txt";
	static final String companyListedFilePath = "D:/Dropbox/Senior project/History Stock Data/listedCompanies_th_TH.xls";
	static final String stockHistoricalListRootPath = "D:/Dropbox/Senior project/History Stock Data/maybank";

	public static void main(String[] args) {
		// insertStockListTable();
		// insertMarketInformation();
		// insertHistoricalTrading();
		insertFinancialStatementData();
	}

	// insert will truncate all old data
	private static void insertStockListTable() {
		try {
			FileReader fileReader = new FileReader(new File(stockListFilePath));
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String txt;
			Connection con;
			con = DatabaseController.openDBConnection();
			String droptableSQL = "SET SQL_SAFE_UPDATES = 0;";
			String droptableSQL2 = "DELETE FROM stock.symbol";
			String droptableSQL3 = "ALTER TABLE symbol AUTO_INCREMENT = 1";
			DatabaseController.executeSQL(con, droptableSQL);
			DatabaseController.executeSQL(con, droptableSQL2);
			DatabaseController.executeSQL(con, droptableSQL3);
			while ((txt = bufferedReader.readLine()) != null) {
				String sql = "INSERT INTO symbol (name,created_date) VALUES ('" + txt + "',NOW())";
				DatabaseController.executeSQL(con, sql);
			}
			DatabaseController.closeDBConnection(con);
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	private static void insertMarketInformation() {
		try {
			FileReader fileReader = new FileReader(new File(companyListedFilePath));
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String txt;
			Connection con;
			con = DatabaseController.openDBConnection();
			int td_count = 0;
			String symbol = "", market = "";
			while ((txt = bufferedReader.readLine()) != null) {
				if (txt.contains("td nowrap")) {
					td_count++;
				}
				if (td_count == 11) {
					symbol = txt.substring(txt.indexOf(">") + 1);
					symbol = symbol.substring(0, symbol.indexOf("<"));
				}
				if (td_count == 13) {
					market = txt.substring(txt.indexOf(">") + 1);
					market = market.substring(0, market.indexOf("<"));
					String sql = "UPDATE stock.symbol SET `market`='" + market + "' WHERE `name`='" + symbol + "'";
					DatabaseController.executeSQL(con, sql);
					td_count = 3;
				}
			}
			DatabaseController.closeDBConnection(con);
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	// insert won't delete old data
	private static void insertHistoricalTrading() {
		FileReader fileReader2;
		Vector<String> stockList = new Vector<>();
		;
		try {
			fileReader2 = new FileReader(new File(stockListFilePath));
			BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
			String txt;
			while ((txt = bufferedReader2.readLine()) != null) {
				stockList.add(txt);
			}

		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		logger.info("number of stock list : " + stockList.size());
		Long startMethodTime = System.currentTimeMillis();
		Connection con = DatabaseController.openDBConnection();
		for (int i = 0; i < stockList.size(); i++) {
			Long startEachStockTime = System.currentTimeMillis();
			logger.info("Start symbol : " + stockList.get(i) + " symbol_id: " + (i + 1));
			try {
				FileReader fileReader = new FileReader(
						new File(stockHistoricalListRootPath + "/" + stockList.get(i) + ".xls"));
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String txt;
				int td_count = -1;
				String historicalStack[] = new String[13];
				String sql = "insert into historicaltrading (`price_date`,`prior`,`open_price`,`high`,`low`,`close_price`,`chg`,`percent_chg`"
						+ ",`average`,`aom_volume`,`aom_value`,`total_volume`,`total_value`,`created_date`,`symbol_id`,`vendor_id`)"
						+ " values('";
				boolean isFirst = true;

				while ((txt = bufferedReader.readLine()) != null) {
					if (txt.contains("<!-- trading -->")) {
						td_count = 0;
						if (!isFirst) {
							sql += ",('";
						}
						isFirst = false;
					} else if (txt.contains("td")) {
						if (td_count > -1) {
							historicalStack[td_count] = txt.substring(txt.indexOf(">") + 1);
							historicalStack[td_count] = historicalStack[td_count].substring(0,
									historicalStack[td_count].indexOf("<"));
							td_count++;
						}
						if (td_count == 13) {
							historicalStack[0] = TextExtractor.fromDMYtoYYYYMMDD(historicalStack[0]);
							sql += historicalStack[0];
							sql += "',";
							for (int idx = 1; idx < 13; idx++) {
								historicalStack[idx] = historicalStack[idx].replaceAll("[,]", "");
								try {
									Double.parseDouble(historicalStack[idx]);
								} catch (NumberFormatException e) {
									historicalStack[idx] = "null";
								}
								sql += historicalStack[idx];
								sql += ",";
							}
							sql += "now(),";
							sql += (i + 1) + ",";
							sql += "1)";
							td_count = -1;
						}
					} else {
						continue;
					}
				}
				DatabaseController.executeSQL(con, sql);
				logger.info("Stock " + stockList.get(i) + " usage time : "
						+ ((System.currentTimeMillis() - startEachStockTime) / 1000.0) + " s");
			} catch (FileNotFoundException e) {
				logger.error(e.getMessage());
				logger.error("**Error Stop at row " + (i + 1));
				return;
			} catch (IOException e) {
				logger.error(e.getMessage());
				logger.error("**Error Stop at row " + (i + 1));
				return;
			}

		}
		DatabaseController.closeDBConnection(con);
		logger.info("Total usage time : " + ((System.currentTimeMillis() - startMethodTime) / 1000.0) + " s");
	}

	// insert won't delete old data
	private static void insertFinancialStatementData() {
		FileReader fileReader2;
		Vector<String> stockList = new Vector<>();
		;
		try {
			fileReader2 = new FileReader(new File(stockListFilePath));
			BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
			String txt;
			while ((txt = bufferedReader2.readLine()) != null) {
				stockList.add(txt);
			}

		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		logger.info("number of stock list : " + stockList.size());
		Long startMethodTime = System.currentTimeMillis();
		Connection con = DatabaseController.openDBConnection();
		for (int i = 0; i < stockList.size(); i++) {
			Long startEachStockTime = System.currentTimeMillis();
			logger.info("Start symbol : " + stockList.get(i) + " symbol_id: " + (i + 1));
			try {
				FileReader fileReader = new FileReader(
						new File(stockHistoricalListRootPath + "/" + stockList.get(i) + ".xls"));
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String txt;
				int td_count = -1;
				String historicalStack[] = new String[13];
				String sql = "insert into financialstatement (`date`,`market_cap`,`p/e`,`p/bv`,`dividend_yield`,`turnover_ratio`,`par`,`listed_shares`"
						+ ",`created_date`,`symbol_id`,`vendor_id`)" + " values('";
				boolean isFirst = true;

				while ((txt = bufferedReader.readLine()) != null) {
					if (txt.contains("<!-- trading -->")) {
						td_count = 0;
						if (!isFirst) {
							sql += ",('";
						}
						isFirst = false;
					} else if (txt.contains("td")) {
						if (td_count == 0) {
							historicalStack[0] = txt.substring(txt.indexOf(">") + 1);
							historicalStack[0] = historicalStack[0].substring(0, historicalStack[0].indexOf("<"));
							td_count++;
						} else if (td_count > 0) {
							if (td_count < 13) {
								td_count++;
								continue;
							}
							historicalStack[td_count - 12] = txt.substring(txt.indexOf(">") + 1);
							historicalStack[td_count - 12] = historicalStack[td_count - 12].substring(0,
									historicalStack[td_count - 12].indexOf("<"));
							td_count++;
						}
						if (td_count == 20) {
							historicalStack[0] = TextExtractor.fromDMYtoYYYYMMDD(historicalStack[0]);
							sql += historicalStack[0];
							sql += "',";
							for (int idx = 1; idx < 8; idx++) {
								historicalStack[idx] = historicalStack[idx].replaceAll("[,]", "");
								try {
									Double.parseDouble(historicalStack[idx]);
								} catch (NumberFormatException e) {
									historicalStack[idx] = "null";
								}
								sql += historicalStack[idx];
								sql += ",";
							}
							sql += "now(),";
							sql += (i + 1) + ",";
							sql += "1)";
							td_count = -1;
						}
					} else {
						continue;
					}
				}
				DatabaseController.executeSQL(con, sql);
				logger.info("Stock " + stockList.get(i) + " usage time : "
						+ ((System.currentTimeMillis() - startEachStockTime) / 1000.0) + " s");
			} catch (FileNotFoundException e) {
				logger.error(e.getMessage());
				logger.error("**Error Stop at row " + (i + 1));
				return;
			} catch (IOException e) {
				logger.error(e.getMessage());
				logger.error("**Error Stop at row " + (i + 1));
				return;
			}

		}
		DatabaseController.closeDBConnection(con);
		logger.info("Total usage time : " + ((System.currentTimeMillis() - startMethodTime) / 1000.0) + " s");
	}

}
