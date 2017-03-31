package databaseMongoDB;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import ii_dataAnalysis.FinancialStatementAnalysis;
import settings.DatabaseConfig;
import settings.ExternalFilePath;
import util.TextExtractor;

/**
 * For Read raw date (Excel file) and put into database by using only Controller
 * class Please not direct insert into database from this class
 * 
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class MongoDBMigrate {
	private static final Logger logger = LogManager.getLogger(MongoDBMigrate.class);
	private static BufferedReader bufferedReader;

	public static void main(String[] args) {
		 imediatelyMigrateToMongoDB();
	}

	private static void imediatelyMigrateToMongoDB() {
		/* Symbol list */
		SymbolController.getInstance().insertIndexing();
		insertStockListTable();
		insertMarketInformation();
		/* Historical Trading */
		HistoricalTradingController.getInstance().insertIndexing();
		insertHistoricalTrading();
		/* Financial position yearly */
		FinancialStatementController.getInstance(DatabaseConfig.FIN_POS_YEARLY_COLLECTION_NAME).insertIndexing();
		insertFinancialStatement(DatabaseConfig.FIN_POS_YEARLY_COLLECTION_NAME,
				ExternalFilePath.SETSMART_FINANCIAL_POSITION_YEARLY_FILEPATH);
		/* Comprehensive income yearly */
		FinancialStatementController.getInstance(DatabaseConfig.COM_IN_YEARLY_COLLECTION_NAME).insertIndexing();
		insertFinancialStatement(DatabaseConfig.COM_IN_YEARLY_COLLECTION_NAME,
				ExternalFilePath.SETSMART_COMPREHENSIVE_INCOME_YEARLY_FILEPATH);
		/* Cash Flow yearly */
		FinancialStatementController.getInstance(DatabaseConfig.CASH_FLOW_YEARLY_COLLECTION_NAME).insertIndexing();
		insertFinancialStatement(DatabaseConfig.CASH_FLOW_YEARLY_COLLECTION_NAME,
				ExternalFilePath.SETSMART_CASH_FLOW_YEARLY_FILEPATH);
		MongoDBConnector.getInstance().closeConnection();
	}

	public static void insertStockListTable() {
		try {
			FileReader fileReader = new FileReader(new File(ExternalFilePath.STOCKLIST_INPUT_FILEPATH));
			bufferedReader = new BufferedReader(fileReader);
			String txt;
			while ((txt = bufferedReader.readLine()) != null) {
				logger.info("inserting stock into symbol collection : " + txt);
				SymbolController.getInstance().insertSymbol(txt);
			}
			bufferedReader.close();
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	public static void insertMarketInformation() {
		try {
			FileReader fileReader = new FileReader(new File(ExternalFilePath.COMPANYLIST_FILEPATH));
			bufferedReader = new BufferedReader(fileReader);
			String txt;
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
					logger.info("inserting market into symbol collection : " + symbol + "," + market);
					SymbolController.getInstance().updateMarket(symbol, market);
					td_count = 3;
				}
			}
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	public static void insertHistoricalTrading() {
		Vector<String> stockList = SymbolController.getInstance().getSymbolList();
		logger.info("number of stock list : " + stockList.size());
		Long startMethodTime = System.currentTimeMillis();
		for (int i = 0; i < stockList.size(); i++) {
			Long startEachStockTime = System.currentTimeMillis();
			logger.info("Start symbol : " + stockList.get(i) + " symbol_id: " + (i + 1));
			try {
				FileReader fileReader = new FileReader(
						new File(ExternalFilePath.SETSMART_HISTORICAL_FILEPATH + stockList.get(i) + ".xls"));
				bufferedReader = new BufferedReader(fileReader);
				String txt, cursor;
				int td_count = -1;
				Vector<String> keyList = HistoricalTradingController.getInstance().getKeyList();
				List<Document> data_list = new ArrayList<Document>();
				Document data_doc = null;
				while ((txt = bufferedReader.readLine()) != null) {
					if (txt.contains("<!-- trading -->")) {
						td_count = 0;
						data_doc = new Document();
					} else if (txt.contains("td")) {
						if (td_count == 0) {
							cursor = txt.substring(txt.indexOf(">") + 1);
							cursor = cursor.substring(0, cursor.indexOf("<"));
							String date_str = TextExtractor.fromDMYtoYYYYMMDD(cursor);
							data_doc.append(keyList.get(td_count), Date.valueOf(date_str));
							td_count++;
						} else if (td_count > 0 && td_count < 20) {
							cursor = txt.substring(txt.indexOf(">") + 1);
							cursor = cursor.substring(0, cursor.indexOf("<"));
							cursor = cursor.trim().replaceAll("[,]", "");
							try {
								data_doc.put(keyList.get(td_count), Double.parseDouble(cursor));
							} catch (NumberFormatException e) {
								data_doc.put(keyList.get(td_count), null);
							}
							td_count++;
						} else if (td_count >= 20) {
							while ((txt = bufferedReader.readLine()) != null) {
								if (txt.contains("</td>")) {
									data_doc.put(keyList.get(td_count), null);
									td_count++;
									break;
								} else if (!txt.trim().equals("")) {
									data_doc.put(keyList.get(td_count), txt.trim());
									td_count++;
									break;
								}
							}
						}
						if (td_count == 22) {
							data_list.add(data_doc);
							td_count = -1;
						}
					} else {
						continue;
					}
				}
				HistoricalTradingController.getInstance().insertHistoricalTradingData(stockList.get(i), data_list);
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
		logger.info("Total usage time : " + ((System.currentTimeMillis() - startMethodTime) / 1000.0) + " s");
	}

	public static void insertFinancialStatement(String collection, String finacial_statement_file_path) {
		Vector<String> symbol_list = SymbolController.getInstance().getSymbolList();
		for (String symbol : symbol_list) {
			logger.info("inserting data stock: " + symbol + " into " + collection);
			Vector<Vector<String>> vec = FinancialStatementAnalysis.readFinancialStatementToVector(symbol,
					finacial_statement_file_path);
			FinancialStatementController con = FinancialStatementController.getInstance(collection);
			List<Document> data_list = new ArrayList<>();
			List<Document> attr_list;
			Document year_doc, attr_doc;
			for (int year_idx = 0; year_idx < vec.get(0).size(); year_idx++) {
				year_doc = new Document("year", vec.get(0).get(year_idx));
				attr_list = new ArrayList<>();
				for (int attr_idx = 1; attr_idx < vec.size(); attr_idx++) {
					attr_doc = new Document("name", vec.get(attr_idx).get(0));
					double value, percent_chg;
					try {
						value = TextExtractor.parseDouble(vec.get(attr_idx).get(year_idx * 2 + 1));
						attr_doc.append("value", value);
					} catch (NumberFormatException e) {
						attr_doc.append("value", null);
					}
					try {
						percent_chg = TextExtractor.parseDouble(vec.get(attr_idx).get(year_idx * 2 + 2));
						attr_doc.append("percent_chg", percent_chg);
					} catch (NumberFormatException e) {
						attr_doc.append("percent_chg", null);
					}
					attr_list.add(attr_doc);
				}
				year_doc.append("attributes", attr_list);
				data_list.add(year_doc);
			}
			con.insertFinancialStatementData(symbol, data_list);
		}
	}
}
