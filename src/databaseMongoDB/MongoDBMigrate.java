package databaseMongoDB;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import settings.ExternalFilePath;

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
		SymbolController.getInstance().insertIndexing();
		// insertStockListTable();
	}

	private static void insertStockListTable() {
		try {
			FileReader fileReader = new FileReader(new File(ExternalFilePath.STOCKLIST_INPUT_FILEPATH));
			bufferedReader = new BufferedReader(fileReader);
			String txt;
			while ((txt = bufferedReader.readLine()) != null) {
				logger.info("insert stock into symbol collection : " + txt);
				SymbolController.getInstance().insertSymbol(txt);
			}
			bufferedReader.close();
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
}
