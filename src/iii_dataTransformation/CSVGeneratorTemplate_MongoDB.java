package iii_dataTransformation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.client.MongoCollection;

import databaseMongoDB.MongoDBConnector;
import settings.ExternalFilePath;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public abstract class CSVGeneratorTemplate_MongoDB {

	protected Logger mLogger;
	protected FileWriter fw;
	protected BufferedWriter bw;
	protected MongoCollection<Document> mCollection;

	public void createCSV(Logger logger, String collection_name, String filename) {
		mLogger = logger;
		initDatabaseConnection(collection_name);
		initBufferedWriter(filename);
		writeHeader();
		writeRow();
		closeBufferedWriter();
	}

	protected void initDatabaseConnection(String collection_name) {
		mLogger.info("initializing database connnection");
		mCollection = MongoDBConnector.getInstance().getCollection(collection_name);
	}

	protected void closeBufferedWriter() {
		mLogger.info("closing buffered writer");
		try {
			bw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
			mLogger.error(e.getMessage());
		}
	}

	protected void closeDatabaseConnection() {
		mLogger.info("closing database connection");
		MongoDBConnector.getInstance().closeConnection();
	}

	protected void initBufferedWriter(String filename) {
		mLogger.info("initializing buffered writer");
		try {
			fw = new FileWriter(new File(ExternalFilePath.OUTPUT_MODEL_CSV_FILEPATH + filename));
		} catch (IOException e) {
			e.printStackTrace();
			mLogger.error(e.getMessage());
		}
		bw = new BufferedWriter(fw);
	}

	public Logger getLogger() {
		return mLogger;
	}

	public BufferedWriter getBufferedWriter() {
		return bw;
	}

	public void writeLine(String line) {
		try {
			bw.write(line);
			bw.newLine();
		} catch (IOException e) {
			e.printStackTrace();
			mLogger.error(e.getMessage());
		}
	}

	public MongoCollection<Document> getCollection() {
		return mCollection;
	}

	public abstract void writeHeader();

	public abstract void writeRow();

}
