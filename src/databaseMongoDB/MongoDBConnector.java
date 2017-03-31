package databaseMongoDB;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import settings.DatabaseConfig;

/**
 * only use for open/close MongoDB connection
 * 
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class MongoDBConnector {
	private static final Logger logger = LogManager.getLogger(MongoDBConnector.class);
	private static MongoDBConnector instance;

	public static MongoDBConnector getInstance() {
		if (instance == null) {
			instance = new MongoDBConnector();
		}
		return instance;
	}

	private MongoDBConnector() {
		openConnection();
	}

	private MongoClient mMongoClient;
	private MongoDatabase mMongoDatabase;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MongoDBConnector con = MongoDBConnector.getInstance();
		MongoCollection<Document> collection = con.getCollection("symbol");
		MongoCursor<Document> cursor = collection.listIndexes().iterator();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
		con.closeConnection();
	}

	public void openConnection() {
		MongoClientURI connectionString = new MongoClientURI(DatabaseConfig.MONGO_URI);
		mMongoClient = new MongoClient(connectionString);
		mMongoDatabase = mMongoClient.getDatabase("stock");
		logger.info("successful open connection");
	}
	public boolean isOpenConnection() {
		return mMongoDatabase != null;
	}

	public void closeConnection() {
		mMongoClient.close();
		mMongoDatabase = null;
		logger.info("successful close connection ");
	}

	public MongoCollection<Document> getCollection(String name) {
		if (isOpenConnection()) {
			return mMongoDatabase.getCollection(name);
		} else {
			logger.error("not yet open connection");
			return null;
		}
	}

}
