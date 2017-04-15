package databaseMongoDB;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

/**
 * @author NAPAT PAOPONGPAIBUL
 * This source code was used in my senior project 2016 for Education purpose ONLY
 * @description 
 */
public class PredictGrowthStockController {

	private final static Logger logger = LogManager.getLogger(PredictGrowthStockController.class);
	private static PredictGrowthStockController instance;

	private PredictGrowthStockController(String collection) {
		this.collection = collection;
		setCollection(collection);
	}

	public static PredictGrowthStockController getInstance(String collection) {
		if (instance == null || !collection.equals(instance.collection)) {
			instance = new PredictGrowthStockController(collection);
		}
		return instance;
	}

	private MongoCollection<Document> mCollection;
	private String collection;

	public void setCollection(String collection) {
		mCollection = MongoDBConnector.getInstance().getCollection(collection);
	}

	public void insertIndexing() {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		mCollection.createIndex(Indexes.ascending("symbol_name"), indexOptions);
	}

	public void insertPredictGrowthStockData(String symbol, List<Document> data_list) {
		symbol = symbol.trim().toUpperCase();
		try {
			Document symbol_doc = new Document("symbol_name", symbol);
			symbol_doc.append("data", data_list);
			mCollection.insertOne(symbol_doc);
		} catch (MongoException e) {
			logger.error(e.getMessage());
		}
	}

}
