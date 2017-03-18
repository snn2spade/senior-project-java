package databaseMongoDB;

import java.sql.Date;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class HistoricalTradingController {
	private static final Logger logger = LogManager.getLogger(HistoricalTradingController.class);
	private static HistoricalTradingController instance;

	public static HistoricalTradingController getInstance() {
		if (instance == null) {
			instance = new HistoricalTradingController();
			instance.mCollection = MongoDBConnector.getInstance().getCollection("historicalTrading");
		}
		return instance;
	}

	private MongoCollection<Document> mCollection;

	public Document getHistoricalTradingData(Date date) {
		return null;
	}

	public void insertIndexing() {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		mCollection.createIndex(Indexes.ascending("date"), indexOptions);
	}

	public boolean insertHistoricalTradingData(Date date, Map<String, Double> arttributeMap) {
		try {
			Document doc = new Document("date", date);
			arttributeMap.keySet().stream().forEach(e -> {
				doc.append(e, arttributeMap.get(e));
			});
			mCollection.insertOne(doc);
			return true;
		} catch (MongoException e) {
			logger.error(e.getMessage());
			return false;
		}
	}
}
