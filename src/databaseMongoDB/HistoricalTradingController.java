package databaseMongoDB;

import java.util.List;
import java.util.Vector;

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

		}
		return instance;
	}

	private HistoricalTradingController() {
		mCollection = MongoDBConnector.getInstance().getCollection("historicalTrading");
	}

	private MongoCollection<Document> mCollection;

	public Document getHistoricalTradingData(String symbol) {
		symbol = symbol.trim().toUpperCase();
		return mCollection.find(new Document("name", symbol)).first();
	}

	public void insertIndexing() {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		mCollection.createIndex(Indexes.ascending("symbol_name", "data.date"), indexOptions);
	}

	public void insertHistoricalTradingData(String symbol, List<Document> data_list) {
		symbol = symbol.trim().toUpperCase();
		try {
			Document symbol_doc = new Document("symbol_name", symbol);
			symbol_doc.append("data", data_list);
			mCollection.insertOne(symbol_doc);
		} catch (MongoException e) {
			logger.error(e.getMessage());
		}
	}

	public Vector<String> getKeyList() {
		Vector<String> vec = new Vector<>();
		vec.add("date");
		vec.add("prior");
		vec.add("open");
		vec.add("high");
		vec.add("low");
		vec.add("close");
		vec.add("change");
		vec.add("percent_change");
		vec.add("average");
		vec.add("aom_value(shares)");
		vec.add("aom_value(baht)");
		vec.add("total_value(shares");
		vec.add("total_value(baht)");
		vec.add("market_cap");
		vec.add("p/e");
		vec.add("p/bv");
		vec.add("divided_yield(%)");
		vec.add("turnover_ratio(%)");
		vec.add("par");
		vec.add("listed_shares(shares)");
		vec.add("trading_sign");
		vec.add("benefit_sign");
		return vec;
	}
}
