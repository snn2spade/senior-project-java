package databaseMongoDB;

import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class SymbolController {
	private static final Logger logger = LogManager.getLogger(SymbolController.class);
	private static SymbolController instance;

	public static SymbolController getInstance() {
		if (instance == null) {
			instance = new SymbolController();
		}
		return instance;
	}

	private SymbolController() {
		mCollection = MongoDBConnector.getInstance().getCollection("symbol");
	}

	private MongoCollection<Document> mCollection;

	public void insertIndexing() {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		mCollection.createIndex(Indexes.ascending("name"), indexOptions);
		logger.info("inserted indexing");
	}

	public Vector<String> getSymbolList() {
		Vector<String> vec = new Vector<>();
		mCollection.find().forEach(new Block<Document>() {
			public void apply(Document t) {
				vec.add(t.getString("name"));
			};
		});
		return vec;
	}

	public void insertSymbol(String symbol) {
		symbol = symbol.trim().toUpperCase();
		if (mCollection.find(new Document("name", symbol)).first() != null) {
			logger.error("cannot insert duplicate item");
		}
		mCollection.insertOne(new Document("name", symbol));
	}

	public void updateMarket(String symbol, String market) {
		symbol = symbol.trim().toUpperCase();
		market = market.trim().toLowerCase();
		try {
			mCollection.updateOne(new Document("name", symbol), new Document("$set", new Document("market", market)));
		} catch (MongoException e) {
			logger.error(e.getMessage());
		}
	}
}
