package datascihomework;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.function.Consumer;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import databaseMongoDB.MongoDBConnector;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class Main {
	private static MongoCollection<Document> mCollection;
	private static Double equity, liabilities, assets;
	private static Double equity_chg, liabilities_chg;
	private static int count;
	private static BufferedWriter bw;
	private static FileWriter fw;

	public static void main(String[] args) {
		try {
			fw = new FileWriter("/Users/snn2spade/Desktop/stock_data_set.csv", false);
			bw = new BufferedWriter(fw);
			bw.write(
					"symbol,year,total_equity,equity_percent_chg,total_liabilities,liabilities_percent_chg,total_assets");
			bw.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		mCollection = MongoDBConnector.getInstance().getCollection("financial_position_yearly");
		Vector<Vector<String>> vec = new Vector<>();
		System.out.println(
				"symbol,year,total_equity,equity_percent_chg,total_liabilities,liabilities_percent_chg,total_assets");
		for (Document cur : mCollection.find()) {
			String symbol = cur.getString("symbol_name");
			List<Document> data = (List<Document>) cur.get("data");
			for (Document year_doc : data) {
				count = 0;
				List<Document> attributes = (List<Document>) year_doc.get("attributes");
				attributes.forEach(new Consumer<Document>() {
					@Override
					public void accept(Document t) {
						String attr_name = t.getString("name");
						if (attr_name.equals("total equity")) {
							equity = t.getDouble("value");
							equity_chg = t.getDouble("percent_chg");
							if (equity != null) {
								count++;
							}
						} else if (attr_name.equals("total liabilities")) {
							liabilities = t.getDouble("value");
							liabilities_chg = t.getDouble("percent_chg");
							if (liabilities != null) {
								count++;
							}
						}
					}
				});
				if (count == 2) {
					assets = equity + liabilities;
					System.out.print(symbol + ",");
					System.out.print(year_doc.getString("year") + ",");
					System.out.print(equity + "," + equity_chg + ",");
					System.out.print(liabilities + "," + liabilities_chg + ",");
					System.out.println(assets);
					try {
						bw.write(symbol + ",");
						bw.write(year_doc.getString("year") + ",");
						bw.write(equity + "," + equity_chg + ",");
						bw.write(liabilities + "," + liabilities_chg + ",");
						bw.write(assets + "");
						bw.newLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
