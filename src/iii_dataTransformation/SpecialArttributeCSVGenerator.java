package iii_dataTransformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.bson.Document;

import com.mongodb.client.MongoCollection;

import databaseMongoDB.MongoDBConnector;
import databaseMongoDB.SymbolController;
import util.CSVModifier;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class SpecialArttributeCSVGenerator extends CSVGeneratorTemplate_MongoDB {

	private int year;

	private MongoCollection<Document> comprehensiveIncomeCollection;
	private MongoCollection<Document> financialPosCollection;
	private MongoCollection<Document> cashFlowCollection;

	private Map<Integer, Map<String, Map<String, Double>>> comprehen_attributes_map = new HashMap<>();
	private Map<Integer, Map<String, Map<String, Double>>> financial_attributes_map = new HashMap<>();
	private Map<Integer, Map<String, Map<String, Double>>> cashflow_attributes_map = new HashMap<>();

	public static void main(String[] args) {
		SpecialArttributeCSVGenerator csvgen = new SpecialArttributeCSVGenerator(2013);
		csvgen.createCSV();
		csvgen.closeDatabaseConnection();
	}

	public SpecialArttributeCSVGenerator(int year) {
		this.year = year;
		mLogger = LogManager.getLogger(SpecialArttributeCSVGenerator.class);
		initDatabaseConnection();
		createAllAttributeMap();
	}

	private void initDatabaseConnection() {
		mLogger.info("initializing database connnection");
		comprehensiveIncomeCollection = MongoDBConnector.getInstance().getCollection("comprehensive_income_yearly");
		financialPosCollection = MongoDBConnector.getInstance().getCollection("financial_position_yearly");
		cashFlowCollection = MongoDBConnector.getInstance().getCollection("cash_flow_yearly");
	}

	public void createCSV() {
		initBufferedWriter("special_attribute_" + year + ".csv");
		writeHeader();
		writeRow();
		closeBufferedWriter();
	}

	@Override
	public void writeHeader() {
		mLogger.info("writing csv header");
		CSVModifier mod = new CSVModifier();
		mod.addItem("symbol").addItem("dividend_paid_chg_" + year).addItem("divendend_per_net_profit_" + year)
				.addItem("divendend_per_retained_earning_" + year).addItem("roa_" + year).addItem("roe_" + year)
				.addItem("roa_chg_" + year).addItem("roe_chg_" + year);
		writeLine(mod.getCSVString());
	}

	@Override
	public void writeRow() {
		for (String symbol : SymbolController.getInstance().getSymbolList()) {
			CSVModifier mod = new CSVModifier();
			mod.addItem(symbol);
			mod.addItem(getPercentChg("dividend paid", symbol, cashflow_attributes_map.get(year - 1),
					cashflow_attributes_map.get(year)));
			Double diviend_per_net_profit = getPortion("dividend paid", "net profit (loss)", symbol,
					cashflow_attributes_map.get(year), comprehen_attributes_map.get(year));
			if (diviend_per_net_profit != null) {
				mod.addItem(-diviend_per_net_profit);
			} else {
				mod.addItem(diviend_per_net_profit);
			}
			Double diviend_per_retained_earning = getPortion("dividend paid", "retained earnings (deficit)", symbol,
					cashflow_attributes_map.get(year), financial_attributes_map.get(year));
			if (diviend_per_retained_earning != null) {
				mod.addItem(-diviend_per_retained_earning);
			} else {
				mod.addItem(diviend_per_retained_earning);
			}
			// find roa
			Double cur_roa = findROA(symbol, year);
			Double his_roa = findROA(symbol, year - 1);
			Double cur_roe = findROE(symbol, year);
			Double his_roe = findROE(symbol, year - 1);
			Double roa_chg = null, roe_chg = null;
			mod.addItem(cur_roa);
			mod.addItem(cur_roe);
			if (cur_roa != null && his_roa != null) {
				roa_chg = (cur_roa - his_roa) / his_roa * 100.0;
			}
			if (cur_roe != null && his_roe != null) {
				roe_chg = (cur_roe - his_roe) / his_roe * 100.0;
			}
			mod.addItem(roa_chg);
			mod.addItem(roe_chg);
			writeLine(mod.getCSVString());
			mLogger.info(mod.getCSVString());
		}
	}

	public Double findROA_2(String symbol, int year) {
		if (financial_attributes_map.get(year).containsKey(symbol)
				&& comprehen_attributes_map.get(year).containsKey(symbol)) {
			Double cur_asset = financial_attributes_map.get(year).get(symbol).get("total assets");
			Double net_profit = comprehen_attributes_map.get(year).get(symbol).get("net profit (loss)");
			Double roa = null;
			if (cur_asset != null & net_profit != null) {
				roa = net_profit / cur_asset * 100.0;
			}
			return roa;
		} else {
			return null;
		}
	}

	public Double findROA(String symbol, int year) {
		if (financial_attributes_map.get(year - 1) != null && financial_attributes_map.get(year).containsKey(symbol)
				&& financial_attributes_map.get(year - 1).containsKey(symbol)
				&& comprehen_attributes_map.get(year).containsKey(symbol)) {
			Double cur_asset = financial_attributes_map.get(year).get(symbol).get("total assets");
			Double his_asset = financial_attributes_map.get(year - 1).get(symbol).get("total assets");
			Double net_profit = comprehen_attributes_map.get(year).get(symbol).get("net profit (loss)");
			Double roa = null;
			if (cur_asset != null & his_asset != null & net_profit != null) {
				Double avg_asset = (cur_asset + his_asset) / 2.0;
				roa = net_profit / avg_asset * 100.0;
			}
			return roa;
		} else {
			return null;
		}
	}

	public Double findROE_2(String symbol, int year) {
		if (financial_attributes_map.get(year).containsKey(symbol)
				&& comprehen_attributes_map.get(year).containsKey(symbol)) {
			Double cur_equity = financial_attributes_map.get(year).get(symbol).get("total equity");
			Double net_profit = comprehen_attributes_map.get(year).get(symbol).get("net profit (loss)");
			Double roe = null;
			if (cur_equity != null & net_profit != null) {
				roe = net_profit / cur_equity * 100.0;
			}
			return roe;
		} else {
			return null;
		}
	}

	public Double findROE(String symbol, int year) {
		if (financial_attributes_map.get(year - 1) != null && financial_attributes_map.get(year).containsKey(symbol)
				&& financial_attributes_map.get(year - 1).containsKey(symbol)
				&& comprehen_attributes_map.get(year).containsKey(symbol)) {
			Double cur_equity = financial_attributes_map.get(year).get(symbol).get("total equity");
			Double his_eauity = financial_attributes_map.get(year - 1).get(symbol).get("total equity");
			Double net_profit = comprehen_attributes_map.get(year).get(symbol).get("net profit (loss)");
			Double roe = null;
			if (cur_equity != null & his_eauity != null & net_profit != null) {
				Double avg_equity = (cur_equity + his_eauity) / 2.0;
				roe = net_profit / avg_equity * 100.0;
			}
			return roe;
		} else {
			return null;
		}
	}

	private void createAllAttributeMap() {
		createAttributeMap(cashflow_attributes_map, cashFlowCollection);
		createAttributeMap(comprehen_attributes_map, comprehensiveIncomeCollection);
		createAttributeMap(financial_attributes_map, financialPosCollection);
	}

	private void createAttributeMap(Map<Integer, Map<String, Map<String, Double>>> cur_attr_map,
			MongoCollection<Document> mCollection) {
		mCollection.find().forEach(new Consumer<Document>() {
			@Override
			public void accept(Document t) {
				String symbol = t.getString("symbol_name");
				List<Document> data = (List<Document>) t.get("data");
				data.forEach(new Consumer<Document>() {
					@Override
					public void accept(Document t) {
						int cur_year = Integer.parseInt(t.getString("year"));
						if (!cur_attr_map.containsKey(cur_year)) {
							cur_attr_map.put(cur_year, new HashMap<>());
						}
						if (!cur_attr_map.get(cur_year).containsKey(symbol)) {
							cur_attr_map.get(cur_year).put(symbol, new HashMap<>());
						}
						List<Document> attrs = (List<Document>) t.get("attributes");
						attrs.forEach(new Consumer<Document>() {
							@Override
							public void accept(Document t) {
								if (!cur_attr_map.get(cur_year).get(symbol).containsKey(t.getString("name").trim())) {
									cur_attr_map.get(cur_year).get(symbol).put(t.getString("name").trim(),
											t.getDouble("value"));
								}
							}
						});
					}
				});
			}
		});
	}

	private Double getPercentChg(String attr_name, String symbol, Map<String, Map<String, Double>> his_attr_map,
			Map<String, Map<String, Double>> cur_attr_map) {
		if (his_attr_map.get(symbol) != null && cur_attr_map.get(symbol) != null) {
			Double cur_val, his_val;
			cur_val = cur_attr_map.get(symbol).get(attr_name.trim());
			his_val = his_attr_map.get(symbol).get(attr_name.trim());
			if (cur_val != null && his_val != null) {
				return (cur_val - his_val) / (his_val) * 100;
			}
		}
		return null;
	}

	private Double getPortion(String upper_attr, String lower_attr, String symbol,
			Map<String, Map<String, Double>> upper_attr_map, Map<String, Map<String, Double>> lower_attr_map) {
		if (upper_attr_map.get(symbol) != null && lower_attr_map.get(symbol) != null) {
			Double upper_val, lower_val;
			upper_val = upper_attr_map.get(symbol).get(upper_attr.trim());
			lower_val = lower_attr_map.get(symbol).get(lower_attr.trim());
			if (upper_val != null && lower_val != null) {
				return (upper_val / lower_val) * 100;
			}
		}
		return null;
	}

}
