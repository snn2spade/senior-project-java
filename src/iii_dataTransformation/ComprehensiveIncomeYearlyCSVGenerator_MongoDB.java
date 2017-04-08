package iii_dataTransformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.bson.Document;

import util.CSVModifier;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class ComprehensiveIncomeYearlyCSVGenerator_MongoDB extends CSVGeneratorTemplate_MongoDB {

	private int year;

	String symbol;

	Map<String, Double> cur_attributes_map;
	Map<String, Double> his_attributes_map;

	public static void main(String[] args) {
		ComprehensiveIncomeYearlyCSVGenerator_MongoDB csvgen = new ComprehensiveIncomeYearlyCSVGenerator_MongoDB(2013);
		csvgen.createCSV(LogManager.getLogger(ComprehensiveIncomeYearlyCSVGenerator_MongoDB.class),
				"comprehensive_income_yearly", "comprehensive_income_" + csvgen.year + ".csv");
	}

	public ComprehensiveIncomeYearlyCSVGenerator_MongoDB(int year) {
		this.year = year;
	}

	private void resetVariable() {
		symbol = null;
		cur_attributes_map = new HashMap<>();
		his_attributes_map = new HashMap<>();
	}

	@Override
	public void writeHeader() {
		getLogger().info("writing header");
		CSVModifier mod = new CSVModifier();
		mod.addItem("symbol").addItem("revenue_goods_services_chg_" + year).addItem("other_income_chg_" + year)
				.addItem("total_revenue_chg_" + year).addItem("cost_goods_services_chg_" + year)
				.addItem("selling_admin_expense_chg_" + year).addItem("total_expense_chg_" + year)
				.addItem("profit_before_fin_tax_chg_" + year).addItem("net_profit_chg_" + year)
				.addItem("profit_attribute_to_equity_holder_chg_" + year).addItem("eps_chg_" + year)
				.addItem("total_other_income_chg_" + year);
		writeLine(mod.getCSVString());
	}

	@Override
	public void writeRow() {
		getCollection().find().forEach(new Consumer<Document>() {
			@Override
			public void accept(Document t) {
				resetVariable();
				symbol = t.getString("symbol_name");
				List<Document> data = (List<Document>) t.get("data");
				data.forEach(new Consumer<Document>() {
					@Override
					public void accept(Document t) {
						if (t.getString("year").equals("" + year)) {
							List<Document> attrs = (List<Document>) t.get("attributes");
							attrs.forEach(new Consumer<Document>() {
								@Override
								public void accept(Document t) {
									if (!cur_attributes_map.containsKey(t.getString("name").trim())) {
										cur_attributes_map.put(t.getString("name").trim(), t.getDouble("value"));
									}
								}
							});
						} else if (t.getString("year").equals("" + (year - 1))) {
							List<Document> attrs = (List<Document>) t.get("attributes");
							attrs.forEach(new Consumer<Document>() {
								@Override
								public void accept(Document t) {
									if (!his_attributes_map.containsKey(t.getString("name").trim())) {
										his_attributes_map.put(t.getString("name").trim(), t.getDouble("value"));
									}
								}
							});
						}
					}
				});
				if (cur_attributes_map.size() > 0 && his_attributes_map.size() > 0) {
					// write data
					CSVModifier mod = new CSVModifier();
					mod.addItem(symbol).addItem(getPercentChg("revenues from sale of goods and rendering of services"))
							.addItem(getPercentChg("other income")).addItem(getPercentChg("total revenues"))
							.addItem(getPercentChg("cost of sale of goods and rendering of services"))
							.addItem(getPercentChg("selling and administrative expenses"))
							.addItem(getPercentChg("total expenses"))
							.addItem(getPercentChg("profit (loss) before finance costs and income tax expenses"))
							.addItem(getPercentChg("net profit (loss)"))
							.addItem(getPercentChg("profit (loss) attributable to equity holders of the parent"))
							.addItem(getPercentChg("basic earnings per share (unit : baht)"))
							.addItem(getPercentChg("total other comprehensive income"));
					writeLine(mod.getCSVString());
					getLogger().info(mod.getCSVString());
				}
			}
		});
	}

	private Double getPercentChg(String attr_name) {
		Double cur_val, his_val;
		cur_val = cur_attributes_map.get(attr_name.trim());
		his_val = his_attributes_map.get(attr_name.trim());
		if (cur_val != null && his_val != null) {
			return (cur_val - his_val) / (his_val) * 100;
		} else {
			return null;
		}
	}

	private Double getPortion(String upper_attr, String lower_attr) {
		Double upper_val, lower_val;
		upper_val = cur_attributes_map.get(upper_attr.trim());
		lower_val = cur_attributes_map.get(lower_attr.trim());
		if (upper_val != null && lower_val != null) {
			return (upper_val / lower_val) * 100;
		} else {
			return null;
		}
	}

}
