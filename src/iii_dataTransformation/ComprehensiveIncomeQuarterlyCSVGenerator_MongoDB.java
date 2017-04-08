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
public class ComprehensiveIncomeQuarterlyCSVGenerator_MongoDB extends CSVGeneratorTemplate_MongoDB {

	private int year;
	private int quarter;

	private String symbol;

	private Map<String, Double> cur_attributes_map;
	private Map<String, Double> his_attributes_map;

	public static void main(String[] args) {
		ComprehensiveIncomeQuarterlyCSVGenerator_MongoDB csvgen = new ComprehensiveIncomeQuarterlyCSVGenerator_MongoDB(
				2015);
		csvgen.createCSV(LogManager.getLogger(ComprehensiveIncomeQuarterlyCSVGenerator_MongoDB.class),
				"comprehensive_income_quarterly", "comprehensive_income_quarterly_" + csvgen.year + ".csv");
	}

	public ComprehensiveIncomeQuarterlyCSVGenerator_MongoDB(int year) {
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
		mod.addItem("symbol");
		String quarter_name;
		for (quarter = 4; quarter >= 1; quarter--) {
			quarter_name = quarter + "/" + year;
			mod.addItem("revenue_goods_services_chg_" + quarter_name).addItem("other_income_chg_" + quarter_name)
					.addItem("total_revenue_chg_" + quarter_name).addItem("cost_goods_services_chg_" + quarter_name)
					.addItem("selling_admin_expense_chg_" + quarter_name).addItem("total_expense_chg_" + quarter_name)
					.addItem("profit_before_fin_tax_chg_" + quarter_name).addItem("net_profit_chg_" + quarter_name)
					.addItem("profit_attribute_to_equity_holder_chg_" + quarter_name).addItem("eps_chg_" + quarter_name)
					.addItem("total_other_income_chg_" + quarter_name);
		}
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
				CSVModifier mod = new CSVModifier();
				mod.addItem(symbol);
				for (quarter = 4; quarter >= 1; quarter--) {
					his_attributes_map = new HashMap<>();
					cur_attributes_map = new HashMap<>();
					data.forEach(new Consumer<Document>() {
						@Override
						public void accept(Document t) {
							if (t.getString("quarter").equals(quarter + "/" + year)) {
								List<Document> attrs = (List<Document>) t.get("attributes");
								attrs.forEach(new Consumer<Document>() {
									@Override
									public void accept(Document t) {
										if (!cur_attributes_map.containsKey(t.getString("name").trim())) {
											cur_attributes_map.put(t.getString("name").trim(), t.getDouble("value"));
										}
									}
								});
							} else if (t.getString("quarter").equals(getBackwardQuarter(quarter, year))) {
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
						mod.addItem(getPercentChg("revenues from sale of goods and rendering of services"))
								.addItem(getPercentChg("other income")).addItem(getPercentChg("total revenues"))
								.addItem(getPercentChg("cost of sale of goods and rendering of services"))
								.addItem(getPercentChg("selling and administrative expenses"))
								.addItem(getPercentChg("total expenses"))
								.addItem(getPercentChg("profit (loss) before finance costs and income tax expenses"))
								.addItem(getPercentChg("net profit (loss)"))
								.addItem(getPercentChg("profit (loss) attributable to equity holders of the parent"))
								.addItem(getPercentChg("basic earnings per share (unit : baht)"))
								.addItem(getPercentChg("total other comprehensive income"));
					}
				}
				writeLine(mod.getCSVString());
				getLogger().info(mod.getCSVString());
			}
		});
	}

	private String getBackwardQuarter(int cur_quarter, int cur_year) {
		if (cur_quarter > 1) {
			return (cur_quarter - 1) + "/" + cur_year;
		} else {
			return 4 + "/" + (cur_year - 1);
		}
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
