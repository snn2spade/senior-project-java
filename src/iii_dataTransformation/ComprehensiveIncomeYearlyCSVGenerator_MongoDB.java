package iii_dataTransformation;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
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

	private int year = 2014;
	private Double bound = 0.0;

	private String symbol;

	private Map<String, Double> cur_attributes_map;
	private Map<String, Double> his_attributes_map;

	public static void main(String[] args) {
		DecimalFormat df = new DecimalFormat("###");
		ComprehensiveIncomeYearlyCSVGenerator_MongoDB csvgen = new ComprehensiveIncomeYearlyCSVGenerator_MongoDB();
		csvgen.createCSV(LogManager.getLogger(ComprehensiveIncomeYearlyCSVGenerator_MongoDB.class),
				"comprehensive_income_yearly",
				"comprehensive_income_" + csvgen.year + "_bound" + df.format(csvgen.bound) + ".csv");
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
					if (bound == 0.0) {
						mod.addItem(symbol)
								.addItem(getPercentChg("revenues from sale of goods and rendering of services"))
								.addItem(getPercentChg("other income")).addItem(getPercentChg("total revenues"))
								.addItem(getPercentChg("cost of sale of goods and rendering of services"))
								.addItem(getPercentChg("selling and administrative expenses"))
								.addItem(getPercentChg("total expenses"))
								.addItem(getPercentChg("profit (loss) before finance costs and income tax expenses"))
								.addItem(getPercentChg("net profit (loss)"))
								.addItem(getPercentChg("profit (loss) attributable to equity holders of the parent"))
								.addItem(getPercentChg("basic earnings per share (unit : baht)"))
								.addItem(getPercentChg("total other comprehensive income"));
					} else {
						mod.addItem(symbol)
								.addItem(getPercentChg("revenues from sale of goods and rendering of services", -bound,
										bound))
								.addItem(getPercentChg("other income", -bound, bound))
								.addItem(getPercentChg("total revenues", -bound, bound))
								.addItem(
										getPercentChg("cost of sale of goods and rendering of services", -bound, bound))
								.addItem(getPercentChg("selling and administrative expenses", -bound, bound))
								.addItem(getPercentChg("total expenses", -bound, bound))
								.addItem(getPercentChg("profit (loss) before finance costs and income tax expenses",
										-bound, bound))
								.addItem(getPercentChg("net profit (loss)", -bound, bound))
								.addItem(getPercentChg("profit (loss) attributable to equity holders of the parent",
										-bound, bound))
								.addItem(getPercentChg("basic earnings per share (unit : baht)", -bound, bound))
								.addItem(getPercentChg("total other comprehensive income", -bound, bound));
					}
					writeLine(mod.getCSVString());
					getLogger().info(mod.getCSVString());
				}
			}
		});
	}

	private Double getPercentChg(String attr_name, Double lower_bound, Double upper_bound) {
		Double cur_val, his_val, res;
		try {
			cur_val = cur_attributes_map.get(attr_name.trim());
			his_val = his_attributes_map.get(attr_name.trim());
			if (cur_val == null || his_val == null) {
				return null;
			}
			if (his_val < 0) {
				res = (cur_val - his_val) / (-his_val) * 100;
			} else {
				res = (cur_val - his_val) / (his_val) * 100;
			}
		} catch (Exception e) {
			return null;
		}
		if (res > upper_bound) {
			res = upper_bound;
		} else if (res < lower_bound) {
			res = lower_bound;
		}
		return res;
	}

	private Double getPercentChg(String attr_name) {
		try {
			Double cur_val, his_val;
			cur_val = cur_attributes_map.get(attr_name.trim());
			his_val = his_attributes_map.get(attr_name.trim());
			if (cur_val == null || his_val == null) {
				return null;
			}
			if (his_val < 0) {
				return (cur_val - his_val) / (-his_val) * 100;
			} else {

				return (cur_val - his_val) / (his_val) * 100;
			}
		} catch (Exception e) {
			return null;
		}
	}

	private Double getPortion(String upper_attr, String lower_attr, Double lower_bound, Double upper_bound) {
		try {
			Double upper_val, lower_val;
			upper_val = cur_attributes_map.get(upper_attr.trim());
			lower_val = cur_attributes_map.get(lower_attr.trim());

			if (upper_val != null && lower_val != null) {
				Double res = (upper_val / lower_val) * 100;
				if (res > upper_bound) {
					res = upper_bound;
				} else if (res < lower_bound) {
					res = lower_bound;
				}
				return res;
			} else {
				return null;
			}
		} catch (Exception e) {
			return null;
		}
	}

	private Double getPortion(String upper_attr, String lower_attr) {
		try {
			Double upper_val, lower_val;
			upper_val = cur_attributes_map.get(upper_attr.trim());
			lower_val = cur_attributes_map.get(lower_attr.trim());

			if (upper_val != null && lower_val != null) {
				return (upper_val / lower_val) * 100;
			} else {
				return null;
			}
		} catch (Exception e) {
			return null;
		}
	}

}
