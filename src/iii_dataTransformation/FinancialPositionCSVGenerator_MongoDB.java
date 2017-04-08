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
public class FinancialPositionCSVGenerator_MongoDB extends CSVGeneratorTemplate_MongoDB {

	private int year;

	String symbol;

	Map<String, Double> cur_attributes_map;
	Map<String, Double> his_attributes_map;

	public static void main(String[] args) {
		FinancialPositionCSVGenerator_MongoDB csvgen = new FinancialPositionCSVGenerator_MongoDB(2015);
		csvgen.createCSV(LogManager.getLogger(FinancialPositionCSVGenerator_MongoDB.class), "financial_position_yearly",
				"financial_position_" + csvgen.year + ".csv");
	}

	public FinancialPositionCSVGenerator_MongoDB(int year) {
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
		mod.addItem("symbol").addItem("cash_chg_" + year)
			.addItem("cash_assets_portion_" + year)
			.addItem("short_investment_chg_"+year)
			.addItem("short_investment_assets_portion_"+year)
				.addItem("trade_account_receivable_chg_" + year)
				.addItem("trade_account_receivable_assets_portion_" + year)
				.addItem("advance_short_loan_chg_"+year)
				.addItem("advance_short_loan_assets_portion_"+year)
				.addItem("inventories_chg_" + year)
				.addItem("inventories_assets_portion_" + year)
				.addItem("property_chg_" + year)
				.addItem("property_assets_portion_" + year)
				.addItem("invest_account_cost_chg_"+year)
				.addItem("invest_account_cost_assets_portion_"+year)
				.addItem("intangible_assets_chg_"+year)
				.addItem("intangible_assets_portion_"+year)
				.addItem("current_assets_chg_" + year)
				.addItem("noncurrent_assets_chg_" + year)
				.addItem("current_noncurrent_assets_portion_" + year)
				.addItem("total_assets_chg_" + year)

				.addItem("liabilities_equity_portion_" + year)
				.addItem("bank_overdraft_chg_"+year)
				.addItem("bank_overdraft_liabilities_portion_"+year)
				.addItem("trade_account_payable_chg_"+year)
				.addItem("trade_account_payable_liabilities_portion_"+year)
				.addItem("current_portion_long_liabilities_chg_"+year)
				.addItem("current_portion_long_liabilities_portion_"+year)
				.addItem("net_current_portion_long_liabilities_chg_"+year)
				.addItem("net_current_portion_long_liabilities_portion_"+year)
				.addItem("net_post_employee_chg_"+year)
				.addItem("net_post_employee_liabilities_portion_"+year)
				.addItem("current_liabilities_chg_" + year)
				.addItem("noncurrent_liabliities_chg_" + year)
				.addItem("current_noncurrent_liabilities_portion_" + year)
				.addItem("total_liabilities_chg_" + year)

				.addItem("share_captital_chg_" + year)
				.addItem("share_captital_equity_portion_" + year)
				.addItem("retained_earning_chg_" + year)
				.addItem("retained_earning_portion_" + year)
				.addItem("total_equity_chg_" + year);
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
					mod.addItem(symbol).addItem(getPercentChg("cash and cash equivalents"))
							.addItem(getPortion("cash and cash equivalents", "total assets"))
							.addItem(getPercentChg("short-term investments"))
							.addItem(getPortion("short-term investments", "total assets"))
							.addItem(getPercentChg("trade accounts and other receivable"))
							.addItem(getPortion("trade accounts and other receivable", "total assets"))
							.addItem(getPercentChg("advances and short-term loans"))
							.addItem(getPortion("advances and short-term loans", "total assets"))
							.addItem(getPercentChg("inventories")).addItem(getPortion("inventories", "total assets"))
							.addItem(getPercentChg("property, plant and equipments - net"))
							.addItem(getPortion("property, plant and equipments - net", "total assets"))
							.addItem(getPercentChg("investment accounted for using cost method"))
							.addItem(getPortion("investment accounted for using cost method", "total assets"))
							.addItem(getPercentChg("intangible assets - net"))
							.addItem(getPortion("intangible assets - net", "total assets"))
							.addItem(getPercentChg("total current assets"))
							.addItem(getPercentChg("total non-current assets"))
							.addItem(getPortion("total current assets", "total non-current assets"))
							.addItem(getPercentChg("total assets"))

							.addItem(getPortion("total liabilities", "total equity"))
							.addItem(getPercentChg("bank overdrafts and short-term borrowings from financial institutions"))
							.addItem(getPortion("bank overdrafts and short-term borrowings from financial institutions", "total liabilities"))
							.addItem(getPercentChg("trade accounts and other payable"))
							.addItem(getPortion("trade accounts and other payable", "total liabilities"))
							.addItem(getPercentChg("current portion of long-term liabilities"))
							.addItem(getPortion("current portion of long-term liabilities", "total liabilities"))
							.addItem(getPercentChg("net of current portion of long-term liabilities"))
							.addItem(getPortion("net of current portion of long-term liabilities", "total liabilities"))
							.addItem(getPercentChg("net of current portion of post employee benefit obligations"))
							.addItem(getPortion("net of current portion of post employee benefit obligations", "total liabilities"))
							.addItem(getPercentChg("total current liabilities"))
							.addItem(getPercentChg("total non-current liabilities"))
							.addItem(getPortion("total current liabilities", "total non-current liabilities"))
							.addItem(getPercentChg("total liabilities"))

							.addItem(getPercentChg("authorized share capital"))
							.addItem(getPortion("authorized share capital", "total equity"))
							.addItem(getPercentChg("retained earnings (deficit)"))
							.addItem(getPortion("retained earnings (deficit)", "total equity"))
							.addItem(getPercentChg("total equity"));
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
