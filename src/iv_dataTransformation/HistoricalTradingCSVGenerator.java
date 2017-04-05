package iv_dataTransformation;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.bson.Document;

import util.CSVModifier;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class HistoricalTradingCSVGenerator extends CSVGenerator {

	private final int YEAR = 2015;

	String symbol;
	Date start_date;
	Double now_pe, now_pbv, now_div_yield;
	List<Double> close_chg;
	List<Double> turn_over_chg;

	public static void main(String[] args) {
		HistoricalTradingCSVGenerator csvgen = new HistoricalTradingCSVGenerator();
		csvgen.createCSV(LogManager.getLogger(HistoricalTradingCSVGenerator.class), "historicalTrading",
				"historicalTrading_2015.csv");
	}

	private void resetVariable() {
		symbol = null;
		start_date = null;
		now_pe = null;
		now_pbv = null;
		now_div_yield = null;
		close_chg = new ArrayList<Double>();
		turn_over_chg = new ArrayList<Double>();
	}

	@Override
	public void writeHeader() {
		String header = "symbol,start_date,now_pe,now_pbv,now_div_yield,";
		for (int i = 1; i <= 30; i++) {
			header += "close_chg_" + i + ",";
		}
		for (int i = 1; i <= 29; i++) {
			header += "turn_over_chg_" + i + ",";
		}
		header += "turn_over_chg_" + 30;
		writeLine(header);
	}

	@Override
	public void writeRow() {
		getCollection().find().forEach(new Consumer<Document>() {
			@Override
			public void accept(Document t) {
				resetVariable();
				symbol = t.getString("symbol_name");
				List<Document> data = (List<Document>) t.get("data");
				Collections.reverse(data.subList(0, data.size()));
				data.forEach(new Consumer<Document>() {
					@Override
					public void accept(Document t) {
						Date date = (Date) t.get("date");
						Calendar cal = Calendar.getInstance();
						cal.setTime(date);
						Calendar cal2 = Calendar.getInstance();
						cal2.set(YEAR, Calendar.JANUARY, 1);
						Calendar cal3 = Calendar.getInstance();
						cal3.set(YEAR, Calendar.FEBRUARY, 1);
						if (start_date == null && cal.after(cal2) && cal.before(cal3)) {
							start_date = date;
						}
						if (start_date != null) {
							if (now_pe == null) {
								now_pe = t.getDouble("p/e");
							}
							if (now_pbv == null) {
								now_pbv = t.getDouble("p/bv");
							}
							if (now_div_yield == null) {
								now_div_yield = t.getDouble("divided_yield(%)");
							}
							if (close_chg.size() < 30 && t.getDouble("percent_change") != null) {
								close_chg.add(t.getDouble("percent_change"));
							}
							if (turn_over_chg.size() < 30 && t.getDouble("turnover_ratio(%)") != null) {
								turn_over_chg.add(t.getDouble("turnover_ratio(%)"));
							}
						}
					}
				});
				// write data
				CSVModifier mod = new CSVModifier();
				mod.addItem(symbol).addItem(start_date).addItem(now_pe).addItem(now_pbv).addItem(now_div_yield);
				for (int i = 0; i < 30; i++) {
					try {
						mod.addItem(close_chg.get(i));
					} catch (IndexOutOfBoundsException e) {
						Double val = null;
						mod.addItem(val);
					}
				}
				for (int i = 0; i < 30; i++) {
					try {
						mod.addItem(turn_over_chg.get(i));
					} catch (IndexOutOfBoundsException e) {
						Double val = null;
						mod.addItem(val);
					}
				}
				getLogger().info(mod.getCSVString());
				writeLine(mod.getCSVString());
			}

		});
	}

}
