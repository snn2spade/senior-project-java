package iii_dataTransformation;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.bson.Document;

import util.CSVModifier;
import util.TimeDateUtil;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class HistoricalTradingCSVGenerator_MongoDB extends CSVGeneratorTemplate_MongoDB {

	private Date sDate;
	private Date minBoundDate_1dayData, minBoundDate_30dayData;
	private int monthDurationAcceptMissingData_1dayData, monthDurationAcceptMissingData_30dayData;

	private String symbol;
	private Date data_start_date;
	private Double now_pe, now_pbv, now_div_yield;
	private List<Double> close, open, high, low, avg, turn_over;
	private List<Double> close_chg, open_chg, high_chg, low_chg, avg_chg;

	private int missing_count_pe, missing_count_pbv, missing_count_div, missing_count_close_chg,
			missing_count_turn_over_chg;

	public static void main(String[] args) {
		HistoricalTradingCSVGenerator_MongoDB csvgen = new HistoricalTradingCSVGenerator_MongoDB(2015, Calendar.JANUARY,
				5, 2, 3);
		SimpleDateFormat sf = new SimpleDateFormat("yyyy_MMM_dd");
		csvgen.createCSV(LogManager.getLogger(HistoricalTradingCSVGenerator_MongoDB.class), "historicalTrading",
				"historicalTrading_" + sf.format(csvgen.sDate) + ".csv");
	}

	public HistoricalTradingCSVGenerator_MongoDB(int year, int month, int date,
			int monthDurationAcceptMissingData_1dayData, int monthDurationAcceptMissingData_30dayData) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month);
		cal.set(Calendar.DATE, date);
		this.monthDurationAcceptMissingData_1dayData = monthDurationAcceptMissingData_1dayData;
		this.monthDurationAcceptMissingData_30dayData = monthDurationAcceptMissingData_30dayData;

		this.sDate = TimeDateUtil.getZeroTimeDate(cal.getTime());

		cal.add(Calendar.MONTH, -monthDurationAcceptMissingData_1dayData);
		this.minBoundDate_1dayData = TimeDateUtil.getZeroTimeDate(cal.getTime());

		cal.add(Calendar.MONTH, monthDurationAcceptMissingData_1dayData);
		cal.add(Calendar.MONTH, -monthDurationAcceptMissingData_30dayData);
		this.minBoundDate_30dayData = TimeDateUtil.getZeroTimeDate(cal.getTime());

		this.missing_count_pe = 0;
		this.missing_count_pbv = 0;
		this.missing_count_div = 0;
		this.missing_count_close_chg = 0;
		this.missing_count_turn_over_chg = 0;
	}

	private void resetVariable() {
		symbol = null;
		data_start_date = null;
		now_pe = null;
		now_pbv = null;
		now_div_yield = null;
		open = new ArrayList<>();
		close = new ArrayList<>();
		low = new ArrayList<>();
		high = new ArrayList<>();
		avg = new ArrayList<>();
		turn_over = new ArrayList<>();
		open_chg = new ArrayList<>();
		close_chg = new ArrayList<>();
		low_chg = new ArrayList<>();
		high_chg = new ArrayList<>();
		avg_chg = new ArrayList<>();
	}

	@Override
	public void writeHeader() {
		String header = "symbol,data_start_date,now_pe,now_pbv,now_div_yield,";
		for (int i = 1; i <= 30; i++) {
			header += "open_chg_" + i + ",";
		}
		for (int i = 1; i <= 30; i++) {
			header += "close_chg_" + i + ",";
		}
		for (int i = 1; i <= 30; i++) {
			header += "high_chg_" + i + ",";
		}
		for (int i = 1; i <= 30; i++) {
			header += "low_chg_" + i + ",";
		}
		for (int i = 1; i <= 30; i++) {
			header += "avg_chg_" + i + ",";
		}
		for (int i = 1; i <= 29; i++) {
			header += "turn_over_" + i + ",";
		}
		header += "turn_over_" + 30;
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
				data.forEach(new Consumer<Document>() {
					@Override
					public void accept(Document t) {
						Date cur_date = (Date) t.get("date");
						cur_date = TimeDateUtil.getZeroTimeDate(cur_date);
						if (data_start_date == null && cur_date.compareTo(sDate) <= 0) {
							data_start_date = cur_date;
						}
						if (data_start_date != null && cur_date.compareTo(minBoundDate_1dayData) >= 0) {
							if (now_pe == null) {
								now_pe = t.getDouble("p/e");
							}
							if (now_pbv == null) {
								now_pbv = t.getDouble("p/bv");
							}
							if (now_div_yield == null) {
								now_div_yield = t.getDouble("divided_yield(%)");
							}
						}
						if (data_start_date != null && cur_date.compareTo(minBoundDate_30dayData) >= 0) {
							if (open.size() < 31 && t.getDouble("open") != null) {
								open.add(t.getDouble("open"));
							}
							if (close.size() < 31 && t.getDouble("close") != null) {
								close.add(t.getDouble("close"));
							}
							if (high.size() < 31 && t.getDouble("high") != null) {
								high.add(t.getDouble("high"));
							}
							if (low.size() < 31 && t.getDouble("low") != null) {
								low.add(t.getDouble("low"));
							}
							if (avg.size() < 31 && t.getDouble("average") != null) {
								avg.add(t.getDouble("average"));
							}
							if (turn_over.size() < 30 && t.getDouble("turnover_ratio(%)") != null) {
								turn_over.add(t.getDouble("turnover_ratio(%)"));
							}
						}
					}
				});
				for (int i = 0; i < open.size() - 1; i++) {
					open_chg.add((open.get(i) - open.get(i + 1)) / open.get(i + 1) * 100);
				}
				for (int i = 0; i < close.size() - 1; i++) {
					close_chg.add((close.get(i) - close.get(i + 1)) / close.get(i + 1) * 100);
				}
				for (int i = 0; i < high.size() - 1; i++) {
					high_chg.add((high.get(i) - high.get(i + 1)) / high.get(i + 1) * 100);
				}
				for (int i = 0; i < low.size() - 1; i++) {
					low_chg.add((low.get(i) - low.get(i + 1)) / low.get(i + 1) * 100);
				}
				for (int i = 0; i < avg.size() - 1; i++) {
					avg_chg.add((avg.get(i) - avg.get(i + 1)) / avg.get(i + 1) * 100);
				}
				if (now_pe == null) {
					missing_count_pe++;
				}
				if (now_pbv == null) {
					missing_count_pbv++;
				}
				if (now_div_yield == null) {
					missing_count_div++;
				}
				if (close_chg.size() < 30) {
					missing_count_close_chg++;
				}
				if (turn_over.size() < 30) {
					missing_count_turn_over_chg++;
				}
				// write data
				CSVModifier mod = new CSVModifier();
				mod.addItem(symbol).addItem(data_start_date).addItem(now_pe).addItem(now_pbv).addItem(now_div_yield);
				for (int i = 0; i < 30; i++) {
					try {
						mod.addItem(open_chg.get(i));
					} catch (IndexOutOfBoundsException e) {
						Double val = null;
						mod.addItem(val);
					}
				}
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
						mod.addItem(high_chg.get(i));
					} catch (IndexOutOfBoundsException e) {
						Double val = null;
						mod.addItem(val);
					}
				}
				for (int i = 0; i < 30; i++) {
					try {
						mod.addItem(low_chg.get(i));
					} catch (IndexOutOfBoundsException e) {
						Double val = null;
						mod.addItem(val);
					}
				}
				for (int i = 0; i < 30; i++) {
					try {
						mod.addItem(avg_chg.get(i));
					} catch (IndexOutOfBoundsException e) {
						Double val = null;
						mod.addItem(val);
					}
				}
				for (int i = 0; i < 30; i++) {
					try {
						mod.addItem(turn_over.get(i));
					} catch (IndexOutOfBoundsException e) {
						Double val = null;
						mod.addItem(val);
					}
				}
				getLogger().info(mod.getCSVString());
				writeLine(mod.getCSVString());
			}
		});
		getLogger().info("missing count pe: " + missing_count_pe + ", pbv: " + missing_count_pbv + ", div: "
				+ missing_count_div + ", close_chg: " + missing_count_close_chg + ", turn_over_chg: "
				+ missing_count_turn_over_chg);
	}

}
