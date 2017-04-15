package iii_dataTransformation;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
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
public class MultifoldClassifyCSVGenerator_MongoDB extends CSVGeneratorTemplate_MongoDB {
	private final static int ISMULTIFOLD_FROMTODAY = 1;
	private final static int ISMULTIFOLD_FROMMINPRICE = 2;

	private Date sDate;
	private Date eDate;
	private Double multiple;
	private int mode;
	private String fileName;
	private int monthDurationAcceptMissingPrice;

	private String symbol;
	private Double startPrice, minPrice, maxPrice;

	private int missing_count_1, missing_count_2;
	private int true_count, false_count;

	public static void main(String[] args) {
		MultifoldClassifyCSVGenerator_MongoDB csvgen = new MultifoldClassifyCSVGenerator_MongoDB(4, Calendar.JANUARY,
				2016, 1.2, ISMULTIFOLD_FROMTODAY, 2);
		csvgen.createCSV(LogManager.getLogger(MultifoldClassifyCSVGenerator_MongoDB.class), "historicalTrading",
				csvgen.fileName);
	}

	public MultifoldClassifyCSVGenerator_MongoDB(int date, int month, int year, Double multiple, int mode,
			int monthDurationAcceptMissingPrice) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DATE, date);
		cal.set(Calendar.MONTH, month);
		cal.set(Calendar.YEAR, year);
		this.sDate = TimeDateUtil.getZeroTimeDate(cal.getTime());
		cal.set(Calendar.MONTH, month + 3);
		this.eDate = TimeDateUtil.getZeroTimeDate(cal.getTime());
		this.multiple = multiple;
		this.mode = mode;
		SimpleDateFormat sf = new SimpleDateFormat("yyyy_MMM_dd");
		if (this.mode == ISMULTIFOLD_FROMTODAY) {
			this.fileName = "1quarter_isMultiFold_fromTodayPrice_mul_" + multiple + "_" + sf.format(sDate) + ".csv";
		} else {
			this.fileName = "1quarter_isMultiFold_fromMinPrice_mul_" + multiple + "_" + sf.format(sDate) + ".csv";
		}
		this.monthDurationAcceptMissingPrice = monthDurationAcceptMissingPrice;
		missing_count_1 = 0;
		missing_count_2 = 0;
		true_count = 0;
		false_count = 0;
	}

	private void resetVariable() {
		symbol = null;
		startPrice = null;
		minPrice = null;
		maxPrice = null;
	}

	@Override
	public void writeHeader() {
		getLogger().info("writing header");
		writeLine("symbol,isMultiFold");
	}

	@Override
	public void writeRow() {
		if (mode == ISMULTIFOLD_FROMTODAY) {
			writeRow_isMultifoldStock_fromTodayPrice();
		} else {
			// TODO
		}
	}

	/**
	 * This method will answer which is Multifold stock if max price between
	 * sDate (inclusive) to eDate (exclusive) / sDate_price >= multiple factor
	 * <br>
	 * <br>
	 * <b>eDate</b>: calculate from sDate plus 1 year
	 **/
	private void writeRow_isMultifoldStock_fromTodayPrice() {

		getCollection().find().forEach(new Consumer<Document>() {
			@Override
			public void accept(Document t) {
				resetVariable();
				symbol = t.getString("symbol_name");
				List<Document> data = (List<Document>) t.get("data");
				Date last_date = TimeDateUtil.getZeroTimeDate(data.get(0).getDate("date"));
				if (last_date.after(eDate) || last_date.compareTo(eDate) == 0) {
					Collections.reverse(data);
					data.forEach(new Consumer<Document>() {
						@Override
						public void accept(Document t) {
							Date date = t.getDate("date");
							Calendar cal = Calendar.getInstance();
							cal.setTime(sDate);
							cal.add(Calendar.MONTH, monthDurationAcceptMissingPrice);
							if (startPrice == null && date.compareTo(sDate) >= 0
									&& date.compareTo(cal.getTime()) <= 0) {
								startPrice = t.getDouble("close");
								maxPrice = t.getDouble("close");
							}
							if (startPrice != null && t.getDouble("close") != null && date.before(eDate)) {
								if (t.getDouble("close") > maxPrice) {
									maxPrice = t.getDouble("close");
								}
							}
						}
					});
					// write data
					if (startPrice != null) {
						CSVModifier mod = new CSVModifier();
						mod.addItem(symbol);
						if (maxPrice / startPrice >= multiple) {
							mod.addItem(true);
							true_count++;
						} else {
							mod.addItem(false);
							false_count++;
						}
						writeLine(mod.getCSVString());
						getLogger()
								.info("STOCK :" + symbol + ", start price: " + startPrice + ", max_price: " + maxPrice);
					} else {
						SimpleDateFormat df = new SimpleDateFormat("yyyy-MMM-dd");
						getLogger().warn("STOCK :" + symbol + " not have close price in specific start date ("
								+ df.format(sDate) + ")");
						missing_count_1++;
					}
				} else {
					SimpleDateFormat df = new SimpleDateFormat("yyyy-MMM-dd");
					getLogger().warn(
							"STOCK :" + symbol + " last date is not enough one year (" + df.format(last_date) + ")");
					missing_count_2++;
				}
			}
		});
		getLogger().info("missing_count_1: " + missing_count_1 + ", missing_count_2: " + missing_count_2);
		getLogger().info("true :"+true_count+", false :"+false_count);
	}
}
