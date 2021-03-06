package util;

import org.apache.logging.log4j.Logger;

import org.apache.logging.log4j.LogManager;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description text extractor library
 */
public class TextExtractor {
	private static final Logger logger = LogManager.getLogger(TextExtractor.class);

	public static void main(String args[]) {
		System.out.println(parseDouble("-"));
	}

	public static String getNumberOrQuestionMark(String in) {
		in = in.replaceAll("[^\\d.]", "");
		try {
			in = "" + Double.parseDouble(in);
		} catch (NumberFormatException e) {
			in = "?";
		}
		return in;
	}

	public static String getNumberOrZero(String in) {
		in = in.replaceAll("[^\\d.]", "");
		try {
			in = "" + Double.parseDouble(in);
		} catch (NumberFormatException e) {
			in = "0";
		}
		return in;
	}

	public static String fromDMYtoYYYYMMDD(String cur_date) {
		cur_date = cur_date.replaceAll("[^\\d.]", "-");
		String day = cur_date.substring(0, cur_date.indexOf("-"));
		if (day.length() == 1) {
			day = "0" + day;
		}
		String month = cur_date.substring(cur_date.indexOf("-") + 1);
		month = month.substring(0, month.indexOf("-"));
		if (month.length() == 1) {
			month = "0" + month;
		}
		String year = cur_date.substring(cur_date.indexOf("-") + 1);
		year = year.substring(year.indexOf("-") + 1);
		if (year.length() == 2) {
			if (Integer.parseInt(year) < 40) {
				year = "20" + year;
			} else {
				year = "19" + year;
			}
		}
		String result = year + "-" + month + "-" + day;
		return result;
	}

	public static Double parseDouble(String val) {
		val = val.trim().replaceAll("[^\\d.-]", "");
		Double result;
		try {
			result = Double.parseDouble(val);
		} catch (NumberFormatException e) {
			throw new NumberFormatException();
		}
		return result;
	}
}
