package util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class CSVModifier {

	private String csv_row = "";
	private int length = 0;

	public CSVModifier addItem(String in) {
		if (in == null) {
			csv_row += "?,";
		} else {
			csv_row += in + ",";
		}
		length++;
		return this;
	}

	public CSVModifier addItem(Double in) {
		if (in == null) {
			csv_row += "?,";
		} else {
			csv_row += in + ",";
		}
		length++;
		return this;
	}

	public CSVModifier addItem(Date in) {
		if (in == null) {
			csv_row += "?,";
		} else {
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-MMM-dd");
			String date = sf.format(in);
			csv_row += date + ",";
		}
		length++;
		return this;
	}

	public String getCSVString() {
		if (csv_row.length() > 0) {
			return csv_row.substring(0, csv_row.length() - 1);
		}
		return "";
	}

	public int length() {
		return length;
	}
}
