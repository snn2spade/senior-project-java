package i_dataGathering;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import settings.ExternalFilePath;

/**
 * @author NAPAT PAOPONGPAIBUL
 * This source code was used in my senior project 2016 for Education purpose ONLY
 * @description
 * stock name list gathering 
 */
public class SETGathering {
	private static final Logger logger = LogManager.getLogger(SETGathering.class);

	public static void main(String[] args) {
		getAllStockName();
	}

	private static Vector<String> getAllStockName() {
		Vector<String> result = new Vector<>();
		try {
			String html = HTTPRequest
					.sendGet("http://www.set.or.th/set/commonslookup.do?language=th&country=TH&prefix=NUMBER");
			while (html.contains("/set/companyprofile")) {
				html = html.substring(html.indexOf("/set/companyprofile"));
				result.add(html.substring(html.indexOf(">") + 1, html.indexOf("<")));
				html = html.substring(html.indexOf("</a>"));
			}
		} catch (Exception e) {
			logger.error("Cannot get stock name list [0-9] from set.or.th");
			e.printStackTrace();
		}
		for (char c = 'A'; c <= 'Z'; c++) {
			try {
				String html = HTTPRequest
						.sendGet("http://www.set.or.th/set/commonslookup.do?language=th&country=TH&prefix=" + c);
				while (html.contains("/set/companyprofile")) {
					html = html.substring(html.indexOf("/set/companyprofile"));
					result.add(html.substring(html.indexOf(">") + 1, html.indexOf("<")));
					html = html.substring(html.indexOf("</a>"));
				}
			} catch (Exception e) {
				logger.error("Cannot get stock name list [A-Z] from set.or.th");
				e.printStackTrace();
			}
		}
		try {
			FileWriter out = new FileWriter(ExternalFilePath.STOCKLIST_OUTPUT_FILEPATH);
			for (String i : result) {
				out.write(i);
				out.write(System.lineSeparator());
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
