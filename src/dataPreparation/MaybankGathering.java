package dataPreparation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MaybankGathering {
	private static final Logger logger = LogManager.getLogger(MaybankGathering.class);

	public static void main(String[] args) {
		 maybankHistoryPriceGathering();
	}

	private static void maybankHistoryPriceGathering() {
		boolean canGo = false;
		Vector<String> stockNameList = new Vector<>();
		try {
			BufferedReader in = new BufferedReader(
					new FileReader("D:/Dropbox/Senior project/History Stock Data/stockList.txt"));
			String txt;
			while ((txt = in.readLine()) != null) {
				stockNameList.add(txt);
			}
			in.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info(stockNameList);
		for (String stock : stockNameList) {
			if (!canGo) {
				System.out.println(stock + ": passed");
				if (stock.equals("S & J")) { //LAST s
					canGo = true;
				}
				continue;
			}
			String fileName = "D:/Dropbox/Senior project/History Stock Data/maybank/" + stock + ".xls";
			String url = "http://sse.maybank-ke.co.th/historicalTrading.html?lstDisplay=T&symbol=" + stock
					+ "&submit=go&decorator=excel&submit.y=12&submit.x=24&showBeginDate=01%2F01%2F2012&lstPeriod=D&endDate=10%2F01%2F2017&beginDate=01%2F01%2F2012&lstMethod=AOM&chkAdjusted=true&quickPeriod=&showEndDate=10%2F01%2F2017";
			String cookieList = "JSESSIONID=4C446EE442E033DD535E2FABA8CA61D5; _gat=1; _ga=GA1.3.1576166775.1484121991";
			try {
				HTTPRequest.getExcelFileFromMaybank(url, fileName, cookieList);
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
			try {
				logger.info("waiting 1s for opening of next connection");
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
