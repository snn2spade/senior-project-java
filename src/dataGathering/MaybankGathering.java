package dataGathering;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import settings.AccountAuthen;
import settings.ExternalFilePath;

/**
 * @author NAPAT PAOPONGPAIBUL
 * This source code was used in my senior project 2016 for Education purpose ONLY
 * @description 
 * set smart historical trading data (Excel file) from MayBank broker gathering
 */
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
					new FileReader(ExternalFilePath.STOCKLIST_INPUT_FILEPATH));
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
			String fileName = ExternalFilePath.SETSMART_MAYBANK_INPUT_FILEPATH + stock + ".xls";
			String url = "http://sse.maybank-ke.co.th/historicalTrading.html?lstDisplay=T&symbol=" + stock
					+ "&submit=go&decorator=excel&submit.y=12&submit.x=24&showBeginDate=01%2F01%2F2012&lstPeriod=D&endDate=10%2F01%2F2017&beginDate=01%2F01%2F2012&lstMethod=AOM&chkAdjusted=true&quickPeriod=&showEndDate=10%2F01%2F2017";
			String cookieList = AccountAuthen.SETSMART_MAYBANK_AUTH_COOKIE;
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
