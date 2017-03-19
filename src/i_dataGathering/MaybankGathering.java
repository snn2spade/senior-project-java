package i_dataGathering;

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
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description set smart historical trading data (Excel file) from MayBank
 *              broker gathering
 */
public class MaybankGathering {
	private static final Logger logger = LogManager.getLogger(MaybankGathering.class);

	public static void main(String[] args) {
	}

	private static void setSmartGathering(String url_before_symbol, String file_output_path, String last_stock_name) {
		boolean canGo = false;
		Vector<String> stockNameList = new Vector<>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(ExternalFilePath.STOCKLIST_INPUT_FILEPATH));
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
			if (last_stock_name != null && !canGo) {
				System.out.println(stock + ": passed");
				if (stock.equals(last_stock_name)) {
					canGo = true;
				}
				continue;
			}
			String fileName = file_output_path + stock + ".xls";
			String url = url_before_symbol + stock;
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

	private static void setSmartHistoryPriceGathering(String last_stock_name) {
		String url = "http://sse.maybank-ke.co.th/historicalTrading.html?lstDisplay=T&submit=go&decorator=excel&submit.y=12&submit.x=24&showBeginDate=01%2F01%2F2012&lstPeriod=D&endDate=10%2F01%2F2017&beginDate=01%2F01%2F2012&lstMethod=AOM&chkAdjusted=true&quickPeriod=&showEndDate=10%2F01%2F2017&symbol=";
		setSmartGathering(url, ExternalFilePath.SETSMART_HISTORICAL_FILEPATH, last_stock_name);
	}

	private static void setSmartFinancialPosYearlyGathering(String last_stock_name) {
		String url = "http://sse.maybank-ke.co.th/financialstatement.html?lstDisplay=T&submit=go&decorator=excel&submit.y=7&submit.x=18&lstCompareType=Y&lstEndYear=2017&lstStatementType=B&lstBeginPeriod=Q9&lstAccountType=U&lstEndPeriod=Q3&lstBeginYear=2012&symbol=";
		setSmartGathering(url, ExternalFilePath.SETSMART_FINANCIAL_POSITION_YEARLY_FILEPATH, last_stock_name);
	}

	private static void setSmartComprehensiveIncomeYearlyGathering(String last_stock_name) {
		String url = "http://sse.maybank-ke.co.th/financialstatement.html?lstDisplay=T&submit=go&decorator=excel&submit.y=7&submit.x=18&lstCompareType=Y&lstEndYear=2017&lstStatementType=I&lstBeginPeriod=Q9&lstAccountType=U&lstEndPeriod=Q3&lstBeginYear=2012&symbol=";
		setSmartGathering(url, ExternalFilePath.SETSMART_COMPREHENSIVE_INCOME_YEARLY_FILEPATH, last_stock_name);
	}

	private static void setSmartCashFlowGatheringYearly(String last_stock_name) {
		String url = "http://sse.maybank-ke.co.th/financialstatement.html?lstDisplay=T&submit=go&decorator=excel&submit.y=7&submit.x=18&lstCompareType=Y&lstEndYear=2017&lstStatementType=C&lstBeginPeriod=Q9&lstAccountType=U&lstEndPeriod=Q3&lstBeginYear=2012&symbol=";
		setSmartGathering(url, ExternalFilePath.SETSMART_CASH_FLOW_YEARLY_FILEPATH, last_stock_name);
	}

}
