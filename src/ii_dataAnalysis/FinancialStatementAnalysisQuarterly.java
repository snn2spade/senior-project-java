package ii_dataAnalysis;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import databaseMongoDB.SymbolController;
import settings.ExternalFilePath;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class FinancialStatementAnalysisQuarterly {
	private static final Logger logger = LogManager.getLogger(FinancialStatementAnalysisQuarterly.class);

	public static void main(String args[]) {
		Vector<String> symbol_list = SymbolController.getInstance().getSymbolList();
		symbol_list.stream().forEach(new Consumer<String>() {
			@Override
			public void accept(String symbol) {
				Vector<Vector<String>> vec = readFinancialStatementToVector(symbol,
						ExternalFilePath.SETSMART_COMPREHENSIVE_INCOME_QUARTERLY_FILEPATH);
				System.out.println(symbol);
				System.out.println(vec);
			}
		});
	}

	/**
	 * @param stock_name
	 *            name of stock
	 * @param file_path
	 *            excel file (.xls) path of Set Smart Financial Statement
	 * @return the first index 0 is Vector that contains avaliable year in file
	 *         <4/2016,3/2016,2/2016,...>. The rest index are vectors contain
	 *         statement infomation <"net_income","100","200","300",..>
	 */
	public static Vector<Vector<String>> readFinancialStatementToVector(String stock_name, String file_path) {
		BufferedReader buf;
		String line, doc = "";
		try {
			FileReader in = new FileReader(file_path + stock_name + ".xls");
			buf = new BufferedReader(in);
			while ((line = buf.readLine()) != null) {
				if (!line.trim().equals("")) {
					doc += line;
					doc += "\n";
				}
			}
			buf.close();
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		doc = doc.toLowerCase();
		Vector<Vector<String>> data = new Vector<>();
		data.addElement(new Vector<String>());
		Scanner scan = new Scanner(doc);
		String txt;
		int count = 0;
		while (scan.hasNextLine()) { // skip 1st table
			txt = scan.nextLine();
			if (txt.contains("/table")) {
				break;
			}
		}
		while (scan.hasNextLine()) {// gather year
			txt = scan.nextLine();
			if (txt.contains("quarter")) {
				if (!txt.contains("/")) {
					txt += scan.nextLine();
				}
				data.lastElement().add(txt.replaceAll("[^\\d/]", ""));
			}
			if (txt.contains("/tr")) {
				count++;
			}
			if (count == 2) {
				break;
			}
		}

		while (scan.hasNextLine()) {
			txt = scan.nextLine();
			logger.debug(" start reading next line : " + txt);
			if (txt.contains("/table")) {
				break;
			}
			if (txt.matches(".*<tr.*")) {
				logger.debug("contains <tr adding new vector");
				data.add(new Vector<String>());
			}
			if (txt.matches(".*<td.*")) {
				if (txt.contains("<strong>")) {
					int last_idx = data.size() - 1;
					data.remove(last_idx);
					logger.debug("remove last vector");
					while (!(txt = scan.nextLine()).contains("/tr")) {
					}
					continue;
				}
				txt = scan.nextLine();
				logger.debug("After found td : " + txt);
				if (txt.contains("&nbsp;")) {
					txt = scan.nextLine();
					logger.debug("line after match &nbsp" + txt);
					txt = txt.trim();
					txt = "(sub) " + txt;
				}

				txt = txt.trim();
				data.lastElement().add(txt);
			}
		}
		scan.close();
		return data;
	}
}
