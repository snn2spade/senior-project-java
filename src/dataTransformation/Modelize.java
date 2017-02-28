package dataTransformation;

import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import database.DatabaseController;
import database.DatabaseViewer;
import settings.ModelParameter;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description For generate RapidMiner training file format
 */
public class Modelize {
	private final static Logger logger = LogManager.getLogger(Modelize.class);
	private static Connection con;

	public static void main(String[] args) {
		insertTrainingDataToDB();
	}

	public static void insertTrainingDataToDB() {
		con = DatabaseController.openDBConnection();
		Vector<String> symbol_list = DatabaseViewer.getSymbolList(con);
		int count_true = 0, count_false = 0;
		for (String symbol : symbol_list) {
			try {
				// System.out.print(symbol + " : ");
				int res = -1;
				switch (ModelParameter.MULTIFOLD_DETECT_MODE) {
				case 1:
					res = AttributeBlender.isMultifoldStock_1(symbol, ModelParameter.S_DATE_MULFOLD,
							ModelParameter.E_DATE_MULFOLD, ModelParameter.MUL_TIME_MULFOLD, con);
					break;
				case 2:
					res = AttributeBlender.isMultifoldStock_2(symbol, ModelParameter.S_DATE_MULFOLD,
							ModelParameter.E_DATE_MULFOLD, ModelParameter.MUL_TIME_MULFOLD, con);
					break;
				}
				// System.out.println(res);
				if (res == 1) {
					count_true++;
				} else if (res == 0) {
					count_false++;
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}
		int answered_size = count_true + count_false;
		int missing_size = symbol_list.size() - answered_size;
		Double missing_percent = Double.valueOf(missing_size) / symbol_list.size() * 100.0;
		Double true_percent = Double.valueOf(count_true) / Double.valueOf(answered_size) * 100.0;
		logger.info("multifold " + ModelParameter.MUL_TIME_MULFOLD + " times num of mulfold stock/answered : "
				+ count_true + "/" + (answered_size) + " (" + new DecimalFormat("#.##").format(true_percent) + "%)");
		logger.info("Missing value :" + missing_size + "/" + symbol_list.size() + " ("
				+ new DecimalFormat("#.##").format(missing_percent) + "%)");
		DatabaseController.closeDBConnection(con);
	}

}
