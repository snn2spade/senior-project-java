package dataCleaning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import database.DatabaseController;
import settings.ExternalFilePath;

/**
 * @author NAPAT PAOPONGPAIBUL
 * This source code was used in my senior project 2016 for Education purpose ONLY
 * @description
 * statistic calculation for visualization and decision in data cleaning process 
 */
public class StatisticAnalysis {
	private static final Logger logger = LogManager.getLogger(StatisticAnalysis.class);
	private static BufferedReader bufferedReader;
	
	public static void main(String[] args) {
		findMissingData();
	}

	public static class Triplet {

		public String string;
		public double d_1;
		public double d_2;

		public Triplet(String string, double d_1, double d_2) {
			this.string = string;
			this.d_1 = d_1;
			this.d_2 = d_2;
		}

		public String getString() {
			return string;
		}

		public double getD_1() {
			return d_1;
		}

		public double getD_2() {
			return d_2;
		}

		@Override
		public int hashCode() {
			return string.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Triplet))
				return false;
			Triplet pairo = (Triplet) o;
			return this.string.equals(pairo.getString());
		}
	}

	private static void findMissingData() {
		try {
			FileReader fileReader = new FileReader(new File(ExternalFilePath.STOCKLIST_INPUT_FILEPATH));
			bufferedReader = new BufferedReader(fileReader);
			Connection con;
			con = DatabaseController.openDBConnection();
			String txt;
			Vector<Triplet> myBuc = new Vector<>();
			logger.info("Start Query all close price from database");
			double sum_d_src = 0, sum_d_all = 0;
			while ((txt = bufferedReader.readLine()) != null) {
				String sql = "select count(*) from historicaltrading left join symbol on symbol.id = historicaltrading.symbol_id";
				sql += " where symbol.name='" + txt + "' and close_price is null;";
				ResultSet result = DatabaseController.executeQuerySQL(con, sql);
				sql = "select count(*) from historicaltrading left join symbol on symbol.id = historicaltrading.symbol_id";
				sql += " where symbol.name='" + txt + "';";
				ResultSet result2 = DatabaseController.executeQuerySQL(con, sql);
				double d_src = 0, d_all = 0;
				while (result.next()) {
					d_src = result.getInt(1);
					sum_d_src += d_src;
					// System.out.print("symbol: " + txt + " missing close_price
					// : " + result.getInt("count(*)") + " / ");
				}
				while (result2.next()) {
					d_all = result2.getInt(1);
					sum_d_all += d_all;
					// System.out.println(result2.getInt("count(*)"));
				}
				myBuc.add(new Triplet(txt, d_src, d_all));
			}
			myBuc.sort(new Comparator<Triplet>() {
				@Override
				public int compare(Triplet o1, Triplet o2) {
					double percent_o1 = o1.getD_1() / o1.getD_2();
					double percent_o2 = o2.getD_1() / o2.getD_2();
					if (percent_o1 > percent_o2)
						return -1;
					else if (percent_o1 == percent_o2)
						return 0;
					else
						return 1;
				}
			});
			myBuc.stream()
					.forEach(e -> System.out.println(e.string + " close_price_missing : "
							+ new DecimalFormat("#0.000").format(e.getD_1() / e.getD_2() * 100.00) + " % (" + e.getD_1()
							+ "/" + e.getD_2() + ")"));
			System.out.println("--------------------------------");
			System.out.println("total_close_price_missing  : " + sum_d_src + " / " + sum_d_all + " line ("
					+ new DecimalFormat("#0.000").format(sum_d_src / sum_d_all * 100.00) + " %)");
			DatabaseController.closeDBConnection(con);
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}

	}

}
