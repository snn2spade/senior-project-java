package settings;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class ModelParameter {
	/**
	 * started date for detect multifold stock (inclusive)
	 */
	public static final String S_DATE_MULFOLD = "2015-01-05";
	public static final String E_DATE_MULFOLD = "2017-01-01";
	public static final Double MUL_TIME_MULFOLD = 1.3;
	/**
	 * mode 1 classify multifold stock by max_price/min_price >= multiple factor 
	 * mode 2 classify multifold stock by max_price/today_price >= multiple factor
	 */
	public static final int MULTIFOLD_DETECT_MODE = 2;
}
