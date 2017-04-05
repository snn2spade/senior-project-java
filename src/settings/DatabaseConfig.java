package settings;

public class DatabaseConfig {
	/* My SQL Database */
	public static final String MYSQL_DB_NAME = "stock";
	public static final String MYSQL_DB_USER = "root";
	public static final String MYSQL_DB_PASS = "1049269";
	/* MongoDB Database */
	public static final String MONGO_URI = "mongodb://localhost:27017";
//	public static final String MONGO_URI = "mongodb://seniorproject-api.tk:27017";
	public static final String SYMBOL_COLLECTION_NAME = "symbol";
	public static final String HIS_TRADING_COLLECTION_NAME = "historicalTrading";
	public static final String COMP_INFO_COLLECTION_NAME = "CompanyInfo";
	public static final String FIN_POS_YEARLY_COLLECTION_NAME = "financial_position_yearly";
	public static final String COM_IN_YEARLY_COLLECTION_NAME = "comprehensive_income_yearly";
	public static final String CASH_FLOW_YEARLY_COLLECTION_NAME = "cash_flow_yearly";
	public static final String FIN_POS_QUARTER_COLLECTION_NAME = "financial_position_quarterly";
	public static final String COM_IN_QUARTER_COLLECTION_NAME = "comprehensive_income_quarterly";
	public static final String CASH_FLOW_QUARTER_COLLECTION_NAME = "cash_flow_quarterly";
}
