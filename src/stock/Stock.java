package stock;

public class Stock {
	public String abbvName;
	public Attribute[] EPS_YOY;
	public Attribute[] Revenue_YOY;
	public Attribute[] NetProfit_YOY;
	public Attribute[] PE;
	public Attribute[] ROE;
	public Attribute[] ROA;
	public Attribute[] PBV;
	public Attribute[] DPS;

	public Stock(String abbvName) {
		this.abbvName = abbvName;
		EPS_YOY = new Attribute[5];
		Revenue_YOY = new Attribute[5];
		NetProfit_YOY = new Attribute[5];
		PE = new Attribute[5];
		ROE = new Attribute[5];
		ROA = new Attribute[5];
		PBV = new Attribute[5];
		DPS = new Attribute[5];
		for (int i = 0; i < 5; i++) {
			EPS_YOY[i] = new Attribute("EPS_YOY", i + 2011);
			Revenue_YOY[i] = new Attribute("Revenue_YOY", i + 2011);
			NetProfit_YOY[i] = new Attribute("NetProfit_YOY", i + 2011);
			PE[i] = new Attribute("PE", i + 2011);
			ROE[i] = new Attribute("ROE", i + 2011);
			ROA[i] = new Attribute("ROA", i + 2011);
			PBV[i] = new Attribute("PBV", i + 2011);
			DPS[i] = new Attribute("DPS", i + 2011);
		}
	}
}
