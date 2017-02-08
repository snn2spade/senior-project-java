package stock;

public class Attribute {
	public String name;
	public int year;
	public String value;
	public String unit;

	public Attribute(String name, int year) {
		this.name = name;
		this.year = year;
		this.value = "?";
		this.unit = "";
	}

	public Attribute(String name, int year, String value, String unit) {
		this.name = name;
		this.year = year;
		this.value = value;
		this.unit = unit;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Attribute)) {
			return false;
		}
		Attribute obj2 = (Attribute) obj;
		return this.name == obj2.name && this.year == obj2.year;
	}
}
