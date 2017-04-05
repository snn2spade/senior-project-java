package iv_dataTransformation;

import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.bson.Document;


/**
 * @author NAPAT PAOPONGPAIBUL
 * This source code was used in my senior project 2016 for Education purpose ONLY
 * @description 
 */
public class SymbolListCSVGenerator extends CSVGenerator {
	
	public static void main(String[] args) {
		SymbolListCSVGenerator gen = new SymbolListCSVGenerator();
		gen.createCSV(LogManager.getLogger(SymbolListCSVGenerator.class),"symbol","symbolList.csv");
	}

	@Override
	public void writeHeader(){
		getLogger().info("starting write file header");
		writeLine("symbol,market");
	}
	
	@Override
	public void writeRow() {
		getLogger().info("starting write row");
		getCollection().find().forEach(new Consumer<Document>() {
			@Override
			public void accept(Document t){
				String name = t.getString("name");
				String market = t.getString("market");
				if(name!=null & market!=null){
					String row = name+","+market;
					writeLine(row);
					getLogger().info(row);
				}
			}
		});
	}
}
