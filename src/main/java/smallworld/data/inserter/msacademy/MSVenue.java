package smallworld.data.inserter.msacademy;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MSVenue {

	private static final Logger logger = LogManager.getLogger();

	private static final int ID = 0;
	private static final int SHORT_NAME = 1;
	private static final int FULL_NAME = 2;
	private static final int URL = 3;
	
	public enum Type {
		JOURNAL, CONFERENCE
	}
		
	private int id;
	private String shortName;
	private String fullName;
	private String url;
	private Type type;
	
	public MSVenue(int id, String shortName, String fullName, String url, Type type) {
		this.id = id;
		this.shortName = shortName;
		this.fullName = fullName;
		this.url = url;
		this.type = type;
	}
	
	public String getFullName() {
		return fullName;
	}

	public int getId() {
		return id;
	}

	public String getShortName() {
		return shortName;
	}

	public String getUrl() {
		return url;
	}

	public Type getType() {
		return type;
	}

	public String toString() {
		return new StringBuilder("[MSVenue] ").append(id).append(" | ")
				.append(shortName).append(" | ").append(getFullName()).append(" | ").append(url)
				.append(" | ").append(type.name()).toString();
	}

	public static Map<Integer, MSVenue> parse(String filename, Type type) {
		Map<Integer, MSVenue> venues = new HashMap<Integer, MSVenue>();
		
		Iterable<CSVRecord> records;
		try {
			CSVFormat.DEFAULT.withQuote('"');
			records = CSVFormat.DEFAULT.parse((new FileReader(filename)));
			for (CSVRecord record : records) {
				try {
					MSVenue venue = new MSVenue(
							Integer.parseInt(record.get(ID)), record.get(SHORT_NAME), record.get(FULL_NAME), record.get(URL), type);
					venues.put(venue.id, venue);
				} catch (NumberFormatException e) {
					logger.error("skipping: " + record + " " + e.getMessage());
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return venues;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Map<Integer, MSVenue> venues = parse("data/msacademy/Conference.csv", Type.CONFERENCE);
		for (MSVenue venue : venues.values()) {
			System.out.println(venue);
		}
	}

}
