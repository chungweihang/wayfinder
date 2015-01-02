package smallworld.data.inserter.msacademy;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class MSVenue {

	private static final int ID = 0;
	private static final int SHORT_NAME = 1;
	private static final int FULL_NAME = 2;
	private static final int URL = 3;
	
	public enum Type {
		JOURNAL, CONFERENCE
	}
		
	public int id;
	public String shortName;
	public String fullName;
	public String url;
	public Type type;
	
	public MSVenue(int id, String shortName, String fullName, String url, Type type) {
		this.id = id;
		this.shortName = shortName;
		this.fullName = fullName;
		this.url = url;
		this.type = type;
	}
	
	public String toString() {
		return new StringBuilder("[MSVenue] ").append(id).append(" | ")
				.append(shortName).append(" | ").append(fullName).append(" | ").append(url)
				.append(" | ").append(type.name()).toString();
	}

	public static Map<Integer, MSVenue> load(String filename, Type type) {
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
					System.err.println("skipping: " + record);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return venues;
	}
	
	/*
	public static Map<Integer, MSVenue> load(String filename, Type type) {
		BufferedReader reader = null; 
		Map<Integer, MSVenue> venues = new HashMap<Integer, MSVenue>();
		
		try {
			reader = new BufferedReader(new FileReader(filename));
			
			// skip first line
			reader.readLine();
			
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				
				String[] chunks = line.split(",", -1);
				
				if (chunks.length != 4) continue;
				
				MSVenue venue = new MSVenue(
						Integer.parseInt(chunks[ID]), chunks[SHORT_NAME], chunks[FULL_NAME], chunks[URL], type);
				venues.put(venue.id, venue);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try { reader.close(); } catch (Exception e) {}
			}
		}
		
		return venues;
	}
	*/
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Map<Integer, MSVenue> venues = load("data/msacademy/Conference.csv", Type.CONFERENCE);
		//Map<Integer, MSVenue> venues = load("data/msacademy/Journal.csv", Type.JOURNAL);
		for (MSVenue venue : venues.values()) {
			System.out.println(venue);
		}
	}

}
