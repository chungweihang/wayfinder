package smallworld.data.inserter.msacademy;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class MSAuthor {

	private static int ID = 0;
	private static int NAME = 1;
	private static int AFFILIATION = 2;
	
	private long id;
	private String name;
	private String affiliation;
	
	public MSAuthor(long id, String name, String affiliation) {
		this.id = id;
		this.name = name;
		this.affiliation = affiliation;
	}
	
	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}


	public String getAffiliation() {
		return affiliation;
	}

	public String toString() {
		return new StringBuilder("[MSAuthor] ").append(id).append(" | ").append(name).append(" | ").append(affiliation).toString();
	}
	
	/*
	public static Map<Integer, MSAuthor> load(String filename) {
		BufferedReader reader = null; 
		Map<Integer, MSAuthor> authors = new HashMap<Integer, MSAuthor>();
		
		try {
			reader = new BufferedReader(new FileReader(filename));
			
			// skip first line
			reader.readLine();
			
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				
				String[] chunks = line.split(",", -1);
				MSAuthor author = new MSAuthor(Integer.parseInt(chunks[ID]), chunks[NAME], chunks[AFFILIATION]);
				authors.put(author.id, author);
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
		
		return authors;
	}
	*/
	
	public static Map<Long, MSAuthor> load(String filename) {
		Map<Long, MSAuthor> authors = new HashMap<Long, MSAuthor>();
		
		Iterable<CSVRecord> records;
		try {
			CSVFormat.DEFAULT.withQuote('"');
			records = CSVFormat.DEFAULT.parse((new FileReader(filename)));
			for (CSVRecord record : records) {
				try {
					MSAuthor author = new MSAuthor(Long.parseLong(record.get(ID)), record.get(NAME), record.get(AFFILIATION));
					authors.put(author.id, author);
				} catch (NumberFormatException e) {
					System.err.println("skipping: " + record);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return authors;
	}
	
	public static void main(String[] args) {
		Map<Long, MSAuthor> authors = load("data/msacademy/Author.csv");
		for (MSAuthor author : authors.values()) {
			System.out.println(author);
		}
	}
}
