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

public class MSAuthor {

	private static final Logger logger = LogManager.getLogger();

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
	
	public static Map<Long, MSAuthor> parse(String filename) {
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
					logger.error("skipping: " + record + " " + e.getMessage());
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
		Map<Long, MSAuthor> authors = parse("data/msacademy/Author.csv");
		for (MSAuthor author : authors.values()) {
			System.out.println(author);
		}
	}
}
