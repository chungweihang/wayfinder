package smallworld.data.inserter.msacademy;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class MSPaper {

	private static int ID = 0;
	private static int TITLE = 1;
	private static int YEAR = 2;
	private static int CONFERENCE_ID = 3;
	private static int JOURNAL_ID = 4;
	private static int KEYWORD = 5;
	
	private int id;
	private String title;
	private String year;
	private int conferenceId;
	private int journalId;
	private String keyword;
	
	public MSPaper(int id, String title, String year, int conferenceId, int journalId, String keyword) {
		this.id = id;
		this.title = title;
		this.year = year;
		this.conferenceId = conferenceId;
		this.journalId = journalId;
		this.keyword = keyword;
	}
	
	public int getId() {
		return id;
	}

	public String getTitle() {
		return title;
	}

	public String getYear() {
		return year;
	}

	public int getConferenceId() {
		return conferenceId;
	}

	public int getJournalId() {
		return journalId;
	}

	public String getKeyword() {
		return keyword;
	}

	public String toString() {
		return new StringBuilder("[MSPaper] ").append(id).append(" | ").append(title).append(" | ").append(year)
				.append(" | ").append(conferenceId).append(" | ").append(journalId).append(" | ").append(keyword).toString();
	}
	
	public static Map<Integer, MSPaper> load(String filename) {
		Map<Integer, MSPaper> papers = new HashMap<Integer, MSPaper>();
		
		Iterable<CSVRecord> records;
		try {
			CSVFormat.DEFAULT.withQuote('"');
			records = CSVFormat.DEFAULT.parse((new FileReader(filename)));
			for (CSVRecord record : records) {
				try {
					MSPaper paper = new MSPaper(Integer.parseInt(record.get(ID)), record.get(TITLE), record.get(YEAR),
							Integer.parseInt(record.get(CONFERENCE_ID)), Integer.parseInt(record.get(JOURNAL_ID)), record.get(KEYWORD));
					papers.put(paper.id, paper);
				} catch (NumberFormatException e) {
					System.err.println("skipping: " + record);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return papers;
	}
	
	/*
	public static Map<Integer, MSPaper> load(String filename) {
		BufferedReader reader = null; 
		Map<Integer, MSPaper> papers = new HashMap<Integer, MSPaper>();
		
		try {
			reader = new BufferedReader(new FileReader(filename));
			
			// skip first line
			reader.readLine();
			
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				
				String[] chunks = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				if (chunks.length != 6) continue;
				MSPaper paper = new MSPaper(Integer.parseInt(chunks[ID]), chunks[TITLE], chunks[YEAR],
						Integer.parseInt(chunks[CONFERENCE_ID]), Integer.parseInt(chunks[JOURNAL_ID]), chunks[KEYWORD]);
				papers.put(paper.id, paper);
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
		
		return papers;
	}
	*/
	
	public static void main(String[] args) {
		Map<Integer, MSPaper> papers = load("data/msacademy/Paper.csv");
		for (MSPaper paper : papers.values()) {
			System.out.println(paper);
		}
	}
}
