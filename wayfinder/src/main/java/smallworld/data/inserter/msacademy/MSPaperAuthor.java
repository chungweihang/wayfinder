package smallworld.data.inserter.msacademy;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

public class MSPaperAuthor {

	private static int PAPER_ID = 0;
	private static int AUTHOR_ID = 1;
	private static int NAME = 2;
	private static int AFFILIATION = 3;
	
	public static Multimap<String, Long> circleAuthorMap = HashMultimap.create();
	
	private int paperId;
	private long authorId;
	private String name;
	private String affiliation;
	
	public MSPaperAuthor(int paperId, long authorId, String name, String affiliation) {
		this.paperId = paperId;
		this.authorId = authorId;
		this.name = name;
		this.affiliation = affiliation;
	}
	
	public int getPaperId() {
		return paperId;
	}

	public String getName() {
		return name;
	}

	public String getAffiliation() {
		return affiliation;
	}

	public long getAuthorId() {
		return authorId;
	}

	public String toString() {
		return new StringBuilder("[MSPaperAuthor] ")
			.append(paperId).append(" | ").append(getAuthorId()).append(" | ")
			.append(name).append(" | ").append(getAffiliation()).toString();
	}
	
	public static Table<Integer, Long, MSPaperAuthor> load(String filename, Map<Integer, MSPaper> papers, 
			Map<Integer, MSVenue> journals, Map<Integer, MSVenue> conferences) {
		Table<Integer, Long, MSPaperAuthor> table = HashBasedTable.create();
		
		Iterable<CSVRecord> records;
		try {
			CSVFormat.DEFAULT.withQuote('"');
			records = CSVFormat.DEFAULT.parse((new FileReader(filename)));
			for (CSVRecord record : records) {
				try {
					MSPaperAuthor author = new MSPaperAuthor(
							Integer.parseInt(record.get(PAPER_ID)), Long.parseLong(record.get(AUTHOR_ID)), 
							record.get(NAME), record.get(AFFILIATION));
					table.put(author.paperId, author.getAuthorId(), author);
					
					// fill in circle information
					MSPaper paper = papers.get(author.paperId);
					if (paper == null) {
						System.err.println("skipping circle for paper: " + author.paperId);
					} else {
						MSVenue venue = journals.get(paper.getJournalId());
						if (venue == null) {
							venue = conferences.get(paper.getConferenceId());
						}
						if (venue != null && venue.getFullName().length() > 0) {
							circleAuthorMap.put(venue.getFullName(), author.getAuthorId());
							circleAuthorMap.put(venue.getFullName() + " " + paper.getYear(), author.getAuthorId());
						}
					}
					circleAuthorMap.put(author.getAffiliation(), author.getAuthorId());
				} catch (NumberFormatException e) {
					System.err.println("skipping: " + record);
				}
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return table;
	}
	
	public static void statistics() {
		int numberOfAuthors = 0;
		for (String circle : circleAuthorMap.keySet()) {
			numberOfAuthors += circleAuthorMap.get(circle).size();
		}
		
		System.out.println("there are " + circleAuthorMap.keySet().size() + " circles with average size " + (double) numberOfAuthors / circleAuthorMap.keySet().size());
	}
	
	/*
	public static Table<Integer, Integer, MSPaperAuthor> load(String filename) {
		BufferedReader reader = null; 
		Table<Integer, Integer, MSPaperAuthor> authors = HashBasedTable.create();
		
		try {
			reader = new BufferedReader(new FileReader(filename));
			
			// skip first line
			reader.readLine();
			
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				
				String[] chunks = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				
				if (chunks.length != 4) {
					System.err.println("skipping line: " + line);
					continue;
				}
				
				MSPaperAuthor author = new MSPaperAuthor(
						Integer.parseInt(chunks[PAPER_ID]), Integer.parseInt(chunks[AUTHOR_ID]), 
						chunks[NAME], chunks[AFFILIATION]);
				authors.put(author.paperId, author.authorId, author);
				//authors.put(author.authorId, author.paperId, author);
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
	
	/*
	public static Multimap<Integer, MSPaperAuthor> load(String filename) {
		BufferedReader reader = null; 
		Multimap<Integer, MSPaperAuthor> authors = HashMultimap.create();
		
		try {
			reader = new BufferedReader(new FileReader(filename));
			
			// skip first line
			reader.readLine();
			
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				
				String[] chunks = line.split(",", -1);
				
				if (chunks.length != 4) continue;
				
				MSPaperAuthor author = new MSPaperAuthor(
						Integer.parseInt(chunks[PAPER_ID]), Integer.parseInt(chunks[AUTHOR_ID]), 
						chunks[NAME], chunks[AFFILIATION]);
				authors.put(author.paperId, author);
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
	
	public static void main(String[] args) {
		/*
		Table<Integer, Long, MSPaperAuthor> authors = load("data/msacademy/PaperAuthor.csv");
		
		System.out.println("total: " + authors.size());
		int total = 0;
		int count = 0;
		for (Integer authorId : authors.rowKeySet()) {
			total += authors.row(authorId).size();
			count ++;
			if (count % 10000 == 0)
				System.out.println(count + "...");
		}
		System.out.println((double) total / count);
		*/
		
		/*
		Multimap<Integer, MSPaperAuthor> authors = load("data/msacademy/PaperAuthor.csv");
		for (MSPaperAuthor author : authors.values()) {
			System.out.println(author);
		}
		*/
	}
}
