package smallworld.data.inserter.msacademy;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

public class MSPaperAuthor {

	private static final Logger logger = LogManager.getLogger();

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
	
	public static Table<Integer, Long, MSPaperAuthor> parse(String filename, Map<Integer, MSPaper> papers, 
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
							//circleAuthorMap.put(venue.getFullName(), author.getAuthorId());
							circleAuthorMap.put(venue.getFullName() + " " + paper.getYear(), author.getAuthorId());
						}
					}
					circleAuthorMap.put(author.getAffiliation(), author.getAuthorId());
				} catch (NumberFormatException e) {
					logger.error("skipping: " + record + " " + e.getMessage());
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
	
}
