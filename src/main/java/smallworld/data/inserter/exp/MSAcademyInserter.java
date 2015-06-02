package smallworld.data.inserter.exp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import smallworld.data.inserter.msacademy.MSPaper;
import smallworld.data.inserter.msacademy.MSPaperAuthor;
import smallworld.data.inserter.msacademy.MSVenue;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

public class MSAcademyInserter {
	
	private static final Logger logger = LogManager.getLogger();

	Neo4JInserter inserter;
	
	Map<Integer, MSPaper> paperMap;
	Table<Integer, Long, MSPaperAuthor> paperAuthorTable;
	Map<Integer, MSVenue> journalMap;
	Map<Integer, MSVenue> conferenceMap;
	Multimap<String, Long> circleAuthorMap;
	
	private MSAcademyInserter(String neo4jPath, String dataPath) throws IOException {
		paperMap = MSPaper.load(dataPath + "/Paper.csv");
		journalMap = MSVenue.load(dataPath + "/Journal.csv", MSVenue.Type.JOURNAL);
		conferenceMap = MSVenue.load(dataPath + "/Conference.csv", MSVenue.Type.CONFERENCE);
		paperAuthorTable = MSPaperAuthor.load(dataPath + "/PaperAuthor.csv", paperMap, journalMap, conferenceMap);
		
		logger.info(new StringBuilder("data loaded: ")
				.append(paperMap.size()).append(" papers ")
				.append(paperAuthorTable.size()).append(" paper authors ")
				.append(journalMap.size()).append(" journals ")
				.append(conferenceMap.size()).append(" conferences.").toString());
		MSPaperAuthor.statistics();
		
        inserter = new Neo4JInserter(neo4jPath);
        
        int counter = 0;
        for (Integer paperId : paperMap.keySet()) {
        	
        	if (++ counter % 1000 == 0) {
				logger.info(counter + "..." + " paper count: " + paperMap.size());
        	}
        
        	MSPaper paper = paperMap.get(paperId);
        	MSVenue venue = conferenceMap.get(paper.getConferenceId());
    		if (venue == null) {
    			venue = journalMap.get(paper.getJournalId());
    		}
    				
        	List<MSPaperAuthor> authors = new ArrayList<MSPaperAuthor>(paperAuthorTable.row(paperId).values());
        	for (int i = 0; i < authors.size(); i++) {
        		MSPaperAuthor author = authors.get(i);
        		this.createAuthor(author);
        		// set "venue" and "venue-year" as circles
        		if (venue != null && venue.getFullName().length() > 0) {
        			inserter.setCircle(venue.getFullName(), author.getAuthorId());
        			inserter.setCircle(venue.getFullName() + ":" + paper.getYear(), author.getAuthorId());
        		} 
        		for (int j = i+1; j < authors.size(); j++) {
        			MSPaperAuthor coauthor = authors.get(j);
        			this.createAuthor(coauthor);
        			inserter.addFriend(author.getAuthorId(), coauthor.getAuthorId());
        			// set "venue" and "venue-year" as circles
        			if (venue != null && venue.getFullName().length() > 0) {
        				inserter.setCircle(venue.getFullName(), coauthor.getAuthorId());
            			inserter.setCircle(venue.getFullName() + ":" + paper.getYear(), coauthor.getAuthorId());
            		}
        		}
        	}
        }
        
        inserter.insert();
        
        System.out.println("paper count: " + paperMap.size());
    }
	
	// Put affiliation as circles
	private void createAuthor(MSPaperAuthor author) {
		if (inserter.personExists(author.getAuthorId())) return;
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("name", author.getName());
		properties.put("affiliation", author.getAffiliation());
		inserter.addPerson(author.getAuthorId(), properties);
	}
	
	public static void insert(String neo4jPath, String dataPath) throws IOException {
		new MSAcademyInserter(neo4jPath, dataPath);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		insert("neo4j/msacademy-circlesize", "data/msacademy");
	}

}
