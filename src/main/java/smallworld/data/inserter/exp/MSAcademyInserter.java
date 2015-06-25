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

import com.google.common.collect.Table;

public class MSAcademyInserter {
	
	private static final Logger logger = LogManager.getLogger();

	GraphInserter inserter;
	
	Map<Integer, MSPaper> paperMap;
	Table<Integer, Long, MSPaperAuthor> paperAuthorTable;
	Map<Integer, MSVenue> journalMap;
	Map<Integer, MSVenue> conferenceMap;
	
	private MSAcademyInserter(String dataPath, GraphInserter inserter) throws IOException {
		this.inserter = inserter;
		
		paperMap = MSPaper.parse(dataPath + "/Paper.csv");
		journalMap = MSVenue.parse(dataPath + "/Journal.csv", MSVenue.Type.JOURNAL);
		conferenceMap = MSVenue.parse(dataPath + "/Conference.csv", MSVenue.Type.CONFERENCE);
		paperAuthorTable = MSPaperAuthor.parse(dataPath + "/PaperAuthor.csv", paperMap, journalMap, conferenceMap);
		
		logger.info(new StringBuilder("data loaded: ")
				.append(paperMap.size()).append(" papers ")
				.append(paperAuthorTable.size()).append(" paper authors ")
				.append(journalMap.size()).append(" journals ")
				.append(conferenceMap.size()).append(" conferences.").toString());
		MSPaperAuthor.statistics();
		
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
        		this.createAuthor(author, paper.getTitle());
        		// set "venue-year" as circles
        		String circle = null;
        		if (venue != null && venue.getFullName().length() > 0) {
        			circle = venue.getFullName() + ":" + paper.getYear();
        			inserter.addCircle(circle);
        			inserter.setCircle(circle, author.getAuthorId());
        		} 
        		for (int j = i+1; j < authors.size(); j++) {
        			MSPaperAuthor coauthor = authors.get(j);
        			this.createAuthor(coauthor, paper.getTitle());
        			inserter.addFriend(author.getAuthorId(), coauthor.getAuthorId());
        			// set "venue" and "venue-year" as circles
        			if (circle != null) {
        				inserter.setCircle(circle, coauthor.getAuthorId());
            		}
        		}
        	}
        }
        
        inserter.insert();
        
        logger.info("paper count: " + paperMap.size());
    }
	
	// Put affiliation as circles
	private void createAuthor(MSPaperAuthor author, String paperTitle) {
		Map<String, Object> interests;
		if (inserter.personExists(author.getAuthorId())) {
			interests = inserter.getPersonFeatures(author.getAuthorId());
		} else {
			interests = new HashMap<>();
		}
		inserter.addPerson(author.getAuthorId(), Interests.addInterests(interests, paperTitle));
		inserter.addCircle(author.getAffiliation());
		inserter.setCircle(author.getAffiliation(), author.getAuthorId());
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Neo4JInserter inserter = new Neo4JInserter("neo4j/msacademy-exp", false);
		inserter.enforceUniqueRelationships = true;
		new MSAcademyInserter("data/msacademy", inserter);
	}

}
