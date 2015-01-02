package smallworld.data.inserter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import smallworld.data.RelationshipTypes;
import smallworld.data.inserter.msacademy.MSPaper;
import smallworld.data.inserter.msacademy.MSPaperAuthor;
import smallworld.data.inserter.msacademy.MSVenue;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

public class MSAcademyBatchInserter {
	
	//Set<Integer> authorIds = new HashSet<Integer>();
	//Set<Integer> paperIds = new HashSet<Integer>();
	BatchInserter inserter;
	
	//Map<Integer, MSAuthor> authorMap;
	Map<Integer, MSPaper> paperMap;
	//Multimap<Integer, MSPaperAuthor> authorMultimap;
	//Multimap<Integer, Integer> paperAuthorMultimap;
	Table<Integer, Long, MSPaperAuthor> paperAuthorTable;
	Map<Integer, MSVenue> journalMap;
	Map<Integer, MSVenue> conferenceMap;
	Multimap<String, Long> circleAuthorMap;
	
	Multimap<Long, Long> edges = HashMultimap.create();
	
	int authorCount = 0;
	int edgeCount = 0;
	
	private MSAcademyBatchInserter(String neo4jPath, String dataPath) {
		//authorMap = MSAuthor.load(dataPath + "/Author.csv");
		paperMap = MSPaper.load(dataPath + "/Paper.csv");
		journalMap = MSVenue.load(dataPath + "/Journal.csv", MSVenue.Type.JOURNAL);
		conferenceMap = MSVenue.load(dataPath + "/Conference.csv", MSVenue.Type.CONFERENCE);
		//authorMultimap = MSPaperAuthor.load(dataPath + "/PaperAuthor.csv");
		//paperAuthorMultimap = MSPaperAuthorCommonsCSV.load(dataPath + "/PaperAuthor.csv");
		paperAuthorTable = MSPaperAuthor.load(dataPath + "/PaperAuthor.csv", paperMap, journalMap, conferenceMap);
		
		System.out.println(new StringBuilder("data loaded: ")
				.append(paperMap.size()).append(" papers ")
				.append(paperAuthorTable.size()).append(" paper authors ")
				.append(journalMap.size()).append(" journals ")
				.append(conferenceMap.size()).append(" conferences.").toString());
		MSPaperAuthor.statistics();
		
		// config
		Map<String, String> config = new HashMap<String, String>();
        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.relationshipstore.db.mapped_memory", "3G");
        config.put("neostore.propertystore.db.mapped_memory", "50M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
        config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
        
        inserter = BatchInserters.inserter(neo4jPath, config);
        
        int counter = 0;
        for (Integer paperId : paperMap.keySet()) {
        	
        	if (++ counter % 1000 == 0) {
        		System.out.print(counter + "...");
        		System.out.print(" paper count: " + paperMap.size());
                System.out.print(" author count: " + authorCount);
                System.out.println(" coauthorship count: " + edgeCount);
        	}
        	//System.out.println(++counter + "...");
        
        	MSPaper paper = paperMap.get(paperId);
        	MSVenue venue = conferenceMap.get(paper.conferenceId);
    		if (venue == null) {
    			venue = journalMap.get(paper.journalId);
    		}
    				
    		/*
    		Map<String, Object> properties = new HashMap<String, Object>();
    		properties.put("title", paper.title);
    		properties.put("year", paper.year);
    		properties.put("keyword", paper.keyword);
    		if (venue != null) {
    			properties.put("venue_id", venue.id);
    			properties.put("venue_short", venue.shortName);
    			properties.put("venue_full", venue.fullName);
    			properties.put("venue_url", venue.url);
    		}
    		*/
        	
        	List<MSPaperAuthor> authors = new ArrayList<MSPaperAuthor>(paperAuthorTable.row(paperId).values());
        	for (int i = 0; i < authors.size(); i++) {
        		MSPaperAuthor author = authors.get(i);
        		this.createAuthor(author);
        		// set "venue" and "venue-year" as circles
        		if (venue != null && venue.fullName.length() > 0) {
        			inserter.setNodeProperty(author.authorId, venue.fullName, MSPaperAuthor.circleAuthorMap.get(venue.fullName).size());
        			inserter.setNodeProperty(author.authorId, venue.fullName + " " + paper.year, MSPaperAuthor.circleAuthorMap.get(venue.fullName + " " + paper.year).size());
        		} 
        		for (int j = i+1; j < authors.size(); j++) {
        			MSPaperAuthor coauthor = authors.get(j);
        			this.createAuthor(coauthor);
        			this.createCoauthorship(author, coauthor);
        			// set "venue" and "venue-year" as circles
        			if (venue != null && venue.fullName.length() > 0) {
        				inserter.setNodeProperty(coauthor.authorId, venue.fullName, MSPaperAuthor.circleAuthorMap.get(venue.fullName).size());
            			inserter.setNodeProperty(coauthor.authorId, venue.fullName + " " + paper.year, MSPaperAuthor.circleAuthorMap.get(venue.fullName + " " + paper.year).size());
            		}
        		}
        	}
        }
        
        inserter.shutdown();
        
        System.out.println("paper count: " + paperMap.size());
        System.out.println("author count: " + authorCount);
        System.out.println("coauthorship count: " + edgeCount);
    }
	
	private void createCoauthorship(MSPaperAuthor author, MSPaperAuthor coauthor) {
		if (edges.containsEntry(author.authorId, coauthor.authorId) || edges.containsEntry(coauthor.authorId, author.authorId)) 
			return;
		
		inserter.createRelationship(author.authorId, coauthor.authorId, RelationshipTypes.COAUTHOR.type(), null);
		edges.put(author.authorId, coauthor.authorId);
		edgeCount++;
	}
	
	// Put affiliation as circles
	private void createAuthor(MSPaperAuthor author) {
		if (inserter.nodeExists(author.authorId)) return;
		Map<String, Object> properties = new HashMap<String, Object>();
		//properties.put("name", author.name);
		//properties.put("affiliation", author.affiliation);
		properties.put(author.affiliation, MSPaperAuthor.circleAuthorMap.get(author.affiliation).size());
		inserter.createNode(author.authorId, properties);
		authorCount++;
	}
	
	private static void delete(String neo4jPath) throws IOException {
		FileUtils.deleteRecursively(new File(neo4jPath));	
	}
	
	public static void insert(String neo4jPath, String dataPath) {
		new MSAcademyBatchInserter(neo4jPath, dataPath);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		delete("neo4j/msacademy-circlesize");
		insert("neo4j/msacademy-circlesize", "data/msacademy");
	}

}
