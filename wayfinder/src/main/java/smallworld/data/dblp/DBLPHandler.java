package smallworld.data.dblp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import smallworld.data.RelationshipTypes;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * This class read DBLP data and import them into Neo4J.
 * 
 * @author chang
 *
 */
public class DBLPHandler extends DefaultHandler {
	// XML element content, e.g., <author>Joe Doe</author>
    private StringBuilder content;
    // List of author IDs of the publication
    private List<Long> authors;
    // Name of the publication
    private String publication;
    // Year of the publication
    private String year;
    // Indicate if current parsing is within <inproceedings>
    private boolean inproceedings = false;
    private boolean article = false;
    
    private BatchInserter inserter;
    private Map<String, Long> authorMap;
    private Multimap<Long, Long> edgeMap;
    private int numberOfPapers = 0;
    
    private Multimap<String, Long> circleToPeople;
    private long totalSizeOfCircles = 0;
    
    public DBLPHandler(String neo4jPath) {
        this.content = new StringBuilder();
        this.authors = new ArrayList<Long>();
 
        this.inserter = getBatchInserter(neo4jPath);
        this.authorMap = new HashMap<String, Long>();
        this.edgeMap = HashMultimap.create();
        
        this.circleToPeople = ArrayListMultimap.create();
    }

    // characters can be called multiple times per element so aggregate the content in a StringBuilder
    public void characters(char[] ch, int start, int length) throws SAXException {
        content.append(ch, start, length);
    }

    public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
    	content.setLength(0);
        
        if (name.equals("inproceedings")) {
        	inproceedings = true;
        } else if (name.equals("article")) {
        	article = true;
        }
    }

    public void endElement(String uri, String localName, String name) throws SAXException {
    	
    	if ((article || inproceedings) && name.equals("author")) {
    		// retrieve name of each author
    		String author = content.toString();
        	if (!authorMap.containsKey(author)) {
        		long id = inserter.createNode(null);
        		authorMap.put(author, id);
        		authors.add(id);
        	} else {
        		authors.add(authorMap.get(author));
        	}
        	
        	// connect author to all the previous found author of the publication
        	if (authors.size() > 1) {
        		long lastAdded = authors.get(authors.size() - 1);
        		for (int i = 0; i < authors.size() - 1; i++) {
        			long id = authors.get(i);
        			if (!edgeMap.containsEntry(lastAdded, id) && !edgeMap.containsEntry(id, lastAdded)) {
        				edgeMap.put(id, lastAdded);
        				inserter.createRelationship(id, lastAdded, RelationshipTypes.FRIEND.type(), null);
        			}
        		}
        	}
        } else if (inproceedings && name.equals("booktitle")) {
        	// retrieve conference name as the publication
        	publication = content.toString();
        } else if (article && name.equals("journal")) {
        	// retrieve journal name as the publication
        	publication = content.toString();
    	} else if ((article || inproceedings) && name.equals("year")) {
    		// retrieve year of the publication
        	year = content.toString();
        } else if (name.equals("inproceedings")) {
        	// add journal and journal:year as each author's social circle
        	if (publication != null) {
	        	for (Long id : authors) {
	        		/*
	        		inserter.setNodeProperty(id, publication + ":" + year, "");
	        		inserter.setNodeProperty(id, publication, "");
	        		*/
	        		circleToPeople.put(publication, id);
	        		circleToPeople.put(publication + ":" + year, id);
	        	}
        	}
        	
        	// reset and update variables
        	inproceedings = false;
        	authors.clear();
            publication = null;
            year = null;
            numberOfPapers++;
    
        } else if (name.equals("article")) {
        	// add journal and journal:year as each author's social circle
        	if (publication != null) {
	        	for (Long id : authors) {
	        		/*
	        		inserter.setNodeProperty(id, publication + ":" + year, "");
	        		inserter.setNodeProperty(id, publication, "");
	        		*/
	        		circleToPeople.put(publication, id);
	        		circleToPeople.put(publication + ":" + year, id);
	        	}
        	}
        	
        	// reset and update variables
        	article = false;
        	authors.clear();
            publication = null;
            year = null;
            numberOfPapers++;
        }
    }
    
    private void processCircles() {
    	System.out.println("number of circles: " + circleToPeople.keySet().size());
    	int progress = 0;
    	// add circles
    	for (String circle : circleToPeople.keySet()) {
    		
    		if (++progress % 1000 == 0) System.err.println("adding circle: " + progress + "/" + circleToPeople.keySet().size());
    		
    		Collection<Long> people = circleToPeople.get(circle);
    		totalSizeOfCircles += people.size();
    		for (Long id : people) {
    			inserter.setNodeProperty(id, circle, people.size());
    			//System.err.println("node " + id + " belong to " + circle + " of size " + people.size());
    		}
    	}
    }
    
    public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException {
    	delete("neo4j/dblp-inproceedings");
    	DBLPHandler handler = new DBLPHandler("neo4j/dblp-inproceedings");
    	SAXParserFactory.newInstance().newSAXParser().parse(new File("data/dblp.xml"), handler);
    	handler.processCircles();
    	handler.inserter.shutdown();
    	System.out.println("number of nodes: " + handler.authorMap.size());
    	System.out.println("number of edges: " + handler.edgeMap.size());
    	System.out.println("number of papers: " + handler.numberOfPapers);
    	//System.out.println("number of circles: " + handler.circleToPeople.size());
    	System.out.println("avg size of circles: " + (handler.circleToPeople.size()/handler.circleToPeople.keySet().size()));
    }
    
    private static BatchInserter getBatchInserter(String neo4jPath) {
    	// config
		Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M");
        config.put( "neostore.relationshipstore.db.mapped_memory", "3G");
        config.put( "neostore.propertystore.db.mapped_memory", "50M");
        config.put( "neostore.propertystore.db.strings.mapped_memory", "100M");
        config.put( "neostore.propertystore.db.arrays.mapped_memory", "0M");
        
        return BatchInserters.inserter(neo4jPath, config);
    }

    private static void delete(String neo4jPath) throws IOException {
    	FileUtils.deleteRecursively(new File(neo4jPath));
    }
    
}