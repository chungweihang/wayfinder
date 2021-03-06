package smallworld.data.inserter.exp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import smallworld.util.ProgressInputStream;

/**
 * This class read DBLP data and import them into Neo4J.
 * 
 * 
 * 
 * @author chang
 *
 */
public class DBLPInserter extends DefaultHandler {
	
	private static final Logger logger = LogManager.getLogger();

	// XML element content, e.g., <author>Joe Doe</author>
    private StringBuilder content;
    
    // List of author IDs of the publication
    private List<String> coauthors;
    
    // Name of the publication
    private String publication;
    
    // Year of the publication
    private String year;
    
    private String title;
    
    // Indicate if current parsing is within <inproceedings>
    private boolean inproceedings = false;
    
    private boolean article = false;
    
    GraphInserter inserter;
    private int numberOfPapers = 0;
    
    private long totalSizeOfCircles = 0;
    
    Map<String, Object> interests = new HashMap<>();
    
    public DBLPInserter(GraphInserter inserter) {
        this.content = new StringBuilder();
        this.coauthors = new ArrayList<String>();
        this.inserter = inserter;
    }
    
    void insert() throws IOException {
    	// this special circle keeps the counts of all interests
    	inserter.addCircle(Interests.INTEREST_NODE_NAME, interests);
    	inserter.insert();
    	logger.info("Number of papers: " + numberOfPapers);
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
    		inserter.addPerson(author);
    		coauthors.add(author);
        	
        	// connect author to all the previous found author of the publication
        	if (coauthors.size() > 1) {
        		String lastAdded = coauthors.get(coauthors.size() - 1);
        		for (int i = 0; i < coauthors.size() - 1; i++) {
        			String coauthor = coauthors.get(i);
        			inserter.addFriend(lastAdded, coauthor);
        		}
        	}
       } 
    	
    	if ((article || inproceedings) && name.equals("title")) {
    		title = content.toString();
    	} 
    	
    	if (inproceedings && name.equals("booktitle")) {
        	// retrieve conference name as the publication
        	publication = content.toString();
        } 
    	
    	if (article && name.equals("journal")) {
        	// retrieve journal name as the publication
        	publication = content.toString();
        } 
    	
    	if ((article || inproceedings) && name.equals("year")) {
    		// retrieve year of the publication
        	year = content.toString();
        }
    	
    	if (name.equals("inproceedings") || name.equals("article")) {
        	// add journal and journal:year as each author's social circle
    		if (publication != null) {
	        	for (String coauthor : coauthors) {
	        		//inserter.addCircle(publication);
	        		//inserter.setCircle(publication, coauthor);
	        		inserter.addCircle(publication + ":" + year);
	        		inserter.setCircle(publication + ":" + year, coauthor);
	        		
	        		// update interests of coauthor
	        		Map<String, Object> properties = inserter.getPersonFeatures(coauthor);
	        		Interests.addInterests(properties, title);
	        		Interests.addInterests(interests, title);
	        		inserter.addPerson(coauthor, properties);
	        	}
        	}
        	
        	// reset and update variables
        	inproceedings = false;
        	coauthors.clear();
            publication = null;
            year = null;
            title = null;
            numberOfPapers++;
        } 
    }
    
    private static void usage() {
    	System.out.println(DBLPInserter.class.getName() + " [neo4j_path (under neo4j folder)] [dblp_xml (under data folder)]");
    	System.exit(0);
    }
    
    public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException {
    	if (args.length != 2) usage();
    	String neo4JPath = "neo4j/" + args[0];
    	String dataPath = "data/" + args[1];
    	Neo4JInserter.CACHE_MAX_SIZE = 100000;
    	Neo4JInserter inserter = new Neo4JInserter(neo4JPath, false);
    	inserter.enforceUniqueRelationships = true;
    	final DBLPInserter handler = new DBLPInserter(inserter);
    	
    	final File file = new File(dataPath);
    	ProgressInputStream is = new ProgressInputStream(new FileInputStream(file), file.length(), new ChangeListener() {
    		@Override
			public void stateChanged(ChangeEvent e) {
    			ProgressInputStream is = (ProgressInputStream) e.getSource();
				logger.info("progress: " + is.getPercentage());
			}
    	});
    	
    	//SAXParserFactory.newInstance().newSAXParser().parse(new File(dataPath), handler);
    	SAXParserFactory.newInstance().newSAXParser().parse(is, handler);
    	
    	handler.insert();
    }
    
}