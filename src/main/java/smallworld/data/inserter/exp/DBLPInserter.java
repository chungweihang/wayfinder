package smallworld.data.inserter.exp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

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
    
    Neo4JInserter inserter;
    private int numberOfPapers = 0;
    
    private long totalSizeOfCircles = 0;
    
    public DBLPInserter(String neo4jPath) {
        this.content = new StringBuilder();
        this.coauthors = new ArrayList<String>();
        this.inserter = new Neo4JInserter(neo4jPath);
    }
    
    void insert() throws IOException {
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
    	} else if (name.equals("title")) {
    		title = content.toString();
        } else if (inproceedings && name.equals("booktitle")) {
        	// retrieve conference name as the publication
        	publication = content.toString();
        } else if (article && name.equals("journal")) {
        	// retrieve journal name as the publication
        	publication = content.toString();
    	} else if ((article || inproceedings) && name.equals("year")) {
    		// retrieve year of the publication
        	year = content.toString();
        } else if (name.equals("inproceedings") || name.equals("article")) {
        	// add journal and journal:year as each author's social circle
        	if (publication != null) {
	        	for (String coauthor : coauthors) {
	        		inserter.addCircle(publication);
	        		inserter.setCircle(publication, coauthor);
	        		inserter.addCircle(publication + ":" + year);
	        		inserter.setCircle(publication + ":" + year, coauthor);
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
    
    public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException {
    	String neo4JPath = "neo4j/dblp-exp";
    	DBLPInserter handler = new DBLPInserter(neo4JPath);
    	final long size = new File("data/dblp-small.xml").length();
    	//SAXParserFactory.newInstance().newSAXParser().parse(new File("data/dblp.xml"), handler);
    	SAXParserFactory.newInstance().newSAXParser().parse(new FilterInputStream(new FileInputStream("data/dblp-small.xml")) {
    		double progress = 0d;
    		double current = 0d;
    		@Override
    		public int read(byte[] buffer, int offset, int count) throws IOException {
    			int c = super.read(buffer, offset, count);
    			progress += c;
    			double percentage = progress / size / 100;
    			if (percentage - current > 0.01d) {
    				logger.info(String.format("Progress=%.2f%%", percentage));
    				current = percentage;
    			}
    			
    			//System.out.println(progress + " / " + size);
    		    return c;
    		}
    	}, handler);
    	
    	handler.insert();
    }
    
}