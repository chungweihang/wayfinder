package smallworld.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Before;
import org.junit.Test;
import org.tartarus.snowball.ext.englishStemmer;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class StemmerTest {
	
	StanfordCoreNLP pipeline;
	
	@Before
	public void setup() {
		Properties properties = new Properties();
		properties.put("annotators", "tokenize");
		properties.put("tokenize.options", "untokenizable=allDelete,ptb3Escaping=false");
		pipeline = new StanfordCoreNLP(properties);
	}
	
	@Test
	public void test() {
		englishStemmer stemmer = new englishStemmer();
		stemmer.setCurrent("extraction extraction");
		if (stemmer.stem()){
		    System.out.println(stemmer.getCurrent());
		}
	}
	
	@Test
	public void test1() throws SAXException, IOException, ParserConfigurationException {
		DBLPHandler handler = new DBLPHandler();
		SAXParserFactory.newInstance().newSAXParser().parse(new File("data/dblp.xml"), handler);
		handler.done();
	}
	
	public String tokenizer(String text) {
		Annotation doc = new Annotation(text);
		pipeline.annotate(doc);
		StringBuilder builder = new StringBuilder();
		for (CoreLabel token : doc.get(TokensAnnotation.class)) {
			String t = token.get(TextAnnotation.class).toLowerCase();
			if (!StopList.INSTANCE.isStopword(t)) {
				builder.append(Utils.stem(t)).append(" ");
			}
		}
		return builder.toString();
	}
	
	class DBLPHandler extends DefaultHandler {
		
		// XML element content, e.g., <author>Joe Doe</author>
	    private StringBuilder content;
	    
	    private String title;
	    
	    // Indicate if current parsing is within <inproceedings>
	    private boolean inproceedings = false;
	    private boolean article = false;
	    
	    private PrintWriter writer;
	    
	    public DBLPHandler() throws IOException {
	        this.content = new StringBuilder();
	        writer = new PrintWriter(new FileWriter("dblp-titles.txt"));
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
	    	
	    	if ((article | inproceedings) && name.equals("title")) {
	    		title = content.toString();
	    		//titles.append(System.lineSeparator()).append(title);
	    		//writer.println(title);
	    		writer.println(tokenizer(title));
	        } 
	    }
	    
	    public void done() {
	    	if (null != writer) writer.close();
	    }
	}
}
