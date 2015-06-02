package smallworld.data.inserter.exp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.SAXException;

public class DBLPHandlerTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	
	@Test
	public void test() throws FileNotFoundException, SAXException, IOException, ParserConfigurationException {
			String neo4JPath = tempFolder.getRoot().getAbsolutePath();
			String xml = "/dblp-small.xml";
			DBLPInserter handler = new DBLPInserter(neo4JPath);
	    	SAXParserFactory.newInstance().newSAXParser().parse(getClass().getResourceAsStream(xml), handler);
	    	
	    	Assert.assertTrue(handler.inserter.isFriend("Gayane Grigoryan", "Oliver Gronz"));
	    	Assert.assertFalse(handler.inserter.isFriend("Oliver Gronz", "Oliver Gronz"));
	    	Assert.assertTrue(handler.inserter.hasCirlce("E. F. Codd", "IBM Research Report, San Jose, California"));
	    	Assert.assertTrue(handler.inserter.hasCirlce("E. F. Codd", "IBM Research Report, San Jose, California:1974"));
	    	Assert.assertFalse(handler.inserter.hasCirlce("E. F. Codd", "Fight Club"));
	}
}
