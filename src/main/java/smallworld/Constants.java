package smallworld;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Constants {

	//private static final Logger logger = Logger.getLogger(ConcurrentMain.class.getName());

	// Set the depth limit of one navigation
	public static int LIMIT_OF_DEPTH = 50;
	// Set the maximum size of the priority queue used in PrioritizedNavigation
	// -1 indicates no limit on size
	public static int PRORITY_QUEUE_MAX_SIZE = 10000;		
	// Because this is a random navigation, it may go infinite deep.
	// Return null when number of nodes explored exceeds a limit.
	public static int LIMIT_OF_NODES_EXPLORED = 1000;
	// The path to the Neo4J database
	// Should be set in ConcurrentMain
	public static String NEO4J_PATH = "";
			
	static {
		try {
			Properties prop = new Properties();
	        String propFileName = "config.properties";
	        InputStream inputStream = Constants.class.getClassLoader().getResourceAsStream(propFileName);
	        if (inputStream != null) {
	        	System.out.println("Loading properties from file...");
	        	prop.load(inputStream);
				LIMIT_OF_DEPTH = Integer.parseInt(prop.getProperty("LIMIT_OF_DEPTH", "50"));
		        PRORITY_QUEUE_MAX_SIZE = Integer.parseInt(prop.getProperty("PRIORITY_QUEUE_MAX_SIZE", "10000"));
		        LIMIT_OF_NODES_EXPLORED = Integer.parseInt(prop.getProperty("LIMIT_OF_NODES_EXPLORED", "1000"));
		    }
	        
	        /*
	        logger.info("LIMIT_OF_DEPTH=" + LIMIT_OF_DEPTH);
	        logger.info("PRORITY_QUEUE_MAX_SIZE=" + PRORITY_QUEUE_MAX_SIZE);
	        logger.info("LIMIT_OF_NODES_EXPLORED=" + LIMIT_OF_NODES_EXPLORED);
	        */
	        System.out.println("LIMIT_OF_DEPTH=" + LIMIT_OF_DEPTH);
	        System.out.println("PRORITY_QUEUE_MAX_SIZE=" + PRORITY_QUEUE_MAX_SIZE);
	        System.out.println("LIMIT_OF_NODES_EXPLORED=" + LIMIT_OF_NODES_EXPLORED);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
        
    }
	
	private Constants() {}
}
