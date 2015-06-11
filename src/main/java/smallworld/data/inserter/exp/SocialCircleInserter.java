package smallworld.data.inserter.exp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Batch insert data to neo4j
 * 
 * @author chang
 *
 */
public class SocialCircleInserter {
	
	private static final Logger logger = LogManager.getLogger();

	final GraphInserter inserter;
	protected final String dataPath;
	
	public SocialCircleInserter(final String dataPath, final GraphInserter inserter) {
		this.inserter = inserter;
		this.dataPath = dataPath;
	}
	
	/**
	 * Utility method to load a file into a list of lines.
	 * 
	 * @param filename
	 * @return
	 */
	private List<String> readLine(String filename) {
		return readLine(new File(filename));
	}
	
	/**
	 * Utility method to load a file into a list of lines.
	 * 
	 * @param filename
	 * @return
	 */
	private List<String> readLine(File f) {
		List<String> lines = new ArrayList<>();
		
		try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				lines.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lines;
	}
	
	/*
	 * Add ego features as properties of a node
	 */
	private Map<String, Object> readEgoFeatures(String filename, List<String> features) {
		Map<String, Object> properties = new HashMap<>();
		
		List<String> lines = readLine(filename);
		for (String line : lines) {
			line = line.trim();
			
			if (line.length() == 0) continue;
			
			String[] tokens = line.split(" ");
			
			for (int i = 0; i < tokens.length; i++) {
				int bit = Integer.parseInt(tokens[i]);
				
				if (1 == bit) {
					properties.put(features.get(i), "");
				}
			}
		}
		
		return properties;
	}
	
	/**
	 * Read feature names from ego.featname
	 * 
	 * @param filename
	 * @return
	 */
	private List<String> readFeatures(String filename) {
		List<String> features = new ArrayList<>();
		
		List<String> lines = readLine(filename);
		for (String line : lines) {
			int index = line.indexOf(" ");
			features.add(line.substring(index));
		}
		
		return features;
	}
	
	// all insert operations are here
	public void insert() throws IOException {
		
		File[] files = null;
        
        // insert
        File dir = new File(dataPath);
		
		files = dir.listFiles(new FileFilter() {
		    public boolean accept(File file) {
		        if (file.getName().endsWith(".edges")) {
		            return true;
		        }
		        return false;
		    }	
		});
		
		for (File f : files) {
			
			String egoString = f.getAbsolutePath().substring(0, f.getAbsolutePath().indexOf("."));
			logger.info("Processing " + egoString);
			
			// Parse ego ID
			String ego = f.getName().substring(0, f.getName().indexOf("."));
			
			// Get feature names
			List<String> features = readFeatures(egoString + ".featnames");

			// Set ego features
			Map<String, Object> properties = readEgoFeatures(egoString + ".egofeat", features);
			// Insert ego
			inserter.addPerson(ego, properties);
			
			
			// ==== EDGES (FRIEND) ====
			List<String> edges = readLine(f);
			
			for (String line : edges) {
				String[] tokens = line.split(" ");
				
				String src = tokens[0];
				String det = tokens[1];
				
				inserter.addPerson(src);
				inserter.addPerson(det);
				
				// ego is assumed to connect to both src and det
				inserter.addFriend(ego, src);
				inserter.addFriend(ego, det);
				inserter.addFriend(src, det);
			}
			
			// ==== FEAT (KNOWS) ====
			List<String> feats = readLine(egoString + ".feat");
			
			for (String line : feats) {
				// target-id feat1 feat2 feat3 ...
				String[] tokens = line.split(" ");
				
				Map<String, Object> props = new HashMap<>();
				String target = tokens[0];
				
				for (int i = 1; i < tokens.length; i++) {
					int bit = Integer.parseInt(tokens[i]);
					
					if (1 == bit) {
						props.put(features.get(i-1), "");
					}
				}
				
				inserter.addPerson(target, props);
				inserter.addFriend(ego, target);
			}
			
			// Consider each circle as a label
			List<String> circles = readLine(egoString + ".circles");
			for (String line : circles) {
				// circle ego1 ego2 ...
				String[] tokens = line.split("\t");
				
				// Size of circle: ego + all the nodes in tokens = 1 + tokens.length - 1 = tokens.length
				/*
				int circleSize = tokens.length;
				if (circleSize > inserter.maxCircle) inserter.maxCircle = circleSize;
				*/
				
				String circleName = ego + ":" + tokens[0];
				inserter.addCircle(circleName);
				inserter.setCircle(circleName, ego);
				
				for (int i = 1; i < tokens.length; i++) {
					String target = tokens[i];
					
					inserter.addPerson(target);
					inserter.addFriend(ego, target);
					inserter.setCircle(circleName, target);
				}
			}
		}
        
		inserter.insert();
   }
	
	public static void usage() {
		System.err.println(new StringBuilder()
			.append("BatchInsert faceboook|gplus|twitter").toString());
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if (args.length != 1) {
			usage();
			System.exit(0);
		}
		
		String dataset = args[0];
		String neo4JPath = "neo4j/" + dataset + "-exp";
		String dataPath = "data/" + dataset;
		
		SocialCircleInserter insert = null;
		
		if (dataset.equals("facebook")) {
			insert = new SocialCircleInserter(dataPath, 
					new Neo4JInserter(neo4JPath, false));
		} else if (dataset.equals("gplus")) {
			insert = new SocialCircleInserter(dataPath, 
					new Neo4JInserter(neo4JPath, true));
		} else if (dataset.equals("twitter")) {
			insert = new SocialCircleInserter(dataPath, 
					new Neo4JInserter(neo4JPath, true));
		} else {
			usage();
			System.exit(0);
		}
		
		insert.insert();
	}

}
