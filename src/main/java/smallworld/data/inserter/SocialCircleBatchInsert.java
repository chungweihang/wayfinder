package smallworld.data.inserter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import smallworld.data.RelationshipTypes;
import smallworld.data.query.Query;
import smallworld.util.Utils;

/**
 * Batch insert data to neo4j
 * 
 * @author chang
 *
 */
public class SocialCircleBatchInsert {

	protected String neo4jPath, dataPath;
	private Map<BigInteger, Long> nodes;
	private Map<Long, Set<Long>> edges; // check duplicate
	private boolean usingBigInteger = false;
	private boolean directed = false;
	private long numEdges = 0;
	
	public SocialCircleBatchInsert(String neo4jPath, String dataPath) {
		this.neo4jPath = neo4jPath;
		this.dataPath = dataPath;
		
		this.nodes = new HashMap<BigInteger, Long>();
		this.nodes.put(new BigInteger("0"), (long) 0);
		this.edges = new HashMap<Long, Set<Long>>();
	}
	
	/**
	 * Get if the dataset is directed graph or not
	 * Default is not.
	 * 
	 * @return
	 */
	public boolean isDirected() {
		return directed;
	}

	/**
	 * Set if the dataset is directed graph or not.
	 * Default is not.
	 * 
	 * @param directed
	 */
	public void setDirected(boolean directed) {
		this.directed = directed;
	}

	/**
	 * Create a FRIEND relationship between two nodes.
	 * If the nodes don't exist, create these nodes first.
	 * 
	 * If the relationship exists, do nothing.
	 * 
	 * @param inserter
	 * @param src
	 * @param det
	 */
	private void createRelationship(BatchInserter inserter, BigInteger src, BigInteger det) {
		createNode(inserter, src);
		createNode(inserter, det);
		
		if (!hasRelationship(nodes.get(src), nodes.get(det))) {
			inserter.createRelationship(nodes.get(src), nodes.get(det), RelationshipTypes.FRIEND.type(), null);
			addRelationship(nodes.get(src), nodes.get(det));
			numEdges++;
			//System.err.println("create FRIEND relationship: " + src + " => " + det);
		} //else System.err.println("duplicate FRIEND relationship: " + src + " => " + det);
	}
	
	/**
	 * Create a node if it does not exist already.
	 * 
	 * If the node exists, do nothing
	 * 
	 * If node ID can't be parsed as a long (e.g., in Google+),
	 * the node ID will be mapped to a ID selected by the system.
	 * 
	 * All the ID mapping is stored.
	 * 
	 * @param inserter
	 * @param node
	 */
	private void createNode(BatchInserter inserter, BigInteger node) {
		if (usingBigInteger) {
			if (!nodes.containsKey(node)) {
				nodes.put(node, inserter.createNode(null));
			}
		} else {
			if (!inserter.nodeExists(node.longValue())) {
				inserter.createNode(node.longValue(), null);
				nodes.put(node, node.longValue());
			}
		}
	}
	
	/**
	 * Add a relationship to the record.
	 * 
	 * Every relationship is kept to prevent duplication.
	 * 
	 * This method is used internally only.
	 * 
	 * @param src
	 * @param det
	 */
	private void addRelationship(Long src, Long det) {
		if (!edges.containsKey(src)) edges.put(src, new HashSet<Long>());
		if (!edges.get(src).contains(det)) edges.get(src).add(det);
		else System.err.println("[ERROR] edge exists: " + src + " => " + det); // shouldn't happen
	}
	
	private boolean hasRelationship(Long src, Long det) {
		if (directed && edges.containsKey(src) && edges.get(src).contains(det)) return true;
		if (!directed &&  
				(edges.containsKey(src) && edges.get(src).contains(det)
				|| edges.containsKey(det) && edges.get(det).contains(src))) return true;
		return false;
	}
	
	/**
	 * Set if the dataset needs ID translation, i.e.,
	 * when ID can't be parsed as a long.
	 * 
	 * @return
	 */
	public boolean isUsingBigInteger() {
		return usingBigInteger;
	}

	/**
	 * Get if the dataset needs ID translation, i.e.,
	 * when ID can't be parsed as a long.
	 * 
	 * @param usingBigInteger
	 */
	public void setUsingBigInteger(boolean usingBigInteger) {
		this.usingBigInteger = usingBigInteger;
	}

	public void delete() throws IOException {
		Utils.delete(neo4jPath);
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
		List<String> lines = new ArrayList<String>();
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(f));
			
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				lines.add(line);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != reader) try { reader.close(); } catch (IOException ignored) {}
		}
		
		return lines;
	}
	
	private Map<String, Object> readEgoFeatures(String filename, List<String> features) {
		Map<String, Object> properties = new HashMap<String, Object>();
		
		List<String> lines = readLine(filename);
		for (String line : lines) {
			line = line.trim();
			
			if (line.length() == 0) continue;
			
			String[] tokens = line.split(" ");
			
			for (int i = 0; i < tokens.length; i++) {
				int bit = Integer.parseInt(tokens[i]);
				
				if (1 == bit) {
					properties.put(features.get(i), "");
					/*
					String feature = features.get(i);
					// ";" for facebook; ":" for google+
					int index = feature.indexOf(";") == -1 ? feature.lastIndexOf(":") : feature.lastIndexOf(";"); // separate key and value
					
					if (index != -1) {
						properties.put(feature.substring(0, index), feature.substring(index + 1));
					} else {
						// twitter
						properties.put(feature, "");
					}
					*/
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
		List<String> features = new ArrayList<String>();
		
		List<String> lines = readLine(filename);
		for (String line : lines) {
			int index = line.indexOf(" ");
			features.add(line.substring(index));
		}
		
		return features;
	}
	
	// all insert operations are here
	public void insert() {
		
		// config
		Map<String, String> config = new HashMap<String, String>();
        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.relationshipstore.db.mapped_memory", "3G");
        config.put("neostore.propertystore.db.mapped_memory", "50M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
        config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
        
        BatchInserter inserter = BatchInserters.inserter(neo4jPath, config);
        
        int maxCircle = 0;
        long totalCircleSize = 0;
        int numberCircles = 0;
        
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
			System.out.println("Processing " + egoString);
			
			// Parse ego ID
			BigInteger ego = new BigInteger(f.getName().substring(0, f.getName().indexOf(".")));
			
			// Insert ego
			this.createNode(inserter, ego);
			
			// Get feature names
			List<String> features = readFeatures(egoString + ".featnames");
			
			// Set ego features
			Map<String, Object> properties = readEgoFeatures(egoString + ".egofeat", features);
			inserter.createRelationship(nodes.get(ego), nodes.get(ego), RelationshipTypes.KNOWS.type(), properties);
			
			// ==== EDGES (FRIEND) ====
			List<String> edges = readLine(f);
			
			for (String line : edges) {
				String[] tokens = line.split(" ");
				
				BigInteger src = new BigInteger(tokens[0]);
				BigInteger det = new BigInteger(tokens[1]);
				
				// ego is assumed to connect to both src and det
				this.createRelationship(inserter, ego, src);
				this.createRelationship(inserter, ego, det);
				this.createRelationship(inserter, src, det);
			}
			
			// ==== FEAT (KNOWS) ====
			List<String> feats = readLine(egoString + ".feat");
			
			for (String line : feats) {
				// target-id feat1 feat2 feat3 ...
				String[] tokens = line.split(" ");
				
				Map<String, Object> props = new HashMap<String, Object>();
				BigInteger target = new BigInteger(tokens[0]);
				
				for (int i = 1; i < tokens.length; i++) {
					int bit = Integer.parseInt(tokens[i]);
					
					if (1 == bit) {
						props.put(features.get(i-1), "");
					}
				}
				
				this.createNode(inserter, target);
				this.createRelationship(inserter, ego, target);
				inserter.createRelationship(nodes.get(ego), nodes.get(target), RelationshipTypes.KNOWS.type(), props);
			}
			
			// Consider each circle as a label
			List<String> circles = readLine(egoString + ".circles");
			for (String line : circles) {
				// circle ego1 ego2 ...
				String[] tokens = line.split("\t");
				
				// Size of circle: ego + all the nodes in tokens = 1 + tokens.length - 1 = tokens.length
				int circleSize = tokens.length;
				if (circleSize > maxCircle) maxCircle = circleSize;
				totalCircleSize += circleSize;
				numberCircles++;
				
				String circleName = ego + ":" + tokens[0];
				Label circleLabel = DynamicLabel.label(circleName);
				inserter.setNodeLabels(nodes.get(ego), Query.addLabel(inserter.getNodeLabels(nodes.get(ego)), circleLabel));
				//inserter.setNodeProperty(nodes.get(ego), circleName, circleSize);
				//System.out.printf("add %s to circle %s of size %d\n", ego, circleName, circleSize);
				
				for (int i = 1; i < tokens.length; i++) {
					BigInteger target = new BigInteger(tokens[i]);
					
					this.createNode(inserter, target);
					this.createRelationship(inserter, ego, target);
					inserter.setNodeLabels(nodes.get(target), Query.addLabel(inserter.getNodeLabels(nodes.get(target)), circleLabel));
					//inserter.setNodeProperty(nodes.get(target), circleName, circleSize);
					//System.out.printf("add %s to circle %s of size %d\n", target, circleName, circleSize);
				}
			}
		}
        
		inserter.shutdown();
        
		if (this.isUsingBigInteger()) {
			System.out.println("==== NODE MAPPING ====");
			for (BigInteger i : nodes.keySet()) {
				System.out.println(i + " " + nodes.get(i));
			}
		}
		
		System.out.println("Number of nodes: " + nodes.size());
		System.out.println("Number of edges: " + numEdges);
		System.out.println("Number of circles: " + numberCircles);
		System.out.println("Max size of circle: " + maxCircle);
		System.out.println("Average circle size: " + ((double) totalCircleSize) / numberCircles);
		System.out.println("Average membership size: " + ((double) totalCircleSize) / nodes.size());
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
		
		SocialCircleBatchInsert insert = new SocialCircleBatchInsert("neo4j/" + dataset, "data/" + dataset);
		
		if (dataset.equals("facebook")) {
			insert.setDirected(false);
			insert.setUsingBigInteger(false);
		} else if (dataset.equals("gplus")) {
			insert.setDirected(true); 
			insert.setUsingBigInteger(true);
		} else if (dataset.equals("twitter")) {
			insert.setDirected(true); 
			insert.setUsingBigInteger(false);
		} else {
			usage();
			System.exit(0);
		}
		
		//BatchInsert insert = new BatchInsert("neo4j/facebook", "data/facebook"); insert.setDirected(false); insert.setUsingBigInteger(false);
		//BatchInsert insert = new BatchInsert("neo4j/gplus", "data/gplus"); insert.setDirected(true); insert.setUsingBigInteger(true);
		//BatchInsert insert = new BatchInsert("neo4j/twitter", "data/twitter"); insert.setDirected(true); insert.setUsingBigInteger(false);
		insert.delete();
		insert.insert();
	}

}
