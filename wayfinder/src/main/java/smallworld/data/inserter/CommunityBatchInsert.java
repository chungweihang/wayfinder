package smallworld.data.inserter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import smallworld.data.RelationshipTypes;

/**
 * Batch insert data to neo4j
 * 
 * @author chang
 *
 */
public class CommunityBatchInsert {
	
	private boolean directed = false;
	private Map<Long, Set<Long>> edges;
	private long numEdges = 0;
	private String neo4jPath, edgeFile, communityFile;
	
	public CommunityBatchInsert(String neo4jPath, String edgeFile, String communityFile) {
		this.neo4jPath = neo4jPath;
		this.edgeFile = edgeFile;
		this.communityFile = communityFile;
		this.edges = new HashMap<Long, Set<Long>>(); // check duplicate
	}
	
	public boolean isDirected() {
		return directed;
	}

	public void setDirected(boolean directed) {
		this.directed = directed;
	}

	public void delete() throws IOException {
		FileUtils.deleteRecursively(new File(neo4jPath));
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
	private void createRelationship(BatchInserter inserter, long src, long det) {
		if (!hasRelationship(src, det)) {
			inserter.createRelationship(src, det, RelationshipTypes.FRIEND.type(), null);
			addRelationship(src, det);
			numEdges++;
			//System.err.println("create FRIEND relationship: " + src + " => " + det);
		} //else System.err.println("duplicate FRIEND relationship: " + src + " => " + det);
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
	private void addRelationship(long src, long det) {
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
	
	// all insert operations are here
	public void insert() {
		
		// config
		Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M");
        config.put( "neostore.relationshipstore.db.mapped_memory", "3G");
        config.put( "neostore.propertystore.db.mapped_memory", "50M");
        config.put( "neostore.propertystore.db.strings.mapped_memory", "100M");
        config.put( "neostore.propertystore.db.arrays.mapped_memory", "0M");
        
        BatchInserter inserter = BatchInserters.inserter(neo4jPath, config);
        
        int numNodes = 0;
        
        // edges
        BufferedReader reader = null;
        try {
        	reader = new BufferedReader(new FileReader(edgeFile));
		
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				
				if (line.startsWith("#")) continue;
				
				String[] tokens = line.split("\t");
						
				long src = Long.parseLong(tokens[0]);
				long det = Long.parseLong(tokens[1]);
						
				if (!inserter.nodeExists(src)) {
					inserter.createNode(src, null);
					//nodesIndex.add(src, null);
					numNodes++;
				}
						
				if (!inserter.nodeExists(det)) { 
					inserter.createNode(det, null);
					//nodesIndex.add(det, null);
					numNodes++;
				}
					
				createRelationship(inserter, src, det);
			}
        } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != reader) try { reader.close(); } catch (IOException ignored) {}
		}
		
        int communityCount = 0;
        int maxCommunity = 0;
        long totalCommunitySize = 0;
        
		BufferedReader communities = null;
		try {
			communities = new BufferedReader(new FileReader(communityFile));
			for (String line = communities.readLine(); line != null; line = communities.readLine()) {
				String[] tokens = line.split("\t");
				
				if (tokens.length < 3) continue;
				
				if (tokens.length > maxCommunity) maxCommunity = tokens.length;
				totalCommunitySize += tokens.length;
				String communityName = new StringBuilder("circle").append(++communityCount).toString();
				
				for (int i = 1; i < tokens.length; i++) {
					long target = Long.parseLong(tokens[i]);
					
					if (!inserter.nodeExists(target)) {
						System.err.println(new StringBuilder("node ").append(target).append(" does not exist!").toString());
					}
					
					// TODO: Use labels instead of properties
					inserter.setNodeProperty(target, communityName, tokens.length);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != communities) try { communities.close(); } catch (IOException ignored) {}
		}
        
		inserter.shutdown();
        
		System.out.println("number of vertices: " + numNodes);
		System.out.println("number of edges: " + numEdges);
		System.out.println("number of communities: " + communityCount);
		System.out.println("max size of community: " + maxCommunity);
		System.out.println("Average community size: " + ((double) totalCommunitySize) / communityCount);
		System.out.println("Average membership size: " + ((double) totalCommunitySize) / numNodes);
	}
	
	public static void usage() {
		System.err.println(new StringBuilder()
			.append("BatchInsert amazon|dblp|youtube").toString());
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
		
		CommunityBatchInsert insert = new CommunityBatchInsert(
				"neo4j/" + dataset, 
				"data/" + dataset + "/com-" + dataset + ".ungraph.txt",
				"data/" + dataset + "/com-" + dataset + ".all.cmty.txt");
		
		if (dataset.equals("dblp")) {
			insert.setDirected(false);
		} else if (dataset.equals("youtube")) {
			insert.setDirected(false); 
		} else if (dataset.equals("amazon")) {
			insert.setDirected(false); 
		} else {
			usage();
			System.exit(0);
		}
		
		// DBLP
		//BatchInsert insert = new BatchInsert("neo4j/dblp", "data/dblp/com-dblp.ungraph.txt", "data/dblp/com-dblp.all.cmty.txt"); insert.setDirected(false);
		// Amazon
		//BatchInsert insert = new BatchInsert("neo4j/amazon", "data/amazon/com-amazon.ungraph.txt", "data/amazon/com-amazon.all.cmty.txt"); insert.setDirected(false);
		
		insert.delete();
		insert.insert();
	}

}
