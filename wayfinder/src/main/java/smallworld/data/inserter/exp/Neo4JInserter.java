package smallworld.data.inserter.exp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import smallworld.data.RelationshipTypes;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class Neo4JInserter {
	
	private static final Logger logger = LogManager.getLogger();
	static final String CIRCLE_NAME = "circleName";
	
	final BatchInserter inserter;
	boolean isDirected;
	final Multimap<Long, Long> friendEdges;
	final Multimap<Long, Long> circleEdges;
	final Map<Object, Long> nodeToIds;
	final Map<String, Long> circleToIds;
	
	// statistics
	int maxCircle = 0; // need to set explicitly
	long totalCircleSize = 0;
	
	public Neo4JInserter(String path) {
		this(path, false);
	}
	
	public Neo4JInserter(String path, boolean isDirected) {
		// config
		Map<String, String> config = new HashMap<>();
        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.relationshipstore.db.mapped_memory", "3G");
        config.put("neostore.propertystore.db.mapped_memory", "50M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
        config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
        inserter = BatchInserters.inserter(path, config);
        
        this.isDirected = isDirected;
        
        friendEdges = HashMultimap.create();
        circleEdges = HashMultimap.create();
        nodeToIds = new HashMap<>();
        circleToIds = new HashMap<>();
   }
	
	void insert() throws IOException {
		delete(inserter.getStoreDir());
		inserter.shutdown();
		 
		logger.info("Number of nodes: " + nodeToIds.size());
		logger.info("Number of friendships: " + friendEdges.size());
		logger.info("Number of circles: " + circleToIds.size());
		logger.info("Max size of circle: " + maxCircle);
		logger.info("Average circle size: " + ((double) totalCircleSize) / circleToIds.size());
		logger.info("Average membership size: " + ((double) totalCircleSize) / nodeToIds.size());
		
	}
	
	public void addCircle(String circleName) {
		addCircle(circleName, emptyMap());
	}
	
	public void addCircle(String circleName, Map<String, Object> properties) {
		if (!circleToIds.containsKey(circleName)) {
			long id = inserter.createNode(properties);
			circleToIds.put(circleName, id);
		} else {
			long id = circleToIds.get(circleName);
			properties.putAll(inserter.getNodeProperties(id));
			inserter.setNodeProperties(id, properties);
		}
	}
	
	public void setCircle(String circleName, Object node) {
		if (!nodeToIds.containsKey(node)) {
			throw new IllegalArgumentException("Node: " + node + " does not exist!"); 
		}
		
		if (circleToIds.containsKey(circleName)) {
			// always from node to circle
			if (addRelationship(nodeToIds.get(node), circleToIds.get(circleName), RelationshipTypes.CIRCLE.type())) {
				circleEdges.put(nodeToIds.get(node), circleToIds.get(circleName));
				totalCircleSize++;
			}
		} else {
			throw new IllegalArgumentException("Cirlce: " + circleName + " does not exist!"); 
		}
	}
	
	public void addNode(Object node) {
		addNode(node, emptyMap());
	}
	
	public void addNode(Object node, Map<String, Object> features) {
		if (!nodeToIds.containsKey(node)) {
			long id = inserter.createNode(features);
			nodeToIds.put(node, id);
		} else {
			long id = nodeToIds.get(node);
			features.putAll(inserter.getNodeProperties(id));
			inserter.setNodeProperties(id, features);
		}
	}
	
	private static Map<String, Object> emptyMap() {
		return new HashMap<>();
	}
	
	private boolean addRelationship(long fromNodeId, long toNodeId, RelationshipType type) {
		if (!relationshipExists(type, fromNodeId, toNodeId)) {
			// relationship does not exist, create new relationship
			inserter.createRelationship(fromNodeId, toNodeId, type, null);
			return true;
		} else {
			//logger.warn("Relationship " + fromNodeId + " => " + toNodeId + " exists!");
			return false;
		}
	}
	
	public void addFriend(Object fromNode, Object toNode) {
		if (!nodeToIds.containsKey(fromNode)) {
			throw new IllegalArgumentException("Node: " + fromNode + " does not exist!"); 
		}
		
		if (!nodeToIds.containsKey(toNode)) {
			throw new IllegalArgumentException("Node: " + toNode + " does not exist!"); 
		}
		if (addRelationship(nodeToIds.get(fromNode), nodeToIds.get(toNode), RelationshipTypes.FRIEND.type())) {
			friendEdges.put(nodeToIds.get(fromNode), nodeToIds.get(toNode));
		}
	}
	
	private boolean relationshipExists(RelationshipType type, long fromNodeId, long toNodeId) {
		if (type == RelationshipTypes.FRIEND.type()) {
			if (isDirected) {
				// directed
				return friendEdges.containsEntry(fromNodeId, toNodeId);
			} else {
				// undirected
				return friendEdges.containsEntry(fromNodeId, toNodeId) || friendEdges.containsEntry(toNodeId, fromNodeId);
			}
		} else if (type == RelationshipTypes.CIRCLE.type()) {
			return circleEdges.containsEntry(fromNodeId, toNodeId);
		} else {
			throw new IllegalArgumentException("Unknown relation type: " + type.name());
		}
		
	}
	
	private void delete(String path) throws IOException {
		FileUtils.deleteRecursively(new File(path));
	}
}
