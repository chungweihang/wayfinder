package smallworld.data;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import smallworld.data.query.Query;
import smallworld.data.query.QueryCircles;
import smallworld.data.query.QueryFeatures;

public class FeaturesCirclesRelationships {
	
	/**
	 * Examine how the number of common features between a pair of nodes relates to if they are in a same circle.
	 * 
	 * @param neo4jPath path to Neo4J
	 * @param nodeLimit number of nodes to be examined; -1 to examine all nodes
	 * @param print for output
	 */
	public static void featuresAndCircles(String neo4jPath, int nodeLimit, PrintStream print) {
		// 
		Query q = new Query(neo4jPath);
		QueryCircles qc = new QueryCircles(q);
		
		Map<Integer, Integer> circleResults = new HashMap<Integer, Integer>();
		Map<Integer, Integer> nonCircleResults = new HashMap<Integer, Integer>();

		long time = System.currentTimeMillis();
		
		// shuffle the nodes
		List<Long> nodeIds = Arrays.asList(q.allNodes());
		Collections.shuffle(nodeIds, new Random(0));
		
		// -1 to examine all nodes
		if (nodeLimit == -1) nodeLimit = nodeIds.size();
		
		for (int i = 0; i < nodeLimit; i++) {
			Node n1 = q.getNode(nodeIds.get(i));

			// get all cirlces n1 belongs to
			Set<String> circles = new HashSet<>();
			for (Iterator<String> iterator = qc.getCircles(n1.getId()).iterator(); iterator.hasNext(); ) {
				circles.add(iterator.next());
			}
			
			for (int j = 0; j < nodeLimit; j++) {
				if (i == j) continue;
				Node n2 = q.getNode(nodeIds.get(j));
				
				int count = 0;
				Properties properties1 = QueryFeatures.getFeatures(n1); //properties1.list(System.out);
				Properties properties2 = QueryFeatures.getFeatures(n2); //properties2.list(System.out);
				for (Iterator<String> it = properties1.stringPropertyNames().iterator(); it.hasNext(); ) {
					String key = it.next();
					if (properties2.containsKey(key) && properties2.getProperty(key).equals(properties1.getProperty(key)))
						count++;
				}
				
				for (Iterator<String> iterator = qc.getCircles(n2.getId()).iterator(); iterator.hasNext(); ) {
					String circle = iterator.next();
					if (circles.contains(circle)) {
						if (!circleResults.containsKey(count)) circleResults.put(count, 1);
						else circleResults.put(count, circleResults.get(count) + 1);
						
						break;
					}
						
				}
				
				if (!nonCircleResults.containsKey(count)) nonCircleResults.put(count, 1);
				else nonCircleResults.put(count, nonCircleResults.get(count) + 1);
			}
		}
		
		for (int key : circleResults.keySet()) {
			print.println(key + ", " + circleResults.get(key));
		}
		
		for (int key : nonCircleResults.keySet()) {
			print.println(key + ", " + nonCircleResults.get(key));
		}
				
		System.out.println("== TIME TAKEN: " + (System.currentTimeMillis() - time) + " ==");
		q.shutdown();
	}
	
	public static void featuresAndRelationships(String neo4jPath, int nodeLimit, RelationshipType type, PrintStream print) {
		Query q = new Query(neo4jPath);
		
		Map<Integer, Integer> hasRelationships = new HashMap<Integer, Integer>();
		Map<Integer, Integer> hasNoRelationships = new HashMap<Integer, Integer>();

		long time = System.currentTimeMillis();
		
		List<Long> nodeIds = Arrays.asList(q.allNodes());
		Collections.shuffle(nodeIds, new Random(0));
		
		if (nodeLimit == -1) nodeLimit = nodeIds.size();
		
		for (int i = 0; i < nodeLimit; i++) {
			Node n1 = q.getNode(nodeIds.get(i));
			for (int j = 0; j < nodeLimit; j++) {
				if (i == j) continue;
				Node n2 = q.getNode(nodeIds.get(j));
				
				int count = 0;
				Properties properties1 = QueryFeatures.getFeatures(n1); //properties1.list(System.out);
				Properties properties2 = QueryFeatures.getFeatures(n2); //properties2.list(System.out);
				for (Iterator<String> it = properties1.stringPropertyNames().iterator(); it.hasNext(); ) {
					String key = it.next();
					if (properties2.containsKey(key) && properties2.getProperty(key).equals(properties1.getProperty(key)))
						count++;
				}
				
				if (Query.hasRelationship(n1, n2, type) || Query.hasRelationship(n2, n1, type)) {
					if (!hasRelationships.containsKey(count)) hasRelationships.put(count, 1);
					else hasRelationships.put(count, hasRelationships.get(count) + 1);
				} else {
					if (!hasNoRelationships.containsKey(count)) hasNoRelationships.put(count, 1);
					else hasNoRelationships.put(count, hasNoRelationships.get(count) + 1);
				}
			}
		}
		
		for (int key : hasRelationships.keySet()) {
			print.println(key + ", " + hasRelationships.get(key));
		}
		
		for (int key : hasNoRelationships.keySet()) {
			print.println(key + ", " + hasNoRelationships.get(key));
		}
		
		System.out.println("== TIME TAKEN: " + (System.currentTimeMillis() - time) + " ==");
		q.shutdown();
	}

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		// featuresAndRelationships("neo4j/facebook2", -1, RelationshipTypes.FRIEND.type(), new PrintStream("feature-friendship-3.log"));
		// featuresAndCircles("neo4j/facebook2", -1, new PrintStream("feature-circle-3.log"));
		
		featuresAndRelationships("neo4j/dblp", -1, RelationshipTypes.FRIEND.type(), new PrintStream("dblp-feature-friendship.log"));
		featuresAndCircles("neo4j/dblp", -1, new PrintStream("dblp-feature-circle.log"));
	}

}
