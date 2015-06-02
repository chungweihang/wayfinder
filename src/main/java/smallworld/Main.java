package smallworld;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Transaction;

import smallworld.data.RelationshipTypes;
import smallworld.data.query.Query;
import smallworld.navigation.AbstractNavigation;
import smallworld.navigation.PrioritizedNavigation;
import smallworld.navigation.evaluator.MostCommonCircleEvaluator;

@Deprecated
public class Main {

	public static void run(PathFinder<Path> nav, String neo4jPath, PrintStream print) {
		run(nav, neo4jPath, -1, print);
	}
	
	public static void run(PathFinder<Path> nav, String neo4jPath, int nodeLimit, PrintStream print) {
		Constants.NEO4J_PATH = neo4jPath;
		Query q = Query.getInstance();

		long time = System.currentTimeMillis();
		
		int count = 0;
		long sum = 0;
		
		List<Long> nodeIds = Arrays.asList(q.allNodes());
		Collections.shuffle(nodeIds, new Random(0));
		
		if (nodeLimit == -1 || nodeLimit > nodeIds.size()) nodeLimit = nodeIds.size();
		
		for (int i = 0; i < nodeLimit; i++) {
			Node n1 = q.getNode(nodeIds.get(i));
			for (int j = 0; j < nodeLimit; j++) {
				if (i == j) continue;
				Node n2 = q.getNode(nodeIds.get(j));
				
				Transaction tx = q.getGraphDatabaseService().beginTx();
				Path p = nav.findSinglePath(n1, n2);
				
				if (p == null || p.length() == 1) continue;
				
				count ++;
				print.println(count + ", " + p.length() + ", " + p);
				sum += p.length();
				tx.close();
			}
		}
		
		System.out.println("== TIME TAKEN: " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - time) + " ==");
		System.out.println("== TOTAL PAIRS: " + count + " ==");
		System.out.println("== AVERAGE: " + (sum / (double) count) + " ==");
		System.out.println("== TRUE POSITIVE %: " + AbstractNavigation.getEvaluationResult() + " ==");
		q.shutdown();
	}

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		
		// Social Circle
		//run(new Navigation(Traversal.pathExpanderForTypes(RelationshipTypes.FRIEND.type(), Direction.OUTGOING), new OutDegreeEvaluator()), "neo4j/facebook3", 100, new PrintStream("outdegree.sample.log"));
		//run(new Navigation(Traversal.pathExpanderForTypes(RelationshipTypes.FRIEND.type(), Direction.OUTGOING), new FewestCommonFeatureEvaluator()), "neo4j/facebook3", 100, new PrintStream("fewest_common_allfeature.sample.log"));
		run(new PrioritizedNavigation(PathExpanders.forTypeAndDirection(RelationshipTypes.FRIEND.type(), Direction.OUTGOING), new MostCommonCircleEvaluator()), "neo4j/facebook", 100, new PrintStream("most_common_allfeature.sample.log"));
		//run(GraphAlgoFactory.shortestPath(PathExpanders.forTypeAndDirection(RelationshipTypes.FRIEND.type(), Direction.OUTGOING), 10), "neo4j/facebook", 100, new PrintStream("shortest.sample.log"));
		
		// Community
		// run(GraphAlgoFactory.shortestPath(Traversal.pathExpanderForTypes(smallworld.data.community.RelationshipTypes.FRIEND.type(), Direction.BOTH), 20), "neo4j/dblp", 100, new PrintStream("dblp.shortest.100sample.log"));
		// run(new Navigation(Traversal.pathExpanderForTypes(RelationshipTypes.FRIEND.type(), Direction.BOTH), new MostCommonFeatureEvaluator()), "neo4j/dblp", 100, new PrintStream("dblp.most_common_allfeature.100sample.log"));
	}

}
