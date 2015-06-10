package smallworld.data.query;

import java.util.Iterator;

import org.neo4j.graphdb.Result;

/**
 * 
 * Calculate clustering coefficient for each node in a Neo4J graph.
 * For a graph with 4000+ nodes, it takes about 20 mins with a personal laptop.
 * 
 * @author chang
 *
 */
public class ClusteringCoefficient {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.err.println(ClusteringCoefficient.class.getName() + " dataset");
			System.exit(0);
		}
		
		String dataset = args[0];
		Query q = new Query("neo4j/" + dataset);
		int count = 0;
		for (Long ego : q.cypherAllNodes()) {
			if (++ count % 100 == 0) System.out.println(count);
			clusteringCoefficient(q, ego);
		}
		
		q.shutdown();
	}
	
	public static void clusteringCoefficient(Query query) {
		query.cypherQuery( 
        		"START a = node(*) " +
        		"MATCH (a)-[:FRIEND]-(b) " +
        		"WITH a, count(distinct b) as n " +
        		"MATCH (a)-[:FRIEND]-()-[r:FRIEND]-()-[:FRIEND]-(a) " +
        		"WITH toFloat(count(distinct r)) * 2 / (n * (n-1)) AS cc, a " +
        		"SET a.clustering_coefficient = cc");
	}
	
	public static Double clusteringCoefficient(Query query, long ego) {
		// clustering coefficient 
        // r / (n! / (2!(n-2)!))
		Result result = query.cypherQuery( 
        		"START a = node(" + ego + ") " +
        		"MATCH (a)-[:FRIEND]-(b) " +
        		"WITH a, count(distinct b) as n " +
        		"MATCH (a)-[:FRIEND]-()-[r:FRIEND]-()-[:FRIEND]-(a) " +
        		"WITH toFloat(count(distinct r)) * 2 / (n * (n-1)) AS cc, a " +
				"SET a.clustering_coefficient = cc " +
        		"RETURN cc;");
		
		for (Iterator<Double> it = result.columnAs("cc"); it.hasNext(); ) {
			Double cc = it.next();
			if (cc != null) return cc;
		}
		
		return null;
	}
}
