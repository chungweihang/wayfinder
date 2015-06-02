package smallworld.data.query;

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
		for (Long ego : q.allNodes()) {
			if (++ count % 100 == 0) System.out.println(count);
			q.clusteringCoefficient(ego);
		}
		
		q.shutdown();
	}
}
