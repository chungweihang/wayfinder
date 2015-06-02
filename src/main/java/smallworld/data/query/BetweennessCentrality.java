package smallworld.data.query;

/**
 * 
 * @author chang
 *
 */
public class BetweennessCentrality {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.err.println(BetweennessCentrality.class.getName() + " dataset");
			System.exit(0);
		}
		
		String dataset = args[0];
		Query q = new Query("neo4j/" + dataset);
		betweennessCentrality(q);
		q.shutdown();
	}
	
	public static void betweennessCentrality(Query query) {
		query.cypherQuery(
				"START n=node(*) " + 
				"WITH collect(n) AS all_nodes " +
				"START src=node(*), det=node(*) " + 
				"MATCH p = allShortestPaths(src-[*]-det) " +
				"WHERE src <> det AND length(p) > 1 " +
				"WITH NODES(p) AS nodes, all_nodes " +
				"WITH COLLECT(nodes) AS paths, all_nodes " +
				"WITH reduce(res=[], x IN all_nodes | res + [ {node:x, bc:length(filter(p IN paths WHERE x in tail(p) AND x <> last(p)))}]) AS bc_pairs " +
				"FOREACH (pair IN bc_pairs | SET pair.node.betweenness_centrality=pair.bc)"
				);
		
	}
}
