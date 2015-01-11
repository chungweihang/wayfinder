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
		q.betweennessCentrality();
		q.shutdown();
	}
}
