package smallworld.data.query;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import smallworld.data.RelationshipTypes;

/**
 * Query a path from social circle dataset
 * 
 * @author chang
 *
 */
public class QueryPath {

	private Query query;
	
	public QueryPath(Query query) {
		this.query = query;
	}
	
	public QueryPath(String path) {
		this(new Query(path));
	}
	
	public void getPath(long from, long to, RelationshipTypes rel) {
		List<List<Node>> paths = query.allShortestPathsDistinctNodes(from, to, rel.name(), 15);
		
		for (int i = 0; i < paths.size(); i++) {
			System.out.println("==== path " + i + " ====");
			printPath(paths.get(i), System.out);
		}
	}
	
	public void printPath(List<Node> path, PrintStream print) {
		
		QueryFeatures qf = new QueryFeatures(query);
		QueryCircles qc = new QueryCircles(query);
		
		Node previous = null;
		for (int i = 0; i < path.size(); i++) {
			Node n = path.get(i);
			
			System.out.println("**** NODE: " + n.getId());
			System.out.println("** features **");
			qf.getFeatures(n.getId()).list(print);
			
			if (previous != null) {
				System.out.println("** circles **");
				
				for (Iterator<String> circles = qc.getCircles(n.getId()).iterator(); circles.hasNext(); ) {
					System.out.println(circles.next());
				}
			}
			
			previous = n;
		}
		
	}
	
	public void shutdown() {
		query.shutdown();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query q = new Query("neo4j/facebook");
		QueryPath qp = new QueryPath(q);
		qp.getPath(0, 620, RelationshipTypes.FRIEND);
		
		q.shutdown();
	}

}
