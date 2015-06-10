package smallworld.data.query;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.Traversal;

import edu.stanford.nlp.util.Maps;
import smallworld.data.RelationshipTypes;
import smallworld.data.inserter.exp.Neo4JInserter;

/**
 * Query a path from social circle dataset
 * 
 * @author chang
 *
 */
public class QueryPaths {

	private Query query;
	
	public QueryPaths(Query query) {
		this.query = query;
	}
	
	public QueryPaths(String path) {
		this(new Query(path));
	}
	
	public List<List<Node>> getCypherAllShortestPathsDistinctNodes(long from, long to, String relationship, int length) {
		Result result = query.cypherQuery( 
        		"START n = node(" + from + "), m = node(" + to + ") " +  
        		"MATCH p = allShortestPaths(n-[:" + relationship + "*.." + length + "]-m) " + 
        		//"RETURN n as from, p as `->`, m as to, length(p);");
				"WITH DISTINCT nodes(p) as nodes " +
        		"RETURN nodes;");
				// "foreach(x in nodes(p) : RETURN x);");
		
		List<List<Node>> paths = new ArrayList<List<Node>>();
		for (Iterator<List<Node>> it = result.columnAs("nodes"); it.hasNext(); ) {
			paths.add(it.next());
			// System.out.println(paths.get(paths.size() - 1).size());
		}
		
		return paths;
	}
	
	public static int countCirclePathInPaths(List<Node> path) {
		
		int count = 0;
		
		Iterator<Node> it = path.iterator();
			
		Node n1 = it.next();
		for (; it.hasNext(); ) {
			
			Node n2 = it.next();
			
			for (Iterator<String> circles = n1.getPropertyKeys().iterator(); circles.hasNext();) {
				if (n2.hasProperty(circles.next())) {
					count++;
					break;
				}
			}
			
			n1 = n2;
		}
		return count;
	}
	
	public void getPath(long from, long to, RelationshipTypes rel) {
		List<List<Node>> paths = getCypherAllShortestPathsDistinctNodes(from, to, rel.name(), 15);
		
		for (int i = 0; i < paths.size(); i++) {
			System.out.println("==== path " + i + " ====");
			printPath(paths.get(i), System.out);
		}
	}
	
	public void printPath(List<Node> path, PrintStream print) {
		
		QueryCircles qc = new QueryCircles(query);
		
		Node previous = null;
		for (int i = 0; i < path.size(); i++) {
			Node n = path.get(i);
			
			System.out.println("**** NODE: " + n.getId());
			System.out.println("** features **");
			print.println(Maps.toStringSorted(QueryFeatures.getFeatures(n)));
			
			if (previous != null) {
				System.out.println("** circles **");
				
				//for (Iterator<Label> circles = qc.getCircleLabels(n).iterator(); circles.hasNext(); ) {
				for (Node circle : qc.getCircles(n)) {
					System.out.println(circle.getProperty(Neo4JInserter.IDENTIFIER));
				}
			}
			
			previous = n;
		}
		
	}
	
	/*
	public int cypherShortestPathLength(long from, long to, String relationship, Direction dir, int length) {
		
		String direction = "-";
		if (dir == Direction.OUTGOING) direction = "->";
		
		Result result = cypherQuery( 
        		"START n = node(" + from + "), m = node(" + to + ") " +  
        		"MATCH p = shortestPath(n-[:" + relationship + "*.." + length + "]" + direction + "m) " + 
        		//"RETURN n as from, p as `->`, m as to, length(p);");
				"RETURN length(p) as length;");
				// "foreach(x in nodes(p) : RETURN x);");
		
		List<Object> values = getCypherQueryResult(result, "length");
		
		if (values.size() == 0) return 0;
		return (Integer) values.get(0);
	}
	*/
	
	public String countCirclesInAllShortestPaths(long from, long to) {
		Node startNode = query.getGraphDatabaseService().getNodeById(from);
		Node endNode = query.getGraphDatabaseService().getNodeById(to);
		
		PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
		        Traversal.expanderForTypes(RelationshipTypes.FRIEND.type(), Direction.OUTGOING ), 15 );
		Iterator<Path> paths = finder.findAllPaths(startNode, endNode).iterator();
		
		int fcount = 0;
		int ccount = 0;
		int length = 0;
		
		while (paths.hasNext()) {
			Path p = paths.next();
			length = p.length();
			// System.out.println(p);
			Iterator<Node> it = p.nodes().iterator();
			Node n1 = it.next();
			for (; it.hasNext(); ) {
				fcount++;
				Node n2 = it.next();
				for (Iterator<Relationship> rels = n1.getRelationships(RelationshipTypes.CIRCLE.type(), Direction.OUTGOING).iterator(); rels.hasNext(); ) {
					Relationship r = rels.next();
					
					if (r.getEndNode().getId() == n2.getId()) {
						// System.out.println(r.getStartNode() + " -" + r.getType() + "-> " + r.getEndNode());
						ccount++;
						break;
					}
				}
				
				n1 = n2;
			}
		}
		
		return String.format("%d, %d, %d, %d, %d", from, to, length, fcount, ccount);
	}
	
	public void shutdown() {
		query.shutdown();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query q = new Query("neo4j/facebook");
		QueryPaths qp = new QueryPaths(q);
		qp.getPath(0, 620, RelationshipTypes.FRIEND);
		
		q.shutdown();
	}

}
