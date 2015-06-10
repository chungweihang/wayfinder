package smallworld.data.query;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import smallworld.Constants;
import smallworld.data.RelationshipTypes;
import smallworld.data.inserter.exp.Neo4JInserter;

import com.google.common.collect.Lists;

public class Query {
	
	private GraphDatabaseService db;
	
	private static Query INSTANCE = null;
	
	public static synchronized Query getInstance() {
		if (INSTANCE == null) INSTANCE = new Query(Constants.NEO4J_PATH);
		return INSTANCE;
	}
	
	public Query(String path) {
		//db = new EmbeddedGraphDatabase(path);
		db = new GraphDatabaseFactory().newEmbeddedDatabase(path);
	}

	public GraphDatabaseService getGraphDatabaseService() {
		return db;
	}

	public Result cypherQuery(String query) {
		return db.execute(query);
	}
	
	public void print(Result result) {
		String rows = "";
        for (; result.hasNext(); )
        {
        	Map<String, Object> row = result.next();
            for (Entry<String, Object> column : row.entrySet())
            {
                rows += column.getKey() + ": " + column.getValue() + "; ";
            }
            rows += "\n";
        }
        
        System.out.println(rows);
	}
	
	private static List<Object> getCypherQueryResult(Result result, String key) {
		List<Object> values = new ArrayList<Object>();
		
		for (; result.hasNext(); )
        {
        	Map<String, Object> row = result.next();
			if (row.containsKey(key)) values.add(row.get(key));
        }
		
		return values;
	}
	
	public void shutdown() {
		db.shutdown();
	}
	
	public void cypherNumberOfNodes() {
		print(cypherQuery("START n=node(*) RETURN count(*)"));
	}
	
	
	public void cypherNumberOfRelationships() {
		print(cypherQuery( 
        		"START n=node(*) " +   
        		"MATCH n-[r]->() " + 
        		"RETURN type(r), count(*);"));
    }
	
	public List<Relationship> cypherGetCircles(long from, String circle) {
		Result result = cypherQuery(
				"START n = node(" + from + ") " +
				"MATCH n-[r?:CIRCLE]->m " +
				"WHERE r.name = \"" + circle + "\" " + 
				"RETURN r;");
		
		List<Relationship> rels = new ArrayList<Relationship>();
		
		for (Iterator<Relationship> it = result.columnAs("r"); it.hasNext();) {
			Relationship r = it.next();
			if (r != null) rels.add(r);
		}
		
		return rels;
	}
	
	public void printEgoFeatures(Node node, PrintStream print) {
		print.println("== PRINT EGO FEATURES OF " + node.getId() + " ==");
		for (Iterator<String> it = node.getPropertyKeys().iterator(); it.hasNext(); ) {
			String key = it.next();
			print.println(key + " --> " + node.getProperty(key));
		}
	}
	
	public Node cypherGetNode(long ego) {
		Result result = cypherQuery(
				"START n = node(" + ego + ") " +
				"RETURN n");
		
		Iterator<Node> nodes = result.columnAs("n");
		if (nodes.hasNext()) return nodes.next();
		return null;
	}
	
	/*
	public Node cypherGetNode(Object person) {
		Result result = cypherQuery(
				"MATCH (n) " +
				"WHERE n:PERSON AND n." + Neo4JInserter.IDENTIFIER + " = " + person + " " +
				"RETURN n");
		
		Iterator<Node> nodes = result.columnAs("n");
		if (nodes.hasNext()) return nodes.next();
		return null;
	}
	*/
	
	public List<String> cypherGetCircleNames(long from) {
		Result result = cypherQuery(
				"START n = node(" + from + ") " +
				"MATCH n-[r:CIRCLE]->m " +
				"RETURN DISTINCT m." + Neo4JInserter.IDENTIFIER + " as circles");
		
		List<String> circles = new ArrayList<String>();
		
		for (Iterator<String> it = result.columnAs("circles"); it.hasNext();) {
			String c = it.next();
			if (c != null) circles.add(c);
		}
		
		return circles;
	}
	
	public List<List<Node>> cypherAllShortestPathsDistinctNodes(long from, long to, String relationship, int length) {
		Result result = cypherQuery( 
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
			
			/*
			if (getRelationships(
					n1.getId(), n2.getId(), 
					RelationshipTypes.CIRCLE.name(), 
					Direction.OUTGOING).size() > 0) {
				count++;
			}
			*/
			
			n1 = n2;
		}
		return count;
	}
	
	/*
	public String countCirclesInAllShortestPaths(long from, long to) {
		Node startNode = db.getNodeById(from);
		Node endNode = db.getNodeById(to);
		
		PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
		        Traversal.expanderForTypes(SocialCircleRelationship.FRIEND.type(), Direction.OUTGOING ), 15 );
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
				for (Iterator<Relationship> rels = n1.getRelationships(SocialCircleRelationship.CIRCLE.type(), Direction.OUTGOING).iterator(); rels.hasNext(); ) {
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
	*/
	
	public List<Relationship> cypherRelationshipsFrom(long from, String relationship, Direction dir) {
		
		String direction = "-";
		if (dir == Direction.OUTGOING) direction = "->";
		
		Result result = cypherQuery(
				"START n = node(" + from + ") " +
				"MATCH n-[r?:" + relationship + "]" + direction + "m " + 
				"RETURN r;");
		
		List<Relationship> rels = new ArrayList<Relationship>();
		
		for (Iterator<Relationship> it = result.columnAs("r"); it.hasNext();) {
			Relationship r = it.next();
			if (r != null) rels.add(r);
		}
		
		return rels;
	}
	
	public List<Relationship> cypherRelationshipsTo(long to, String relationship, Direction dir) {
		
		String direction = "-";
		if (dir == Direction.OUTGOING) direction = "->";
		
		Result result = cypherQuery(
				"START m = node(" + to + ") " +
				"MATCH n-[r?:" + relationship + "]" + direction + "m " + 
				"RETURN r;");
		
		List<Relationship> rels = new ArrayList<Relationship>();
		
		for (Iterator<Relationship> it = result.columnAs("r"); it.hasNext();) {
			Relationship r = it.next();
			if (r != null) rels.add(r);
		}
		
		return rels;
	}
	
	public List<Relationship> cypherRelationships(long from, long to, String relationship, Direction dir) {
		
		String direction = "-";
		if (dir == Direction.OUTGOING) direction = "->";
		
		Result result = cypherQuery(
				"START n = node(" + from + "), m = node(" + to + ") " +
				"MATCH n-[r?:" + relationship + "]" + direction + "m " + 
				"RETURN r;");
		
		List<Relationship> rels = new ArrayList<Relationship>();
		
		for (Iterator<Relationship> it = result.columnAs("r"); it.hasNext();) {
			Relationship r = it.next();
			if (r != null) rels.add(r);
		}
		
		return rels;
	}
	
	public Long[] cypherAllNodes() {
		Result result = cypherQuery("START n = node(*) RETURN ID(n) as id ORDER BY id;");
		
		List<Object> list = getCypherQueryResult(result, "id");

		Long[] ids = new Long[list.size()];
		for (int i = 0; i < list.size(); i++) {
			ids[i] = (Long) list.get(i);
			// System.out.println(ids[i]);
		}
		
		return ids;
	}
	
	public void analyzeNumberOfCircleRelationshipsInFriendPaths() {
		// See how many circles in friend shortest paths
		BufferedWriter writer = null;
		// from, to, friend-length, # friend links, # circle links, circle-length
		try {
			writer = new BufferedWriter(new FileWriter("facebook-circle-in-friendship.log"));
			Long[] nodes = cypherAllNodes();
			
			// for sampling
			int limit = 5;
			int count = 0;
			
			for (long n1 : nodes) {
				count++;
				System.out.println(count);
				if (count >= limit) break;
				
				for (long n2 : nodes) {
					if (n1 == n2) continue;
					// String str = countCirclesInAllShortestPaths(n1, n2);
					List<List<Node>> paths = 
							cypherAllShortestPathsDistinctNodes(n1, n2, RelationshipTypes.FRIEND.name(), 15);
					
					StringBuilder str = new StringBuilder();
					
					int numberOfPaths = paths.size();
					int length = 0;
					int numberOfCirclePaths = 0;
					
					for (int i = 0; i < numberOfPaths; i++) {
						List<Node> path = paths.get(i);
						length = path.size() - 1;
						numberOfCirclePaths += countCirclePathInPaths(path);
					}
					
					writer.write(str
							.append(n1).append(",")
							.append(n2).append(",")
							.append(length).append(",")
							.append(numberOfPaths).append(",")
							.append(length * numberOfPaths).append(",")
							.append(numberOfCirclePaths).toString());
							//.append(shortestPathLength(n1, n2, RelationshipTypes.CIRCLE.name(), Direction.OUTGOING, 15)).toString());
					writer.newLine();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != writer) {
				try { writer.close(); } catch (IOException ignored) {}
			}
		}
		
	}
	
	public static boolean hasRelationship(Node start, Node end, RelationshipType type) {
		for (Relationship rel : start.getRelationships(Direction.OUTGOING, type)) {
			if (rel.getEndNode().getId() == end.getId()) {
				return true;
			}
		}
		
		return false;
	}
	
	public static int getDegree(Node n, RelationshipType type, Direction direction) {
		int count = 0;
		for (@SuppressWarnings("unused") Relationship rel : n.getRelationships(direction, type)) {
			count++;
		}
		
		return count;
	}
	
	private static final Label[] EMPTY_LABEL_ARRAY = new Label[0]; 
	public static Label[] addLabel(Iterable<Label> currentLabels, Label newLabel) {
		List<Label> labels = Lists.newArrayList(currentLabels);
		labels.add(newLabel);
		return labels.toArray(EMPTY_LABEL_ARRAY);
	}
	
}
