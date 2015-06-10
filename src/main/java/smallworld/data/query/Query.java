package smallworld.data.query;

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

import com.google.common.collect.Lists;

public class Query {
	
	private GraphDatabaseService db;
	
	private static Query INSTANCE = null;
	
	public static synchronized Query getInstance() {
		if (INSTANCE == null) INSTANCE = new Query(Constants.NEO4J_PATH);
		return INSTANCE;
	}
	
	public Query(String path) {
		db = new GraphDatabaseFactory().newEmbeddedDatabase(path);
	}

	public GraphDatabaseService getGraphDatabaseService() {
		return db;
	}

	public Result cypherQuery(String query) {
		return db.execute(query);
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
	
	/*
	public List<Relationship> cypherGetRelationships(long from, long to, String relationship, Direction dir) {
		
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
	*/
	public Long[] cypherGetAllNodes() {
		Result result = cypherQuery("START n = node(*) RETURN ID(n) as id ORDER BY id;");
		
		List<Object> list = getCypherQueryResult(result, "id");

		Long[] ids = new Long[list.size()];
		for (int i = 0; i < list.size(); i++) {
			ids[i] = (Long) list.get(i);
			// System.out.println(ids[i]);
		}
		
		return ids;
	}
	
	public static boolean hasRelationship(Node start, Node end, RelationshipType type) {
		for (Relationship rel : start.getRelationships(Direction.OUTGOING, type)) {
			if (rel.getEndNode().getId() == end.getId()) {
				return true;
			}
		}
		
		return false;
	}
	
	@Deprecated
	private static final Label[] EMPTY_LABEL_ARRAY = new Label[0]; 
	@Deprecated
	public static Label[] addLabel(Iterable<Label> currentLabels, Label newLabel) {
		List<Label> labels = Lists.newArrayList(currentLabels);
		labels.add(newLabel);
		return labels.toArray(EMPTY_LABEL_ARRAY);
	}
	
}
