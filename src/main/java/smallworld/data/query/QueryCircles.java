package smallworld.data.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import smallworld.data.RelationshipTypes;
import smallworld.data.inserter.exp.Neo4JInserter;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class QueryCircles {
	
	private Query query;
	
	private static QueryCircles INSTANCE = null;
	
	public static synchronized QueryCircles getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new QueryCircles(Query.getInstance());
		}
		return INSTANCE;
	}
	
	final TraversalDescription getCirclesTD;
	
	public QueryCircles(Query query) {
		this.query = query;
		
		getCirclesTD = query.getGraphDatabaseService().traversalDescription()
				.relationships(RelationshipTypes.CIRCLE.type(), Direction.OUTGOING)
				.evaluator(Evaluators.fromDepth(1))                     
			    .evaluator(Evaluators.toDepth(1))
			    .evaluator(Evaluators.excludeStartPosition());
	}
	
	public QueryCircles(String path) {
		this(new Query(path));
	}
	
	public Set<Node> getCircles(Node person) {
		Set<Node> circles = new HashSet<>();
		try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
			for (final Path position : getCirclesTD.traverse(person)) {
				circles.add(position.endNode());
			}
		}

		return circles;
	}
	
	public ImmutableSet<Node> getCommonCircles(Node personA, Node personB) {
		return Sets.intersection(getCircles(personA), getCircles(personB)).immutableCopy();
	}
	
	public int getCircleSize(Node circle) {
		return circle.getDegree(RelationshipTypes.CIRCLE.type(), Direction.INCOMING);
	}
	
	public int getMinCommonCircle(Node personA, Node personB) {
		ImmutableSet<Node> commonCircles = getCommonCircles(personA, personB);
		
		int min = Integer.MAX_VALUE;
		
		for (Node circle : commonCircles) {
			int size = getCircleSize(circle);
			if (min > size) {
				min = size;
			}
		}
		
		return min;
	}
	
	public int getMaxCommonCircle(Node personA, Node personB) {
		ImmutableSet<Node> commonCircles = getCommonCircles(personA, personB);
		
		int max = 0;
		
		for (Node circle : commonCircles) {
			int size = getCircleSize(circle);
			if (max < size) {
				max = size;
			}
		}
		
		return max;
	}
	
	public List<Relationship> cypherGetCircles(long from, String circle) {
		Result result = query.cypherQuery(
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
	
	public List<String> cypherGetCircleNames(long from) {
		Result result = query.cypherQuery(
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
	
	public void shutdown() {
		query.shutdown();
	}
	
}
