package smallworld.data.query;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import smallworld.data.RelationshipTypes;

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
	
	public Set<Node> getCircles(long ego) {
		return getCircles(query.cypherGetNode(ego));
	}
	
	public Set<Node> getCircles(Node n) {
		Set<Node> circles = new HashSet<>();
		try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
			for (final Path position : getCirclesTD.traverse(n)) {
				      circles.add(position.endNode());
				}
		}
		
		return circles;
	}
	
	public ImmutableSet<Node> getCommonCircles(Node m, Node n) {
		return Sets.intersection(getCircles(m), getCircles(n)).immutableCopy();
	}
	
	/*
	public List<Long> getStartNodes(List<Relationship> rels) {
		List<Long> nodes = new ArrayList<Long>(rels.size());
		
		for (int i = 0; i < rels.size(); i++) {
			nodes.add(rels.get(i).getStartNode().getId());
		}
		
		return nodes;
	}
	*/
	/*
	public List<Long> getEndNodes(List<Relationship> rels) {
		List<Long> nodes = new ArrayList<Long>(rels.size());
		
		for (int i = 0; i < rels.size(); i++) {
			nodes.add(rels.get(i).getEndNode().getId());
		}
		
		return nodes;
	}
	*/
	/*
	public static Iterable<String> getCircles(Node n) {
		return n.getPropertyKeys();
	}
	*/
	/*
	public static int getMinCommonCircle(Node m, Node n) {
		Set<String> circles = getCommonCircles(m, n);
		
		int min = Integer.MAX_VALUE;
		
		for (String circle : circles) {
			int size = (int) n.getProperty(circle);
			if (min > size) {
				min = size;
			}
		}
	
		return min;
	}
	
	public static int getMaxCommonCircle(Node m, Node n) {
		Set<String> circles = getCommonCircles(m, n);
		
		int max = 0;
		
		for (String circle : circles) {
			int size = (int) n.getProperty(circle);
			if (max < size) {
				max = size;
			}
		}
		
		return max;
	}
	
	public static Set<String> getCommonCircles(Node m, Node n) {
		Set<String> commons = new HashSet<>();
		for (Iterator<String> circles = getCircles(m).iterator(); circles.hasNext(); ) {
			String circle = circles.next();
			if (n.hasProperty(circle)) {
				commons.add(circle);
			}
		}
		
		return commons;
	}
	*/
	
	/*
	public Iterable<Label> getCircleLabels(Node n) {
		try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
			return n.getLabels();
		}
	}
	*/
	/*
	public Set<Label> getCommonCircleLabels(Node m, Node n) {
		Set<Label> commons = new HashSet<>();
		try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
			for (Iterator<Label> circles = getCircleLabels(m).iterator(); circles.hasNext(); ) {
		
				Label circle = circles.next();
				if (n.hasLabel(circle)) {
					commons.add(circle);
				}
			}
		}
		return commons;
	}
	*/
	/*
	public Long sizeOfCircleLabel(Label circle) {
		Result result = query.cypherQuery("MATCH (n:`" + circle.name() + "`) RETURN COUNT(n) AS size");
		for (Iterator<Long> it = result.columnAs("size"); it.hasNext(); ) {
			Long size = it.next();
			if (size != null) return size;
		}
		
		return 0l;
	}
	*/
	/*
	public Long getMinCommonCircleLabel(Node m, Node n) {
		Set<Label> circles = getCommonCircleLabels(m, n);
		
		Long min = Long.MAX_VALUE;
		
		for (Label circle : circles) {
			Long size = sizeOfCircleLabel(circle);
			if (min > size) {
				min = size;
			}
		}
	
		return min;
	}
	*/
	/*
	public Long getMaxCommonCircleLabel(Node m, Node n) {
		Set<Label> circles = getCommonCircleLabels(m, n);
		
		Long max = 0l;
		
		for (Label circle : circles) {
			Long size = sizeOfCircleLabel(circle);
			if (max < size) {
				max = size;
			}
		}
		
		return max;
	}
	*/
	public void shutdown() {
		query.shutdown();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*
		Query q = new Query("neo4j/facebook");
		QueryCircles qc = new QueryCircles(q);
		
		Long[] nodes = q.cypherAllNodes();
		int count = 0;
		int total = 0;
		for (int i = 0; i < nodes.length; i++) {
			//Iterator<String> circles = qc.getCircles(nodes[i]).iterator();
			Iterator<Label> circles = qc.getCircleLabels(q.cypherGetNode(nodes[i])).iterator();
			if (circles.hasNext()) count++;
			while (circles.hasNext()) {
				circles.next();
				total ++; 
			}
		}
		System.out.println(count + " / " + nodes.length + " total: " + total);
		*/
		
		
		/*
		List<String> names = qc.getCircleNames(0);
		for (int i = 0; i < names.size(); i++) {
			System.out.println(names.get(i));
		}
		*/
		
		/*
		// List<Relationship> rels = q.belongToCircles(0);
		List<Relationship> rels = qc.getCircles(0, "circle15");
		List<String> circles = qc.getCircles(rels);
		List<Long> starts = qc.getStartNodes(rels);
		List<Long> ends = qc.getEndNodes(rels);
		for (int i = 0; i < circles.size(); i++) {
			System.out.println(starts.get(i) + " - " + circles.get(i) + " -> " + ends.get(i));
		}
		*/
		
		//q.shutdown();
	}

}
