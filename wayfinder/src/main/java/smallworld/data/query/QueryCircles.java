package smallworld.data.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class QueryCircles {
	
	private Query query;
	
	public QueryCircles(Query query) {
		this.query = query;
	}
	
	public QueryCircles(String path) {
		this(new Query(path));
	}
	
	public Iterable<String> getCircles(long ego) {
		return getCircles(query.getNode(ego));
	}
	
	public List<Long> getStartNodes(List<Relationship> rels) {
		List<Long> nodes = new ArrayList<Long>(rels.size());
		
		for (int i = 0; i < rels.size(); i++) {
			nodes.add(rels.get(i).getStartNode().getId());
		}
		
		return nodes;
	}
	
	public List<Long> getEndNodes(List<Relationship> rels) {
		List<Long> nodes = new ArrayList<Long>(rels.size());
		
		for (int i = 0; i < rels.size(); i++) {
			nodes.add(rels.get(i).getEndNode().getId());
		}
		
		return nodes;
	}
	
	public static Iterable<String> getCircles(Node n) {
		return n.getPropertyKeys();
	}
	
	public static Iterable<Label> getCircleLabels(Node n) {
		return n.getLabels();
	}
	
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
	
	public Long sizeOfCircle(String circle) {
		ExecutionResult result = query.cypherQuery("MATCH (n:`" + circle + "`) RETURN COUNT(n) AS size");
		for (Iterator<Long> it = result.columnAs("size"); it.hasNext(); ) {
			Long size = it.next();
			if (size != null) return size;
		}
		
		return 0l;
	}
	
	public void shutdown() {
		query.shutdown();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query q = new Query("neo4j/facebook");
		QueryCircles qc = new QueryCircles(q);
		/*
		Long[] nodes = q.allNodes();
		int count = 0;
		int total = 0;
		for (int i = 0; i < nodes.length; i++) {
			Iterator<String> circles = qc.getCircles(nodes[i]).iterator();
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
		
		q.shutdown();
	}

}
