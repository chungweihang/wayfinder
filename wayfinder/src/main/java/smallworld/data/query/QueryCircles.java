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
	
	public Iterable<Label> getCircles(long ego) {
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
	
	public static Iterable<Label> getCircles(Node n) {
		//return n.getPropertyKeys();
		return n.getLabels();
	}
	
	public Long getMinCommonCircle(Node m, Node n) {
		Set<Label> circles = getCommonCircles(m, n);
		
		Long min = Long.MAX_VALUE;
		
		for (Label circle : circles) {
			Long size = sizeOfCircle(circle.name());
			if (min > size) {
				min = size;
			}
		}
	
		return min;
	}
	
	public Long getMaxCommonCircle(Node m, Node n) {
		Set<Label> circles = getCommonCircles(m, n);
		
		Long max = 0l;
		
		for (Label circle : circles) {
			Long size = sizeOfCircle(circle.name());
			if (max < size) {
				max = size;
			}
		}
		
		return max;
	}
	
	public static Set<Label> getCommonCircles(Node m, Node n) {
		Set<Label> commons = new HashSet<Label>();
		for (Iterator<Label> circles = getCircles(m).iterator(); circles.hasNext(); ) {
			Label circle = circles.next();
			if (n.hasLabel(circle)) {
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
