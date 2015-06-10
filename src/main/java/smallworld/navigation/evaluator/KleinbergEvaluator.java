package smallworld.navigation.evaluator;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import smallworld.data.query.QueryCircles;

import com.google.common.collect.ImmutableSet;

public class KleinbergEvaluator implements Evaluator<Integer> {
	
	private final RelationshipType type;
	private final Direction direction;

	public KleinbergEvaluator(RelationshipType type, Direction direction) {
		this.type = type;
		this.direction = direction;
	}
	
	public KleinbergEvaluator(KleinbergEvaluator another) {
		this.type = another.type;
		this.direction = another.direction;
	}
	
	@Override
	public Evaluator<Integer> copy() {
		return new KleinbergEvaluator((KleinbergEvaluator) this);
	}

	@Override
	public Integer getCost(Path path, Node target) {
		return getCost(path.endNode(), target);
	}


	@Override
	public Integer getCost(Node end, Node target) {
		// Node previous = path.lastRelationship().getOtherNode(current);
		
		//Set<String> previousCommonCircles = QueryCircles.getCommonCircles(previous, target);
		ImmutableSet<Node> currentCommonCircles = QueryCircles.getInstance().getCommonCircles(end, target);
		//Set<Label> currentCommonCircles = QueryCircles.getInstance().getCommonCircleLabels(end, target);
		
		/*
		if (previousCommonCircles.size() >= 0) { // previous is in target's circles
			previousCommonCircles.retainAll(currentCommonCircles);
			if (previousCommonCircles.size() != 0)
				return 0 - previousCommonCircles.size();
			
		} 
		*/
		if (currentCommonCircles.size() > 0) { // current is in target's circles
			return 0-currentCommonCircles.size();
		} else {
			int degree = Integer.MAX_VALUE;
			for (Iterator<Relationship> it = 
					end.getRelationships(type, direction).iterator();
					it.hasNext(); ) {
				it.next();
				degree--;
			}
			
			return degree;
		}
	
	}

	/*
	public static void main(String[] args) {
		Query q = new Query("neo4j/simple");
		
		PrioritizedDFSNavigation nav = 
				new PrioritizedDFSNavigation(
						PathExpanders.forTypeAndDirection(
								RelationshipTypes.FRIEND.type(), 
								Direction.BOTH), 
							new KleinbergEvaluator(RelationshipTypes.FRIEND.type(), Direction.BOTH));
		
		Node source = q.getNode(7);
		Node target = q.getNode(4);
		System.out.println(source + " to " + target);
		Path p = nav.findSinglePath(source, target);
		System.out.println(p);
		q.shutdown();
	}
	*/
}
