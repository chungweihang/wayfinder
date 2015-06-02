package smallworld.navigation.evaluator;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class DegreeEvaluator implements Evaluator<Integer> {
	
	private final RelationshipType type;
	private final Direction direction;
	
	public DegreeEvaluator(RelationshipType type, Direction direction) {
		this.type = type;
		this.direction = direction;
	}

	public DegreeEvaluator(DegreeEvaluator another) {
		this.type = another.type;
		this.direction = another.direction;
	}
	
	@Override
	public Evaluator<Integer> copy() {
		return new DegreeEvaluator(this);
	}

	@Override
	public Integer getCost(Path path, Node target) {
		return getCost(path.endNode(), target);
	}

	@Override
	public Integer getCost(Node end, Node target) {
		Iterator<Relationship> it = end.getRelationships(
				direction,
				type).iterator();
		int count = 0;
		while (it.hasNext()) {
			count++;
			it.next();
		}
		
		return 0-count;
	}

}
