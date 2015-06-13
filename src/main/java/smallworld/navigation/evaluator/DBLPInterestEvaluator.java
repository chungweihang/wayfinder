package smallworld.navigation.evaluator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import smallworld.data.inserter.exp.Interests;

public class DBLPInterestEvaluator implements Evaluator<Integer> {
	
	public DBLPInterestEvaluator() {
	}

	public DBLPInterestEvaluator(DBLPInterestEvaluator another) {
	}
	
	@Override
	public Evaluator<Integer> copy() {
		return new DBLPInterestEvaluator(this);
	}

	@Override
	public Integer getCost(Path path, Node target) {
		return getCost(path.endNode(), target);
	}

	@Override
	public Integer getCost(Node end, Node target) {
		return (int) Math.round(100 * Interests.proximity(end, target));
	}

}
