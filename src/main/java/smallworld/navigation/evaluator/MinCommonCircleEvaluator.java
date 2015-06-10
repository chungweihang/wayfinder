package smallworld.navigation.evaluator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import smallworld.data.query.QueryCircles;

public class MinCommonCircleEvaluator implements Evaluator<Integer> {
	
	public MinCommonCircleEvaluator() {}
	
	public MinCommonCircleEvaluator(MinCommonCircleEvaluator another) {}
	
	@Override
	public Evaluator<Integer> copy() {
		return new MinCommonCircleEvaluator((MinCommonCircleEvaluator) this);
	}

	@Override
	public Integer getCost(Path path, Node target) {
		return getCost(path.endNode(), target);
	}

	@Override
	public Integer getCost(Node end, Node target) {
		return QueryCircles.getInstance().getMinCommonCircle(end, target);
	}

}
