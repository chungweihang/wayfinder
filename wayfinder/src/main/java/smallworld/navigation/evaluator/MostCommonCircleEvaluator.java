package smallworld.navigation.evaluator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import smallworld.data.query.QueryCircles;

public class MostCommonCircleEvaluator implements Evaluator<Integer> {

	@Override
	public Evaluator<Integer> copy() {
		return new MostCommonCircleEvaluator();
	}

	@Override
	public Integer getCost(Path path, Node target) {
		return getCost(path.endNode(), target);
	}

	@Override
	public Integer getCost(Node end, Node target) {
		int count = 0;
		//count = QueryCircles.getCommonCircles(target, end).size();
		count = QueryCircles.getInstance().getCommonCircleLabels(target, end).size();
		
		return 0-count;
	}

}
