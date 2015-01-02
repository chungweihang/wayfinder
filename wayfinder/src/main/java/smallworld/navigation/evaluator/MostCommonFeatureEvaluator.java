package smallworld.navigation.evaluator;

import java.util.Iterator;
import java.util.Properties;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import smallworld.data.query.QueryFeatures;

public class MostCommonFeatureEvaluator implements Evaluator<Integer> {

	@Override
	public Evaluator<Integer> copy() {
		return new MostCommonFeatureEvaluator();
	}

	@Override
	public Integer getCost(Path path, Node target) {
		return getCost(path.endNode(), target);
	}

	@Override
	public Integer getCost(Node end, Node target) {
		int count = 0;
		Properties properties1 = QueryFeatures.getFeatures(end); //properties1.list(System.out);
		Properties properties2 = QueryFeatures.getFeatures(target); //properties2.list(System.out);
		for (Iterator<String> it = properties1.stringPropertyNames().iterator(); it.hasNext(); ) {
			String key = it.next();
			if (properties2.containsKey(key) && properties2.getProperty(key).equals(properties1.getProperty(key)))
				count++;
		}
		
		return 0-count;
	}

}
