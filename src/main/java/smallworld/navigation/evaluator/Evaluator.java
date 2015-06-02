package smallworld.navigation.evaluator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

public interface Evaluator<T> {
	Evaluator<T> copy();
	T getCost(Path path, Node target);
	T getCost(Node end, Node target);
}
