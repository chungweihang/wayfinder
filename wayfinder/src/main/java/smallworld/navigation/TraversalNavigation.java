package smallworld.navigation;

import static org.neo4j.graphdb.traversal.Evaluators.includeWhereEndNodeIs;
import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;
import static org.neo4j.kernel.Traversal.traversal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.BestFirstSelectorFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import smallworld.data.RelationshipTypes;
import smallworld.data.query.Query;
import smallworld.navigation.evaluator.DegreeEvaluator;
import smallworld.navigation.evaluator.Evaluator;

import com.google.common.collect.Lists;

public class TraversalNavigation implements PathFinder<Path> {

	private final TraversalDescription traversalDescription;
	private Traverser lastTraverser;
	private final Evaluator<Integer> evaluator;
	
	public TraversalNavigation(PathExpander<?> expander,
			Evaluator<Integer> evaluator) {
		//this.costEvaluator = costEvaluator;
		//this.estimateEvaluator = estimateEvaluator;
		this.evaluator = evaluator;
		this.traversalDescription = traversal().uniqueness(Uniqueness.NONE).expand(expander);
	}

	@Override
	public Iterable<Path> findAllPaths(Node source, Node sink) {
		Path path = findSinglePath(source, sink);
		return path != null ? Arrays.asList(path) : Collections.<Path>emptyList();
	}

	@Override
	public Path findSinglePath(Node start, Node end) {
		return firstOrNull(findPaths(start, end));
	}

	private Iterable<Path> findPaths(Node start, Node end) {
		lastTraverser = traversalDescription
				.order(new SelectorFactory(end))
				.evaluator(includeWhereEndNodeIs(end)).traverse(start);
		return Lists.newArrayList(lastTraverser.iterator());
	}

	@Override
	public TraversalMetadata metadata() {
		return lastTraverser.metadata();
	}

	private static class PositionData implements Comparable<PositionData> {
		private final double cost;
		
		public PositionData(double cost) {
			this.cost = cost;
		}

		@Override
		public int compareTo(PositionData o) {
			return Double.compare(this.cost, o.cost);
		}

		@Override
		public String toString() {
			return "cost: " + cost;
		}
	}

	private class SelectorFactory extends
			BestFirstSelectorFactory<PositionData, Double> {
		private final Node end;

		SelectorFactory(Node end) {
			super(BestFirstSelectorFactory.pathInterest(true, true));
			this.end = end;
		}

		@Override
		protected PositionData addPriority(TraversalBranch source,
				PositionData currentAggregatedValue, Double value) {
			// not using current aggregated value
			return new PositionData(evaluator.getCost(source.endNode(), end));
		}

		@Override
		protected Double calculateValue(TraversalBranch next) {
			return next.length() == 0 ? 0d : evaluator.getCost(next.endNode(), end);
		}

		@Override
		protected PositionData getStartData() {
			return new PositionData(0);
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		//String dataset = "twitter"; Direction direction = Direction.OUTGOING;
		//String dataset = "gplus"; Direction direction = Direction.OUTGOING;
		String dataset = "dblp-inproceedings"; Direction direction = Direction.BOTH;
		//String dataset = "simple"; Direction direction = Direction.BOTH;
				
		RelationshipType type = RelationshipTypes.FRIEND.type();
		
		PathFinder<Path> nav = new TraversalNavigation(PathExpanders.forTypeAndDirection(type, direction), new DegreeEvaluator(type, direction));
		//new ConcurrentMain(nav, "neo4j/" + dataset, 1, 1, "temp.shortest.log");
		
		Query q = new Query("neo4j/" + dataset);
		System.err.println(nav.findSinglePath(q.getNode(373654), q.getNode(1557)));
		//System.err.println(nav.findSinglePath(q.getNode(1), q.getNode(7)));
		
	}
}