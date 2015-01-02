package smallworld.navigation;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.Traversal;

import smallworld.Constants;
import smallworld.Main;
import smallworld.data.RelationshipTypes;
import smallworld.navigation.evaluator.SARSAEvaluator;
import smallworld.navigation.feature.DistanceMeasure;
import smallworld.navigation.feature.FeatureBuilder;

/**
 * Use ONLY for learning SARSA parameters
 * 
 * @author chang
 *
 */
@SuppressWarnings("deprecation")
public class SARSANavigation extends AbstractNavigation implements Comparator<Path> {

	private final PathExpander<?> expander;
	private Node currentSink = null;
	
	private final SARSAEvaluator evaluator;
	
	/*
	public SARSANavigation(PathExpander<?> expander, FeatureBuilder features) {
		this(expander, new SARSAEvaluator(features));
	}
	*/
	private SARSANavigation(PathExpander<?> expander, SARSAEvaluator evaluator) {
		this.expander = expander;
		//this.features = new FeatureBuilder(features);
		this.evaluator = evaluator;
	}

	public SARSANavigation(SARSANavigation another) {
		this(another.expander, another.evaluator);
	}

	public Path findSinglePath(Node source, Node sink) {

		if (null == source || null == sink) return null;
		
		currentSink = sink;
		lastMetadata = new Metadata();
		Doer doer = new Doer(source, sink);
		while (doer.hasNext()) {
			Path p = doer.next();
			
			//lastMetadata.correctedNodesExplored += super.getDistanceMeasure().getFeature(p, sink) == 1 ? 1 : 0; 
			lastMetadata.totalNodesExplored++;
			//AbstractNavigation.evaluate(p, sink);
			//this.evaluateVisitedNode(p, sink);
			
			if (p.endNode().equals(sink)) {
				lastMetadata.paths++;
				return p;
			}
			
			if (lastMetadata.totalNodesExplored > Constants.LIMIT_OF_NODES_EXPLORED) break;
		}
		return null;
	}

	private class Doer extends PrefetchingIterator<Path> implements Path {
		private final Node start;
		private final Node end;
		private Node lastNode;
		private boolean expand;
		private final Set<Long> visitedNodes = new HashSet<Long>();
		private final Set<Long> queueNodes = new HashSet<Long>();
		private Path currentPath = null;
		//private final Map<Long, Long> cameFrom = new HashMap<Long, Long>();
		private final Deque<Path> queue = new ArrayDeque<Path>();
		
		Doer(Node start, Node end) {
			this.start = start;
			this.end = end;

			//queue.add(toPath(start, null)); // queue
			queue.push(toPath(start, null)); // stack
			queueNodes.add(start.getId());
		}

		@Override
		protected Path fetchNextOrNull() {
			// FIXME
			if (!this.expand) {
				this.expand = true;
			} else {
				expand();
			}

			if (queue.isEmpty()) return null;
			
			//currentPath = queue.poll(); // queue
			currentPath = queue.pop(); // stack
			Node node = currentPath.endNode();
			visitedNodes.add(node.getId());
			this.lastNode = node;
			
			SARSANavigation.this.evaluateVisitedNode(currentPath, end);
			
			return currentPath;
		}

		@SuppressWarnings("unchecked")
		private void expand() {
			// Construct a list of relationships in the current path
			List<Relationship> rels = new ArrayList<Relationship>();
			for (Relationship rel : currentPath.relationships()) {
				rels.add(rel);
			}
			
			List<Path> pathsToAdd = new ArrayList<Path>();
			
			for (Relationship rel : expander.expand(this, BranchState.NO_STATE)) {
				lastMetadata.rels++;
				Node node = rel.getOtherNode(this.lastNode);
				if (visitedNodes.contains(node.getId())) {
					continue;
				}

				rels.add(rel);
				Path path = toPath(start, rels);
				
				if (!queueNodes.contains(node.getId())) {
					queueNodes.add(node.getId());
					pathsToAdd.add(path);
					
					evaluator.update(path, end);
					
					if (node.equals(end)) {
						queue.push(path);
						break;
					}
					
				}
				
				rels.remove(rels.size() - 1);
			}
		
			Path[] paths = pathsToAdd.toArray(new Path[0]);
			Arrays.sort(paths, SARSANavigation.this);
			
			for (int i = paths.length - 1; i >= 0; i--) {
				queue.push(paths[i]);
			}
		}

		@Override
		public Node startNode() {
			return start;
		}

		@Override
		public Node endNode() {
			return lastNode;
		}

		@Override
		public Relationship lastRelationship() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<Relationship> relationships() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<Relationship> reverseRelationships() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<Node> nodes() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<Node> reverseNodes() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int length() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<PropertyContainer> iterator() {
			throw new UnsupportedOperationException();
		}

	}
	
	@Override
	public int compare(Path p1, Path p2) {
		return (int) Math.round((evaluator.getCost(p1, currentSink) - evaluator.getCost(p2, currentSink)) * 1000d);
	}
	
	public static void main(String[] args) throws IOException {
		
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		String dataset = "facebook";
		
		RelationshipType type = RelationshipTypes.FRIEND.type();
		Direction direction = Direction.BOTH;
		//int depth = 20;
		
		DistanceMeasure distanceFeature = new DistanceMeasure(dataset, type, direction);
		FeatureBuilder.setScaling(true);
		FeatureBuilder features = new FeatureBuilder()
								.addFeature(FeatureBuilder.getCommonCirclesWithParent(dataset))
								.addFeature(FeatureBuilder.getCommonCirclesWithTarget(dataset))
								.addFeature(FeatureBuilder.getCommonCirclesBetweenParentTarget(dataset))
								.addFeature(FeatureBuilder.getEndNodeDegreeFeature(dataset, type, direction))
								.addFeature(FeatureBuilder.getParentNodeDegreeFeature(dataset, type, direction))
								.addFeature(FeatureBuilder.getPathLengthFeature(dataset));
								//.addFeature(Features.getDistanceFromEndNode(type, direction, depth))
								//.addFeature(Features.getDistanceFromParentNode(type, direction, depth));
								//.addFeature(FeatureBuilder.getDistanceLabel(type, direction, depth));
		
		SARSAEvaluator evaluator = new SARSAEvaluator(features);
		
		PathFinder<Path> nav = new SARSANavigation(Traversal.pathExpanderForTypes(type, direction), evaluator);
		
		AbstractNavigation.setDistanceMeasure(distanceFeature);

		Main.run(nav, "neo4j/" + dataset, 10, new PrintStream("/dev/null"));
		
		evaluator.saveParameters(dataset + ".sarsa");
				
		distanceFeature.saveCache();
	}

}
