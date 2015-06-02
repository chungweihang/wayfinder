package smallworld.navigation;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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

import com.google.common.collect.Lists;

import smallworld.ConcurrentMain;
import smallworld.Constants;
import smallworld.data.RelationshipTypes;
import smallworld.navigation.feature.DistanceMeasure;
import smallworld.navigation.feature.Feature;
import smallworld.navigation.feature.FeatureBuilder;
import smallworld.util.Utils;
import weka.core.converters.ArffSaver;

/**
 * Use ONLY for generating training samples for binary classification.
 * 
 * @author chang
 *
 */
@SuppressWarnings("unused")
public class TrainingNavigation extends AbstractNavigation {

	private static final boolean CLASS_BALANCING = true;
	
	private final PathExpander<?> expander;
	
	private final FeatureBuilder features;
	
	public TrainingNavigation(PathExpander<?> expander, FeatureBuilder features) {
		this.expander = expander;
		this.features = features;
	}

	public TrainingNavigation(TrainingNavigation another) {
		this(another.expander, another.features);
	}

	public Path findSinglePath(Node source, Node sink) {

		if (null == source || null == sink) return null;
		
		//currentSink = sink;
		lastMetadata = new Metadata();
		Doer doer = new Doer(source, sink);
		while (doer.hasNext()) {
			Path p = doer.next();
			
			lastMetadata.totalNodesExplored++;
			
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
		private final Deque<Path> queue = new ArrayDeque<Path>();
		
		Doer(Node start, Node end) {
			this.start = start;
			this.end = end;

			queue.push(Utils.toPath(start, null)); // stack
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
			
			currentPath = queue.pop(); // stack
			
			Node node = currentPath.endNode();
			visitedNodes.add(node.getId());
			this.lastNode = node;
			
			return currentPath;
		}
		
		@SuppressWarnings("unchecked")
		private void expand() {
		
			if (currentPath.length() >= Constants.LIMIT_OF_DEPTH) {
				//System.err.println(start + " => " + currentPath.endNode() + " => " + end + " length exceeds " + currentPath.length() + "! Stop expanding.");
				return;
			}
			
			visitedNodes.add(currentPath.endNode().getId());
			
			// Construct a list of relationships in the current path
			List<Relationship> rels = Lists.newArrayList(currentPath.relationships());
						
			List<Path> pathsToAdd = new ArrayList<Path>();
			List<Path> negativePaths = new ArrayList<Path>();
			int instanceToAdd = 0;
			
			// The last feature---the class label
			Feature<Double> feature = features.features().get(features.features().size() - 1);
			
			for (Relationship rel : expander.expand(this, BranchState.NO_STATE)) {
				lastMetadata.rels++;
				Node node = rel.getOtherNode(this.lastNode);
				if (visitedNodes.contains(node.getId())) {
					continue;
				}

				if (!queueNodes.contains(node.getId())) {
					rels.add(rel);
					Path path = Utils.toPath(start, rels);
					
					queueNodes.add(node.getId());
					pathsToAdd.add(path);
				
					if (node.equals(end)) {
						queue.push(path);
						return;
					}
					
					// check if this path shortens the distance
					// if add positive instance, or keep the negative instance
					// negative instances will be added when there is positive ones found
					double label = feature.getFeature(path, end);
					if (label == 1d) {
						instanceToAdd ++;
						features.getTrainingInstance(path, end);
					} else {
						negativePaths.add(path);
					}
					
					// add negative instances if there are enough number of positive instances
					while (instanceToAdd > 0 && !negativePaths.isEmpty()) {
						features.getTrainingInstance(negativePaths.remove(negativePaths.size() - 1), end);
						instanceToAdd--;
					}
					
					rels.remove(rels.size() - 1);
				}
				
			}
		
		
			// randomly choose a neighbor
			Collections.shuffle(pathsToAdd, ThreadLocalRandom.current());
			for (Path p : pathsToAdd) {
				queue.push(p);
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
	
	/*
	public static void main(String[] args) throws IOException {
		
		//String dataset = "twitter"; Direction direction = Direction.OUTGOING;
		//String dataset = "gplus"; Direction direction = Direction.OUTGOING;
		String dataset = "facebook"; Direction direction = Direction.BOTH;
		
		RelationshipType type = RelationshipTypes.FRIEND.type();
		//int depth = 20;
		
		DistanceMeasure distanceFeature = new DistanceMeasure(dataset, type, direction);
		AbstractNavigation.setDistanceMeasure(distanceFeature);
		
		FeatureBuilder features = new FeatureBuilder()
								.addFeature(FeatureBuilder.getCommonCirclesWithParent(dataset))
								.addFeature(FeatureBuilder.getCommonCirclesWithTarget(dataset))
								.addFeature(FeatureBuilder.getCommonCirclesBetweenParentTarget(dataset))
								.addFeature(FeatureBuilder.getMinCommonCircleWithTarget(dataset))
								.addFeature(FeatureBuilder.getMaxCommonCircleWithTarget(dataset))
								.addFeature(FeatureBuilder.getEndNodeDegreeFeature(dataset, type, direction))
								.addFeature(FeatureBuilder.getParentNodeDegreeFeature(dataset, type, direction))
								.addFeature(FeatureBuilder.getPathLengthFeature(dataset))
								.addClassFeature(distanceFeature);
		
		PathFinder<Path> nav = new MachineLearningNavigation(Traversal.pathExpanderForTypes(type, direction), features);
		new ConcurrentMain(nav, "neo4j/" + dataset, 10, new PrintStream(dataset + ".training.100.log"));
		//Main.run(nav, "neo4j/" + dataset, 10, new PrintStream("/dev/null"));
		
		ArffSaver saver = new ArffSaver();
		saver.setFile(new File(dataset + ".arff"));
		saver.setInstances(features.getInstances());
		saver.writeBatch();
		
		distanceFeature.saveCache();
	}
	*/
}
