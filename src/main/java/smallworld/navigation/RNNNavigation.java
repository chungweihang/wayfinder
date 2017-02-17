package smallworld.navigation;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import smallworld.ConcurrentMain;
import smallworld.Constants;
import smallworld.data.RelationshipTypes;
import smallworld.navigation.feature.DistanceMeasure;
import smallworld.util.Utils;

import com.google.common.collect.Lists;

/**
 * Use ONLY for generating training samples for binary classification.
 * 
 * @author chang
 *
 */
@SuppressWarnings("unused")
public class RNNNavigation extends AbstractNavigation {

	private static final boolean CLASS_BALANCING = true;
	
	private final PathExpander<?> expander;
	private final DistanceMeasure measure;
	
	private static final Logger logger = LogManager.getLogger("RNNTraining");
	//private Logger logger = Logger.getLogger();
	static final String EOL = System.lineSeparator();
	
	public RNNNavigation(PathExpander<?> expander, DistanceMeasure measure) {
		this.expander = expander;
		this.measure = measure;
	}

	public RNNNavigation(RNNNavigation another) {
		this(another.expander, another.measure);
	}

	@Override
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
					double label = measure.getFeature(path, end);
					logger.info(node + ":" + label + " ");
					
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
	
	public static void main(String[] args) throws IOException {
		
		//String dataset = "twitter"; Direction direction = Direction.OUTGOING;
		//String dataset = "gplus"; Direction direction = Direction.OUTGOING;
		String dataset = "facebook-exp"; Direction direction = Direction.BOTH;
		
		RelationshipType type = RelationshipTypes.FRIEND.type();
		//int depth = 20;
		
		DistanceMeasure distanceFeature = new DistanceMeasure(dataset, type, direction);
		//AbstractNavigation.setDistanceMeasure(distanceFeature);
		
		PathFinder<Path> nav = new RNNNavigation(Traversal.pathExpanderForTypes(type, direction), distanceFeature);
		new ConcurrentMain(nav, "neo4j/" + dataset, 100, 0, dataset + ".rnn.100.log");
		//Main.run(nav, "neo4j/" + dataset, 10, new PrintStream("/dev/null"));
		
		//distanceFeature.saveCache();
	}
}
