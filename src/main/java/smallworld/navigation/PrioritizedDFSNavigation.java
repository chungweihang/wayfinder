package smallworld.navigation;

import java.io.IOException;
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
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.PrefetchingIterator;

import smallworld.Constants;
import smallworld.data.RelationshipTypes;
import smallworld.data.query.Query;
import smallworld.navigation.evaluator.DegreeEvaluator;
import smallworld.navigation.evaluator.Evaluator;
import smallworld.navigation.feature.DistanceMeasure;
import smallworld.util.Utils;

import com.google.common.collect.Lists;

/**
 * DFS Navigation but exploring neighbors of a node in a prioritized order.
 * 
 * The navigation follows DFS.  It uses a stack to keep unexplored nodes.
 * When exploring a node, it prioritizes its neighbors based on a {@code Evaluator}.
 * It first pushes the lowest value neighbor into the stack, and then the higher value 
 * neighbors such that the navigation will explore the highest value neighbor first.
 * 
 * Different from {@code PrioritizedNavigation}, {@code PrioritizedDFSNavigation}
 * prioritizes only the neighbors of a particular nodes.  Once these neighbors are 
 * pushed into the stack, the order of them remain unchanged.  
 * 
 * {@code PrioritizedNavigation} maintains a priority queue of all unexplored nodes.
 * It always navigates to the high value nodes among all in the priority queue.
 * 
 * @see AbstractNavigation
 * @see PrioritizedNavigation
 * @author chang
 *
 */
@Deprecated
public class PrioritizedDFSNavigation extends AbstractNavigation implements Comparator<Path> {

	private final PathExpander<?> expander;
	private final Evaluator<Integer> evaluator;
	private Node currentSink = null;

	/**
	 * Instantiate a navigation by specifying a {@code PathExpander} and a {@code Evaluator}.
	 * 
	 * @param expander
	 * @param evaluator
	 */
	public PrioritizedDFSNavigation(PathExpander<?> expander, Evaluator<Integer> evaluator) {
		this.expander = expander;
		this.evaluator = evaluator.copy();
	}

	public PrioritizedDFSNavigation(PrioritizedDFSNavigation another) {
		this(another.expander, another.evaluator);
	}

	public Path findSinglePath(Node source, Node sink) {

		if (null == source || null == sink) return null;
		
		lastMetadata = new Metadata();
		currentSink = sink;
		Doer doer = new Doer(source, sink);
		while (doer.hasNext()) {
			Path p = doer.next();
			
			lastMetadata.totalNodesExplored++;
			
			if (p.endNode().equals(sink)) {
				lastMetadata.paths++;
				return p;
			}
			
		}
		return null;
	}

	private class Doer extends PrefetchingIterator<Path> implements Path {
		private final Node start;
		private final Node end;
		private Node lastNode;
		private boolean expand;
		private final Set<Long> visitedNodes = new HashSet<Long>();
		private final Set<Long> stackNodes = new HashSet<Long>();
		private Path currentPath = null;
		//private final Stack<Path> stack = new Stack<Path>();
		private final Deque<Path> stack = new ArrayDeque<Path>();
		
		Doer(Node start, Node end) {
			this.start = start;
			this.end = end;

			stack.push(Utils.toPath(start, null));
			stackNodes.add(start.getId());
		}

		@Override
		protected Path fetchNextOrNull() {
			// FIXME
			if (!this.expand) {
				this.expand = true;
			} else {
				expand();
			}

			if (stack.isEmpty()) return null;
			
			currentPath = stack.pop();
			
			Node node = currentPath.endNode();
			this.lastNode = node;
			
			return currentPath;
		}

		@SuppressWarnings("unchecked")
		private void expand() {
			if (currentPath.length() >= Constants.LIMIT_OF_DEPTH) {
				// System.err.println(start + " => " + currentPath.endNode() + " => " + end + " length exceeds " + currentPath.length() + "! Stop expanding.");
				return;
			}
			
			visitedNodes.add(currentPath.endNode().getId());
			
			// Construct a list of relationships in the current path
			List<Relationship> rels = Lists.newArrayList(currentPath.relationships());
			
			// cannot use TreeSet because Path is not a properly implemented object,
			// so when distinct paths can be considered duplicate
			List<Path> neighbors = new ArrayList<Path>();
			
			for (Relationship rel : expander.expand(this, BranchState.NO_STATE)) {
				lastMetadata.rels++;
				Node node = rel.getOtherNode(currentPath.endNode());
				if (visitedNodes.contains(node.getId())) {
					continue;
				}

				if (!stackNodes.contains(node.getId())) {
					rels.add(rel);
					Path path = Utils.toPath(start, rels);
					
					if (!neighbors.add(path)) System.err.println("duplicate");
					stackNodes.add(node.getId());
					
					if (node.equals(end)) {
						stack.push(path);
						return;
					}
					rels.remove(rels.size() - 1);
				}
				
			}
			
			
			Path[] paths = neighbors.toArray(new Path[0]);
			Arrays.sort(paths, PrioritizedDFSNavigation.this);
			
			for (int i = paths.length - 1; i >= 0; i--) {
				stack.push(paths[i]);
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
		return evaluator.getCost(p1, currentSink) - evaluator.getCost(p2, currentSink);
	}

	
	public static void main(String[] args) throws IOException {
		
		//String dataset = "twitter"; Direction direction = Direction.OUTGOING;
		//String dataset = "gplus"; Direction direction = Direction.OUTGOING;
		String dataset = "dblp-inproceedings"; Direction direction = Direction.BOTH;
		//String dataset = "simple"; Direction direction = Direction.BOTH;
		
		RelationshipType type = RelationshipTypes.FRIEND.type();
		
		DistanceMeasure distanceFeature = new DistanceMeasure(dataset, type, direction);
		AbstractNavigation.setDistanceMeasure(distanceFeature);
		
		PathFinder<Path> nav = new PrioritizedDFSNavigation(PathExpanders.forTypeAndDirection(type, direction), new DegreeEvaluator(type, direction));
		
		Query q = new Query("neo4j/" + dataset);
		nav.findSinglePath(q.cypherGetNode(373654), q.cypherGetNode(1557));
		
	}
}
