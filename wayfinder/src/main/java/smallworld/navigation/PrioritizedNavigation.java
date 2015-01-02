package smallworld.navigation;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.PrefetchingIterator;

import smallworld.Constants;
import smallworld.navigation.evaluator.Evaluator;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

/**
 * DFS Navigation but exploring nodes in a prioritized order.
 * 
 * The navigation uses a priority queue to keep unexplored nodes.
 * {@code PrioritizedNavigation} navigates to the high value (defined 
 * by {@code Evaluator})nodes among all in the priority queue.
 * 
 * Different from {@code PrioritizedDFSNavigation}, which prioritizes 
 * only the neighbors of a particular nodes.  Once these neighbors are 
 * pushed into the stack, the order of them remain unchanged.  
 * 
 * @see AbstractNavigation
 * @see PrioritizedDFSNavigation
 * @author chang
 *
 */
public class PrioritizedNavigation extends AbstractNavigation implements Comparator<Path> {
	
	private static final Logger logger = Logger.getLogger(PrioritizedNavigation.class.getName());

	private final PathExpander<?> expander;
	private final Evaluator<Integer> evaluator;
	private Node currentSink = null;

	public PrioritizedNavigation(PathExpander<?> expander, Evaluator<Integer> evaluator) {
		this.expander = expander;
		this.evaluator = evaluator.copy();
	}
	
	public PrioritizedNavigation(PrioritizedNavigation another) {
		this(another.expander, another.evaluator);
	}

	public Path findSinglePath(Node source, Node sink) {

		lastMetadata = new Metadata();
		currentSink = sink;
		Doer doer = new Doer(source, sink);
		while (doer.hasNext()) {
			Path p = doer.next();
			
			//numberOfVisitedNodes++;
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
		private final Set<Long> queueNodes = new HashSet<Long>();
		private Path currentPath = null;
		//private final PriorityQueue<Path> queue = new PriorityQueue<Path>(10, PrioritizedNavigation.this);
		@SuppressWarnings("unused")
		private final MinMaxPriorityQueue<Path> queue = (Constants.PRORITY_QUEUE_MAX_SIZE == -1 ? 
				MinMaxPriorityQueue.orderedBy(PrioritizedNavigation.this).create() :
				MinMaxPriorityQueue.orderedBy(PrioritizedNavigation.this).maximumSize(Constants.PRORITY_QUEUE_MAX_SIZE).create());
		
		Doer(Node start, Node end) {
			this.start = start;
			this.end = end;

			queue.add(toPath(start, null));
			queueNodes.add(start.getId());
		}

		@Override
		protected Path fetchNextOrNull() {
			if (!this.expand) {
				this.expand = true;
			} else {
				if (!expand()) return null;
			}
			//if (!expand()) return null;
			
			if (queue.isEmpty()) return null;
			
			currentPath = queue.poll();
			Node node = currentPath.endNode();
			this.lastNode = node;
			
			return currentPath;
		}

		@SuppressWarnings("unchecked")
		private boolean expand() {
			if (currentPath.length() >= Constants.LIMIT_OF_DEPTH) {
				logger.log(Level.FINE, start + " => " + currentPath.endNode() + " => " + end + " length exceeds " + currentPath.length() + "! Stop expanding.");
				return false;
			}
			
			visitedNodes.add(currentPath.endNode().getId());
			
			// Construct a list of relationships in the current path
			List<Relationship> rels = Lists.newArrayList(currentPath.relationships());
			
			for (Relationship rel : expander.expand(this, BranchState.NO_STATE)) {
				lastMetadata.rels++;
				Node node = rel.getOtherNode(this.lastNode);
				if (visitedNodes.contains(node.getId())) {
					continue;
				}

				if (!queueNodes.contains(node.getId())) {
					rels.add(rel);
					Path path = toPath(start, rels);
					
					queue.add(path);
					queueNodes.add(path.endNode().getId());
					rels.remove(rels.size() - 1);
					if (path.endNode().equals(end)) return true;
				}
				
				
			}
			
			return true;
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

}
