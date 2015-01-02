package smallworld.navigation;

import java.io.IOException;
import java.util.ArrayDeque;
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
import smallworld.data.RelationshipTypes;
import smallworld.data.query.Query;
import smallworld.navigation.feature.DistanceMeasure;

import com.google.common.collect.Lists;

public class ShortestNavigation extends AbstractNavigation {

	private final PathExpander<?> expander;
	@SuppressWarnings("unused")
	private Node currentSink = null;

	/**
	 * Instantiate a navigation by specifying a {@code PathExpander} and a {@code Evaluator}.
	 * 
	 * @param expander
	 * @param evaluator
	 */
	public ShortestNavigation(PathExpander<?> expander) {
		this.expander = expander;
	}

	public ShortestNavigation(ShortestNavigation another) {
		this(another.expander);
	}

	public Path findSinglePath(Node source, Node sink) {
		
		numberOfVisitedNodes = 0;
		numberOfVisitedNodesShorteningPaths = 0;

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
		
		// nodes that are already expanded
		private final Set<Long> expandedNodes = new HashSet<Long>();
		
		// nodes that are already visited
		private final Set<Long> visitedNodes = new HashSet<Long>();
		private Path currentPath = null;
		private final Deque<Path> queue = new ArrayDeque<Path>();
		
		Doer(Node start, Node end) {
			this.start = start;
			this.end = end;

			queue.addLast(toPath(start, null));
			visitedNodes.add(start.getId());
		}

		@Override
		protected Path fetchNextOrNull() {
			expand();
			
			if (queue.isEmpty()) return null;
			
			currentPath = queue.pollFirst();
			
			// DEBUG
			/*
			if (currentPath.length() > currentMaxPathLength) {
				currentMaxPathLength = currentPath.length();
				System.err.println(start.getId() + " to " + end.getId() + ": current max length " + currentMaxPathLength);
				System.err.flush();
			}
			*/
			
			Node node = currentPath.endNode();
			this.lastNode = node;
			
			// COMPUTATIONALLY EXPENSIVE: potentially do two shortest paths
			//ShortestNavigation.this.evaluateVisitedNode(currentPath, end);
			
			return currentPath;
		}

		@SuppressWarnings("unchecked")
		private void expand() {
			if (currentPath == null) return;
			
			if (expandedNodes.contains(currentPath.endNode().getId())) {
				System.err.println(currentPath.endNode().getId() + " expanded already!");
				return;
			}
			
			if (currentPath.length() >= Constants.LIMIT_OF_DEPTH) {
				System.err.println("path length limit reached; stopped!");
				return;
			}
			
			expandedNodes.add(currentPath.endNode().getId());
			
			// Construct a list of relationships in the current path
			List<Relationship> rels = Lists.newArrayList(currentPath.relationships());
			
			//for (Relationship rel : currentPath.endNode().getRelationships()) {
			for (Relationship rel : expander.expand(currentPath, BranchState.NO_STATE)) {
				lastMetadata.rels++;
				
				// DEBUG
				//if (lastMetadata.rels % 1000000 == 0) System.err.println("total relations explored: " + lastMetadata.rels);
				
				Node node = rel.getOtherNode(this.lastNode);
				
				//System.err.println("node: " + node.getId() + " queue: " + Arrays.toString(visitedNodes.toArray()));
				/*
				if (expandedNodes.contains(node.getId())) {
					continue;
				}
				*/
				if (!visitedNodes.contains(node.getId())) {
					visitedNodes.add(node.getId());
					
					rels.add(rel);
					Path path = toPath(start, rels);
					queue.addLast(path);
					rels.remove(rels.size() - 1);
					//System.err.println("adding path: " + path);
					if (path.endNode().equals(end)) return;
				}
				
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
		String dataset = "dblp-inproceedings"; Direction direction = Direction.BOTH;
		
		RelationshipType type = RelationshipTypes.FRIEND.type();
		
		DistanceMeasure distanceFeature = new DistanceMeasure(dataset, type, direction);
		AbstractNavigation.setDistanceMeasure(distanceFeature);
		
		PathFinder<Path> nav = new ShortestNavigation(Traversal.pathExpanderForTypes(type, direction));
		//new ConcurrentMain(nav, "neo4j/" + dataset, 1, 1, "temp.shortest.log");
		
		Query q = new Query("neo4j/" + dataset);
		nav.findSinglePath(q.getNode(373654), q.getNode(1557));
		
	}

}
