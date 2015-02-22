package smallworld.navigation;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalMetadata;

import smallworld.navigation.feature.DistanceMeasure;

/**
 * 
 * Base class for navigation.  Extend this class and implement 
 * {@code findSinglePath}.
 * 
 * It provides two static methods for navigation performance evaluation.
 * Please call {@code evaluate} method when the navigation visits a node. 
 * Call {@code result} to get the true positive rate, i.e., the percentage
 * of navigating to a neighbor that reduces the path to the target.
 * 
 * Before using {@code evaluate} and {@code result}, remember to set a 
 * {@code DistanceMeasure}.
 * 
 * @author chang
 *
 */
public abstract class AbstractNavigation implements PathFinder<Path> {

	// For performance evaluation
	private static DistanceMeasure distanceMeasure = null;
	private static AtomicLong total = new AtomicLong();
	private static AtomicLong correct = new AtomicLong();
	
	protected int numberOfVisitedNodes = 0;
	protected int numberOfVisitedNodesShorteningPaths = 0;
	
	public AbstractNavigation() {}
	
	@Deprecated
	protected void evaluateVisitedNode(Path path, Node visited) {
		if (distanceMeasure == null) throw new IllegalStateException("Distance measure is not set");
		
		if (path.length() < 1) return;
		
		numberOfVisitedNodes++;
		if (distanceMeasure.getFeature(path, visited) == 1) numberOfVisitedNodesShorteningPaths++;
	}
	
	/**
	 * Evaluate a chosen path.  If the new added node of this path reduces
	 * the distance to the target, then it is a good choice.
	 * 
	 * A path that contains only one node will not be counted.
	 * 
	 * @param path the current navigation path
	 * @param target the node to be reached
	 */
	@Deprecated
	protected static void evaluate(Path path, Node target) {
		
		if (distanceMeasure == null) throw new IllegalStateException("Distance measure is not set");
		
		if (path.length() < 1) return;
		
		total.incrementAndGet();
		if (distanceMeasure.getFeature(path, target) == 1) correct.incrementAndGet();
		
		// System.out.println(path + " ==> " + target + ": " + distanceMeasure.getFeature(path, target) + " " + getEvaluationResult());
	}
	
	/**
	 * Get the performance of the navigation, i.e., the percentage of good choices.
	 * @see evaluate
	 * 
	 * @return the percentage of good choices
	 */
	@Deprecated
	public static double getEvaluationResult() {
		long t = total.get();
		long c = correct.get();
		if (t == 0) return 0;
		return (double) c / (double) t;
	}
	
	/**
	 * Get the distance measure for performance evaluation.
	 * 
	 * @return
	 */
	public static DistanceMeasure getDistanceMeasure() {
		return distanceMeasure;
	}

	/**
	 * Set the distance measure for performance evaluation.
	 * 
	 * @param distanceMeasure
	 */
	public static void setDistanceMeasure(DistanceMeasure distanceMeasure) {
		AbstractNavigation.distanceMeasure = distanceMeasure;
	}

	public int getNumberOfVisitedNodes() {
		return numberOfVisitedNodes;
	}

	public int getNumberOfVisitedNodesShorteningPaths() {
		return numberOfVisitedNodesShorteningPaths;
	}

	/**
	 * Duplicate a navigation.  Used for concurrency.
	 * 
	 * @param nav
	 * @return
	 */
	public static PathFinder<Path> copy(PathFinder<Path> nav) {
		if (nav instanceof PrioritizedNavigation) {
			return new PrioritizedNavigation((PrioritizedNavigation) nav);
		} else if (nav instanceof PrioritizedDFSNavigation) {
			return new PrioritizedDFSNavigation((PrioritizedDFSNavigation) nav);
		} else if (nav instanceof TrainingNavigation) {
			return new TrainingNavigation((TrainingNavigation) nav);
		/*
		} else if (nav instanceof SARSANavigation) {
			return new SARSANavigation((SARSANavigation) nav);
		*/
		} else if (nav instanceof PathFinder<?>) {
			return nav;
		}
		
		throw new IllegalArgumentException("no such subclass of AbstractNavigation");
	}
	
	protected Metadata lastMetadata;
	
	@Override
	public Iterable<Path> findAllPaths(Node source, Node sink) {
		Path path = findSinglePath(source, sink);
		return path != null ? Arrays.asList(path) : Collections.<Path>emptyList();
	}

	@Override
	public TraversalMetadata metadata() {
		return lastMetadata;
	}

	protected static class Metadata implements TraversalMetadata {
		protected int rels;
		protected int paths;
		//protected int correctedNodesExplored;
		protected long totalNodesExplored;
		
		@Override
		public int getNumberOfPathsReturned() {
			return paths;
		}

		@Override
		public int getNumberOfRelationshipsTraversed() {
			return rels;
		}

		/*
		public int getCorrectedNodesExplored() {
			return correctedNodesExplored;
		}
		*/

		public long getTotalNodesExplored() {
			return totalNodesExplored;
		}
	}

}
