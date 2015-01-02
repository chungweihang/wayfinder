package smallworld.navigation.feature;

import java.util.concurrent.ExecutionException;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.Traversal;

import smallworld.Constants;
import smallworld.navigation.AbstractNavigation;
import smallworld.util.Pair;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class DistanceMeasure implements Feature<Double> {

	private final PathFinder<Path> finder;
	//private final FeatureCache cache;
	LoadingCache<Pair<Node, Node>, Integer> cache;
	private static final String NAME = "DistanceLabel";
	
	public DistanceMeasure(String dataset, RelationshipType type, Direction direction) {
		finder = GraphAlgoFactory.shortestPath(Traversal.pathExpanderForTypes(type, direction), Constants.LIMIT_OF_DEPTH);
		
		//cache = FeatureCache.getInstance(dataset);
		cache = CacheBuilder.newBuilder()
				.maximumSize(200000)
				.build(
						new CacheLoader<Pair<Node, Node>, Integer>() {
							public Integer load(Pair<Node, Node> key) {
								Path p = finder.findSinglePath(key.getFirst(), key.getSecond());
								return p == null ? Integer.MAX_VALUE : p.length();
							}
						});
	}
	
	@Override
	public String getName() {
		return NAME;
	}

	public void saveCache() {
		/*
		if (cache != null) {
			cache.save();
		}
		*/
	}
	
	@Override
	public Double getFeature(Path path, Node target) {
		Node current = path.endNode();
		Node previous = path.lastRelationship().getOtherNode(current);
		
		//Integer l1 = cache.getFeature(current.getId(), target.getId());
		//Integer l2 = cache.getFeature(previous.getId(), target.getId());
		Integer l1 = Integer.MAX_VALUE;
		Integer l2 = Integer.MAX_VALUE;
		try {
			l1 = cache.get(new Pair<Node, Node>(current, target));
			l2 = cache.get(new Pair<Node, Node>(previous, target));
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		/*
		if (l1 == null) {
			Path p = finder.findSinglePath(current, target);
			l1 = p == null ? Integer.MAX_VALUE : p.length();
			cache.setFeature(current.getId(), target.getId(), l1);
		}
		*/
		
		/*
		if (l2 == null) {
			Path p = finder.findSinglePath(previous, target);
			l2 = p == null ? Integer.MAX_VALUE : p.length();
			cache.setFeature(previous.getId(), target.getId(), l2);
		}
		*/
		
		return l1 < l2 ? 1d : 0d;
	}

}
