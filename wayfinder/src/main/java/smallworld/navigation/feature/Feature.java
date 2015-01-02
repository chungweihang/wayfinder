package smallworld.navigation.feature;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

/**
 * This class defines a feature used for a machine learning approach.
 * 
 * All features should implement this interface.
 * 
 * @see FeatureBuilder
 * @author chang
 *
 * @param <T>
 */
public interface Feature<T> {
	
	/**
	 * Return a unique name for this feature.
	 * 
	 * Used for distinguishing different features in an instance.
	 * 
	 * @return
	 */
	public String getName();
	
	/**
	 * Return a feature, usually a number, given the current path
	 * in this navigation, and the target of the navigation.
	 * 
	 * @param path the path of the current navigation
	 * @param target
	 * @return
	 */
	public T getFeature(Path path, Node target);
}
