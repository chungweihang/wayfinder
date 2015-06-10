package smallworld.data.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import edu.stanford.nlp.util.Maps;
import smallworld.data.RelationshipTypes;

public class QueryFeatures {

	private Query query;
	
	public QueryFeatures(Query query) {
		this.query = query;
	}
	
	public QueryFeatures(String path) {
		this(new Query(path));
	}
	
	public Map<String, Object> getFeatures(long ego) {
		Node n = query.cypherGetNode(ego);
		return getFeatures(n);
	}
	
	public void shutdown() {
		query.shutdown();
	}
	
	public static Map<String, Object> getFeatures(Node n) {
		Map<String, Object> features = new HashMap<>();
		for (String key : n.getPropertyKeys()) {
			features.put(key, n.getProperty(key));
		}
		
		return features;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query q = new Query("neo4j/facebook");
		QueryFeatures qf = new QueryFeatures(q);
		
		System.out.println(Maps.toStringSorted(QueryFeatures.getFeatures(q.cypherGetNode(0l))));
		q.shutdown();

	}

}
