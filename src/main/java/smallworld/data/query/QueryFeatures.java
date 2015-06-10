package smallworld.data.query;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import smallworld.data.RelationshipTypes;

public class QueryFeatures {

	private Query query;
	
	public QueryFeatures(Query query) {
		this.query = query;
	}
	
	public QueryFeatures(String path) {
		this(new Query(path));
	}
	
	public Properties getFeatures(long ego) {
		List<Relationship> rels = query.cypherRelationshipsTo(ego, RelationshipTypes.KNOWS.name(), Direction.OUTGOING);
		
		Properties features = new Properties();
		
		for (int i = 0; i < rels.size(); i++) {
			Relationship rel = rels.get(i);
			
			if (rel.isType(RelationshipTypes.KNOWS.type())) {
				for (Iterator<String> it = rel.getPropertyKeys().iterator(); it.hasNext();) {
					String key = it.next();
					features.put(key, (String) rel.getProperty(key));
				}
			}
		}
		
		return features;
	}
	
	public void shutdown() {
		query.shutdown();
	}
	
	public static Properties getFeatures(Node n) {
		Properties features = new Properties();
		
		for (Iterator<Relationship> rels = n.getRelationships(Direction.INCOMING, RelationshipTypes.KNOWS.type()).iterator(); rels.hasNext();) {
			Relationship rel = rels.next();
			
			if (rel.isType(RelationshipTypes.KNOWS.type())) {
				for (Iterator<String> it = rel.getPropertyKeys().iterator(); it.hasNext();) {
					String key = it.next();
					features.put(key, (String) rel.getProperty(key));
				}
			}
		}
		
		return features;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query q = new Query("neo4j/facebook");
		QueryFeatures qf = new QueryFeatures(q);
		
		/*
		long[] nodes = q.allNodes();
		int count = 0;
		int total = 0;
		for (int i = 0; i < nodes.length; i++) {
			Properties features = qf.getFeatures(nodes[i]);
			if (features.size() > 0) {
				total += features.size(); 
				count++;
			}
		}
		
		System.out.println(count + " / " + nodes.length + " total: " + total);
		*/
		
		
		qf.getFeatures(0).list(System.out);
		
		q.shutdown();

	}

}
