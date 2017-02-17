package smallworld.data.inserter.exp;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import smallworld.data.query.Query;

import com.google.common.collect.Lists;

public class DuplicateRelationshipRemoverTest {
	
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	private static Query query;
	
	@Test
	public void test() throws IOException {
		if (query == null) {
			Neo4JInserter inserter = new Neo4JInserter(folder.getRoot().getAbsolutePath());
			inserter.enforceUniqueRelationships = false;
			
			inserter.addPerson("Lebron James");
			inserter.addPerson("Kyrie Irving");
			
			inserter.addCircle("Cleveland Cavaliers");
			inserter.setCircle("Cleveland Cavaliers", "Lebron James");
			inserter.setCircle("Cleveland Cavaliers", "Lebron James");
			inserter.setCircle("Cleveland Cavaliers", "Lebron James");
			inserter.setCircle("Cleveland Cavaliers", "Kyrie Irving");
			
			inserter.addFriend("Lebron James", "Kyrie Irving");
			inserter.addFriend("Kyrie Irving", "Lebron James");
			
			inserter.addPerson("Stephen Curry");
			
			inserter.addCircle("Golden State Warriors");
			inserter.setCircle("Golden State Warriors", "Stephen Curry");
			inserter.addFriend("Lebron James", "Stephen Curry");
			
			inserter.insert();
			
			query = new Query(folder.getRoot().getAbsolutePath());
			
			Node lbj = query.cypherGetPerson("Lebron James");
			try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
				System.out.println(Lists.newArrayList(lbj.getRelationships()).size());
			}
			
			removeDuplicateRelationships(query);
			try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
				System.out.println(Lists.newArrayList(lbj.getRelationships()).size());
			}
			
			query.shutdown();
		}
	}
	
	private void removeDuplicateRelationships(Query query) {
		
		query.cypherQuery(""
				//+ "START r=relationship(*) "
				+ "MATCH s-[r:FRIEND]-e "
				+ "WITH s,e,type(r) as typ, tail(collect(r)) as coll "
				+ "FOREACH(x in coll | delete x)");
	}
	
	@AfterClass
	public static void cleanup() {
		if (null != query) query.shutdown();
	}
}
