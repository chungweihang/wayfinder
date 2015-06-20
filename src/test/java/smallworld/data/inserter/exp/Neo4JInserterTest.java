package smallworld.data.inserter.exp;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import smallworld.data.query.Query;

import com.google.common.collect.Lists;

public class Neo4JInserterTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	private static Query query;
	
	@Test
	public void testAddingDuplicateUndirectedRelationships() throws IOException {
		if (query == null) {
			Neo4JInserter inserter = new Neo4JInserter(folder.getRoot().getAbsolutePath(), false);
			inserter.enforceUniqueRelationships = true;
			
			createSimpleGraph(inserter);
			
			query = new Query(folder.getRoot().getAbsolutePath());
			
			Node lbj = query.cypherGetPerson("Lebron James");
			try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
				// lebron => cavs, lebron => kyrie, lebron => curry
				Assert.assertEquals(3, Lists.newArrayList(lbj.getRelationships()).size());
			}
			
			query.shutdown();
		}
	}
	
	@Test
	public void testAddingDuplicateDirectedRelationships() throws IOException {
		if (query == null) {
			Neo4JInserter inserter = new Neo4JInserter(folder.getRoot().getAbsolutePath(), true);
			inserter.enforceUniqueRelationships = true;
			
			createSimpleGraph(inserter);
			
			query = new Query(folder.getRoot().getAbsolutePath());
			
			Node lbj = query.cypherGetPerson("Lebron James");
			try (Transaction tx = query.getGraphDatabaseService().beginTx()) {
				// lebron => cavs, lebron => kyrie, lebron => curry, kyrie => lebron
				Assert.assertEquals(4, Lists.newArrayList(lbj.getRelationships()).size());
			}
			
			query.shutdown();
		}
	}
	
	private static void createSimpleGraph(GraphInserter inserter) throws IOException {
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
	}
	
	
}
