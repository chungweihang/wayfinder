package smallworld.data.inserter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import smallworld.data.RelationshipTypes;
import smallworld.data.inserter.exp.GraphInserter;
import smallworld.data.inserter.exp.Neo4JInserter;

public class InserterTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	
	@Test
	public void test1() {
		GraphInserter inserter = new Neo4JInserter(tempFolder.getRoot().getAbsolutePath());
		inserter.addPerson(1);
		inserter.addPerson(2);
		inserter.addFriend(1, 2);
		inserter.addFriend(2, 1);
		inserter.addCircle("circle1");
		inserter.setCircle("circle1", 2);
		inserter.setCircle("circle1", 2);
	}
	
	@Test
	public void test() {
		// config
		Map<String, String> config = new HashMap<String, String>();
        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.relationshipstore.db.mapped_memory", "3G");
        config.put("neostore.propertystore.db.mapped_memory", "50M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
        config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
        
        BatchInserter inserter = BatchInserters.inserter(tempFolder.getRoot().getAbsolutePath(), config);
        inserter.createNode(0, null);
        inserter.createNode(1, null);
        inserter.createRelationship(0, 1, RelationshipTypes.FRIEND.type(), null);
        inserter.createRelationship(0, 1, RelationshipTypes.FRIEND.type(), null);
        inserter.shutdown();
        
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(tempFolder.getRoot().getAbsolutePath());
        ExecutionEngine engine = new ExecutionEngine(db);
        
        ExecutionResult result = engine.execute(
				"START n = node(0) " +
				"MATCH n-[r:FRIEND]-m " + 
				"RETURN r;");
		
		List<Relationship> rels = new ArrayList<Relationship>();
		
		for (Iterator<Relationship> it = result.columnAs("r"); it.hasNext();) {
			Relationship r = it.next();
			if (r != null) rels.add(r);
		}
		
		Assert.assertEquals(2, rels.size());
	}

}
