package smallworld.data.query;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import smallworld.data.RelationshipTypes;
import smallworld.data.inserter.exp.Neo4JInserter;
import smallworld.data.inserter.exp.SimpleGraphInserter;

public class QueryTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	private static Query query;
	
	@Before
	public void initialize() throws IOException {
		if (query == null) {
			Neo4JInserter inserter = new Neo4JInserter(folder.getRoot().getAbsolutePath());
			new SimpleGraphInserter(inserter);
			query = new Query(folder.getRoot().getAbsolutePath());
		}
	}
	
	@AfterClass
	public static void cleanup() {
		if (null != query) query.shutdown();
	}
	
	@Test
	public void testCypherNumberOfNodes() {
		print(query.cypherQuery("START n=node(*) RETURN count(*)"));
	}
	
	@Test
	public void testCypherNumberOfRelationships() {
		print(query.cypherQuery( 
        		"START n=node(*) " +   
        		"MATCH n-[r]->() " + 
        		"RETURN type(r), count(*);"));
    }
	
	//System.out.println(query.cypherShortestPathLength(1, 7, "FRIEND", Direction.BOTH, 15));
	//q.allShortestPathsNodes(0, 426, "FRIEND", 15);
	// q.allShortestPathsDistinctNodes(0, 348, "FRIEND", 15);
	//q.allShortestPathsNodes(0, 1, "CIRCLE", 15);
	// System.out.println(q.getRelationships(0, 11, "CIRCLE").size());
	//q.existRelationships(119, 1, "CIRCLE");
	//q.allNodes();
	
	private void print(Result result) {
		String rows = "";
        for (; result.hasNext(); )
        {
        	Map<String, Object> row = result.next();
            for (Entry<String, Object> column : row.entrySet())
            {
                rows += column.getKey() + ": " + column.getValue() + "; ";
            }
            rows += "\n";
        }
        
        System.out.println(rows);
	}
	
}
