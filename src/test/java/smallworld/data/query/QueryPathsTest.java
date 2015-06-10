package smallworld.data.query;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Node;

import smallworld.data.RelationshipTypes;
import smallworld.data.inserter.exp.Neo4JInserter;
import smallworld.data.inserter.exp.SimpleGraphInserter;

public class QueryPathsTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	private static Query query;
	private static QueryPaths queryPaths;
	
	@Before
	public void initialize() throws IOException {
		if (query == null) {
			Neo4JInserter inserter = new Neo4JInserter(folder.getRoot().getAbsolutePath());
			new SimpleGraphInserter(inserter);
			query = new Query(folder.getRoot().getAbsolutePath());
			queryPaths = new QueryPaths(query);
		}
	}
	
	@AfterClass
	public static void cleanup() {
		if (null != query) query.shutdown();
	}
	
	@Ignore @Test
	// TODO: need to refactor
	public void testAnalyzeNumberOfCircleRelationshipsInFriendPaths() {
		// See how many circles in friend shortest paths
		BufferedWriter writer = null;
		// from, to, friend-length, # friend links, # circle links, circle-length
		try {
			writer = new BufferedWriter(new FileWriter("facebook-circle-in-friendship.log"));
			Long[] nodes = query.cypherGetAllNodes();
			
			// for sampling
			int limit = 5;
			int count = 0;
			
			for (long n1 : nodes) {
				count++;
				System.out.println(count);
				if (count >= limit) break;
				
				for (long n2 : nodes) {
					if (n1 == n2) continue;
					// String str = countCirclesInAllShortestPaths(n1, n2);
					List<List<Node>> paths = 
							queryPaths.getCypherAllShortestPathsDistinctNodes(n1, n2, RelationshipTypes.FRIEND.name(), 15);
					
					StringBuilder str = new StringBuilder();
					
					int numberOfPaths = paths.size();
					int length = 0;
					int numberOfCirclePaths = 0;
					
					for (int i = 0; i < numberOfPaths; i++) {
						List<Node> path = paths.get(i);
						length = path.size() - 1;
						numberOfCirclePaths += QueryPaths.countCirclePathInPaths(path);
					}
					
					writer.write(str
							.append(n1).append(",")
							.append(n2).append(",")
							.append(length).append(",")
							.append(numberOfPaths).append(",")
							.append(length * numberOfPaths).append(",")
							.append(numberOfCirclePaths).toString());
							//.append(shortestPathLength(n1, n2, RelationshipTypes.CIRCLE.name(), Direction.OUTGOING, 15)).toString());
					writer.newLine();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != writer) {
				try { writer.close(); } catch (IOException ignored) {}
			}
		}
		
	}
	
	
}
