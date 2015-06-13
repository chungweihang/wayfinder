package smallworld.data.query;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import smallworld.data.inserter.exp.Neo4JInserter;
import smallworld.data.inserter.exp.SimpleGraphInserter;

public class QueryCirclesTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	private static Query query;
	private static QueryCircles queryCircles;
	
	@Before
	public void initialize() throws IOException {
		if (query == null) {
			Neo4JInserter inserter = new Neo4JInserter(folder.getRoot().getAbsolutePath());
			new SimpleGraphInserter(inserter);
			query = new Query(folder.getRoot().getAbsolutePath());
			queryCircles = new QueryCircles(query);
		}
	}
	
	@AfterClass
	public static void cleanup() {
		if (null != query) query.shutdown();
	}
	
	@Test
	public void testGetCircle() {
		System.out.println(queryCircles.cypherGetCirlce("circle1"));
		System.out.println(queryCircles.cypherGetCirlce("circle2"));
		System.out.println(queryCircles.cypherGetCirlce("circle3"));
	}
	
	@Test
	public void test() {
		System.out.println(queryCircles.getCircles(query.cypherGetNode(0)));
		System.out.println(queryCircles.getCircles(query.cypherGetNode(1)));
		System.out.println(queryCircles.getCircles(query.cypherGetNode(2)));
		System.out.println(queryCircles.getCircles(query.cypherGetNode(3)));
		System.out.println(queryCircles.getCircles(query.cypherGetNode(4)));
		System.out.println(queryCircles.getCircles(query.cypherGetNode(5)));
		System.out.println(queryCircles.getCircles(query.cypherGetNode(6)));
	}
	
	@Test
	public void testGetCircleName() {
		System.out.println(queryCircles.cypherGetCircleNames(5));
	}

	/*
	System.out.println(qc.getCircleLabels(q.cypherGetNode(1)));
	System.out.println(qc.getCircleLabels(q.cypherGetNode(4)));
	System.out.println(qc.sizeOfCircleLabel(DynamicLabel.label("circle1")));
	System.out.println(qc.getMaxCommonCircleLabel(q.cypherGetNode(4), q.cypherGetNode(1)));
	System.out.println(qc.getMaxCommonCircleLabel(q.cypherGetNode(4), q.cypherGetNode(5)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(1), q.cypherGetNode(4)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(5), q.cypherGetNode(4)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(5), q.cypherGetNode(7)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(5), q.cypherGetNode(1)));
	*/
}